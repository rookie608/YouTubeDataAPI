# -*- coding: utf-8 -*-
"""
YouTube Data API v3 で:
- キーワードでチャンネル検索 (search.list)
- チャンネル詳細(登録者数/開設日/アップロード用プレイリスト)取得 (channels.list)
- 直近動画の公開日時取得:
    基本は playlistItems.list
    404/空などは search.list にフォールバック
- 重複排除 / 日本語寄り検索 / CSV保存

使い方:
  export YOUTUBE_API_KEY="あなたのAPIキー"
  python main.py
"""
import os
import sys
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import csv
import requests

API_KEY = os.environ.get("YOUTUBE_API_KEY")
BASE = "https://www.googleapis.com/youtube/v3"


def yt_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    共通GET（簡易バックオフ付き）
    ここでは HTTP エラーを raise せず、呼び出し側が判定できるように返す。
    """
    params = {**params, "key": API_KEY}
    last_resp = None
    for i in range(3):
        resp = requests.get(f"{BASE}/{path}", params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        last_resp = resp
        time.sleep(1 + i)  # 軽いバックオフ

    # 200 以外
    return {
        "__error__": True,
        "__http_status": (last_resp.status_code if last_resp else None),
        "__text": (last_resp.text if last_resp else None),
        "__url": (last_resp.url if last_resp else None),
    }


def search_channels(keyword: str, max_pages: int = 2, region_code: Optional[str] = "JP") -> List[str]:
    """search.list で channelId を集める（ページング対応・重複排除）"""
    ids_set: set[str] = set()
    page_token = None
    for _ in range(max_pages):
        resp = yt_get("search", {
            "part": "snippet",
            "q": keyword,
            "type": "channel",
            "maxResults": 50,
            "regionCode": region_code,
            "relevanceLanguage": "ja",
            "pageToken": page_token
        })
        if resp.get("__error__"):
            # search 自体が失敗：スキップ（状況に応じて sys.stderr に出力）
            print(f"[WARN] search.list error {resp['__http_status']}: {resp.get('__url')}", file=sys.stderr)
            break

        items = resp.get("items", [])
        for it in items:
            snip = it.get("snippet") or {}
            ch_id = snip.get("channelId")
            if ch_id:
                ids_set.add(ch_id)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return list(ids_set)


def chunk(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def get_channels_details(channel_ids: List[str]) -> List[Dict[str, Any]]:
    """channels.list で statistics, snippet, contentDetails をまとめて取得（50件ずつ）"""
    results: List[Dict[str, Any]] = []
    for batch in chunk(channel_ids, 50):
        resp = yt_get("channels", {
            "part": "statistics,snippet,contentDetails",
            "id": ",".join(batch),
            "maxResults": 50
        })
        if resp.get("__error__"):
            print(f"[WARN] channels.list error {resp['__http_status']}: {resp.get('__url')}", file=sys.stderr)
            continue
        results += resp.get("items", [])
    return results


def search_latest_video_published_at_by_channel(channel_id: str) -> Optional[str]:
    """フォールバック: search.list でチャンネル内最新動画を1件取得（order=date）"""
    resp = yt_get("search", {
        "part": "snippet",
        "channelId": channel_id,
        "type": "video",
        "order": "date",
        "maxResults": 1
    })
    if resp.get("__error__"):
        # ここでまで失敗なら None を返す（上位でスキップ判定）
        print(f"[INFO] fallback search.list failed {resp['__http_status']}: {resp.get('__url')}", file=sys.stderr)
        return None

    items = resp.get("items", [])
    if not items:
        return None
    return items[0]["snippet"].get("publishedAt")


def get_latest_upload_published_at(uploads_playlist_id: str, channel_id: str) -> Optional[str]:
    """
    基本: uploads プレイリストから最新動画1件を取得。
    404（プレイリスト非公開/存在せず）や items 空は search にフォールバック。
    """
    if not uploads_playlist_id:
        return search_latest_video_published_at_by_channel(channel_id)

    resp = yt_get("playlistItems", {
        "part": "snippet,contentDetails",
        "playlistId": uploads_playlist_id,
        "maxResults": 1
    })

    if resp.get("__error__"):
        status = resp.get("__http_status")
        # 404は想定内 → フォールバック
        if status == 404:
            return search_latest_video_published_at_by_channel(channel_id)
        # それ以外（403など）も実務上はフォールバックしてしまう
        print(f"[INFO] playlistItems error {status}: {resp.get('__url')} -> fallback search", file=sys.stderr)
        return search_latest_video_published_at_by_channel(channel_id)

    items = resp.get("items", [])
    if not items:
        return search_latest_video_published_at_by_channel(channel_id)

    return items[0]["snippet"].get("publishedAt")


def iso_to_dt(iso: Optional[str]) -> Optional[datetime]:
    if not iso:
        return None
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)


def main():
    if not API_KEY:
        print("環境変数 YOUTUBE_API_KEY が未設定です。APIキーを設定してください。", file=sys.stderr)
        sys.exit(1)

    # === 検索条件 ===
    KEYWORD = "パイソン"
    MAX_PAGES = 3
    MAX_SUBSCRIBERS = 10_000
    LATEST_AFTER = "2024-01-01T00:00:00Z"
    STARTED_AFTER = None

    latest_after_dt = iso_to_dt(LATEST_AFTER) if LATEST_AFTER else None
    started_after_dt = iso_to_dt(STARTED_AFTER) if STARTED_AFTER else None

    # 1) 検索
    channel_ids = search_channels(KEYWORD, max_pages=MAX_PAGES, region_code="JP")
    if not channel_ids:
        print("該当チャンネルが見つかりませんでした。")
        return

    # 2) 詳細取得
    details = get_channels_details(channel_ids)

    # 3) 条件フィルター
    results = []
    for ch in details:
        stat = ch.get("statistics", {}) or {}
        snip = ch.get("snippet", {}) or {}
        cdet = ch.get("contentDetails", {}) or {}
        related = (cdet.get("relatedPlaylists") or {})
        uploads_id = related.get("uploads")
        channel_id = ch["id"]

        # 登録者数（非公開は除外）
        sub_raw = stat.get("subscriberCount")
        if sub_raw is None:
            continue
        subscribers = int(sub_raw)
        if subscribers >= MAX_SUBSCRIBERS:
            continue

        # 開設日フィルター
        ch_started_iso = snip.get("publishedAt")
        ch_started_dt = iso_to_dt(ch_started_iso) if ch_started_iso else None
        if started_after_dt and ch_started_dt and ch_started_dt < started_after_dt:
            continue

        # 最新動画の公開日時（playlistItems → 失敗/空なら search にフォールバック）
        latest_iso = get_latest_upload_published_at(uploads_id, channel_id) if latest_after_dt else None
        latest_dt = iso_to_dt(latest_iso) if latest_iso else None
        if latest_after_dt:
            if latest_dt is None or latest_dt < latest_after_dt:
                continue

        # （任意）ボールパイソン除外例
        title_lower = (snip.get("title") or "").lower()
        if any(kw in title_lower for kw in ["ボールパイソン", "ball python"]):
            continue

        results.append({
            "channel_id": channel_id,
            "title": snip.get("title"),
            "subscribers": subscribers,
            "channel_started_at": ch_started_iso,
            "latest_video_published_at": latest_iso
        })

    # 4) 重複除去＆ソート
    uniq_by_id = {}
    for r in results:
        uniq_by_id[r["channel_id"]] = r
    results = list(uniq_by_id.values())
    results.sort(key=lambda r: (r["subscribers"], r["title"] or ""))

    if not results:
        print("条件に合致するチャンネルはありませんでした。条件を緩めて再実行してみてください。")
        return

    # 5) 出力
    print("title,subscribers,latest_video_published_at,channel_id")
    for r in results:
        title = (r["title"] or "").replace(",", "，")
        print(f'{title},{r["subscribers"]},{r["latest_video_published_at"] or ""},{r["channel_id"]}')

    # CSV保存
    out_path = os.path.abspath("./output_youtube_channels.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "title", "subscribers", "latest_video_published_at", "channel_id", "channel_started_at"
        ])
        writer.writeheader()
        for r in results:
            writer.writerow({
                "title": r.get("title"),
                "subscribers": r.get("subscribers"),
                "latest_video_published_at": r.get("latest_video_published_at"),
                "channel_id": r.get("channel_id"),
                "channel_started_at": r.get("channel_started_at")
            })
    print(f"\n✅ CSVに保存しました → {out_path}")


if __name__ == "__main__":
    main()
