# -*- coding: utf-8 -*-
"""
YouTube Data API v3 で:
- トイレ・住宅関連チャンネルを検索
- 登録者 9,000〜300,000
- 直近6ヶ月以内に更新あり
- （任意）企業・協会・大手メディア・有名人っぽいチャンネルを除外
- 結果に YouTubeチャンネルURL を含めてCSV保存

使い方:
  export YOUTUBE_API_KEY="あなたのAPIキー"
  python main.py
"""
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import csv
import re
import requests

API_KEY = os.environ.get("YOUTUBE_API_KEY")
BASE = "https://www.googleapis.com/youtube/v3"

# ======== 検索条件 ========
KEYWORDS = [
    "トイレ", "トイレ リフォーム", "水回り リフォーム", "洗面所 リフォーム",
    "住宅", "住宅 リフォーム", "間取り", "内装 DIY", "リノベーション",
    "トイレ DIY", "バスルーム リフォーム", "キッチン リフォーム", "中古住宅 リフォーム",
]

REGION_CODE = "JP"
MAX_PAGES_PER_KEYWORD = 2

MIN_SUBSCRIBERS = 1_000
MAX_SUBSCRIBERS = 500_000

LATEST_WITHIN_DAYS = 183  # 直近6ヶ月以内

EXCLUDE_BRANDY_CHANNELS = True
EXCLUDE_NAME_PATTERNS = [
    r"公式", r"株式会社", r"Inc\.?", r"Co\.?", r"Ltd\.?", r"有限会社", r"協会", r"組合",
    r"NHK", r"日経", r"朝日", r"毎日", r"読売", r"産経", r"テレビ", r"TV",
    r"LIXIL", r"リクシル", r"TOTO", r"パナソニック", r"Panasonic",
    r"住友林業", r"積水ハウス", r"大和ハウス", r"旭化成", r"YKK", r"タカラスタンダード",
    r"HOMES?", r"SUUMO", r"リノベる", r"UR", r"HOUSING", r"不動産", r"住宅情報誌",
    r"芸能|有名人|公式チャンネル"
]
# ==============================================


def yt_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """共通GET（エラーをraiseせず__error__で返す）"""
    params = {**params, "key": API_KEY}
    last_resp = None
    for i in range(3):
        resp = requests.get(f"{BASE}/{path}", params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        last_resp = resp
        time.sleep(1 + i)
    return {
        "__error__": True,
        "__http_status": (last_resp.status_code if last_resp else None),
        "__url": (last_resp.url if last_resp else None)
    }


def search_channels(keyword: str, max_pages: int = 2, region_code: Optional[str] = "JP") -> List[str]:
    """search.list で channelId を集める"""
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
            print(f"[WARN] search error {resp.get('__http_status')}: {resp.get('__url')}", file=sys.stderr)
            break
        for it in resp.get("items", []):
            snip = it.get("snippet") or {}
            ch_id = snip.get("channelId")
            if ch_id:
                ids_set.add(ch_id)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return list(ids_set)


def search_channels_multi(keywords: List[str], max_pages: int, region_code: str) -> List[str]:
    """複数キーワードOR結合"""
    all_ids: set[str] = set()
    for kw in keywords:
        ids = search_channels(kw, max_pages=max_pages, region_code=region_code)
        all_ids.update(ids)
    return list(all_ids)


def chunk(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def get_channels_details(channel_ids: List[str]) -> List[Dict[str, Any]]:
    """channels.list で詳細取得"""
    results: List[Dict[str, Any]] = []
    for batch in chunk(channel_ids, 50):
        resp = yt_get("channels", {
            "part": "statistics,snippet,contentDetails",
            "id": ",".join(batch)
        })
        if resp.get("__error__"):
            print(f"[WARN] channels error {resp.get('__http_status')}: {resp.get('__url')}", file=sys.stderr)
            continue
        results += resp.get("items", [])
    return results


def search_latest_video_published_at_by_channel(channel_id: str) -> Optional[str]:
    """フォールバック: search.list で最新動画取得"""
    resp = yt_get("search", {
        "part": "snippet",
        "channelId": channel_id,
        "type": "video",
        "order": "date",
        "maxResults": 1
    })
    if resp.get("__error__"):
        return None
    items = resp.get("items", [])
    if not items:
        return None
    return items[0]["snippet"].get("publishedAt")


def get_latest_upload_published_at(uploads_playlist_id: str, channel_id: str) -> Optional[str]:
    """uploads playlist→404時はsearchフォールバック"""
    if not uploads_playlist_id:
        return search_latest_video_published_at_by_channel(channel_id)
    resp = yt_get("playlistItems", {
        "part": "snippet,contentDetails",
        "playlistId": uploads_playlist_id,
        "maxResults": 1
    })
    if resp.get("__error__") or not resp.get("items"):
        return search_latest_video_published_at_by_channel(channel_id)
    return resp["items"][0]["snippet"].get("publishedAt")


def iso_to_dt(iso: Optional[str]) -> Optional[datetime]:
    if not iso:
        return None
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)


def any_match(patterns: List[str], text: str) -> bool:
    if not text:
        return False
    for p in patterns:
        if re.search(p, text, flags=re.IGNORECASE):
            return True
    return False


def main():
    if not API_KEY:
        print("環境変数 YOUTUBE_API_KEY が未設定です。", file=sys.stderr)
        sys.exit(1)

    latest_after_dt = datetime.now(timezone.utc) - timedelta(days=LATEST_WITHIN_DAYS)

    # 1️⃣ チャンネル収集
    channel_ids = search_channels_multi(KEYWORDS, max_pages=MAX_PAGES_PER_KEYWORD, region_code=REGION_CODE)
    if not channel_ids:
        print("該当チャンネルが見つかりません。")
        return

    # 2️⃣ 詳細取得
    details = get_channels_details(channel_ids)

    # 3️⃣ フィルター
    results = []
    for ch in details:
        stat = ch.get("statistics", {}) or {}
        snip = ch.get("snippet", {}) or {}
        cdet = ch.get("contentDetails", {}) or {}
        uploads_id = (cdet.get("relatedPlaylists") or {}).get("uploads", "")
        channel_id = ch["id"]
        title = snip.get("title") or ""
        desc = snip.get("description") or ""

        # 大手・企業除外
        if EXCLUDE_BRANDY_CHANNELS:
            if any_match(EXCLUDE_NAME_PATTERNS, title) or any_match(EXCLUDE_NAME_PATTERNS, desc):
                continue

        sub_raw = stat.get("subscriberCount")
        if sub_raw is None:
            continue
        subscribers = int(sub_raw)
        if subscribers < MIN_SUBSCRIBERS or subscribers > MAX_SUBSCRIBERS:
            continue

        latest_iso = get_latest_upload_published_at(uploads_id, channel_id)
        latest_dt = iso_to_dt(latest_iso) if latest_iso else None
        if not latest_dt or latest_dt < latest_after_dt:
            continue

        channel_url = f"https://www.youtube.com/channel/{channel_id}"

        results.append({
            "channel_id": channel_id,
            "title": title,
            "description": desc,
            "subscribers": subscribers,
            "latest_video_published_at": latest_iso,
            "channel_started_at": snip.get("publishedAt"),
            "channel_url": channel_url
        })

    # 4️⃣ 重複除去＆出力
    uniq = {r["channel_id"]: r for r in results}
    results = sorted(uniq.values(), key=lambda x: (x["subscribers"], x["title"] or ""))

    if not results:
        print("条件に合致するチャンネルはありません。")
        return

    print("title,subscribers,latest_video_published_at,channel_url")
    for r in results:
        t = r["title"].replace(",", "，")
        print(f'{t},{r["subscribers"]},{r["latest_video_published_at"]},{r["channel_url"]}')

    # CSV保存
    out_path = os.path.abspath("./output_youtube_channels.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "title", "subscribers", "latest_video_published_at",
            "channel_url", "channel_id", "channel_started_at", "description"
        ])
        w.writeheader()
        for r in results:
            w.writerow(r)
    print(f"\n✅ CSVに保存しました → {out_path}")


if __name__ == "__main__":
    main()
