# main_deck-brush.py
# -*- coding: utf-8 -*-
"""
YouTube Data API v3 で:
- 清掃関連（ハウスキーパー/家事代行/屋内清掃など）のチャンネルを OR 条件で収集
- 登録者 9,000〜300,000 を通す（約9,000人以上 〜 30万人以下）
- 直近6ヶ月以内に更新があるチャンネルのみ
- 大手企業/協会/メディア/有名人っぽいチャンネルを緩めに除外
- CSV出力: 先頭列 = チャンネル名 / ハンドル / チャンネルURL / ハンドルURL / 登録者数 / 最終投稿日 / 動画本数
- 実行日時入りファイル名（YYYYMMDD_HHMM）

使い方:
  export YOUTUBE_API_KEY="あなたのAPIキー"
  python main_deck-brush.py
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

# ======== 検索キーワード（清掃系・OR条件） ========
KEYWORDS = ["清掃", "掃除", "ハウスキーパー", "家事代行", "ハウスクリーニング", "屋内清掃", "ルームクリーニング", "片付け", "整理収納", "水回り掃除", "トイレ掃除", "バスルーム掃除", "キッチン掃除", "換気扇掃除", "エアコンクリーニング", "床ワックス掃除", "カビ取り掃除", "主婦のチャンネル", "主婦ライフ", "主婦 vlog", "家事ルーティン", "暮らしの工夫", "生活の知恵", "家事のコツ", "掃除ルーティン", "時短家事", "家事モチベーション", "お片付け", "断捨離", "収納アイデア", "整理術", "ミニマリスト", "シンプルライフ", "収納グッズ", "片付け術", "収納方法", "インテリア整理", "引越し業者", "引越し準備", "引越しサポート", "不用品回収", "荷造り", "荷解き", "転居サポート", "引越し片付け", "引越し清掃", "引越し後掃除"]

REGION_CODE = "JP"
MAX_PAGES_PER_KEYWORD = 1          # クォータ節約
STOP_AFTER_N_RESULTS = 200         # CSV出力上限目安

# ======== 数値条件 ========
MIN_SUBSCRIBERS = 9_000
MAX_SUBSCRIBERS = 300_000
LATEST_WITHIN_DAYS = 183           # 直近6ヶ月以内

# ======== 除外パターン ========
EXCLUDE_BRANDY_CHANNELS = True
EXCLUDE_NAME_PATTERNS = [
    r"NHK", r"日経", r"朝日", r"毎日", r"読売", r"産経", r"テレビ", r"TV",
    r"ダスキン", r"おそうじ本舗", r"ベアーズ", r"CaSy", r"ニチイ",
    r"芸能|有名人|アイドル",
]

# ================== HTTPユーティリティ ==================
def yt_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    import random
    params = {**params, "key": API_KEY}
    last_resp = None
    last_exc = None
    for i in range(4):
        try:
            resp = requests.get(f"{BASE}/{path}", params=params, timeout=(5, 40))
            if resp.status_code == 200:
                return resp.json()
            last_resp = resp
        except requests.exceptions.RequestException as e:
            last_exc = repr(e)
        time.sleep((2 ** i) + random.uniform(0, 0.5))
    safe_params = {k: v for k, v in params.items() if k != "key"}
    return {
        "__error__": True,
        "__http_status": (last_resp.status_code if last_resp else None),
        "__url": (last_resp.url if last_resp else f"{BASE}/{path}"),
        "__text": (last_resp.text[:500] if last_resp else None),
        "__exc": last_exc,
        "__params": safe_params,
    }

def warn(tag: str, resp: Dict[str, Any]):
    print(
        f"[WARN] {tag} status={resp.get('__http_status')} url={resp.get('__url')}\n"
        f"       exc={resp.get('__exc')}\n"
        f"       body={resp.get('__text')}\n"
        f"       params={resp.get('__params')}\n",
        file=sys.stderr
    )

def sanity_check() -> bool:
    test_id = "UC_x5XG1OV2P6uZZ5FSM9Ttw"
    resp = yt_get("channels", {"part": "id", "id": test_id})
    if resp.get("__error__"):
        print("[SANITY] FAILED", file=sys.stderr)
        warn("channels.list (sanity)", resp)
        return False
    print("[SANITY] OK")
    return True

# ================== APIラッパ ==================
def search_channels(keyword: str, max_pages: int = 1, region_code: Optional[str] = "JP") -> List[str]:
    ids: set[str] = set()
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
            warn("search.list", resp)
            break
        for it in resp.get("items", []):
            snip = it.get("snippet") or {}
            cid = snip.get("channelId")
            if cid:
                ids.add(cid)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return list(ids)

def search_channels_multi(keywords: List[str], max_pages: int, region_code: str) -> List[str]:
    """全キーワードを順番に検索（途中で止めず、進捗表示付き）"""
    all_ids: set[str] = set()
    total = len(keywords)
    for idx, kw in enumerate(keywords, 1):
        print(f"🔍 Searching ({idx}/{total}): {kw}")
        got = search_channels(kw, max_pages=max_pages, region_code=region_code)
        before = len(all_ids)
        all_ids.update(got)
        added = len(all_ids) - before
        print(f"    ↳ fetched={len(got)} / unique_added={added} / unique_total={len(all_ids)}")
    return list(all_ids)

def chunk(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def get_channels_details(channel_ids: List[str]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for batch in chunk(channel_ids, 50):
        resp = yt_get("channels", {
            "part": "statistics,snippet,contentDetails",
            "id": ",".join(batch)
        })
        if resp.get("__error__"):
            warn("channels.list", resp)
            continue
        results += resp.get("items", [])
    return results

def get_latest_upload_published_at(uploads_playlist_id: str) -> Optional[str]:
    if not uploads_playlist_id:
        return None
    resp = yt_get("playlistItems", {
        "part": "snippet,contentDetails",
        "playlistId": uploads_playlist_id,
        "maxResults": 1
    })
    if resp.get("__error__"):
        warn("playlistItems.list", resp)
        return None
    items = resp.get("items", [])
    if not items:
        return None
    return items[0]["snippet"].get("publishedAt")

# ================== ヘルパ ==================
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

def save_csv(rows: List[Dict[str, Any]]) -> str:
    now_str = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"output_youtube_channels_{now_str}.csv"
    out_path = os.path.abspath(filename)
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=[
            "チャンネル名", "ハンドル", "YouTubeのURL", "ハンドルURL",
            "登録者数（人）", "最終投稿日", "動画本数",
            "channel_id", "channel_started_at", "description"
        ])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"\n✅ CSVに保存しました → {out_path}")
    return out_path

# ================== メイン ==================
def main():
    if not API_KEY:
        print("環境変数 YOUTUBE_API_KEY が未設定です。", file=sys.stderr)
        sys.exit(1)

    if not sanity_check():
        print("クォータ超過/ネットワーク/キー制限の可能性があります。", file=sys.stderr)
        sys.exit(2)

    latest_after_dt = datetime.now(timezone.utc) - timedelta(days=LATEST_WITHIN_DAYS)

    # 1) 候補収集
    channel_ids = search_channels_multi(KEYWORDS, max_pages=MAX_PAGES_PER_KEYWORD, region_code=REGION_CODE)
    if not channel_ids:
        print("該当チャンネルが見つかりません。")
        return

    # 2) 詳細取得
    details = get_channels_details(channel_ids)

    # 3) フィルタリング
    results = []
    for ch in details:
        stat = ch.get("statistics", {}) or {}
        snip = ch.get("snippet", {}) or {}
        cdet = ch.get("contentDetails", {}) or {}

        uploads_id = (cdet.get("relatedPlaylists") or {}).get("uploads", "")
        channel_id = ch.get("id")
        title = snip.get("title") or ""
        desc = snip.get("description") or ""
        handle = snip.get("customUrl") or ""
        handle_url = f"https://www.youtube.com/{handle}" if handle else ""
        channel_url = f"https://www.youtube.com/channel/{channel_id}"

        if EXCLUDE_BRANDY_CHANNELS and any_match(EXCLUDE_NAME_PATTERNS, f"{title} {desc}"):
            continue

        sub_raw = stat.get("subscriberCount")
        video_count_raw = stat.get("videoCount")
        if sub_raw is None:
            continue
        subscribers = int(sub_raw)
        video_count = int(video_count_raw) if video_count_raw else None

        if subscribers < MIN_SUBSCRIBERS or subscribers > MAX_SUBSCRIBERS:
            continue

        latest_iso = get_latest_upload_published_at(uploads_id)
        latest_dt = iso_to_dt(latest_iso) if latest_iso else None
        if not latest_dt or latest_dt < latest_after_dt:
            continue

        results.append({
            "チャンネル名": title,
            "ハンドル": handle,
            "YouTubeのURL": channel_url,
            "ハンドルURL": handle_url,
            "登録者数（人）": subscribers,
            "最終投稿日": latest_iso,
            "動画本数": video_count,
            "channel_id": channel_id,
            "channel_started_at": snip.get("publishedAt"),
            "description": desc
        })

        if len(results) >= STOP_AFTER_N_RESULTS:
            break

    if not results:
        print("条件に合致するチャンネルはありません。")
        return

    uniq = {r["channel_id"]: r for r in results}
    results = sorted(uniq.values(), key=lambda x: (x["登録者数（人）"], x["チャンネル名"] or ""))

    print("チャンネル名,ハンドル,YouTubeのURL,ハンドルURL,登録者数（人）,最終投稿日,動画本数")
    for r in results:
        t = (r["チャンネル名"] or "").replace(",", "，")
        print(f'{t},{r["ハンドル"]},{r["YouTubeのURL"]},{r["ハンドルURL"]},{r["登録者数（人）"]},{r["最終投稿日"]},{r["動画本数"]}')

    save_csv(results)

if __name__ == "__main__":
    main()
