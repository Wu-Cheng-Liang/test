from curl_cffi import requests
import pandas as pd
import time
import random
import datetime
import os


# ========= 基本設定 =========
DATA_DIR = "data"
KOL_INFO_FILE = os.path.join(DATA_DIR, "kol_info.csv")
STATIC_FILE = os.path.join(DATA_DIR, "reels_static_info.csv")
DYNAMIC_FILE = os.path.join(DATA_DIR, "reels_dynamic_info.csv")

PROFILE_API = "https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
DETAIL_API = "https://www.instagram.com/graphql/query/"
DETAIL_DOC_ID = "8845758582119845"
IG_APP_ID = "936619743392459"

NEW_REELS_WINDOW_MINUTES = 30
PROFILE_SLEEP_RANGE = (3, 6)
DETAIL_SLEEP_RANGE = (2, 4)

BASE_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "x-ig-app-id": IG_APP_ID,
}

STATIC_COLUMNS = [
    "kol_account",
    "reels_shortcode",
    "post_time",
    "duration",
    "caption",
]

DYNAMIC_COLUMNS = [
    "reels_shortcode",
    "views",
    "plays",
    "likes",
    "comments",
    "timestamp",
]


# ========= 工具函式 =========
def now_local():
    return datetime.datetime.now()


def sleep_random(sec_range):
    time.sleep(random.uniform(*sec_range))


def ensure_parent_dir(filepath):
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)


def read_or_init_csv(filepath, columns):
    ensure_parent_dir(filepath)

    if os.path.exists(filepath):
        try:
            df = pd.read_csv(filepath)
            print(f"✅ 成功讀取 {filepath}")

            # 補齊缺少欄位
            for col in columns:
                if col not in df.columns:
                    df[col] = None

            return df[columns]
        except Exception as e:
            print(f"⚠️ 讀取 {filepath} 失敗，改用空表：{e}")

    return pd.DataFrame(columns=columns)


def save_csv(df, filepath, columns):
    ensure_parent_dir(filepath)

    for col in columns:
        if col not in df.columns:
            df[col] = None

    df = df[columns]
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"✅ 已儲存 {filepath}，共 {len(df)} 筆")


def safe_json_get(url, headers, referer=None, timeout=15):
    req_headers = headers.copy()
    if referer:
        req_headers["referer"] = referer

    try:
        resp = requests.get(
            url,
            headers=req_headers,
            impersonate="chrome120",
            timeout=timeout
        )
        print(f"DEBUG GET {url} status={resp.status_code}")

        if resp.status_code != 200:
            print(f"❌ GET 失敗 status={resp.status_code}")
            print(resp.text[:300])
            return None

        return resp.json()

    except Exception as e:
        print(f"❌ GET 發生錯誤: {e}")
        return None


# ========= Step 1: 抓帳號近期主頁資料 =========
def get_profile_info(username):
    url = PROFILE_API.format(username=username)
    return safe_json_get(
        url=url,
        headers=BASE_HEADERS,
        referer=f"https://www.instagram.com/{username}/"
    )


def extract_recent_new_reels_from_profile(username, profile_json, existing_shortcodes, within_minutes=30):
    """
    從 profile timeline 裡找：
    1. 是影片
    2. 發文時間在最近 30 分鐘內
    3. shortcode 不在 static CSV
    """
    results = []

    try:
        user = profile_json["data"]["user"]
        edges = user.get("edge_owner_to_timeline_media", {}).get("edges", [])
    except Exception as e:
        print(f"❌ {username} profile JSON 結構異常: {e}")
        return results

    cutoff = now_local() - datetime.timedelta(minutes=within_minutes)

    for edge in edges:
        node = edge.get("node", {})
        shortcode = node.get("shortcode")
        is_video = node.get("is_video", False)
        timestamp = node.get("taken_at_timestamp")

        if not shortcode or not is_video or not timestamp:
            continue

        try:
            post_dt = datetime.datetime.fromtimestamp(timestamp)
        except Exception:
            continue

        if post_dt < cutoff:
            continue

        if str(shortcode) in existing_shortcodes:
            continue

        duration = node.get("video_duration", None)
        caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
        caption_text = caption_edges[0].get("node", {}).get("text", "") if caption_edges else ""

        results.append({
            "kol_account": username,
            "reels_shortcode": shortcode,
            "post_time": post_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "duration": duration,
            "caption": caption_text,
        })

    return results


# ========= Step 2: 用 shortcode 抓單篇詳細資料 =========
def get_reel_detail_by_shortcode(shortcode):
    variables = (
        f'{{"shortcode":"{shortcode}",'
        f'"fetch_tagged_user_count":null,'
        f'"hoisted_comment_id":null,'
        f'"hoisted_reply_id":null}}'
    )

    try:
        resp = requests.get(
            DETAIL_API,
            headers={
                **BASE_HEADERS,
                "referer": f"https://www.instagram.com/reel/{shortcode}/"
            },
            params={
                "doc_id": DETAIL_DOC_ID,
                "variables": variables
            },
            impersonate="chrome120",
            timeout=15
        )

        print(f"DEBUG GET {resp.url} status={resp.status_code}")

        if resp.status_code != 200:
            print(f"❌ GET 失敗 status={resp.status_code}")
            print(resp.text[:300])
            return None

        data = resp.json()

    except Exception as e:
        print(f"❌ get_reel_detail_by_shortcode 發生錯誤 shortcode={shortcode}: {e}")
        return None

    node = data.get("data", {}).get("xdt_shortcode_media")
    if not node:
        print(f"❌ shortcode={shortcode} 找不到 xdt_shortcode_media")
        print(str(data)[:300])
        return None

    return node


def parse_likes(node):
    return (
        node.get("edge_liked_by", {}).get("count")
        or node.get("edge_media_preview_like", {}).get("count")
        or 0
    )


def parse_comments_count(node):
    return (
        node.get("edge_media_to_comment", {}).get("count")
        or node.get("edge_media_to_parent_comment", {}).get("count")
        or 0
    )


def parse_duration(node):
    return node.get("video_duration", None)


def build_dynamic_snapshot(shortcode, node):
    return {
        "reels_shortcode": shortcode,
        "views": node.get("video_view_count", 0),
        "plays": node.get("video_play_count", 0),
        "likes": parse_likes(node),
        "comments": parse_comments_count(node),
        "timestamp": now_local().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ========= 表更新 =========
def append_static_rows(static_df, new_rows):
    if not new_rows:
        return static_df

    new_df = pd.DataFrame(new_rows)
    merged = pd.concat([static_df, new_df], ignore_index=True)

    # 同一支 reel 只保留一筆 static 資料
    merged = merged.drop_duplicates(subset=["reels_shortcode"], keep="first")
    return merged


def append_dynamic_rows(dynamic_df, new_rows):
    if not new_rows:
        return dynamic_df

    new_df = pd.DataFrame(new_rows)
    merged = pd.concat([dynamic_df, new_df], ignore_index=True)

    # 避免同一輪重複寫入同 shortcode + timestamp
    merged = merged.drop_duplicates(subset=["reels_shortcode", "timestamp"], keep="last")
    return merged


def sort_static_df(df):
    if "post_time" in df.columns:
        try:
            df["_sort_post_time"] = pd.to_datetime(df["post_time"], errors="coerce")
            df = df.sort_values(["_sort_post_time", "reels_shortcode"], ascending=[False, True])
            df = df.drop(columns=["_sort_post_time"])
        except Exception:
            pass
    return df


def sort_dynamic_df(df):
    if "timestamp" in df.columns:
        try:
            df["_sort_timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df = df.sort_values(["_sort_timestamp", "reels_shortcode"], ascending=[False, True])
            df = df.drop(columns=["_sort_timestamp"])
        except Exception:
            pass
    return df


# ========= 主流程 =========
def main():
    # 讀表一
    try:
        kol_df = pd.read_csv(KOL_INFO_FILE)
        print(f"✅ 成功讀取 {KOL_INFO_FILE}")
    except FileNotFoundError:
        raise SystemExit(f"❌ 找不到 {KOL_INFO_FILE}")

    if "kol_account" not in kol_df.columns:
        raise SystemExit("❌ 表一缺少 kol_account 欄位")

    # 讀表二、表三
    static_df = read_or_init_csv(STATIC_FILE, STATIC_COLUMNS)
    dynamic_df = read_or_init_csv(DYNAMIC_FILE, DYNAMIC_COLUMNS)

    existing_shortcodes = set(static_df["reels_shortcode"].dropna().astype(str).tolist())

    # =========================
    # Part A: 檢查每個 KOL 是否有新發 Reels（30 分鐘內）
    # =========================
    print("\n🚀 Part A: 檢查各 KOL 最近 30 分鐘內的新 Reels")

    new_static_rows = []
    new_dynamic_rows = []

    for _, row in kol_df.iterrows():
        username = str(row["kol_account"]).strip()
        if not username:
            continue

        print(f"\n=== 檢查帳號: {username} ===")
        profile_json = get_profile_info(username)
        if not profile_json:
            sleep_random(PROFILE_SLEEP_RANGE)
            continue

        recent_new_reels = extract_recent_new_reels_from_profile(
            username=username,
            profile_json=profile_json,
            existing_shortcodes=existing_shortcodes,
            within_minutes=NEW_REELS_WINDOW_MINUTES
        )

        print(f"ℹ️ {username} 最近 30 分鐘內新 Reels 數量: {len(recent_new_reels)}")

        for static_row in recent_new_reels:
            shortcode = static_row["reels_shortcode"]

            detail_node = get_reel_detail_by_shortcode(shortcode)
            if not detail_node:
                sleep_random(DETAIL_SLEEP_RANGE)
                continue

            if static_row["duration"] is None:
                static_row["duration"] = parse_duration(detail_node)

            dynamic_row = build_dynamic_snapshot(shortcode, detail_node)

            new_static_rows.append(static_row)
            new_dynamic_rows.append(dynamic_row)

            existing_shortcodes.add(str(shortcode))
            sleep_random(DETAIL_SLEEP_RANGE)

        sleep_random(PROFILE_SLEEP_RANGE)

    static_df = append_static_rows(static_df, new_static_rows)
    dynamic_df = append_dynamic_rows(dynamic_df, new_dynamic_rows)

    # =========================
    # Part B: 對表二全部 shortcode，再抓一次最新 snapshot
    # =========================
    print("\n🚀 Part B: 更新表二所有 Reels 的最新動態資訊")

    all_shortcodes = static_df["reels_shortcode"].dropna().astype(str).tolist()
    latest_dynamic_rows = []

    for shortcode in all_shortcodes:
        print(f"=== 更新 shortcode: {shortcode} ===")

        detail_node = get_reel_detail_by_shortcode(shortcode)
        if not detail_node:
            sleep_random(DETAIL_SLEEP_RANGE)
            continue

        dynamic_row = build_dynamic_snapshot(shortcode, detail_node)
        latest_dynamic_rows.append(dynamic_row)

        sleep_random(DETAIL_SLEEP_RANGE)

    dynamic_df = append_dynamic_rows(dynamic_df, latest_dynamic_rows)

    # 排序
    static_df = sort_static_df(static_df)
    dynamic_df = sort_dynamic_df(dynamic_df)

    # 存檔
    save_csv(static_df, STATIC_FILE, STATIC_COLUMNS)
    save_csv(dynamic_df, DYNAMIC_FILE, DYNAMIC_COLUMNS)

    print("\n✅ 完成")
    print(f"✅ 已更新 {STATIC_FILE}")
    print(f"✅ 已更新 {DYNAMIC_FILE}")


if __name__ == "__main__":
    main()