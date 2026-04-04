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

PROFILE_API = "https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
IG_APP_ID = "936619743392459"

REELS_WINDOW_DAYS = 30
PROFILE_SLEEP_RANGE = (5, 10)

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


def deduplicate_static_df(df):
    if df.empty:
        return df.copy()

    df = df.copy()

    # 先把時間轉成 datetime，方便排序後保留較新的資料
    df["_post_time_dt"] = pd.to_datetime(df["post_time"], errors="coerce")

    # 新的在前，drop_duplicates 時保留 keep="first"
    df = df.sort_values(
        by=["_post_time_dt", "reels_shortcode"],
        ascending=[False, True]
    )

    df = df.drop_duplicates(subset=["reels_shortcode"], keep="first")
    df = df.drop(columns=["_post_time_dt"], errors="ignore")

    return df


def filter_static_df_within_days(df, within_days=30):
    if df.empty:
        return df.copy()

    df = df.copy()
    cutoff = now_local() - datetime.timedelta(days=within_days)

    df["_post_time_dt"] = pd.to_datetime(df["post_time"], errors="coerce")
    df = df[df["_post_time_dt"].notna()]
    df = df[df["_post_time_dt"] >= cutoff]
    df = df.drop(columns=["_post_time_dt"], errors="ignore")

    return df


def sort_static_df(df):
    if "post_time" in df.columns:
        try:
            df = df.copy()
            df["_sort_post_time"] = pd.to_datetime(df["post_time"], errors="coerce")
            df = df.sort_values(
                ["_sort_post_time", "reels_shortcode"],
                ascending=[False, True]
            )
            df = df.drop(columns=["_sort_post_time"])
        except Exception:
            pass
    return df


# ========= 抓帳號主頁資料 =========
def get_profile_info(username):
    url = PROFILE_API.format(username=username)
    return safe_json_get(
        url=url,
        headers=BASE_HEADERS,
        referer=f"https://www.instagram.com/{username}/"
    )


def extract_reels_within_days(username, profile_json, within_days=30):
    """
    從 profile timeline 裡找：
    1. 是影片
    2. 發文時間在最近 N 天內
    """
    results = []

    try:
        user = profile_json["data"]["user"]
        edges = user.get("edge_owner_to_timeline_media", {}).get("edges", [])
    except Exception as e:
        raise ValueError(f"{username} profile JSON 結構異常: {e}")

    cutoff = now_local() - datetime.timedelta(days=within_days)

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

        duration = node.get("video_duration", None)
        caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
        caption_text = (
            caption_edges[0].get("node", {}).get("text", "")
            if caption_edges else ""
        )

        results.append({
            "kol_account": username,
            "reels_shortcode": str(shortcode),
            "post_time": post_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "duration": duration,
            "caption": caption_text,
        })

    return results


def build_and_save_current_results(static_df, all_static_rows):
    """
    把舊資料 + 本次已抓到資料合併後：
    1. 去重
    2. 只保留最近 30 天
    3. 排序
    4. 存檔
    """
    new_static_df = pd.DataFrame(all_static_rows, columns=STATIC_COLUMNS)

    merged_df = pd.concat([static_df, new_static_df], ignore_index=True)

    merged_df = deduplicate_static_df(merged_df)
    merged_df = filter_static_df_within_days(merged_df, within_days=REELS_WINDOW_DAYS)
    merged_df = sort_static_df(merged_df)

    save_csv(merged_df, STATIC_FILE, STATIC_COLUMNS)


# ========= 主流程 =========
def main():
    # 讀表一
    try:
        kol_df = pd.read_csv(KOL_INFO_FILE)
        print(f"✅ 成功讀取 {KOL_INFO_FILE}")
    except FileNotFoundError:
        raise SystemExit(f"❌ 找不到 {KOL_INFO_FILE}")
    except Exception as e:
        raise SystemExit(f"❌ 讀取 {KOL_INFO_FILE} 失敗: {e}")

    if "kol_account" not in kol_df.columns:
        raise SystemExit("❌ 表一缺少 kol_account 欄位")

    # 讀表二
    static_df = read_or_init_csv(STATIC_FILE, STATIC_COLUMNS)

    print(f"\n🚀 開始抓取各 KOL 最近 {REELS_WINDOW_DAYS} 天內發布的 Reels")

    all_static_rows = []
    processed_count = 0

    try:
        for _, row in kol_df.iterrows():
            username = str(row["kol_account"]).strip()
            if not username or username.lower() == "nan":
                continue

            print(f"\n=== 檢查帳號: {username} ===")

            profile_json = get_profile_info(username)
            if not profile_json:
                raise RuntimeError(f"{username} profile_json 取得失敗")

            reels_rows = extract_reels_within_days(
                username=username,
                profile_json=profile_json,
                within_days=REELS_WINDOW_DAYS
            )

            print(f"ℹ️ {username} 最近 {REELS_WINDOW_DAYS} 天內 Reels 數量: {len(reels_rows)}")
            all_static_rows.extend(reels_rows)
            processed_count += 1

            sleep_random(PROFILE_SLEEP_RANGE)

        print("\n✅ 全部帳號處理完成")

    except Exception as e:
        print("\n🚨 發生錯誤，停止執行！")
        print(f"錯誤原因: {e}")
        print(f"目前已成功處理帳號數: {processed_count}")

    finally:
        print("\n💾 開始儲存目前已抓到的資料...")
        build_and_save_current_results(static_df, all_static_rows)
        print("✅ 已儲存目前結果")
        print(f"✅ 已更新 {STATIC_FILE}")


if __name__ == "__main__":
    main()