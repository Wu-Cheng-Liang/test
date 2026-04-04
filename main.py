import os
import re
import time
import random
import datetime as dt

import pandas as pd
from curl_cffi import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ========= Basic config =========
DATA_DIR = "data"
KOL_INFO_FILE = os.path.join(DATA_DIR, "kol_info.csv")
STATE_FILE = os.path.join(DATA_DIR, "profile_post_state.csv")
STATIC_FILE = os.path.join(DATA_DIR, "reels_static_info.csv")
DYNAMIC_FILE = os.path.join(DATA_DIR, "reels_dynamic_info.csv")

PROFILE_API = "https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
DETAIL_API = "https://www.instagram.com/graphql/query/"
DETAIL_DOC_ID = "8845758582119845"
IG_APP_ID = "936619743392459"

REELS_WINDOW_DAYS = 30
PROFILE_SLEEP_RANGE = (2, 4)
DETAIL_SLEEP_RANGE = (2, 4)

BASE_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
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

STATE_COLUMNS = [
    "kol_account",
    "profile_post_count",
    "last_checked_at",
    "last_changed_at",
    "check_status",
]


# ========= Helpers =========
def now_local() -> dt.datetime:
    return dt.datetime.now()


def now_str() -> str:
    return now_local().strftime("%Y-%m-%d %H:%M:%S")


def sleep_random(sec_range: tuple[int, int]) -> None:
    time.sleep(random.uniform(*sec_range))


def ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)


def read_or_init_csv(filepath: str, columns: list[str]) -> pd.DataFrame:
    ensure_parent_dir(filepath)

    if os.path.exists(filepath):
        try:
            df = pd.read_csv(filepath)
            print(f"✅ Read {filepath}")
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            return df[columns]
        except Exception as exc:
            print(f"⚠️ Failed to read {filepath}, using empty table: {exc}")

    return pd.DataFrame(columns=columns)


def save_csv(df: pd.DataFrame, filepath: str, columns: list[str]) -> None:
    ensure_parent_dir(filepath)

    for col in columns:
        if col not in df.columns:
            df[col] = None

    df = df[columns]
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"✅ Saved {filepath} ({len(df)} rows)")


def dedupe_and_sort_static(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["_post_time_dt"] = pd.to_datetime(out["post_time"], errors="coerce")
    out = out.sort_values(["_post_time_dt", "reels_shortcode"], ascending=[False, True])
    out = out.drop_duplicates(subset=["reels_shortcode"], keep="first")

    cutoff = now_local() - dt.timedelta(days=REELS_WINDOW_DAYS)
    out = out[out["_post_time_dt"].notna()]
    out = out[out["_post_time_dt"] >= cutoff]
    out = out.drop(columns=["_post_time_dt"], errors="ignore")
    return out


def dedupe_and_sort_dynamic(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["_ts_dt"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.sort_values(["_ts_dt", "reels_shortcode"], ascending=[False, True])
    out = out.drop_duplicates(subset=["reels_shortcode", "timestamp"], keep="last")
    out = out.drop(columns=["_ts_dt"], errors="ignore")
    return out


def dedupe_and_sort_state(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["_checked_dt"] = pd.to_datetime(out["last_checked_at"], errors="coerce")
    out = out.sort_values(["_checked_dt", "kol_account"], ascending=[False, True])
    out = out.drop_duplicates(subset=["kol_account"], keep="first")
    out = out.drop(columns=["_checked_dt"], errors="ignore")
    return out


def safe_json_get(url: str, headers: dict, referer: str | None = None, timeout: int = 15):
    req_headers = headers.copy()
    if referer:
        req_headers["referer"] = referer

    try:
        resp = requests.get(
            url,
            headers=req_headers,
            impersonate="chrome120",
            timeout=timeout,
        )
        print(f"DEBUG GET {url} status={resp.status_code}")

        if resp.status_code != 200:
            print(f"❌ GET failed status={resp.status_code}")
            print(resp.text[:300])
            return None

        return resp.json()
    except Exception as exc:
        print(f"❌ GET error: {exc}")
        return None


def normalize_count_text(text: str) -> int | None:
    if text is None:
        return None

    raw = str(text).strip()
    if not raw:
        return None

    raw = raw.replace(",", "").replace(" ", "")
    match = re.search(r"(\d+(?:\.\d+)?)([KMBkmb]?)", raw)
    if not match:
        return None

    value = float(match.group(1))
    unit = match.group(2).lower()

    if unit == "k":
        value *= 1_000
    elif unit == "m":
        value *= 1_000_000
    elif unit == "b":
        value *= 1_000_000_000

    return int(round(value))


# ========= Selenium profile gate =========
def build_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,2200")
    options.add_argument("--lang=en-US")
    options.add_experimental_option("prefs", {"intl.accept_languages": "en,en_US"})

    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin:
        options.binary_location = chrome_bin

    return webdriver.Chrome(options=options)


def extract_post_count_from_meta(driver: webdriver.Chrome) -> int | None:
    metas = driver.find_elements(By.XPATH, "//meta[@property='og:description']")
    for meta in metas:
        content = meta.get_attribute("content") or ""
        match = re.search(r"([0-9][0-9,\.KMBkmb]*)\s+Posts\b", content, flags=re.I)
        if match:
            return normalize_count_text(match.group(1))
    return None


def extract_post_count_from_xpath(driver: webdriver.Chrome) -> int | None:
    xpaths = [
        "//header//ul/li[1]//span[@title]",
        "//header//ul/li[1]//span/span",
        "//main//header//section//ul/li[1]//span[@title]",
        "//main//header//section//ul/li[1]//span/span",
    ]

    for xpath in xpaths:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
            for el in elements:
                candidate = (el.get_attribute("title") or el.text or "").strip()
                count = normalize_count_text(candidate)
                if count is not None:
                    return count
        except Exception:
            continue
    return None


def get_profile_post_count(driver: webdriver.Chrome, username: str) -> int | None:
    url = f"https://www.instagram.com/{username}/"
    driver.get(url)

    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    time.sleep(2)

    count = extract_post_count_from_meta(driver)
    if count is not None:
        print(f"ℹ️ {username} current profile post count (meta): {count}")
        return count

    count = extract_post_count_from_xpath(driver)
    if count is not None:
        print(f"ℹ️ {username} current profile post count (xpath): {count}")
        return count

    print(f"⚠️ Could not read post count for {username}")
    return None


# ========= API fetch for changed accounts only =========
def get_profile_info(username: str):
    url = PROFILE_API.format(username=username)
    return safe_json_get(
        url=url,
        headers=BASE_HEADERS,
        referer=f"https://www.instagram.com/{username}/",
    )


def extract_reels_within_days(username: str, profile_json: dict, existing_shortcodes: set[str]) -> list[dict]:
    results: list[dict] = []

    try:
        user = profile_json["data"]["user"]
        edges = user.get("edge_owner_to_timeline_media", {}).get("edges", [])
    except Exception as exc:
        print(f"❌ {username} profile JSON structure error: {exc}")
        return results

    cutoff = now_local() - dt.timedelta(days=REELS_WINDOW_DAYS)

    for edge in edges:
        node = edge.get("node", {})
        shortcode = str(node.get("shortcode") or "").strip()
        is_video = node.get("is_video", False)
        timestamp = node.get("taken_at_timestamp")

        if not shortcode or not is_video or not timestamp:
            continue

        try:
            post_dt = dt.datetime.fromtimestamp(timestamp)
        except Exception:
            continue

        if post_dt < cutoff:
            continue

        if shortcode in existing_shortcodes:
            continue

        caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
        caption_text = caption_edges[0].get("node", {}).get("text", "") if caption_edges else ""

        results.append({
            "kol_account": username,
            "reels_shortcode": shortcode,
            "post_time": post_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "duration": node.get("video_duration"),
            "caption": caption_text,
        })

    return results


def get_reel_detail_by_shortcode(shortcode: str):
    variables = (
        f'{{"shortcode":"{shortcode}",'
        f'"fetch_tagged_user_count":null,'
        f'"hoisted_comment_id":null,'
        f'"hoisted_reply_id":null}}'
    )

    try:
        resp = requests.get(
            DETAIL_API,
            headers={**BASE_HEADERS, "referer": f"https://www.instagram.com/reel/{shortcode}/"},
            params={"doc_id": DETAIL_DOC_ID, "variables": variables},
            impersonate="chrome120",
            timeout=15,
        )
        print(f"DEBUG GET {resp.url} status={resp.status_code}")

        if resp.status_code != 200:
            print(f"❌ Detail GET failed status={resp.status_code}")
            print(resp.text[:300])
            return None

        data = resp.json()
    except Exception as exc:
        print(f"❌ get_reel_detail_by_shortcode error shortcode={shortcode}: {exc}")
        return None

    node = data.get("data", {}).get("xdt_shortcode_media")
    if not node:
        print(f"❌ shortcode={shortcode} missing xdt_shortcode_media")
        return None

    return node


def parse_likes(node: dict) -> int:
    return (
        node.get("edge_liked_by", {}).get("count")
        or node.get("edge_media_preview_like", {}).get("count")
        or 0
    )


def parse_comments_count(node: dict) -> int:
    return (
        node.get("edge_media_to_comment", {}).get("count")
        or node.get("edge_media_to_parent_comment", {}).get("count")
        or 0
    )


def build_dynamic_snapshot(shortcode: str, node: dict) -> dict:
    return {
        "reels_shortcode": shortcode,
        "views": node.get("video_view_count", 0),
        "plays": node.get("video_play_count", 0),
        "likes": parse_likes(node),
        "comments": parse_comments_count(node),
        "timestamp": now_str(),
    }


def upsert_state_row(state_df: pd.DataFrame, username: str, current_count: int | None, status: str, changed: bool) -> pd.DataFrame:
    checked_at = now_str()
    changed_at = checked_at if changed else None

    existing_changed_at = None
    if not state_df.empty:
        match = state_df[state_df["kol_account"].astype(str) == username]
        if not match.empty:
            existing_changed_at = match.iloc[0].get("last_changed_at")

    row = {
        "kol_account": username,
        "profile_post_count": current_count,
        "last_checked_at": checked_at,
        "last_changed_at": changed_at or existing_changed_at,
        "check_status": status,
    }

    out = pd.concat([state_df, pd.DataFrame([row])], ignore_index=True)
    return dedupe_and_sort_state(out)


# ========= Main =========
def main() -> None:
    start_ts = time.time()

    try:
        kol_df = pd.read_csv(KOL_INFO_FILE)
        print(f"✅ Read {KOL_INFO_FILE}")
    except FileNotFoundError:
        raise SystemExit(f"❌ Missing {KOL_INFO_FILE}")
    except Exception as exc:
        raise SystemExit(f"❌ Failed to read {KOL_INFO_FILE}: {exc}")

    if "kol_account" not in kol_df.columns:
        raise SystemExit("❌ kol_info.csv must contain kol_account column")

    state_df = read_or_init_csv(STATE_FILE, STATE_COLUMNS)
    static_df = read_or_init_csv(STATIC_FILE, STATIC_COLUMNS)
    dynamic_df = read_or_init_csv(DYNAMIC_FILE, DYNAMIC_COLUMNS)

    existing_shortcodes = set(static_df["reels_shortcode"].dropna().astype(str).tolist())
    new_static_rows: list[dict] = []
    new_dynamic_rows: list[dict] = []

    processed = 0
    changed_accounts = 0
    skipped_accounts = 0

    driver = build_driver()

    try:
        for _, row in kol_df.iterrows():
            username = str(row["kol_account"]).strip()
            if not username or username.lower() == "nan":
                continue

            print(f"\n=== Checking account: {username} ===")

            previous_count = None
            if not state_df.empty:
                matched = state_df[state_df["kol_account"].astype(str) == username]
                if not matched.empty:
                    try:
                        previous_count = int(float(matched.iloc[0]["profile_post_count"]))
                    except Exception:
                        previous_count = None

            current_count = get_profile_post_count(driver, username)
            processed += 1

            if current_count is None:
                state_df = upsert_state_row(state_df, username, previous_count, "count_read_failed", False)
                sleep_random(PROFILE_SLEEP_RANGE)
                continue

            if previous_count is not None and current_count == previous_count:
                print(f"⏭️ {username} unchanged ({current_count}), skipped")
                state_df = upsert_state_row(state_df, username, current_count, "skipped_same_count", False)
                skipped_accounts += 1
                sleep_random(PROFILE_SLEEP_RANGE)
                continue

            print(f"🔄 {username} changed: previous={previous_count}, current={current_count}")
            state_df = upsert_state_row(state_df, username, current_count, "changed_fetching", True)
            changed_accounts += 1

            profile_json = get_profile_info(username)
            if not profile_json:
                print(f"⚠️ {username} changed but profile JSON fetch failed")
                state_df = upsert_state_row(state_df, username, current_count, "changed_profile_fetch_failed", True)
                sleep_random(PROFILE_SLEEP_RANGE)
                continue

            recent_new_reels = extract_reels_within_days(username, profile_json, existing_shortcodes)
            print(f"ℹ️ {username} new reels to append: {len(recent_new_reels)}")

            for static_row in recent_new_reels:
                shortcode = static_row["reels_shortcode"]
                detail_node = get_reel_detail_by_shortcode(shortcode)

                if detail_node:
                    if static_row["duration"] is None:
                        static_row["duration"] = detail_node.get("video_duration")
                    new_dynamic_rows.append(build_dynamic_snapshot(shortcode, detail_node))
                else:
                    print(f"⚠️ Detail fetch failed for shortcode={shortcode}; static row will still be saved")

                new_static_rows.append(static_row)
                existing_shortcodes.add(shortcode)
                sleep_random(DETAIL_SLEEP_RANGE)

            state_df = upsert_state_row(state_df, username, current_count, "changed_saved", True)
            sleep_random(PROFILE_SLEEP_RANGE)

    finally:
        driver.quit()

    static_df = dedupe_and_sort_static(pd.concat([static_df, pd.DataFrame(new_static_rows)], ignore_index=True))
    dynamic_df = dedupe_and_sort_dynamic(pd.concat([dynamic_df, pd.DataFrame(new_dynamic_rows)], ignore_index=True))
    state_df = dedupe_and_sort_state(state_df)

    save_csv(static_df, STATIC_FILE, STATIC_COLUMNS)
    save_csv(dynamic_df, DYNAMIC_FILE, DYNAMIC_COLUMNS)
    save_csv(state_df, STATE_FILE, STATE_COLUMNS)

    elapsed = round(time.time() - start_ts, 2)
    print("\n✅ Done")
    print(f"✅ Processed accounts: {processed}")
    print(f"✅ Changed accounts: {changed_accounts}")
    print(f"✅ Skipped accounts: {skipped_accounts}")
    print(f"✅ Total runtime: {elapsed} seconds")


if __name__ == "__main__":
    main()
