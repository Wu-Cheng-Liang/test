import datetime as dt
import html
import json
import os
import random
import re
import shutil
import time
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


# ========= Basic config =========
DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
KOL_INFO_FILE = DATA_DIR / "kol_info.csv"
STATE_FILE = DATA_DIR / "profile_post_state.csv"
STATIC_FILE = DATA_DIR / "reels_static_info.csv"
DYNAMIC_FILE = DATA_DIR / "reels_dynamic_info.csv"

REELS_WINDOW_DAYS = int(os.environ.get("REELS_WINDOW_DAYS", "30"))
PROFILE_SLEEP_RANGE = (0.8, 1.5)
DETAIL_SLEEP_RANGE = (0.8, 1.5)
MAX_PROFILE_SCROLLS = int(os.environ.get("MAX_PROFILE_SCROLLS", "3"))
PAGE_TIMEOUT_SECONDS = int(os.environ.get("PAGE_TIMEOUT_SECONDS", "20"))

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


def sleep_random(sec_range: tuple[float, float]) -> None:
    time.sleep(random.uniform(*sec_range))


def ensure_parent_dir(filepath: Path | str) -> None:
    parent = Path(filepath).parent
    parent.mkdir(parents=True, exist_ok=True)


def read_or_init_csv(filepath: Path, columns: list[str]) -> pd.DataFrame:
    ensure_parent_dir(filepath)

    if filepath.exists():
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


def save_csv(df: pd.DataFrame, filepath: Path, columns: list[str]) -> None:
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


def normalize_count_text(text: str | int | float | None) -> Optional[int]:
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


def _unescape_text(value: str | None) -> str:
    if not value:
        return ""
    return html.unescape(value).replace("\\n", "\n").replace("\\/", "/").strip()


def _find_first(patterns: Iterable[str], text: str, flags: int = 0) -> Optional[str]:
    for pattern in patterns:
        match = re.search(pattern, text, flags)
        if match:
            return match.group(1)
    return None


def parse_dt_string(value: str | None) -> Optional[dt.datetime]:
    if not value:
        return None

    value = value.strip()

    if re.fullmatch(r"\d{10}", value):
        try:
            return dt.datetime.fromtimestamp(int(value))
        except Exception:
            return None

    candidates = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in candidates:
        try:
            return dt.datetime.strptime(value, fmt)
        except ValueError:
            continue

    try:
        cleaned = value.replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(cleaned)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        return None


# ========= Selenium =========
def resolve_browser_binary() -> Optional[str]:
    env_bin = os.environ.get("CHROME_BIN")
    if env_bin and Path(env_bin).exists():
        return env_bin

    macos_chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if Path(macos_chrome).exists():
        return macos_chrome

    for candidate in ["chrome", "google-chrome", "chromium", "chromium-browser"]:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None


def resolve_chromedriver() -> Optional[str]:
    env_path = os.environ.get("CHROMEDRIVER_PATH")
    if env_path and Path(env_path).exists():
        return env_path

    for candidate in ["chromedriver"]:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None


def build_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,2200")
    options.add_argument("--lang=en-US")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--hide-scrollbars")
    options.add_argument("--log-level=3")
    options.page_load_strategy = "eager"
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option("prefs", {"intl.accept_languages": "en,en_US"})

    chrome_bin = resolve_browser_binary()
    if chrome_bin:
        options.binary_location = chrome_bin
        print(f"ℹ️ Using browser binary: {chrome_bin}")
    else:
        print("⚠️ Could not resolve Chrome binary; relying on system default")

    service = None
    chromedriver_path = resolve_chromedriver()
    if chromedriver_path:
        print(f"ℹ️ Using chromedriver: {chromedriver_path}")
        service = Service(executable_path=chromedriver_path)
    else:
        print("⚠️ Could not resolve chromedriver; relying on Selenium Manager")

    driver = webdriver.Chrome(service=service, options=options) if service else webdriver.Chrome(options=options)
    driver.set_page_load_timeout(PAGE_TIMEOUT_SECONDS)
    driver.implicitly_wait(1)
    return driver


def safe_get(driver: webdriver.Chrome, url: str) -> bool:
    try:
        driver.get(url)
        try:
            WebDriverWait(driver, PAGE_TIMEOUT_SECONDS).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            print(f"⚠️ Timed out waiting for body on {url}")
        return True
    except WebDriverException as exc:
        print(f"❌ Browser error while opening {url}: {exc}")
        return False


def extract_post_count_from_page_source(driver: webdriver.Chrome) -> Optional[int]:
    html_text = driver.page_source or ""
    count = _find_first(
        [
            r'edge_owner_to_timeline_media\\":\\\{\\"count\\":(\d+)',
            r'"edge_owner_to_timeline_media"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
            r'"posts"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
        ],
        html_text,
    )
    return normalize_count_text(count)


def extract_post_count_from_meta(driver: webdriver.Chrome) -> Optional[int]:
    try:
        metas = driver.find_elements(By.XPATH, "//meta[@property='og:description']")
    except Exception:
        return None

    for meta in metas:
        content = (meta.get_attribute("content") or "").strip()
        if not content:
            continue

        match = re.search(r"([0-9][0-9,\.KMBkmb]*)\s+posts?\b", content, flags=re.I)
        if match:
            count = normalize_count_text(match.group(1))
            if count is not None:
                return count

        match = re.search(r"^\s*([0-9][0-9,\.KMBkmb]*)\b", content)
        if match:
            count = normalize_count_text(match.group(1))
            if count is not None:
                return count

    return None


def extract_post_count_from_xpath(driver: webdriver.Chrome) -> Optional[int]:
    xpaths = [
        "//header//ul/li[1]//span[@title]",
        "//header//ul/li[1]//span/span",
        "//main//header//section//ul/li[1]//span[@title]",
        "//main//header//section//ul/li[1]//span/span",
    ]

    for xpath in xpaths:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
        except Exception:
            elements = []
        for el in elements:
            candidate = (el.get_attribute("title") or el.text or "").strip()
            count = normalize_count_text(candidate)
            if count is not None:
                return count
    return None


def get_profile_post_count(driver: webdriver.Chrome, username: str) -> Optional[int]:
    url = f"https://www.instagram.com/{username}/"
    if not safe_get(driver, url):
        return None

    for extractor_name, extractor in [
        ("page_source", extract_post_count_from_page_source),
        ("meta", extract_post_count_from_meta),
        ("xpath", extract_post_count_from_xpath),
    ]:
        try:
            count = extractor(driver)
            if count is not None:
                print(f"ℹ️ {username} current profile post count ({extractor_name}): {count}")
                return count
        except Exception as exc:
            print(f"⚠️ {username} post count extractor {extractor_name} failed: {exc}")

    print(f"⚠️ Could not read post count for {username}")
    return None


# ========= Page parsing =========
def extract_reel_shortcodes_from_profile(driver: webdriver.Chrome) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()

    def collect_from_dom() -> None:
        try:
            anchors = driver.find_elements(By.XPATH, "//a[contains(@href, '/reel/')]")
        except Exception:
            anchors = []
        for anchor in anchors:
            href = anchor.get_attribute("href") or ""
            match = re.search(r"/reel/([^/?#]+)/?", href)
            if match:
                shortcode = match.group(1)
                if shortcode not in seen:
                    seen.add(shortcode)
                    found.append(shortcode)

    def collect_from_html() -> None:
        html_text = driver.page_source or ""
        for shortcode in re.findall(r"/reel/([^/\"'?&#]+)/", html_text):
            if shortcode not in seen:
                seen.add(shortcode)
                found.append(shortcode)

    previous_total = -1
    for _ in range(MAX_PROFILE_SCROLLS + 1):
        collect_from_dom()
        collect_from_html()
        if len(found) == previous_total:
            break
        previous_total = len(found)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.0)

    return found


def _extract_json_ld_objects(html_text: str) -> list[dict]:
    objects: list[dict] = []
    scripts = re.findall(
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
        html_text,
        flags=re.S | re.I,
    )

    for script_text in scripts:
        script_text = script_text.strip()
        if not script_text:
            continue
        try:
            data = json.loads(script_text)
        except Exception:
            continue

        if isinstance(data, dict):
            objects.append(data)
        elif isinstance(data, list):
            objects.extend([item for item in data if isinstance(item, dict)])

    return objects


def _extract_reel_timestamp(html_text: str, json_ld_objects: list[dict]) -> Optional[dt.datetime]:
    for obj in json_ld_objects:
        for key in ("uploadDate", "datePublished", "dateCreated"):
            value = obj.get(key)
            parsed = parse_dt_string(value)
            if parsed is not None:
                return parsed

    raw = _find_first(
        [
            r'"taken_at_timestamp":(\d+)',
            r'"uploadDate":"([^"]+)"',
            r'"datePublished":"([^"]+)"',
            r'"dateCreated":"([^"]+)"',
        ],
        html_text,
    )
    return parse_dt_string(raw)


def _extract_reel_caption(html_text: str, json_ld_objects: list[dict]) -> str:
    for obj in json_ld_objects:
        for key in ("caption", "description", "name"):
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                return _unescape_text(value)

    raw = _find_first(
        [
            r'"caption":\{"text":"(.*?)"\}',
            r'"accessibility_caption":"(.*?)"',
            r'<meta[^>]+property="og:description"[^>]+content="(.*?)"',
        ],
        html_text,
        flags=re.S,
    )
    return _unescape_text(raw)


def _extract_reel_duration(html_text: str, json_ld_objects: list[dict]) -> Optional[float]:
    for obj in json_ld_objects:
        if "duration" in obj and isinstance(obj["duration"], str):
            iso_duration = obj["duration"]
            match = re.fullmatch(r"PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?", iso_duration)
            if match:
                minutes = float(match.group(1) or 0)
                seconds = float(match.group(2) or 0)
                return minutes * 60 + seconds

    raw = _find_first([r'"video_duration":([0-9.]+)'], html_text)
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _extract_metric_from_json_ld(json_ld_objects: list[dict], metric_name: str) -> Optional[int]:
    for obj in json_ld_objects:
        stats = obj.get("interactionStatistic")
        if not stats:
            continue

        if isinstance(stats, dict):
            stats = [stats]
        if not isinstance(stats, list):
            continue

        for item in stats:
            if not isinstance(item, dict):
                continue
            interaction_type = item.get("interactionType")
            if isinstance(interaction_type, dict):
                interaction_type = interaction_type.get("@type")
            if not isinstance(interaction_type, str):
                continue
            if metric_name.lower() in interaction_type.lower():
                count = normalize_count_text(item.get("userInteractionCount"))
                if count is not None:
                    return count
    return None


def get_reel_detail_by_shortcode(shortcode: str, driver: webdriver.Chrome) -> Optional[dict]:
    url = f"https://www.instagram.com/reel/{shortcode}/"
    if not safe_get(driver, url):
        return None

    time.sleep(0.5)
    html_text = driver.page_source or ""
    if not html_text:
        print(f"⚠️ Empty HTML for reel {shortcode}")
        return None

    json_ld_objects = _extract_json_ld_objects(html_text)

    post_dt = _extract_reel_timestamp(html_text, json_ld_objects)
    caption = _extract_reel_caption(html_text, json_ld_objects)
    duration = _extract_reel_duration(html_text, json_ld_objects)

    views = normalize_count_text(
        _find_first([r'"video_view_count":(\d+)', r'"play_count":(\d+)'], html_text)
    )
    plays = normalize_count_text(
        _find_first([r'"video_play_count":(\d+)', r'"play_count":(\d+)'], html_text)
    )
    likes = normalize_count_text(
        _find_first(
            [
                r'"edge_liked_by"\s*:\s*\{\s*"count":(\d+)',
                r'"like_count":(\d+)',
            ],
            html_text,
        )
    )
    comments = normalize_count_text(
        _find_first(
            [
                r'"edge_media_to_comment"\s*:\s*\{\s*"count":(\d+)',
                r'"comment_count":(\d+)',
            ],
            html_text,
        )
    )

    if views is None:
        views = _extract_metric_from_json_ld(json_ld_objects, "Watch")
    if plays is None:
        plays = views
    if likes is None:
        likes = _extract_metric_from_json_ld(json_ld_objects, "Like")
    if comments is None:
        comments = _extract_metric_from_json_ld(json_ld_objects, "Comment")

    return {
        "reels_shortcode": shortcode,
        "post_time": post_dt.strftime("%Y-%m-%d %H:%M:%S") if post_dt else None,
        "duration": duration,
        "caption": caption,
        "views": views or 0,
        "plays": plays or 0,
        "likes": likes or 0,
        "comments": comments or 0,
    }


def extract_recent_reels_from_profile_page(
    driver: webdriver.Chrome,
    username: str,
    existing_shortcodes: set[str],
) -> list[dict]:
    shortcodes = extract_reel_shortcodes_from_profile(driver)
    if not shortcodes:
        print(f"⚠️ {username} no reel links found on profile page")
        return []

    print(f"ℹ️ {username} reel links discovered on profile page: {len(shortcodes)}")

    cutoff = now_local() - dt.timedelta(days=REELS_WINDOW_DAYS)
    results: list[dict] = []

    for shortcode in shortcodes:
        if shortcode in existing_shortcodes:
            continue

        detail = get_reel_detail_by_shortcode(shortcode, driver)
        if not detail:
            print(f"⚠️ Detail fetch failed for shortcode={shortcode}")
            sleep_random(DETAIL_SLEEP_RANGE)
            continue

        post_time_raw = detail.get("post_time")
        post_dt = parse_dt_string(post_time_raw)
        if post_dt is None:
            print(f"⚠️ Missing timestamp for shortcode={shortcode}; skipped")
            sleep_random(DETAIL_SLEEP_RANGE)
            continue

        if post_dt < cutoff:
            print(f"⏭️ Reel {shortcode} older than {REELS_WINDOW_DAYS} days; skipped")
            sleep_random(DETAIL_SLEEP_RANGE)
            continue

        results.append(detail)
        existing_shortcodes.add(shortcode)
        sleep_random(DETAIL_SLEEP_RANGE)

    return results


def build_dynamic_snapshot(detail: dict) -> dict:
    return {
        "reels_shortcode": detail["reels_shortcode"],
        "views": detail.get("views", 0),
        "plays": detail.get("plays", 0),
        "likes": detail.get("likes", 0),
        "comments": detail.get("comments", 0),
        "timestamp": now_str(),
    }


def build_static_row(username: str, detail: dict) -> dict:
    return {
        "kol_account": username,
        "reels_shortcode": detail["reels_shortcode"],
        "post_time": detail.get("post_time"),
        "duration": detail.get("duration"),
        "caption": detail.get("caption"),
    }


def upsert_state_row(
    state_df: pd.DataFrame,
    username: str,
    current_count: Optional[int],
    status: str,
    changed: bool,
) -> pd.DataFrame:
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
    ensure_parent_dir(KOL_INFO_FILE)

    if not KOL_INFO_FILE.exists():
        print(f"⚠️ {KOL_INFO_FILE} not found, creating sample file")
        sample_df = pd.DataFrame({"kol_account": ["instagram", "nasa", "cristiano"]})
        sample_df.to_csv(KOL_INFO_FILE, index=False, encoding="utf-8-sig")
        print(f"✅ Created sample {KOL_INFO_FILE} - please edit with real accounts")
        raise SystemExit(0)

    try:
        kol_df = pd.read_csv(KOL_INFO_FILE)
        print(f"✅ Read {KOL_INFO_FILE}")
    except Exception as exc:
        raise SystemExit(f"❌ Failed to read {KOL_INFO_FILE}: {exc}")

    if "kol_account" not in kol_df.columns:
        raise SystemExit("❌ kol_info.csv must contain kol_account column")

    if kol_df.empty or len(kol_df[kol_df["kol_account"].notna()]) == 0:
        raise SystemExit(f"❌ No valid accounts in {KOL_INFO_FILE}")

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

            recent_reels = extract_recent_reels_from_profile_page(driver, username, existing_shortcodes)
            print(f"ℹ️ {username} new reels to append: {len(recent_reels)}")

            if not recent_reels:
                state_df = upsert_state_row(state_df, username, current_count, "changed_no_recent_reels", True)
                sleep_random(PROFILE_SLEEP_RANGE)
                continue

            for detail in recent_reels:
                new_static_rows.append(build_static_row(username, detail))
                new_dynamic_rows.append(build_dynamic_snapshot(detail))

            state_df = upsert_state_row(state_df, username, current_count, "changed_saved", True)
            sleep_random(PROFILE_SLEEP_RANGE)

    except Exception as exc:
        print(f"❌ Unexpected error: {exc}")
        raise
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
