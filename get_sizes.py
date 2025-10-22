import os
import json
import time
import shutil
import sqlite3
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta, datetime, timezone
import random
import requests
import pandas as pd

# Optionaler Selenium-Fallback zum Auffrischen der Cookies
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FxService
from selenium.webdriver.firefox.options import Options as FxOptions

from utils import get_firefox_profile


# -------------------- Monitoring & Utils --------------------

class PerformanceMonitor:
    def __init__(self):
        self.batch_times = []  # [{ 'batch_size': int, 'time': float }]
        self.segment_results = {}  # { unique_key: { 'time': float, 'attempts': int, 'success': bool } }

    def add_batch_time(self, batch_size, time_taken):
        self.batch_times.append({
            'batch_size': batch_size,
            'time': time_taken
        })

    def add_segment_result(self, unique_key, time_taken, attempts, success: bool):
        self.segment_results[unique_key] = {
            'time': time_taken,
            'attempts': attempts,
            'success': success
        }

    def get_statistics(self):
        if self.batch_times:
            avg_batch_time = sum(b['time'] for b in self.batch_times) / len(self.batch_times)
        else:
            avg_batch_time = 0

        total_segments = len(self.segment_results)
        failed_segments = sum(1 for s in self.segment_results.values() if not s['success'])
        successes = total_segments - failed_segments
        avg_success_rate = (successes / total_segments) if total_segments else 0

        return {
            'average_batch_time': avg_batch_time,
            'average_success_rate': avg_success_rate,
            'total_segments': total_segments,
            'failed_segments': failed_segments
        }


def format_time(seconds):
    """Format seconds to H:MM:SS string."""
    return str(timedelta(seconds=round(seconds)))


def _current_tz_offset_hours():
    """Detect local timezone offset to UTC in hours (e.g., CET/CEST -> 1/2)."""
    now = datetime.now()
    utcnow = datetime.now(timezone.utc).replace(tzinfo=None)
    diff = now - utcnow
    return int(round(diff.total_seconds() / 3600.0))


# -------------------- HTTP Session & Cookies --------------------

BULLSEYE_DOMAIN = "bullseye2-eu.amazon.com"
LOAD_VERSIONS_URL = f"https://{BULLSEYE_DOMAIN}/request/loadSegmentVersions"


def _copy_sqlite_readonly(src_path):
    """Copy locked Firefox cookies.sqlite to a temp file for safe reading."""
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"cookies.sqlite not found at: {src_path}")
    tmpdir = tempfile.mkdtemp(prefix="ff_cookies_")
    dst = os.path.join(tmpdir, "cookies.sqlite")
    shutil.copy2(src_path, dst)
    return dst, tmpdir


def _load_firefox_cookies_for_domain(profile_path, domain_suffix):
    """
    Load cookies from Firefox cookies.sqlite for a given domain suffix.
    Returns a RequestsCookieJar.
    """
    cookies_db = os.path.join(profile_path, "cookies.sqlite")
    cleanup_dir = None
    try:
        conn = sqlite3.connect(f"file:{cookies_db}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        copied_path, cleanup_dir = _copy_sqlite_readonly(cookies_db)
        conn = sqlite3.connect(copied_path)

    jar = requests.cookies.RequestsCookieJar()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name, value, host, path, isSecure FROM moz_cookies WHERE host LIKE ?",
            (f"%{domain_suffix}",)
        )
        for name, value, host, path, isSecure in cur.fetchall():
            jar.set(name, value, domain=host, path=path, secure=bool(isSecure))
    finally:
        conn.close()
        if cleanup_dir and os.path.isdir(cleanup_dir):
            shutil.rmtree(cleanup_dir, ignore_errors=True)
    return jar


def _build_http_session(profile_path):
    """Create a requests.Session with Firefox cookies and sane headers."""
    jar = _load_firefox_cookies_for_domain(profile_path, BULLSEYE_DOMAIN)
    s = requests.Session()
    s.cookies = jar
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "text/plain, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": f"https://{BULLSEYE_DOMAIN}",
        "Connection": "keep-alive",
    })
    return s


# -------------------- Selenium Cookie Refresh (Fallback) --------------------

def _cookiejar_from_selenium_cookies(cookies_list):
    jar = requests.cookies.RequestsCookieJar()
    for c in cookies_list:
        jar.set(
            c.get("name"),
            c.get("value"),
            domain=c.get("domain"),
            path=c.get("path", "/"),
            secure=bool(c.get("secure", False))
        )
    return jar


def _selenium_refresh_session_cookies(profile_path, headless=True, segment_id_for_referer=None):
    """
    Start headless Firefox with given profile, open a Bullseye page to refresh cookies,
    then return them as RequestsCookieJar.
    """
    options = FxOptions()
    options.add_argument("-profile")
    options.add_argument(profile_path)
    if headless:
        options.add_argument("--headless")
        options.add_argument("--width=1920")
        options.add_argument("--height=1080")

    service_path = "geckodriver.exe" if os.name == "nt" else "geckodriver"
    service = FxService(service_path)

    driver = None
    try:
        driver = webdriver.Firefox(service=service, options=options)
        url = f"https://{BULLSEYE_DOMAIN}/segment?id={segment_id_for_referer}" if segment_id_for_referer else f"https://{BULLSEYE_DOMAIN}/"
        driver.get(url)
        time.sleep(3)  # allow SSO / JS to set fresh cookies
        cookies = driver.get_cookies()
        return _cookiejar_from_selenium_cookies(cookies)
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


# -------------------- Core HTTP Fetch --------------------

def _choose_version(versions):

    return versions[0] if versions else None



def _fetch_one_segment_http(
        session,
        segment_id,
        tz_offset_hours=0,
        limit=250,
        timeout=(5, 30),            # (connect_timeout, read_timeout)
        max_attempts=4,
        base_backoff=1.6
    ):
    """
    Call POST /request/loadSegmentVersions with robust retries.
    - Retries bei Timeout, 5xx, 429 (beachtet Retry-After).
    - Wählt immer versions[0] (neueste).
    """
    t0 = time.time()
    attempts = 0

    payload = {
        "id": int(segment_id),
        "limit": int(limit),
        "timeZoneOffset": int(tz_offset_hours)
    }
    headers = {
        "Referer": f"https://{BULLSEYE_DOMAIN}/segment?id={segment_id}"
    }

    last_error = None

    for attempt in range(1, max_attempts + 1):
        attempts = attempt
        try:
            resp = session.post(
                LOAD_VERSIONS_URL,
                headers=headers,
                data=json.dumps(payload),
                timeout=timeout
            )

            # Auth-Fehler nicht retrien
            if resp.status_code in (401, 403):
                return {
                    "BE ID": segment_id,
                    "Segment Size": "AuthFailed",
                    "Status": "Unauthorized",
                    "Attempts": attempts,
                    "Processing Time": round(time.time() - t0, 3)
                }

            # Rate-Limit → Retry-After beachten
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and str(retry_after).isdigit():
                    sleep_s = int(retry_after)
                else:
                    sleep_s = (base_backoff ** attempt) + (random.random() * 0.2)
                time.sleep(sleep_s)
                continue

            # 5xx → retry mit Backoff
            if 500 <= resp.status_code < 600:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue

            # andere Fehler → keine Retries
            resp.raise_for_status()

            # JSON parsen (auch bei text/plain)
            try:
                data = resp.json()
            except Exception:
                data = json.loads(resp.text)

            versions = data.get("versions", [])
            chosen = _choose_version(versions)
            if not chosen:
                # evtl. transient → retry außer beim letzten Versuch
                if attempt < max_attempts:
                    time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                    continue
                return {
                    "BE ID": segment_id,
                    "Segment Size": "NoVersions",
                    "Status": "Unknown",
                    "Attempts": attempts,
                    "Processing Time": round(time.time() - t0, 3)
                }

            size_val = chosen.get("yesSize", chosen.get("size"))
            status_text = chosen.get("status", "Unknown")

            if size_val is None:
                if attempt < max_attempts:
                    time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                    continue
                return {
                    "BE ID": segment_id,
                    "Segment Size": "ParseFailed",
                    "Status": str(status_text),
                    "Attempts": attempts,
                    "Processing Time": round(time.time() - t0, 3)
                }

            return {
                "BE ID": segment_id,
                "Segment Size": int(size_val),
                "Status": str(status_text),
                "Attempts": attempts,
                "Processing Time": round(time.time() - t0, 3)
            }

        except requests.Timeout as e:
            last_error = e
            if attempt < max_attempts:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue
            return {
                "BE ID": segment_id,
                "Segment Size": "Timeout",
                "Status": "Unknown",
                "Attempts": attempts,
                "Processing Time": round(time.time() - t0, 3)
            }

        except requests.RequestException as e:
            last_error = e
            # Netzwerk-/Transportfehler retrybar
            if attempt < max_attempts:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue
            return {
                "BE ID": segment_id,
                "Segment Size": "Failed",
                "Status": f"Error: {str(e)}",
                "Attempts": attempts,
                "Processing Time": round(time.time() - t0, 3)
            }

        except Exception as e:
            last_error = e
            # unbekannter Fehler → kein Retry (kann Parser-Fehler sein)
            return {
                "BE ID": segment_id,
                "Segment Size": "Failed",
                "Status": f"Error: {str(e)}",
                "Attempts": attempts,
                "Processing Time": round(time.time() - t0, 3)
            }

    # sollte nicht erreicht werden, aber falls doch:
    return {
        "BE ID": segment_id,
        "Segment Size": "Failed",
        "Status": f"Error: {str(last_error) if last_error else 'Unknown'}",
        "Attempts": attempts or 1,
        "Processing Time": round(time.time() - t0, 3)
    }




# -------------------- Public API (HTTP with Selenium fallback) --------------------

def get_segment_sizes_http(segment_ids, status_callback=None, progress_callback=None, headless=False):
    """
    HTTP-based, parallel fetch of segment sizes.
    - Keeps output order equal to input order (including duplicates)
    - Returns (df, filename)
    """
    start_time = time.time()
    performance_monitor = PerformanceMonitor()

    if status_callback:
        status_callback("Preparing HTTP session...")

    profile_path = get_firefox_profile()
    if not profile_path:
        if status_callback:
            status_callback("No Firefox profile found!")
        return None

    session = _build_http_session(profile_path)
    tz_offset = _current_tz_offset_hours()

    # Preflight auth check on the first ID (if present); refresh cookies via Selenium if needed
    if segment_ids:
        test_id = segment_ids[0]
        pre_row = _fetch_one_segment_http(session, test_id, tz_offset, 250, (5, 15), 2)

        if pre_row.get("Segment Size") == "AuthFailed":
            if status_callback:
                status_callback("Auth failed. Refreshing cookies via headless Firefox...")
            new_jar = _selenium_refresh_session_cookies(profile_path, headless=True, segment_id_for_referer=test_id)
            if new_jar:
                session.cookies = new_jar
                pre_row = _fetch_one_segment_http(session, test_id, tz_offset, 250, 15)
                if pre_row.get("Segment Size") == "AuthFailed" and status_callback:
                    status_callback("Auth still failing after refresh. Please re-login in Firefox.")
            else:
                if status_callback:
                    status_callback("Could not refresh cookies via Selenium.")

    # Prepare ordered result list
    results_list = [None] * len(segment_ids)

    max_workers = min(12, max(2, (os.cpu_count() or 4)))
    if status_callback:
        status_callback(f"Fetching {len(segment_ids)} segments via HTTP (workers={max_workers})")

    processed = 0
    t_batch_start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for idx, seg_id in enumerate(segment_ids):
            fut = executor.submit(_fetch_one_segment_http, session, seg_id, tz_offset, 250, (5, 30), 4)

            futures[fut] = (idx, seg_id)

        for fut in as_completed(futures):
            idx, seg_id = futures[fut]
            row = fut.result()
            results_list[idx] = row

            if progress_callback:
                progress_callback(processed)
            processed += 1

            attempts_used = int(row.get("Attempts", 1)) if isinstance(row.get("Attempts", 1), int) else 1
            perf_time = row.get("Processing Time") or 0.0
            success = isinstance(row.get("Segment Size"), int)
            performance_monitor.add_segment_result(f"{seg_id}#{idx}", perf_time, attempts_used, success)


    # Fake a batch timing (total time) so the UI has a value
    performance_monitor.add_batch_time(len(segment_ids), time.time() - t_batch_start)

    # Stats & UI messages
    stats = performance_monitor.get_statistics()
    total_time = time.time() - start_time

    if status_callback:
        status_callback("\nPerformance Statistics:")
        status_callback(f"Total processing time: {format_time(total_time)}")
        status_callback(f"Average batch time: {format_time(stats['average_batch_time'])}")
        status_callback(f"Success rate: {stats['average_success_rate']*100:.1f}%")
        status_callback(f"Failed segments: {stats['failed_segments']}")
        if len(segment_ids) > 0:
            status_callback(f"Average time per segment: {format_time(total_time/len(segment_ids))}")

    # DataFrame & Excel with timestamped filename
    df = pd.DataFrame(results_list)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"segment_sizes_{timestamp}.xlsx"
    df.to_excel(filename, index=False)
    return df, filename


# Backward-compatible name (UI calls get_segment_sizes)
def get_segment_sizes(segment_ids, status_callback=None, progress_callback=None, headless=False):
    """
    Alias to HTTP implementation with Selenium fallback.
    """
    return get_segment_sizes_http(segment_ids, status_callback, progress_callback, headless)


if __name__ == "__main__":
    # Minimal test (requires a valid Firefox profile session & network)
    test_ids = ["1709631602"]
    print("Starting HTTP fetch test...")
    result = get_segment_sizes(test_ids, print, lambda i: None, headless=True)
    if result is not None:
        df, fname = result
        print("Saved:", fname)
        print(df)
