import os
import json
import time
import random
import shutil
import sqlite3
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta, datetime, timezone

import requests
import pandas as pd

from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FxService
from selenium.webdriver.firefox.options import Options as FxOptions

from utils import get_firefox_profile


# -------------------- Monitoring & Utils --------------------

class PerformanceMonitor:
    def __init__(self):
        self.batch_times = []  # [{ 'batch_size': int, 'time': float, 'success_rate': float }]
        self.segment_times = {}  # { unique_key: { 'time': float, 'attempts': int, 'success': bool } }

    def add_batch_time(self, batch_size, time_taken, success_count):
        self.batch_times.append({
            'batch_size': batch_size,
            'time': time_taken,
            'success_rate': (success_count / batch_size) if batch_size else 0.0
        })

    def add_segment_time(self, unique_key, time_taken, attempts, success):
        self.segment_times[unique_key] = {
            'time': time_taken,
            'attempts': attempts,
            'success': success
        }

    def get_statistics(self):
        if not self.batch_times:
            return {
                'average_batch_time': 0,
                'average_success_rate': 0,
                'total_segments': 0,
                'failed_segments': 0
            }
        avg_batch_time = sum(b['time'] for b in self.batch_times) / len(self.batch_times)
        avg_success_rate = sum(b['success_rate'] for b in self.batch_times) / len(self.batch_times)
        total_segments = len(self.segment_times)
        failed_segments = sum(1 for v in self.segment_times.values() if not v['success'])
        return {
            'average_batch_time': avg_batch_time,
            'average_success_rate': avg_success_rate,
            'total_segments': total_segments,
            'failed_segments': failed_segments
        }


def format_time(seconds):
    return str(timedelta(seconds=round(seconds)))


def _current_tz_offset_hours():
    now = datetime.now()
    utcnow = datetime.now(timezone.utc).replace(tzinfo=None)
    diff = now - utcnow
    return int(round(diff.total_seconds() / 3600.0))


# -------------------- HTTP Session & Cookies --------------------

BULLSEYE_DOMAIN = "bullseye2-eu.amazon.com"
QUEUE_URL = f"https://{BULLSEYE_DOMAIN}/request/queueQuery"


def _copy_sqlite_readonly(src_path):
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"cookies.sqlite not found at: {src_path}")
    tmpdir = tempfile.mkdtemp(prefix="ff_cookies_")
    dst = os.path.join(tmpdir, "cookies.sqlite")
    shutil.copy2(src_path, dst)
    return dst, tmpdir


def _load_firefox_cookies_for_domain(profile_path, domain_suffix):
    """
    Lädt Cookies aus Firefox für mehrere relevante Hosts:
    - bullseye2-eu.amazon.com (Service-spezifisch)
    - .amazon.com (domainweite SSO-Cookies wie amzn_sso_token, session-token, …)
    - midway-auth.amazon.com (SSO-Flow)
    """
    cookies_db = os.path.join(profile_path, "cookies.sqlite")
    cleanup_dir = None
    try:
        conn = sqlite3.connect(f"file:{cookies_db}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        copied_path, cleanup_dir = _copy_sqlite_readonly(cookies_db)
        conn = sqlite3.connect(copied_path)

    patterns = [
        f"%{domain_suffix}",          # bullseye2-eu.amazon.com
        "%.amazon.com",               # domainweite Cookies
        "%amazon.com",                # fallback (ohne Punkt)
        "%midway-auth.amazon.com%",   # SSO
    ]

    jar = requests.cookies.RequestsCookieJar()
    try:
        cur = conn.cursor()
        query = "SELECT name, value, host, path, isSecure FROM moz_cookies WHERE " + " OR ".join(["host LIKE ?"] * len(patterns))
        cur.execute(query, patterns)
        for name, value, host, path, isSecure in cur.fetchall():
            jar.set(name, value, domain=host, path=path, secure=bool(isSecure))
    finally:
        conn.close()
        if cleanup_dir and os.path.isdir(cleanup_dir):
            shutil.rmtree(cleanup_dir, ignore_errors=True)
    return jar



def _build_http_session(profile_path):
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
    options = FxOptions()
    options.add_argument("-profile")
    options.add_argument(profile_path)
    if headless:
        options.add_argument("--headless")
        options.add_argument("--width=1920")
        options.add_argument("--height=1080")

    service = FxService("geckodriver.exe")
    driver = None
    try:
        driver = webdriver.Firefox(service=service, options=options)
        url = f"https://{BULLSEYE_DOMAIN}/segment?id={segment_id_for_referer}" if segment_id_for_referer else f"https://{BULLSEYE_DOMAIN}/"
        driver.get(url)
        time.sleep(3)
        cookies = driver.get_cookies()
        return _cookiejar_from_selenium_cookies(cookies)
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


# -------------------- Auth-Preflight ohne Side-Effects --------------------

def _auth_preflight(session, segment_id):
    """
    Prüft Cookies ohne Side-Effects. 401/403 ODER Redirect auf Login ⇒ False.
    """
    try:
        url = f"https://{BULLSEYE_DOMAIN}/segment?id={segment_id}"
        resp = session.get(url, timeout=(3, 10), allow_redirects=False)
        if resp.status_code in (401, 403):
            return False
        if 300 <= resp.status_code < 400:
            loc = (resp.headers.get("Location") or "").lower()
            if any(k in loc for k in ("signin", "login", "midway", "auth")):
                return False
        # Manche Gateways antworten 200 mit Login-Seite:
        body = (resp.text or "").lower()
        if any(k in body for k in ("signin", "sign-in", "midway", "authenticate")):
            return False
        return True
    except Exception:
        return True



# -------------------- Core: Queue-Request mit Retries --------------------

def _queue_one_segment_http(
        session,
        segment_id,
        tz_offset_hours=0,
        timeout=(5, 30),
        max_attempts=4,
        base_backoff=1.6
    ):
    """
    POST /request/queueQuery mit Retries (Timeout, 5xx, 429).
    Erfolg = Response-JSON enthält numerisches Feld 'queued' (Wert egal).
    Gibt eine Ergebniszeile (dict) zurück.
    """
    t0 = time.time()
    attempts = 0
    last_error = None

    payload = {
        "id": int(segment_id),
        "timeZoneOffset": int(tz_offset_hours)
    }
    headers = {
        "Referer": f"https://{BULLSEYE_DOMAIN}/segment?id={segment_id}"
    }

    for attempt in range(1, max_attempts + 1):
        attempts = attempt
        try:
            resp = session.post(QUEUE_URL, headers=headers, data=json.dumps(payload), timeout=timeout)

            # Auth-Fehler nicht retrien
            if resp.status_code in (401, 403):
                return {
                    "BE ID": segment_id,
                    "Queue Status": "AuthFailed",
                    "Queue ID": None,
                    "Attempts": attempts,
                    "Processing Time": round(time.time() - t0, 3),
                    "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                }

            # Rate-Limit → Retry-After beachten
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                sleep_s = int(ra) if (ra and str(ra).isdigit()) else (base_backoff ** attempt) + (random.random() * 0.2)
                time.sleep(sleep_s)
                continue

            # 5xx → Retry
            if 500 <= resp.status_code < 600:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue

            # andere 4xx → kein Retry
            if not (200 <= resp.status_code < 300):
                return {
                    "BE ID": segment_id,
                    "Queue Status": f"HTTP {resp.status_code}",
                    "Queue ID": None,
                    "Attempts": attempts,
                    "Processing Time": round(time.time() - t0, 3),
                    "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                }

            # 2xx -> JSON parsen und 'queued' numerisch auswerten
            text_body = resp.text or ""
            try:
                json_obj = resp.json()
            except Exception:
                try:
                    json_obj = json.loads(text_body) if text_body else None
                except Exception:
                    json_obj = None

            queued_id = None
            if isinstance(json_obj, dict) and "queued" in json_obj:
                val = json_obj.get("queued")
                if isinstance(val, (int, float)):
                    queued_id = str(int(val))
                elif isinstance(val, str):
                    s = val.strip()
                    if s.isdigit():
                        queued_id = s

            if queued_id is not None:
                return {
                    "BE ID": segment_id,
                    "Queue Status": "Success",
                    "Queue ID": queued_id,
                    "Attempts": attempts,
                    "Processing Time": round(time.time() - t0, 3),
                    "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                }
            else:
                # Unerwartete 2xx-Antwort ohne numerisches 'queued' → ggf. Retry
                if attempt < max_attempts:
                    time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                    continue
                return {
                    "BE ID": segment_id,
                    "Queue Status": "UnexpectedResponse",
                    "Queue ID": None,
                    "Attempts": attempts,
                    "Processing Time": round(time.time() - t0, 3),
                    "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                }

        except requests.Timeout as e:
            last_error = e
            if attempt < max_attempts:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue
            return {
                "BE ID": segment_id,
                "Queue Status": "Timeout",
                "Queue ID": None,
                "Attempts": attempts,
                "Processing Time": round(time.time() - t0, 3),
                "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            }

        except requests.RequestException as e:
            last_error = e
            if attempt < max_attempts:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue
            return {
                "BE ID": segment_id,
                "Queue Status": f"Error: {str(e)}",
                "Queue ID": None,
                "Attempts": attempts,
                "Processing Time": round(time.time() - t0, 3),
                "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            last_error = e
            return {
                "BE ID": segment_id,
                "Queue Status": f"Error: {str(e)}",
                "Queue ID": None,
                "Attempts": attempts,
                "Processing Time": round(time.time() - t0, 3),
                "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            }

    return {
        "BE ID": segment_id,
        "Queue Status": f"Error: {str(last_error) if last_error else 'Unknown'}",
        "Queue ID": None,
        "Attempts": attempts or 1,
        "Processing Time": round(time.time() - t0, 3),
        "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }


# -------------------- Öffentliche API --------------------

def get_adaptive_batch_size(remaining_segments, max_batch=10, min_batch=3):
    if remaining_segments <= max_batch:
        return max(min_batch, remaining_segments)
    return max_batch


def queue_segments(segment_ids, status_callback=None, progress_callback=None, headless=False):
    """
    HTTP-basierte Queue-Operationen (parallel), Reihenfolge == Eingabe (inkl. Duplikate).
    Schreibt 'queue_results.xlsx' und gibt ein DataFrame zurück (kompatibel zu deiner UI).
    """
    start_time = time.time()
    perf = PerformanceMonitor()

    if status_callback:
        status_callback("Preparing HTTP session...")

    profile_path = get_firefox_profile()
    if not profile_path:
        if status_callback:
            status_callback("No Firefox profile found!")
        return None

    session = _build_http_session(profile_path)
    tz_offset = _current_tz_offset_hours()

    # Auth-Preflight (ohne Side-Effects). Bei 401/403 -> Cookies via Selenium auffrischen.
    if segment_ids:
        if not _auth_preflight(session, segment_ids[0]):
            if status_callback:
                status_callback("Auth failed. Refreshing cookies via headless Firefox...")
            new_jar = _selenium_refresh_session_cookies(profile_path, headless=True, segment_id_for_referer=segment_ids[0])
            if new_jar:
                session.cookies = new_jar
            else:
                if status_callback:
                    status_callback("Could not refresh cookies via Selenium.")

    # Ergebnisse in Eingabereihenfolge
    results_list = [None] * len(segment_ids)

    max_workers = min(12, max(2, (os.cpu_count() or 4)))
    if status_callback:
        status_callback(f"Queueing {len(segment_ids)} segments via HTTP (workers={max_workers})")

    processed = 0
    success_count = 0
    t_batch_start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for idx, seg_id in enumerate(segment_ids):
            fut = executor.submit(_queue_one_segment_http, session, seg_id, tz_offset, (5, 30), 4)
            futures[fut] = (idx, seg_id)

        for fut in as_completed(futures):
            idx, seg_id = futures[fut]
            row = fut.result()
            results_list[idx] = row

            if progress_callback:
                progress_callback(processed)
            processed += 1

            is_success = (row.get("Queue Status") == "Success")
            if is_success:
                success_count += 1

            attempts_used = row.get("Attempts", 1)
            try:
                attempts_used = int(attempts_used)
            except (TypeError, ValueError):
                attempts_used = 1

            perf_time = row.get("Processing Time") or 0.0
            perf.add_segment_time(f"{seg_id}#{idx}", perf_time, attempts_used, is_success)

    # Eine Batch-Zeit erfassen (Gesamtlaufzeit)
    perf.add_batch_time(len(segment_ids), time.time() - t_batch_start, success_count)

    # Stats an UI
    stats = perf.get_statistics()
    total_time = time.time() - start_time
    if status_callback:
        status_callback("\nPerformance Statistics:")
        status_callback(f"Total processing time: {format_time(total_time)}")
        status_callback(f"Average batch time: {format_time(stats['average_batch_time'])}")
        status_callback(f"Success rate: {stats['average_success_rate']*100:.1f}%")
        status_callback(f"Failed segments: {stats['failed_segments']}")
        if len(segment_ids) > 0:
            status_callback(f"Average time per segment: {format_time(total_time/len(segment_ids))}")

    # Excel schreiben (kompatibler Dateiname wie bisher)
    df = pd.DataFrame(results_list)
    timestamp = time.strftime("%Y%m%d_%H%M%S"); filename = f"queue_results_{timestamp}.xlsx"
    df.to_excel(filename, index=False)
    return df, filename


    if status_callback:
        status_callback("Results saved to queue_results.xlsx")

    return df


if __name__ == "__main__":
    test_ids = ['1709573602']
    print("Starting HTTP queue test...")
    out = queue_segments(test_ids, print, lambda i: None, headless=True)
    print(out)
