# extract_rules.py
# HTTP-basierte Rules-Extraktion → eine flache, generische Tabelle (segment_rules_flat_min)
# Spalten:
# marketplace_id | be_id | version | scope | scope_operator | rule_id | parent_rule_id |
# group_operator | def_id | constraint_key | constraint_op | constraint_value | fetched_at

import os
import json
import time
import shutil
import random
import sqlite3
import tempfile
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        self.batch_times = []
        self.segment_results = {}  # key -> {time, attempts, success}

    def add_batch_time(self, batch_size, time_taken):
        self.batch_times.append({'batch_size': batch_size, 'time': time_taken})

    def add_segment_result(self, unique_key, time_taken, attempts, success: bool):
        self.segment_results[unique_key] = {'time': time_taken, 'attempts': attempts, 'success': success}

    def get_statistics(self):
        avg_batch_time = (sum(b['time'] for b in self.batch_times) / len(self.batch_times)) if self.batch_times else 0
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
    return str(timedelta(seconds=round(seconds)))


def _current_tz_offset_hours():
    now = datetime.now()
    utcnow = datetime.now(timezone.utc).replace(tzinfo=None)
    diff = now - utcnow
    return int(round(diff.total_seconds() / 3600.0))


def _idx_to_alpha(idx: int) -> str:
    s = ""
    n = idx
    while True:
        s = chr(ord('a') + (n % 26)) + s
        n = n // 26 - 1
        if n < 0:
            break
    return s


# -------------------- HTTP Session & Cookies --------------------

BULLSEYE_DOMAIN = "bullseye2-eu.amazon.com"
LOAD_VERSIONS_URL = f"https://{BULLSEYE_DOMAIN}/request/loadSegmentVersions"
LOAD_QUERY_URL = f"https://{BULLSEYE_DOMAIN}/request/loadQuery"
LOAD_LATEST_VERSION_URL = f"https://{BULLSEYE_DOMAIN}/request/loadLatestQueryVersion"


def _copy_sqlite_readonly(src_path):
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"cookies.sqlite not found at: {src_path}")
    tmpdir = tempfile.mkdtemp(prefix="ff_cookies_")
    dst = os.path.join(tmpdir, "cookies.sqlite")
    shutil.copy2(src_path, dst)
    return dst, tmpdir


def _load_firefox_cookies_for_domain(profile_path, domain_suffix):
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
    jar = _load_firefox_cookies_for_domain(profile_path, BULLSEYE_DOMAIN)
    s = requests.Session()
    s.cookies = jar
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "application/json, text/plain, */*",
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


# -------------------- Robust HTTP calls with retry --------------------

def _parse_retry_after(value):
    if not value:
        return None
    value = str(value).strip()
    if value.isdigit():
        return int(value)
    try:
        for fmt in ("%a, %d %b %Y %H:%M:%S %Z",
                    "%a, %d %b %Y %H:%M:%S GMT",
                    "%a, %d %b %Y %H:%M:%S %z"):
            try:
                dt = datetime.strptime(value, fmt)
                delta = (dt.replace(tzinfo=timezone.utc) - datetime.now(timezone.utc)).total_seconds()
                return max(0, int(delta))
            except Exception:
                continue
    except Exception:
        return None
    return None


def _choose_version(versions):
    if not versions:
        return None
    try:
        return max(versions, key=lambda v: int(v.get("version", -1)))
    except Exception:
        return versions[0]


def _post_json(session: requests.Session, url: str, payload: dict, extra_headers: dict, timeout=(5, 30),
               max_attempts=4, base_backoff=1.6):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.post(url, headers=extra_headers, data=json.dumps(payload), timeout=timeout)
            if resp.status_code in (401, 403):
                return resp
            if resp.status_code == 429:
                ra = _parse_retry_after(resp.headers.get("Retry-After"))
                sleep_s = ra if ra is not None else (base_backoff ** attempt) + (random.random() * 0.2)
                time.sleep(sleep_s)
                continue
            if 500 <= resp.status_code < 600:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue
            resp.raise_for_status()
            return resp
        except requests.Timeout as e:
            last_error = e
            if attempt < max_attempts:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue
            raise
        except requests.RequestException as e:
            last_error = e
            if attempt < max_attempts:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue
            raise
        except Exception as e:
            last_error = e
            raise
    raise last_error if last_error else RuntimeError("Unknown HTTP error")


# -------------------- Query -> Flat Table (minimal schema) --------------------

def _build_flat_min(loadquery_json: dict, be_id: str, version: int) -> list[dict]:
    basic = loadquery_json.get("basic", {}) or {}
    marketplace_id = basic.get("marketplaceId")
    fetched_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    out = []

    def walk_scope(scope_name: str, scope_obj: dict):
        scope_op = (scope_obj or {}).get("operator") or "ALL"
        rules = (scope_obj or {}).get("rules") or []

        def walk_rule(rule_obj: dict, parent_rule_id: str | None, depth: int, child_idx: int | None,
                      root_id: int, parent_group_op: str | None):
            node_def = rule_obj.get("defId")
            node_op = rule_obj.get("operator") or "NONE"
            sub_rules = rule_obj.get("subRules") or []
            constraints = rule_obj.get("constraints") or []

            if depth == 0 and child_idx is None:
                rid = f"{'INC' if scope_name == 'Include' else 'EXC'}{root_id}"
            else:
                letter = _idx_to_alpha(child_idx if child_idx is not None else 0)
                rid = f"{'INC' if scope_name == 'Include' else 'EXC'}{root_id}.{letter}"

            if sub_rules:
                out.append({
                    "marketplace_id": marketplace_id,
                    "be_id": str(be_id),
                    "version": int(version) if version is not None else None,
                    "scope": scope_name,
                    "scope_operator": scope_op,
                    "rule_id": rid,
                    "parent_rule_id": parent_rule_id,
                    "group_operator": node_op if node_op != "NONE" else "ANY",
                    "def_id": node_def,
                    "constraint_key": None,
                    "constraint_op": None,
                    "constraint_value": None,
                    "fetched_at": fetched_at,
                })
                for i, sr in enumerate(sub_rules):
                    walk_rule(sr, rid, depth + 1, i, root_id, parent_group_op=(node_op if node_op != "NONE" else "ANY"))
                return

            if not constraints:
                out.append({
                    "marketplace_id": marketplace_id,
                    "be_id": str(be_id),
                    "version": int(version) if version is not None else None,
                    "scope": scope_name,
                    "scope_operator": scope_op,
                    "rule_id": rid,
                    "parent_rule_id": parent_rule_id,
                    "group_operator": parent_group_op,
                    "def_id": node_def,
                    "constraint_key": None,
                    "constraint_op": None,
                    "constraint_value": None,
                    "fetched_at": fetched_at,
                })
            else:
                for c in constraints:
                    key = c.get("defId")
                    op = c.get("op") or "EQ"
                    vals = c.get("values")
                    if isinstance(vals, list):
                        val_text = ",".join([str(v) for v in vals])
                    elif vals is None:
                        val_text = None
                    else:
                        val_text = str(vals)

                    out.append({
                        "marketplace_id": marketplace_id,
                        "be_id": str(be_id),
                        "version": int(version) if version is not None else None,
                        "scope": scope_name,
                        "scope_operator": scope_op,
                        "rule_id": rid,
                        "parent_rule_id": parent_rule_id,
                        "group_operator": parent_group_op,
                        "def_id": node_def,
                        "constraint_key": key,
                        "constraint_op": op,
                        "constraint_value": val_text,
                        "fetched_at": fetched_at,
                    })

        for idx, r in enumerate(rules, start=1):
            walk_rule(r, parent_rule_id=None, depth=0, child_idx=None, root_id=idx, parent_group_op=None)

    include = basic.get("include") if isinstance(basic, dict) else None
    exclude = basic.get("exclude") if isinstance(basic, dict) else None
    walk_scope("Include", include or {})
    walk_scope("Exclude", exclude or {})
    return out


# -------------------- Segment fetch (versions + query) --------------------

def _warm_up(session: requests.Session, seg_id: str | int):
    url = f"https://{BULLSEYE_DOMAIN}/segment?id={seg_id}"
    try:
        session.get(url, headers={"Referer": url}, timeout=(5, 10))
        time.sleep(0.15 + random.random() * 0.15)  # kleiner Jitter
    except Exception:
        pass


def _fetch_versions(session: requests.Session, seg_id: str | int, tz_offset_hours: int,
                    timeout=(5, 30), max_attempts=4):
    payload = {"id": int(seg_id), "limit": 250, "timeZoneOffset": int(tz_offset_hours)}
    headers = {"Referer": f"https://{BULLSEYE_DOMAIN}/segment?id={seg_id}"}
    return _post_json(session, LOAD_VERSIONS_URL, payload, headers, timeout=timeout, max_attempts=max_attempts)


def _fetch_query(session: requests.Session, seg_id: str | int, version: int, tz_offset_hours: int,
                 timeout=(5, 30), max_attempts=4):
    payload = {"id": int(seg_id), "version": int(version), "editMode": False, "timeZoneOffset": int(tz_offset_hours)}
    headers = {"Referer": f"https://{BULLSEYE_DOMAIN}/segment?id={seg_id}"}
    return _post_json(session, LOAD_QUERY_URL, payload, headers, timeout=timeout, max_attempts=max_attempts)


def _fetch_latest_version(session: requests.Session, seg_id: str | int, tz_offset_hours: int,
                          timeout=(5, 30), max_attempts=4):
    payload = {"id": int(seg_id), "timeZoneOffset": int(tz_offset_hours)}
    headers = {"Referer": f"https://{BULLSEYE_DOMAIN}/segment?id={seg_id}"}
    return _post_json(session, LOAD_LATEST_VERSION_URL, payload, headers, timeout=timeout, max_attempts=max_attempts)


def _fetch_one_segment_rules_flat(session: requests.Session, segment_id: str, tz_offset_hours: int,
                                  timeout=(5, 30), max_attempts=4):
    """
    Liefert (rows_list, meta_dict).
    meta_dict: {
        be_id, success, status,
        attempts_versions, attempts_query,
        http_latest, http_versions, http_query,
        version_source, published, version,
        row_count, notes
    }
    """
    meta = {
        "be_id": str(segment_id),
        "success": False,
        "status": "INIT",
        "attempts_versions": 0,
        "attempts_query": 0,
        "http_versions": None,
        "http_query": None,
        "version": None,
        "row_count": 0,
        "notes": "",
        "http_latest": None,
        "version_source": None,
        "published": None,
    }

    # Warm-Up
    _warm_up(session, segment_id)

    # --- 1) Latest-Version bevorzugen ---
    resp_latest = None
    try:
        resp_latest = _fetch_latest_version(session, segment_id, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
        meta["http_latest"] = resp_latest.status_code
        if resp_latest.status_code in (401, 403):
            _warm_up(session, segment_id)
            resp_latest = _fetch_latest_version(session, segment_id, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
            meta["http_latest"] = resp_latest.status_code
    except Exception as e:
        meta["notes"] = f"latest_err={type(e).__name__}: {e}"

    latest_version = None
    latest_published = None
    latest_query_json = None

    if resp_latest is not None and resp_latest.ok:
        try:
            lj = resp_latest.json()
        except Exception:
            lj = json.loads(resp_latest.text)
        latest_version = lj.get("version")
        latest_published = lj.get("published")

    if latest_version is not None:
        meta["version_source"] = "latest"
        meta["published"] = bool(latest_published)
        attempts_q = 0
        try:
            attempts_q += 1
            resp_q = _fetch_query(session, segment_id, latest_version, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
            meta["http_query"] = resp_q.status_code
            if resp_q.status_code in (401, 403):
                _warm_up(session, segment_id)
                attempts_q += 1
                resp_q = _fetch_query(session, segment_id, latest_version, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
                meta["http_query"] = resp_q.status_code
        except Exception as e:
            meta["status"] = "Error(QueryLatest)"
            meta["attempts_query"] = attempts_q or 1
            meta["notes"] = f"{meta.get('notes','')}; q_latest={type(e).__name__}: {e}"
            resp_q = None

        if resp_q is not None and resp_q.ok:
            try:
                latest_query_json = resp_q.json()
            except Exception:
                latest_query_json = json.loads(resp_q.text)

            if latest_query_json.get("notFound") is not True:
                rows = _build_flat_min(latest_query_json, be_id=str(segment_id), version=int(latest_version))
                meta["version"] = int(latest_version)
                meta["attempts_query"] = attempts_q
                meta["row_count"] = len(rows)
                meta["success"] = True
                meta["status"] = "OK" if rows else "OK(ZeroRows)"
                return rows, meta
            else:
                meta["notes"] = f"{meta.get('notes','')}; latest_notFound"

    # --- 2) Fallback: Versionsliste durchprobieren ---
    attempts_v = 0
    try:
        attempts_v += 1
        resp_v = _fetch_versions(session, segment_id, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
        meta["http_versions"] = resp_v.status_code
        if resp_v.status_code in (401, 403):
            _warm_up(session, segment_id)
            attempts_v += 1
            resp_v = _fetch_versions(session, segment_id, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
            meta["http_versions"] = resp_v.status_code
    except Exception as e:
        meta["status"] = "Error(Versions)"
        meta["attempts_versions"] = attempts_v or 1
        meta["notes"] = f"{meta.get('notes','')}; v_err={type(e).__name__}: {e}"
        return [], meta

    meta["attempts_versions"] = attempts_v
    try:
        data_v = resp_v.json()
    except Exception:
        data_v = json.loads(resp_v.text)

    versions = (data_v.get("versions") or [])
    for v in versions:
        vid = v.get("version")
        if vid is None:
            continue
        try:
            meta["version_source"] = "fallback_list"
            meta["version"] = int(vid)
            attempts_q = meta.get("attempts_query", 0) or 0
            attempts_q += 1
            r = _fetch_query(session, segment_id, vid, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
            meta["http_query"] = r.status_code
            if r.status_code in (401, 403):
                _warm_up(session, segment_id)
                attempts_q += 1
                r = _fetch_query(session, segment_id, vid, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
                meta["http_query"] = r.status_code
            meta["attempts_query"] = attempts_q
            if not r.ok:
                continue

            try:
                jq = r.json()
            except Exception:
                jq = json.loads(r.text)
            if jq.get("notFound") is True:
                continue

            rows = _build_flat_min(jq, be_id=str(segment_id), version=int(vid))
            meta["row_count"] = len(rows)
            meta["success"] = True
            meta["status"] = "OK" if rows else "OK(ZeroRows)"
            return rows, meta
        except Exception as e:
            meta["notes"] = f"{meta.get('notes','')}; q_try_v{vid}={type(e).__name__}: {e}"
            continue

    meta["status"] = "NotFoundAcrossVersions"
    return [], meta


# -------------------- Public API (HTTP with Selenium fallback) --------------------

def get_segment_rules_http(segment_ids, status_callback=None, progress_callback=None, headless=False, max_workers=None):
    start_time = time.time()
    performance_monitor = PerformanceMonitor()

    if status_callback:
        status_callback("Preparing HTTP session...")

    profile_path = get_firefox_profile()
    if not profile_path:
        if status_callback:
            status_callback("No Firefox profile found!")
        return None

    base_session = _build_http_session(profile_path)
    tz_offset = _current_tz_offset_hours()

    # Globaler Preflight + optional Selenium-Refresh
    if segment_ids:
        test_id = segment_ids[0]
        try:
            pre_v = _fetch_versions(base_session, test_id, tz_offset, timeout=(5, 15), max_attempts=2)
            if pre_v.status_code in (401, 403):
                if status_callback:
                    status_callback("Auth failed. Refreshing cookies via headless Firefox...")
                new_jar = _selenium_refresh_session_cookies(profile_path, headless=True, segment_id_for_referer=test_id)
                if new_jar:
                    base_session.cookies = new_jar
                else:
                    if status_callback:
                        status_callback("Could not refresh cookies via Selenium.")
        except Exception as e:
            if status_callback:
                status_callback(f"Preflight error: {str(e)}")

    all_rows = []
    meta_rows = []

    if max_workers is None:
        max_workers = min(4, len(segment_ids))  # defensiv starten

    if status_callback:
        status_callback(f"Fetching {len(segment_ids)} segments via HTTP (workers={max_workers})")

    t_batch_start = time.time()

    def make_worker_session() -> requests.Session:
        s = requests.Session()
        s.headers.update(base_session.headers.copy())
        jar = requests.cookies.RequestsCookieJar()
        for c in base_session.cookies:
            jar.set(c.name, c.value, domain=c.domain, path=c.path, secure=c.secure)
        s.cookies = jar
        return s

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for idx, seg_id in enumerate(segment_ids):
            sess = make_worker_session()
            fut = executor.submit(_fetch_one_segment_rules_flat, sess, seg_id, tz_offset, (5, 30), 4)
            futures[fut] = (idx, seg_id)

        for fut in as_completed(futures):
            idx, seg_id = futures[fut]
            t0 = time.time()
            try:
                rows, meta = fut.result()
            except Exception as e:
                rows, meta = [], {
                    "be_id": str(seg_id),
                    "success": False,
                    "status": "Exception",
                    "attempts_versions": 0,
                    "attempts_query": 0,
                    "http_latest": None,
                    "http_versions": None,
                    "http_query": None,
                    "version_source": None,
                    "published": None,
                    "version": None,
                    "row_count": 0,
                    "notes": f"{type(e).__name__}: {e}",
                }

            all_rows.extend(rows)
            meta_rows.append(meta)

            if status_callback:
                if meta["success"]:
                    status_callback(
                        f"[{seg_id}] {meta['status']}: {meta['row_count']} row(s), v={meta.get('version')} "
                        f"(src={meta.get('version_source')}, published={meta.get('published')}), "
                        f"httpL={meta.get('http_latest')}, httpV={meta.get('http_versions')}, httpQ={meta.get('http_query')}"
                    )
                else:
                    status_callback(
                        f"[{seg_id}] {meta['status']} "
                        f"(httpL={meta.get('http_latest')}, httpV={meta.get('http_versions')}, httpQ={meta.get('http_query')}) — {meta.get('notes','')}"
                    )

            if progress_callback:
                progress_callback(idx)

            performance_monitor.add_segment_result(
                f"{seg_id}#{idx}",
                time.time() - t0,
                (meta.get("attempts_versions", 0) + meta.get("attempts_query", 0)) or 1,
                bool(meta.get("success"))
            )

    performance_monitor.add_batch_time(len(segment_ids), time.time() - t_batch_start)

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

    # DataFrames & Excel
    df = pd.DataFrame(all_rows, columns=[
        "marketplace_id",
        "be_id",
        "version",
        "scope",
        "scope_operator",
        "rule_id",
        "parent_rule_id",
        "group_operator",
        "def_id",
        "constraint_key",
        "constraint_op",
        "constraint_value",
        "fetched_at",
    ])

    df_meta = pd.DataFrame(meta_rows, columns=[
        "be_id",
        "success",
        "status",
        "attempts_versions",
        "attempts_query",
        "http_latest",
        "http_versions",
        "http_query",
        "version_source",
        "published",
        "version",
        "row_count",
        "notes",
    ])

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    rules_filename = f"segment_rules_flat_{timestamp}.xlsx"
    meta_filename = f"segment_rules_meta_{timestamp}.xlsx"

    df.to_excel(rules_filename, index=False)
    df_meta.to_excel(meta_filename, index=False)

    return df, rules_filename


# Backward-/UI-kompatibler Alias
def extract_rules(segment_ids, status_callback=None, progress_callback=None, headless=False):
    return get_segment_rules_http(segment_ids, status_callback, progress_callback, headless)


if __name__ == "__main__":
    test_ids = ["1709631602", "1733947602"]
    print("Starting HTTP rules-flat test...")
    result = get_segment_rules_http(test_ids, print, lambda i: None, headless=True, max_workers=4)
    if result is not None:
        df, fname = result
        print("Saved:", fname)
        print(df.groupby("be_id").size())
