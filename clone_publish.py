# clone_publish.py
# Klont Bullseye-Segmente via HTTP-API und publiziert sie sofort.
# - Name kommt (bevorzugt) aus UI-Paaren [(be_id, desired_name), ...]
# - Fallback: source_name + " Clone", wenn nur be_ids übergeben werden
# - marketplaceId, queryString, basic, advancedOptions vom Source übernehmen
# - Owner (Objekt) aus Quelle übernehmen; ownerEmail = Profil-Alias
# - destination/destinations/notify/usageCategory/requesterLOB fix gemäß Vorgabe
# - parallele Verarbeitung mehrerer BE-IDs
# - schreibt zwei Excel-Dateien: Ergebnisse + Meta

import os
import json
import time
import random
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

import requests
import pandas as pd

# Optionaler Selenium-Fallback zum Auffrischen der Cookies
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FxService
from selenium.webdriver.firefox.options import Options as FxOptions

from utils import get_firefox_profile

def _resolve_profile_alias(explicit_alias: str | None = None) -> str | None:
    if explicit_alias and str(explicit_alias).strip():
        return str(explicit_alias).strip()
    for key in ("BULLSEYE_OWNER_ALIAS", "AMZN_ALIAS", "ALIAS"):
        val = os.environ.get(key)
        if val and val.strip():
            return val.strip()
    return None



# -------------------- Utils & Monitoring --------------------

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


# -------------------- HTTP & Cookies --------------------

BULLSEYE_DOMAIN = "bullseye2-eu.amazon.com"
LOAD_LATEST_VERSION_URL = f"https://{BULLSEYE_DOMAIN}/request/loadLatestQueryVersion"
LOAD_VERSIONS_URL = f"https://{BULLSEYE_DOMAIN}/request/loadSegmentVersions"
LOAD_QUERY_URL = f"https://{BULLSEYE_DOMAIN}/request/loadQuery"
CREATE_SEGMENT_URL = f"https://{BULLSEYE_DOMAIN}/request/createSegment"
LOAD_SEGMENT_URL = f"https://{BULLSEYE_DOMAIN}/request/loadSegment"


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


# -------------------- Selenium Cookie Refresh --------------------

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


# -------------------- Robust HTTP POST helper --------------------

def _post_json(session: requests.Session, url: str, payload: dict, extra_headers: dict,
               timeout=(5, 30), max_attempts=4, base_backoff=1.6):
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


# -------------------- Bullseye-specific fetches --------------------

def _warm_up(session: requests.Session, seg_id: str | int):
    url = f"https://{BULLSEYE_DOMAIN}/segment?id={seg_id}"
    try:
        session.get(url, headers={"Referer": url}, timeout=(5, 10))
        time.sleep(0.15 + random.random() * 0.15)
    except Exception:
        pass


def _fetch_latest_version(session: requests.Session, seg_id: str | int, tz_offset_hours: int,
                          timeout=(5, 30), max_attempts=4):
    payload = {"id": int(seg_id), "timeZoneOffset": int(tz_offset_hours)}
    headers = {"Referer": f"https://{BULLSEYE_DOMAIN}/segment?id={seg_id}"}
    return _post_json(session, LOAD_LATEST_VERSION_URL, payload, headers, timeout=timeout, max_attempts=max_attempts)


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



def _fetch_segment(session: requests.Session, seg_id: str | int, tz_offset_hours: int,
                   timeout=(5, 30), max_attempts=4):
    payload = {"id": int(seg_id), "timeZoneOffset": int(tz_offset_hours)}
    headers = {"Referer": f"https://{BULLSEYE_DOMAIN}/segment?id={seg_id}"}
    return _post_json(session, LOAD_SEGMENT_URL, payload, headers, timeout=timeout, max_attempts=max_attempts)



# -------------------- Build createSegment payload --------------------

def _build_referer(seg_id: str | int, version: int, destination: str, usage_category: str) -> str:
    return (f"https://{BULLSEYE_DOMAIN}/query"
            f"?id={seg_id}&version={version}&favorite=n&dst={destination}&usgCategory={usage_category}")

def _build_create_payload_from_source(qj: dict,
                                      seg_id: str | int,
                                      version: int,
                                      new_name: str,
                                      tz_offset_hours: int,
                                      destination: str = "e",
                                      usage_category: str = "OTHER",
                                      publish_now: bool = True,
                                      owner_email_override: str | None = None,
                                      owner_obj_override: dict | None = None):


    """
    Nimmt die loadQuery-Antwort (qj) und baut daraus das createSegment-Payload,
    mit den Fixwerten (destination/email/etc.) laut Vorgabe.
    """
    # Felder aus Source übernehmen (defensiv)
    basic = qj.get("basic", {}) or {}
    marketplace_id = basic.get("marketplaceId")

    query_string = qj.get("queryString")
    advanced = qj.get("advancedOptions", {}) or {}
    realtime = bool(qj.get("realtime", True))
    asap = bool(qj.get("asap", False))
    website = bool(qj.get("website", False))
    email_flag = bool(qj.get("email", True))


    # Hard-Fixes laut Anforderung / cURL
    notify_level = "NOTIFY_NONE"
    requester_lob = "STORES"
    destinations = ["EMAIL"]  # entspricht destination="e"
    secured = bool(qj.get("secured", False))
    confidential = bool(qj.get("confidential", False))

    # „publish“ sofort
    publish = bool(publish_now)

    # createSegment-Payload
    payload = {
        "type": qj.get("type") or {"upper": "BASIC", "lower": "basic", "name": "Basic", "ordinal": 1},
        "marketplaceId": marketplace_id,
        "advancedOptions": {
            "kindleAsins": bool(advanced.get("kindleAsins", False)),
            "includeVariables": bool(advanced.get("includeVariables", False)),
            "allowLargeSegment": bool(advanced.get("allowLargeSegment", False)),
            "auditEvents": bool(advanced.get("auditEvents", False)),
            "consumerQuery": bool(advanced.get("consumerQuery", True)),
        },
        "listeners": {"change": [None]},  # wie im cURL-Beispiel
        "notFound": False,
        "queryString": query_string,
        "realtime": bool(realtime),
        "asap": bool(asap),
        "website": bool(website),
        "email": bool(email_flag),
        "name": new_name,

        "secured": bool(secured),
        "notifyLevel": notify_level,
        "ccemails": [],
        "basic": basic,  # komplette Include/Exclude-Struktur übernehmen
        "canBeRequeued": False,
        # Quelle referenzieren (id/queryVersion) – wie beim Clone-Flow üblich
        "id": int(seg_id),
        "queryVersion": int(version),
        "usageCategory": usage_category,
        "alarms": qj.get("alarms", []),
        "asapUnsafe": bool(qj.get("asapUnsafe", False)),
        "confidential": bool(confidential),
        "source": "QUERY",
        "publish": publish,
        "destination": destination,         # "e"
        "isFavorite": False,
        "segmentVersionValidations": [],
        "destinations": destinations,       # ["EMAIL"]
        "requesterLOB": requester_lob,
        "timeZoneOffset": int(tz_offset_hours),
    }

    # Owner (Objekt) aus Quelle + ownerEmail = Profil-Alias gleichzeitig setzen
    if owner_email_override:
        payload["ownerEmail"] = owner_email_override   # Alias als "created by"
    if owner_obj_override:
        payload["owner"] = owner_obj_override          # Team/Owner der Quelle für "Owned by"


    return payload




# -------------------- Worker: clone one --------------------

def _clone_one_segment(session: requests.Session,
                       seg_id: str,
                       tz_offset_hours: int,
                       destination: str = "e",
                       usage_category: str = "OTHER",
                       publish_now: bool = True,
                       override_name: str | None = None,
                       timeout=(5, 30),
                       max_attempts=4,
                       owner_alias: str | None = None):


    """
    Gibt (row_dict, meta_dict) zurück.
    row_dict: pro erfolgreichem Clone ein Datensatz (u.a. new_be_id, name, market, owner...)
    meta_dict: HTTP/Versuchs-Infos zur Fehlersuche
    """
    meta = {
        "source_be_id": str(seg_id),
        "http_latest": None,
        "http_versions": None,
        "http_query": None,
        "http_create": None,
        "attempts_query": 0,
        "attempts_create": 0,
        "version": None,
        "published_flag": None,
        "status": "INIT",
        "success": False,
        "notes": "",
        "referer_used": None,
    }

    profile_alias = _resolve_profile_alias(owner_alias)  # z.B. "nwreth"
    meta["owner_email_used"] = profile_alias
    row = {
        "source_be_id": str(seg_id),
        "source_version": None,
        "source_name": None,
        "new_be_id": None,
        "new_name": None,
        "marketplace_id": None,
        "owner_email": None,
        "owner_name": None,
        "published": None,
        "destination": destination,
        "usage_category": usage_category,
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    # Warm up
    _warm_up(session, seg_id)

    # 1) Latest-Version holen
    try:
        resp_latest = _fetch_latest_version(session, seg_id, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
        meta["http_latest"] = resp_latest.status_code
        if resp_latest.status_code in (401, 403):
            _warm_up(session, seg_id)
            resp_latest = _fetch_latest_version(session, seg_id, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
            meta["http_latest"] = resp_latest.status_code
    except Exception as e:
        meta["status"] = "Error(Latest)"
        meta["notes"] = f"latest_err={type(e).__name__}: {e}"
        return row, meta

    latest_version = None
    latest_published = None
    if resp_latest.ok:
        try:
            lj = resp_latest.json()
        except Exception:
            lj = json.loads(resp_latest.text)
        latest_version = lj.get("version")
        latest_published = lj.get("published")

    if latest_version is None:
        # Fallback: Versionsliste – nehme neueste
        try:
            resp_v = _fetch_versions(session, seg_id, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
            meta["http_versions"] = resp_v.status_code
            if resp_v.status_code in (401, 403):
                _warm_up(session, seg_id)
                resp_v = _fetch_versions(session, seg_id, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
                meta["http_versions"] = resp_v.status_code
            if resp_v.ok:
                try:
                    jv = resp_v.json()
                except Exception:
                    jv = json.loads(resp_v.text)
                versions = (jv.get("versions") or [])
                if versions:
                    latest_version = max(versions, key=lambda v: int(v.get("version", -1))).get("version")
        except Exception as e:
            meta["status"] = "Error(VersionsFallback)"
            meta["notes"] = f"{meta.get('notes','')}; v_fallback_err={type(e).__name__}: {e}"
            return row, meta

    if latest_version is None:
        meta["status"] = "NoVersion"
        meta["notes"] = f"{meta.get('notes','')}; no version resolved"
        return row, meta

    row["source_version"] = int(latest_version)
    meta["version"] = int(latest_version)
    meta["published_flag"] = bool(latest_published) if latest_published is not None else None

    # 2) loadQuery der ermittelten Version ziehen (ggf. notFound abfangen)
    attempts_q = 0
    try:
        attempts_q += 1
        resp_q = _fetch_query(session, seg_id, latest_version, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
        meta["http_query"] = resp_q.status_code
        if resp_q.status_code in (401, 403):
            _warm_up(session, seg_id)
            attempts_q += 1
            resp_q = _fetch_query(session, seg_id, latest_version, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
            meta["http_query"] = resp_q.status_code
    except Exception as e:
        meta["status"] = "Error(Query)"
        meta["attempts_query"] = attempts_q or 1
        meta["notes"] = f"{meta.get('notes','')}; query_err={type(e).__name__}: {e}"
        return row, meta

    meta["attempts_query"] = attempts_q
    if not resp_q.ok:
        meta["status"] = f"QueryHTTP{resp_q.status_code}"
        return row, meta

    try:
        qj = resp_q.json()
    except Exception:
        qj = json.loads(resp_q.text)

    if qj.get("notFound") is True:
        meta["status"] = "QueryNotFound"
        return row, meta

    # 3) Namen aus Quelle oder Override (bestehender Code)
    source_name = qj.get("name") or f"BE_{seg_id}"
    chosen_name = (override_name.strip() if isinstance(override_name, str) and override_name.strip() else f"{source_name} Clone")
    row["source_name"] = source_name
    row["new_name"] = chosen_name
    row["marketplace_id"] = (qj.get("basic", {}) or {}).get("marketplaceId")

    # 3b) Zusatz: loadSegment (Quelle) → owner/createdBy sicher auslesen
    seg_owner_email = None
    seg_owner_obj = None
    seg_created_by = None
    try:
        resp_seg = _fetch_segment(session, seg_id, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
        # optional für Meta:
        # meta["http_load_segment_src"] = resp_seg.status_code
        if resp_seg.ok:
            try:
                sj = resp_seg.json()
            except Exception:
                sj = json.loads(resp_seg.text)
            seg = sj.get("segment") or {}
            seg_owner_email = seg.get("ownerEmail")
            seg_owner_obj = seg.get("owner")
            seg_created_by = seg.get("createdBy")
    except Exception as e:
        # wir brechen nicht ab – wir haben noch den Fallback aus qj
        pass

    # diese Infos in den Output aufnehmen (nur Reporting)
    row["source_owner_email"] = seg_owner_email
    row["source_owner_name"] = (seg_owner_obj or {}).get("name") if isinstance(seg_owner_obj, dict) else None
    row["source_created_by"] = seg_created_by

    # 4) createSegment Payload bauen
    referer = _build_referer(seg_id, int(latest_version), destination, usage_category)
    meta["referer_used"] = referer





    create_payload = _build_create_payload_from_source(
        qj=qj,
        seg_id=seg_id,
        version=int(latest_version),
        new_name=chosen_name,
        tz_offset_hours=tz_offset_hours,
        destination=destination,
        usage_category=usage_category,
        publish_now=True,
        owner_email_override= profile_alias,                 # <- HIER: Alias erzwingen
        owner_obj_override= seg_owner_obj or qj.get("owner") # <- HIER: TEAM aus Source
    )

    # 5) createSegment POSTen
    attempts_c = 0
    headers = {"Referer": referer}
    try:
        attempts_c += 1
        resp_c = _post_json(session, CREATE_SEGMENT_URL, create_payload, headers, timeout=timeout, max_attempts=max_attempts)
        meta["http_create"] = resp_c.status_code
        if resp_c.status_code in (401, 403):
            _warm_up(session, seg_id)
            attempts_c += 1
            resp_c = _post_json(session, CREATE_SEGMENT_URL, create_payload, headers, timeout=timeout, max_attempts=max_attempts)
            meta["http_create"] = resp_c.status_code
    except Exception as e:
        meta["status"] = "Error(Create)"
        meta["attempts_create"] = attempts_c or 1
        meta["notes"] = f"{meta.get('notes','')}; create_err={type(e).__name__}: {e}"
        return row, meta

    meta["attempts_create"] = attempts_c
    if not resp_c.ok:
        meta["status"] = f"CreateHTTP{resp_c.status_code}"
        return row, meta

    # 6) Ergebnis interpretieren
    try:
        cj = resp_c.json()
    except Exception:
        cj = json.loads(resp_c.text)

    new_id = cj.get("id") or cj.get("segmentId") or cj.get("newId")
    row["new_be_id"] = str(new_id) if new_id is not None else None
    row["published"] = True  # publish=True gesetzt
    meta["status"] = "OK" if new_id else "OK(UnknownId)"
    meta["success"] = new_id is not None

    # → Finalen Owner des neu erzeugten Segments fürs Reporting holen
    try:
        if new_id:
            resp_new = _fetch_segment(session, new_id, tz_offset_hours, timeout=timeout, max_attempts=max_attempts)
            if resp_new.ok:
                try:
                    sj_new = resp_new.json()
                except Exception:
                    sj_new = json.loads(resp_new.text)
                seg_new = (sj_new.get("segment") or {})
                row["owner_email"] = seg_new.get("ownerEmail")
                row["owner_name"]  = (seg_new.get("owner") or {}).get("name")
                # Hinweis, falls Backend doch noch den Source-Owner belässt
                if seg_owner_email and row["owner_email"] == seg_owner_email:
                    meta["notes"] = (meta.get("notes","") + "; owner matches source").strip("; ")
    except Exception:
        pass


    return row, meta


# -------------------- Public API --------------------

def clone_and_publish_segments(be_ids=None,
                               pairs=None,
                               status_callback=None,
                               progress_callback=None,
                               headless=True,
                               max_workers=None,
                               owner_alias: str | None = None):

    """
    Klont mehrere Segmente parallel.
    - Bevorzugt 'pairs' = [(be_id, desired_name), ...] → Name wird GENAU so verwendet.
    - Fallback: wenn nur 'be_ids' gegeben sind, wird new_name = source_name + ' Clone'.
    Rückgabe: DataFrame (Ergebnis), Dateiname
    """
    # Eingaben normalisieren
    id_name_pairs: list[tuple[str, str | None]] = []

    if pairs and isinstance(pairs, (list, tuple)):
        for be, nm in pairs:
            be_id = str(be).strip()
            desired = (str(nm).strip() if nm is not None else None)
            if be_id:
                id_name_pairs.append((be_id, desired))
    elif be_ids and isinstance(be_ids, (list, tuple)):
        for be in be_ids:
            be_id = str(be).strip()
            if be_id:
                id_name_pairs.append((be_id, None))

    if not id_name_pairs:
        if status_callback:
            status_callback("No BE IDs provided.")
        return None

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

    # Preflight/Auth
    try:
        test_id = id_name_pairs[0][0]
        r = _fetch_latest_version(base_session, test_id, tz_offset, timeout=(5, 15), max_attempts=2)
        if r.status_code in (401, 403):
            if status_callback:
                status_callback("Auth failed. Refreshing cookies via headless Firefox...")
            new_jar = _selenium_refresh_session_cookies(profile_path, headless=headless, segment_id_for_referer=test_id)
            if new_jar:
                base_session.cookies = new_jar
            else:
                if status_callback:
                    status_callback("Could not refresh cookies via Selenium.")
    except Exception as e:
        if status_callback:
            status_callback(f"Preflight error: {str(e)}")

    if max_workers is None:
        max_workers = min(4, max(1, len(id_name_pairs)))  # vorsichtig parallelisieren

    if status_callback:
        status_callback(f"Cloning {len(id_name_pairs)} segment(s) (workers={max_workers})")

    results = []
    meta_rows = []

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
        for idx, (seg_id, desired_name) in enumerate(id_name_pairs):
            sess = make_worker_session()
            fut = executor.submit(
                _clone_one_segment,
                sess, seg_id, tz_offset,
                "e", "OTHER", True, desired_name,  # destination, usage_category, publish_now, override_name
                (5, 30), 4,
                owner_alias    
            )
            futures[fut] = (idx, seg_id, desired_name)
        
        done_count = 0
        for fut in as_completed(futures):
            idx, seg_id, desired_name = futures[fut]
            t0 = time.time()
            try:
                row, meta = fut.result()
            except Exception as e:
                row = {
                    "source_be_id": str(seg_id),
                    "source_version": None,
                    "source_name": None,
                    "new_be_id": None,
                    "new_name": desired_name or None,
                    "marketplace_id": None,
                    "owner_email": None,
                    "published": None,
                    "destination": "e",
                    "usage_category": "OTHER",
                    "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                meta = {
                    "source_be_id": str(seg_id),
                    "http_latest": None,
                    "http_versions": None,
                    "http_query": None,
                    "http_create": None,
                    "attempts_query": 0,
                    "attempts_create": 0,
                    "version": None,
                    "published_flag": None,
                    "status": "Exception",
                    "success": False,
                    "notes": f"{type(e).__name__}: {e}",
                    "referer_used": None,
                }

            results.append(row)
            meta_rows.append(meta)

            if status_callback:
                if meta.get("success"):
                    status_callback(f"[{seg_id}] OK → new_id={row.get('new_be_id')} name='{row.get('new_name')}'")
                else:
                    status_callback(f"[{seg_id}] {meta.get('status')} — {meta.get('notes','')}")

            done_count += 1
            if progress_callback:
                progress_callback(done_count - 1)

            performance_monitor.add_segment_result(
                f"{seg_id}#{idx}",
                time.time() - t0,
                (meta.get("attempts_query", 0) + meta.get("attempts_create", 0)) or 1,
                bool(meta.get("success"))
            )

    performance_monitor.add_batch_time(len(id_name_pairs), time.time() - t_batch_start)

    stats = performance_monitor.get_statistics()
    total_time = time.time() - start_time
    if status_callback:
        status_callback("\nPerformance Statistics:")
        status_callback(f"Total processing time: {format_time(total_time)}")
        status_callback(f"Average batch time: {format_time(stats['average_batch_time'])}")
        status_callback(f"Success rate: {stats['average_success_rate']*100:.1f}%")
        status_callback(f"Failed segments: {stats['failed_segments']}")
        if len(id_name_pairs) > 0:
            status_callback(f"Average time per segment: {format_time(total_time/len(id_name_pairs))}")

    # DataFrames & Excel
    df = pd.DataFrame(results, columns=[
        "source_be_id",
        "source_version",
        "source_name",
        "new_be_id",
        "new_name",
        "marketplace_id",
        "owner_email",
        "owner_name",
        "published",
        "destination",
        "usage_category",
        "created_at",
    ])

    df_meta = pd.DataFrame(meta_rows, columns=[
        "source_be_id",
        "http_latest",
        "http_versions",
        "http_query",
        "http_create",
        "attempts_query",
        "attempts_create",
        "version",
        "published_flag",
        "status",
        "success",
        "notes",
        "referer_used",
        "owner_email_used"
    ])

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_main = f"clone_publish_results_{ts}.xlsx"
    out_meta = f"clone_publish_meta_{ts}.xlsx"

    df.to_excel(out_main, index=False)
    df_meta.to_excel(out_meta, index=False)

    return df, out_main

# --- NEU: Mass-Clone mit fester Base-ID (1749101702) ---
def mass_clone_fixed(names: list[str],
                     status_callback=None,
                     progress_callback=None,
                     headless: bool = True,
                     max_workers: int | None = None,
                     owner_alias: str | None = None):
    """
    Klont das feste Basis-Segment 1749101702 genau so oft,
    wie Namen in 'names' übergeben wurden (ein Name pro Clone).
    """
    BASE_BE_ID = "1749101702"

    if not isinstance(names, list) or len(names) == 0:
        if status_callback:
            status_callback("No names provided for mass clone.")
        return None

    # Paare (immer gleiche BE-ID, aber unterschiedliche Zielnamen)
    pairs = []
    for nm in names:
        nm_clean = (nm or "").strip()
        if not nm_clean:
            if status_callback:
                status_callback("Empty name encountered; aborting.")
            return None
        pairs.append((BASE_BE_ID, nm_clean))

    # Parallel & robust wie gehabt
    return clone_and_publish_segments(
        pairs=pairs,
        status_callback=status_callback,
        progress_callback=progress_callback,
        headless=headless,
        max_workers=(max_workers if max_workers is not None else min(6, max(1, len(pairs)))),
        owner_alias=owner_alias             
    )



# Backward-kompatible Funktion (falls UI noch diesen Namen importiert):
def clone_and_publish(be_ids=None, pairs=None, status_callback=None, progress_callback=None, headless=True, owner_alias: str | None = None):
    return clone_and_publish_segments(
        be_ids=be_ids,
        pairs=pairs,
        status_callback=status_callback,
        progress_callback=progress_callback,
        headless=headless,
        max_workers=None,
        owner_alias=owner_alias        
    )


if __name__ == "__main__":
    # Mini-Test (setzt gültige Firefox-Session voraus)
    test_pairs = [("1740996002", "My Custom Cloned Segment")]
    print("Starting clone & publish test...")
    result = clone_and_publish_segments(pairs=test_pairs, status_callback=print, progress_callback=lambda i: None, headless=True)
    if result is not None:
        df, fname = result
        print("Saved:", fname)
        print(df)



# ==== Cross-Marketplace Clone (UK/DE/FR/IT/ES) =================================

ORDERED_MP_IDS = [3, 4, 5, 35691, 44551]

# Mapping: Marketplace -> Code + Hygiene
MP_CODE_BY_ID = {
    3: "UK",
    4: "DE",
    5: "FR",
    35691: "IT",
    44551: "ES",
}
MP_ID_BY_CODE = {v: k for k, v in MP_CODE_BY_ID.items()}

HYGIENE_BY_MP = {
    3: 1266805602,   # UK
    4: 1266778402,   # DE
    5: 1266807602,   # FR
    35691: 1266817602,  # IT
    44551: 1266813602,  # ES
}
KNOWN_HYGIENE_IDS = set(HYGIENE_BY_MP.values())

DOMAIN_HINTS = {
    "UK": "amazon.co.uk",
    "DE": "amazon.de",
    "FR": "amazon.fr",
    "IT": "amazon.it",
    "ES": "amazon.es",
}

LANGUAGE_HINT_KEYS = ("languageCode", "locale", "lang")  # nur Hinweise (wir ändern nicht automatisch)


def _detect_source_marketplace(qj: dict) -> int | None:
    basic = (qj.get("basic") or {})
    mp = basic.get("marketplaceId")
    try:
        return int(mp) if mp is not None else None
    except Exception:
        return None


def _transform_name_for_market(source_name: str, src_code: str, dst_code: str) -> str:
    """
    Ersetzt den Ländercode im Namen. Wenn keiner gefunden wird, hänge ' dst_code' hinten an.
    Beispiele:
      'UK_SL_programs_X' -> 'DE_SL_programs_X'
      'Trends - UK'      -> 'Trends - DE'
      'SegmentName'      -> 'SegmentName DE' (Fallback)
    """
    if not source_name:
        return f"{dst_code}"

    # 1) Tokenbasiert: Start/Ende oder Separatoren (_ - Leerzeichen)
    token_pat = re.compile(rf'(^|[ _\-])({re.escape(src_code)})([ _\-]|$)')

    def repl(m: re.Match) -> str:
        left, _mid, right = m.group(1), m.group(2), m.group(3)
        return f"{left}{dst_code}{right}"

    new_name = token_pat.sub(repl, source_name)
    if new_name != source_name:
        return new_name

    # 2) Versuch über Wortgrenzen (für Fälle ohne die obigen Separatoren)
    word_pat = re.compile(rf'\b{re.escape(src_code)}\b')
    new_name2 = word_pat.sub(dst_code, source_name)
    if new_name2 != source_name:
        return new_name2

    # 3) Fallback: naive Ersetzung
    if src_code in source_name:
        return source_name.replace(src_code, dst_code)

    # 4) Letzter Fallback: einfach anhängen
    return f"{source_name} {dst_code}"



def _update_basic_marketplace(basic_obj: dict, src_mp: int, dst_mp: int) -> dict:
    """
    Setzt basic.marketplaceId und passt alle constraints mit defId=marketplaceId an.
    """
    import copy
    basic = copy.deepcopy(basic_obj or {})
    basic["marketplaceId"] = dst_mp

    def _fix_constraints(rule):
        cons = rule.get("constraints") or []
        for c in cons:
            if c.get("defId") == "marketplaceId":
                vals = c.get("values") or []
                new_vals = []
                for v in vals:
                    # Typ beibehalten (String bleibt String)
                    if isinstance(v, str):
                        new_vals.append(str(dst_mp))
                    else:
                        new_vals.append(dst_mp)
                c["values"] = new_vals

    # include
    inc = (basic.get("include") or {})
    for r in (inc.get("rules") or []):
        if r.get("subRules"):
            for sr in r["subRules"]:
                _fix_constraints(sr)
        _fix_constraints(r)

    # exclude
    exc = (basic.get("exclude") or {})
    for r in (exc.get("rules") or []):
        if r.get("subRules"):
            for sr in r["subRules"]:
                _fix_constraints(sr)
        _fix_constraints(r)

    return basic


# --- clone_publish.py ---

def _replace_hygiene_in_basic(basic_obj: dict, new_hygiene_id: int) -> tuple[dict, bool, list[int]]:
    import copy
    basic = copy.deepcopy(basic_obj or {})
    replaced = False
    old_ids = []

    def _maybe_replace(rule):
        nonlocal replaced, old_ids
        cons = rule.get("constraints") or []
        # ❌ vorher: nur wenn rule.defId == "segment"
        # ✅ wir prüfen direkt die Constraints – robust gegen unterschiedliche Rule-IDs
        for c in cons:
            defid = (c.get("defId") or "").lower()
            if defid in ("segment_id", "segmentid"):
                vals = c.get("values") or []
                if not vals:
                    continue
                if any(str(v).isdigit() and int(str(v)) in KNOWN_HYGIENE_IDS for v in vals):
                    old_ids.extend([int(str(v)) for v in vals if str(v).isdigit()])
                    c["values"] = [int(new_hygiene_id)]
                    replaced = True

    # include
    inc = (basic.get("include") or {})
    for r in (inc.get("rules") or []):
        _maybe_replace(r)
        for sr in (r.get("subRules") or []):
            _maybe_replace(sr)

    # exclude
    exc = (basic.get("exclude") or {})
    for r in (exc.get("rules") or []):
        _maybe_replace(r)
        for sr in (r.get("subRules") or []):
            _maybe_replace(sr)

    return basic, replaced, old_ids


def _replace_hygiene_in_querystring(
    qs: str, old_hygiene_ids: list[int] | list[str], target_hygiene: int | None
) -> tuple[str, int]:
    if not target_hygiene or not old_hygiene_ids:
        return qs, 0

    import re
    old_set = {str(x) for x in old_hygiene_ids}

    pat = re.compile(r'(segment\(\s*)(\d+)(\s*\))', re.IGNORECASE)

    replaced = 0
    def repl(m):
        nonlocal replaced
        pre, val, post = m.group(1), m.group(2), m.group(3)
        if val in old_set:
            replaced += 1
            return f"{pre}{target_hygiene}{post}"
        return m.group(0)

    return pat.sub(repl, qs), replaced

def _replace_marketplace_in_querystring(qs: str, src_mp: int, dst_mp: int) -> tuple[str, int]:
    import re
    # erlaubt: marketplaceId = 4, marketplaceId==4, marketplaceId = '4', marketplaceId=="4"
    pat = re.compile(r'(\bmarketplaceId\s*[=]{1,2}\s*)(["\']?)(\d+)(\2)')
    replaced = 0
    def repl(m):
        nonlocal replaced
        prefix, quote, old_val, _ = m.group(1), m.group(2), m.group(3), m.group(4)
        if old_val == str(src_mp):
            replaced += 1
            return f"{prefix}{quote}{dst_mp}{quote}"
        return m.group(0)
    return pat.sub(repl, qs), replaced


def _scan_notes_for_manual_checks(qs: str) -> list[str]:
    """
    Heuristiken, die manuell geprüft werden sollten (Domains, languageCode).
    Ohne Look-behinds.
    """
    notes = []
    # Domain/MP grobe Checks (nur Hinweise)
    if ".co.uk" in qs and ("marketplaceId = 4" in qs or "marketplaceId == 4" in qs):
        notes.append(".co.uk gefunden, aber MP=DE – URLs prüfen")
    if ".de" in qs and ("marketplaceId = 3" in qs or "marketplaceId == 3" in qs):
        notes.append(".de gefunden, aber MP=UK – URLs prüfen")
    if "languageCode" in qs:
        notes.append("languageCode im Query gefunden – manuell prüfen")
    return notes





def _clone_to_market_variation(session, base_seg_id, latest_version, base_qj, target_mp, tz_offset_hours, status_callback=None, source_owner_obj=None, base_name=None):


    """
    Baut aus der geladenen Query (base_qj) eine Ziel-Variante für target_mp
    und ruft createSegment. Gibt (row, meta) zurück – analog zum Schema aus _clone_one_segment.
    """
    import copy
    meta = {
        "source_be_id": str(base_seg_id),
        "target_market": target_mp,
        "http_create": None,
        "status": "INIT",
        "success": False,
        "notes": "",
        "referer_used": None,
    }
    row = {
        "source_be_id": str(base_seg_id),
        "source_version": int(latest_version),
        "source_name": base_qj.get("name"),
        "new_be_id": None,
        "new_name": None,
        "marketplace_id": target_mp,
        "owner_email": None,
        "owner_name": None,
        "published": None,
        "destination": "e",
        "usage_category": "OTHER",
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    src_mp = _detect_source_marketplace(base_qj)
    if src_mp is None:
        meta["status"] = "NoSourceMP"
        meta["notes"] = "Could not detect source marketplaceId in basic"
        return row, meta

    src_code = MP_CODE_BY_ID.get(src_mp, str(src_mp))
    dst_code = MP_CODE_BY_ID.get(target_mp, str(target_mp))

    # Namen anpassen
    new_name = _transform_name_for_market(base_qj.get("name") or f"BE_{base_seg_id}", src_code, dst_code)
    row["new_name"] = new_name

    # Kopie der Source-Query bauen und basic/queryString umbauen
    qj_mod = copy.deepcopy(base_qj)
    basic_src = qj_mod.get("basic") or {}
    qs_src = qj_mod.get("queryString") or ""

    # 1) basic.marketplaceId + alle marketplaceId-Constraints
    basic_new = _update_basic_marketplace(basic_src, src_mp, target_mp)

    # 2) Hygiene-Rule in basic ersetzen (nur bekannte Hygiene-IDs)
    target_hygiene = HYGIENE_BY_MP.get(target_mp)
    basic_new, replaced_basic_hyg, old_hyg_ids = _replace_hygiene_in_basic(basic_new, target_hygiene)

    # 3) marketplaceId + Hygiene in queryString spiegeln
    qs_new, count_mp = _replace_marketplace_in_querystring(qs_src, src_mp, target_mp)
    qs_new, count_hyg = _replace_hygiene_in_querystring(qs_new, old_hyg_ids, target_hygiene)

    qj_mod["basic"] = basic_new
    qj_mod["queryString"] = qs_new

    # Hinweise sammeln (Domains, languageCode)
    notes = _scan_notes_for_manual_checks(qs_new)
    if notes:
        meta["notes"] = "; ".join(notes)

    # createSegment Payload + POST
    referer = _build_referer(base_seg_id, int(latest_version), "e", "OTHER")
    meta["referer_used"] = referer

    profile_alias = _resolve_profile_alias()
    meta["owner_email_used"] = profile_alias



    create_payload = _build_create_payload_from_source(
        qj=qj_mod,
        seg_id=base_seg_id,
        version=int(latest_version),
        new_name=new_name,
        tz_offset_hours=tz_offset_hours,
        destination="e",
        usage_category="OTHER",
        publish_now=True,
        owner_email_override= profile_alias,                   
        owner_obj_override= source_owner_obj or base_qj.get("owner")          
    )

    headers = {"Referer": referer}
    try:
        resp_c = _post_json(session, CREATE_SEGMENT_URL, create_payload, headers, timeout=(5, 30), max_attempts=4)
        meta["http_create"] = resp_c.status_code
        if not resp_c.ok:
            meta["status"] = f"CreateHTTP{resp_c.status_code}"
            return row, meta
        try:
            cj = resp_c.json()
        except Exception:
            cj = json.loads(resp_c.text)
        new_id = cj.get("id") or cj.get("segmentId") or cj.get("newId")
        row["new_be_id"] = str(new_id) if new_id is not None else None
        row["published"] = True
        meta["status"] = "OK" if new_id else "OK(UnknownId)"
        meta["success"] = new_id is not None

        try:
           if new_id:
               resp_new = _fetch_segment(session, new_id, tz_offset_hours, timeout=(5, 30), max_attempts=4)
               if resp_new.ok:
                   try:
                       sj_new = resp_new.json()
                   except Exception:
                       sj_new = json.loads(resp_new.text)
                   seg_new = (sj_new.get("segment") or {})
                   row["owner_email"] = seg_new.get("ownerEmail")
                   row["owner_name"]  = (seg_new.get("owner") or {}).get("name")
        except Exception:
           pass




        if status_callback:
            status_callback(f"[{base_seg_id} → {dst_code}] OK new_id={row['new_be_id']}  (MP: {target_mp}, mp_repl={count_mp}, hyg_repl={count_hyg})")
        return row, meta
    except Exception as e:
        meta["status"] = "Error(Create)"
        meta["notes"] = f"{meta.get('notes','')}; create_err={type(e).__name__}: {e}"
        return row, meta


def clone_across_marketplaces(
        be_id: str | None = None,
        source_be_id: str | None = None,
        status_callback=None,
        progress_callback=None,
        headless: bool = True,
        max_workers: int | None = None,
):
    # beide Varianten unterstützen
    src_id = (source_be_id or be_id or "").strip()
    if not src_id:
        raise ValueError("source_be_id/be_id is required")

    start_time = time.time()

    if status_callback:
        status_callback(f"Preparing HTTP session for cross-market clones of {src_id}...")

    profile_path = get_firefox_profile()
    if not profile_path:
        if status_callback:
            status_callback("No Firefox profile found!")
        return None

    base_session = _build_http_session(profile_path)
    tz_offset = _current_tz_offset_hours()

    # Auth/Warm-up
    try:
        r = _fetch_latest_version(base_session, src_id, tz_offset, timeout=(5, 15), max_attempts=2)
        if r.status_code in (401, 403):
            if status_callback:
                status_callback("Auth failed. Refreshing cookies via headless Firefox...")
            new_jar = _selenium_refresh_session_cookies(profile_path, headless=headless, segment_id_for_referer=src_id)
            if new_jar:
                base_session.cookies = new_jar
            else:
                if status_callback:
                    status_callback("Could not refresh cookies via Selenium.")
    except Exception as e:
        if status_callback:
            status_callback(f"Preflight error: {str(e)}")

    _warm_up(base_session, src_id)

    # 1) Version bestimmen
    resp_latest = _fetch_latest_version(base_session, src_id, tz_offset, timeout=(5, 30), max_attempts=4)
    if not resp_latest.ok:
        if status_callback:
            status_callback(f"loadLatestQueryVersion failed: HTTP {resp_latest.status_code}")
        return None
    latest = resp_latest.json()
    latest_version = latest.get("version")
    if latest_version is None:
        if status_callback:
            status_callback("No latest version found.")
        return None


    # 2) loadQuery holen
    resp_q = _fetch_query(base_session, src_id, latest_version, tz_offset, timeout=(5, 30), max_attempts=4)
    if not resp_q.ok:
        if status_callback:
            status_callback(f"loadQuery failed: HTTP {resp_q.status_code}")
        return None
    qj = resp_q.json()
    if qj.get("notFound"):
        if status_callback:
            status_callback("Query not found in loadQuery.")
        return None

    # Namen + Owner aus loadSegment holen und im qj hinterlegen
    source_name = None
    seg_owner_email = None
    seg_owner_obj = None
    try:
       resp_seg = _fetch_segment(base_session, src_id, tz_offset, timeout=(5, 30), max_attempts=4)
       if resp_seg.ok:
            try:
               sj = resp_seg.json()
            except Exception:
                sj = json.loads(resp_seg.text)
            seg = (sj.get("segment") or {})
            source_name = seg.get("name")
            seg_owner_email = seg.get("ownerEmail")
            seg_owner_obj = seg.get("owner")
    except Exception:
            pass

    if source_name:
        qj["name"] = source_name



    src_mp = _detect_source_marketplace(qj)
    if src_mp is None:
        if status_callback:
            status_callback("Could not detect source marketplaceId in query.")
        return None

    target_markets = [mp for mp in ORDERED_MP_IDS if mp != src_mp]

    if max_workers is None:
        max_workers = min(5, max(2, len(target_markets)))

    if status_callback:
        src_code = MP_CODE_BY_ID.get(src_mp, str(src_mp))
        status_callback(
            f"Cloning {src_id} ({src_code}) to markets: "
            + ", ".join(MP_CODE_BY_ID[m] for m in target_markets)
            + f" (workers={max_workers})"
        )

    def make_worker_session() -> requests.Session:
        s = requests.Session()
        s.headers.update(base_session.headers.copy())
        jar = requests.cookies.RequestsCookieJar()
        for c in base_session.cookies:
            jar.set(c.name, c.value, domain=c.domain, path=c.path, secure=c.secure)
        s.cookies = jar
        return s

    results = []
    meta_rows = []
    t_batch_start = time.time()

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {}
        for idx, dst_mp in enumerate(target_markets):
            sess = make_worker_session()
            fut = ex.submit(
                _clone_to_market_variation,
                sess, src_id, int(latest_version), qj, int(dst_mp), tz_offset,
                status_callback=status_callback,
                source_owner_obj=seg_owner_obj,          # ← hier das ursprüngliche Team reingeben
                 base_name=source_name
            )
            futs[fut] = (idx, dst_mp)
        done_count = 0

        for fut in as_completed(futs):
            idx, dst_mp = futs[fut]
            try:
                row, meta = fut.result()
            except Exception as e:
                row = {
                    "source_be_id": str(src_id),
                    "source_version": int(latest_version),
                    "source_name": qj.get("name"),
                    "new_be_id": None,
                    "new_name": None,
                    "marketplace_id": int(dst_mp),
                    "owner_email": None,
                    "owner_name": None,
                    "published": None,
                    "destination": "e",
                    "usage_category": "OTHER",
                    "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                meta = {
                    "source_be_id": str(src_id),
                    "target_market": int(dst_mp),
                    "http_create": None,
                    "status": "Exception",
                    "success": False,
                    "notes": f"{type(e).__name__}: {e}",
                    "referer_used": None,
                }
            results.append(row)
            meta_rows.append(meta)
            done_count += 1
            if progress_callback:
                progress_callback(done_count - 1)

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_main = f"clone_cross_market_results_{src_id}_{ts}.xlsx"
    out_meta = f"clone_cross_market_meta_{src_id}_{ts}.xlsx"

    ORDER_INDEX = {mp: i for i, mp in enumerate(ORDERED_MP_IDS)}
    results.sort(key=lambda r: ORDER_INDEX.get(int(r.get("marketplace_id", 10**9)), 999))
    meta_rows.sort(key=lambda m: ORDER_INDEX.get(int(m.get("target_market", 10**9)), 999))


    pd.DataFrame(results, columns=[
        "source_be_id","source_version","source_name","new_be_id","new_name",
        "marketplace_id","owner_email","owner_name","published","destination","usage_category","created_at"
    ]).to_excel(out_main, index=False)

    pd.DataFrame(meta_rows, columns=[
        "source_be_id","target_market","http_create","status","success","notes","referer_used","owner_email_used"
    ]).to_excel(out_meta, index=False)

    if status_callback:
        total = time.time() - start_time
        status_callback(f"Done. Saved {out_main} and {out_meta} in {format_time(total)}.")

    return results, out_main

