# sonar_apply.py — HTTP-based implementation (no Selenium UI automation)
# - Loads BE segment metadata (currentVersion, marketplaceId) via Bullseye HTTP
# - Adds segment to Sonar campaign via HTTP
# - Waits for UNAPPROVED metrics to appear, then does PENDING → APPROVED
# - Verifies via metricsSummary (approved>0 & uploaded=0) and UNAPPROVED=0
#
# Compatible with the existing GUI:
#   apply_segments_to_sonar_pairs(pairs, status_callback, progress_callback, headless)
#
# Output: writes sonar_apply_results_<timestamp>.xlsx and returns a DataFrame

import os
import re
import time
import json
import math
import shutil
import sqlite3
import random
import tempfile
from typing import Any, Dict, List, Optional, Tuple
from datetime import timedelta, datetime, timezone

import requests
import pandas as pd

# Optional Selenium only to refresh cookies (NO UI clicking)
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FxService
from selenium.webdriver.firefox.options import Options as FxOptions

from utils import get_firefox_profile


# -------------------- Constants --------------------

SONAR_DOMAIN = "sonar-eu.amazon.com"
BULLSEYE_DOMAIN = "bullseye2-eu.amazon.com"
METRICS_DOMAIN = "sonar-service-eu-ca-dub.dub.proxy.amazon.com"




# Poll-Settings
METRICS_POLL_INTERVAL_SECONDS = 5
METRICS_POLL_MAX_ATTEMPTS_UPLOAD = 12      # ~60s
METRICS_POLL_MAX_ATTEMPTS_APPROVED = 12    # ~60s

BULLSEYE_LOAD_SEGMENT_URL = f"https://{BULLSEYE_DOMAIN}/request/loadSegment"

# Defaults for approval flow
WAIT_AFTER_UPLOAD_SECONDS_DEFAULT = 5.0          # kleine Wartezeit vor erster Metrics-Abfrage
PENDING_MAX_ATTEMPTS_DEFAULT = 4
APPROVED_MAX_ATTEMPTS_DEFAULT = 4
APPROVED_INITIAL_DELAY_SECONDS_DEFAULT = 2.0     # kurze Pause nach PENDING
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 30


def _load_requester_from_profile_only() -> str:
    """
    Liefert den Requester-Alias ausschließlich aus ~/.bullseye_automation/profile.json,
    bereinigt auf [A-Za-z0-9._-]. Fallback: "nwreth".
    Erwartetes JSON-Format: {"alias": "...", "email": "..."}
    """
    try:
        profile_path = os.path.join(os.path.expanduser("~"), ".bullseye_automation", "profile.json")
        if os.path.exists(profile_path):
            with open(profile_path, "r", encoding="utf-8") as f:
                prof = json.load(f)
            alias = (prof.get("alias") or "").strip()
            email = (prof.get("email") or "").strip()
            raw = alias or (email.split("@", 1)[0] if "@" in email else "")
            cleaned = re.sub(r"[^A-Za-z0-9._-]", "", raw)
            return cleaned or "nwreth"
    except Exception:
        pass
    return "nwreth"


REQUESTER_USERNAME_DEFAULT = _load_requester_from_profile_only()


# -------------------- Small utils --------------------

def _fmt(seconds: float) -> str:
    return str(timedelta(seconds=round(seconds)))

def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _timestamp_for_filename() -> str:
    return time.strftime("%Y%m%d_%H%M%S")

def _current_tz_offset_hours() -> int:
    now = datetime.now()
    utcnow = datetime.now(timezone.utc).replace(tzinfo=None)
    diff = now - utcnow
    return int(round(diff.total_seconds() / 3600.0))

def parse_campaign_id_from_url(url: str) -> str:
    m = re.search(r"/campaigns/(\d+)", str(url))
    return m.group(1) if m else ""

def parse_marketplace_id_from_sonar_url(url: str) -> Optional[int]:
    m = re.search(r"#/(\d+)/campaigns/\d+", str(url))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


# -------------------- Firefox cookies → requests.Session --------------------

def _copy_sqlite_readonly(src_path) -> Tuple[str, Optional[str]]:
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"cookies.sqlite not found at: {src_path}")
    tmpdir = tempfile.mkdtemp(prefix="ff_cookies_")
    dst = os.path.join(tmpdir, "cookies.sqlite")
    shutil.copy2(src_path, dst)
    return dst, tmpdir

def _load_firefox_cookies_for_domain(profile_path: str, domain_suffix: str) -> requests.cookies.RequestsCookieJar:
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

def _build_bullseye_session(profile_path: str) -> requests.Session:
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

def _build_sonar_session(profile_path: str) -> requests.Session:
    jar = _load_firefox_cookies_for_domain(profile_path, SONAR_DOMAIN)
    s = requests.Session()
    s.cookies = jar
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/json;charset=utf-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": f"https://{SONAR_DOMAIN}",
        "Connection": "keep-alive",
        "Referer": f"https://{SONAR_DOMAIN}/",
    })
    return s


# -------------------- Selenium cookie refresh (no UI ops) --------------------

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

def _selenium_refresh_session_cookies(profile_path: str, headless: bool = True, url_for_referer: Optional[str] = None):
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
        url = url_for_referer or f"https://{SONAR_DOMAIN}/"
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


# -------------------- HTTP helpers --------------------

def _safe_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        try:
            return json.loads(resp.text)
        except Exception:
            return None

def _rate_limit_sleep_if_any(resp: requests.Response) -> Optional[float]:
    if resp is not None and resp.status_code == 429:
        ra = resp.headers.get("Retry-After")
        if ra and str(ra).isdigit():
            return float(ra)
    return None

def _backoff_seconds(attempt: int, base: float = 1.6, jitter: float = 0.25) -> float:
    return (base ** attempt) + random.random() * jitter


# -------------------- Bullseye preflight --------------------

def _bullseye_preflight(
    session: requests.Session,
    be_id: str,
    tz_offset: int,
    timeout: Tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT),
    max_attempts: int = 4
) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    payload = {"id": int(be_id), "timeZoneOffset": int(tz_offset)}
    last_err = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.post(
                BULLSEYE_LOAD_SEGMENT_URL,
                data=json.dumps(payload),
                timeout=timeout,
                headers={"Referer": f"https://{BULLSEYE_DOMAIN}/segment?id={be_id}"}
            )

            if resp.status_code in (401, 403):
                return None, None, f"AuthFailed ({resp.status_code})"

            rl = _rate_limit_sleep_if_any(resp)
            if rl is not None and attempt < max_attempts:
                time.sleep(rl)
                continue

            if 500 <= resp.status_code < 600 and attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt))
                continue

            resp.raise_for_status()
            data = _safe_json(resp)
            if not isinstance(data, dict):
                return None, None, "Unexpected Bullseye response"

            current_version = None
            try:
                segment = data.get("segment") or {}
                current_version = segment.get("currentVersion")
                if isinstance(current_version, str) and current_version.isdigit():
                    current_version = int(current_version)
            except Exception:
                current_version = None

            marketplace_id = None
            try:
                qvi = data.get("queryVersionInfo") or {}
                qmeta = qvi.get("queryMetadata")
                if isinstance(qmeta, str):
                    try:
                        qmeta_obj = json.loads(qmeta)
                        marketplace_id = qmeta_obj.get("marketplaceId")
                    except Exception:
                        m = re.search(r'"marketplaceId"\s*:\s*(\d+)', qmeta)
                        if m:
                            marketplace_id = int(m.group(1))
                elif isinstance(qmeta, dict):
                    marketplace_id = qmeta.get("marketplaceId")

                if marketplace_id is None:
                    qobj = qvi.get("queryObject")
                    if isinstance(qobj, str):
                        try:
                            qobj_obj = json.loads(qobj)
                            marketplace_id = (
                                ((qobj_obj.get("basic") or {}).get("marketplaceId"))
                                or ((qobj_obj.get("advanced") or {}).get("marketplaceId"))
                                or ((qobj_obj.get("expert") or {}).get("marketplaceId"))
                            )
                        except Exception:
                            m2 = re.search(r'"marketplaceId"\s*:\s*(\d+)', qobj or "")
                            if m2:
                                marketplace_id = int(m2.group(1))
                    elif isinstance(qobj, dict):
                        marketplace_id = (
                            ((qobj.get("basic") or {}).get("marketplaceId"))
                            or ((qobj.get("advanced") or {}).get("marketplaceId"))
                            or ((qobj.get("expert") or {}).get("marketplaceId"))
                        )
                if isinstance(marketplace_id, str) and marketplace_id.isdigit():
                    marketplace_id = int(marketplace_id)
            except Exception:
                marketplace_id = None

            if current_version is None:
                return None, None, "Missing currentVersion in Bullseye response"
            if marketplace_id is None:
                return None, None, "Missing marketplaceId in Bullseye response"

            return current_version, marketplace_id, None

        except requests.Timeout as e:
            last_err = f"Timeout: {e}"
            if attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt))
                continue
            return None, None, last_err
        except requests.RequestException as e:
            last_err = f"HTTP error: {e}"
            if attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt))
                continue
            return None, None, last_err
        except Exception as e:
            return None, None, f"Error: {e}"

    return None, None, (last_err or "Unknown error")


# -------------------- Sonar endpoints --------------------

def _sonar_upload_segment(
    session: requests.Session,
    campaign_id: str,
    be_id: str,
    current_version: int,
    marketplace_id: int,
    wait_for_newest: bool = False,
    load_type: str = "ADD",
    timeout: Tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT),
    max_attempts: int = 4
) -> Tuple[bool, Optional[str]]:
    base = f"https://{SONAR_DOMAIN}/ajax/campaign/{campaign_id}/targeting/bullseyeSegments"
    params = {
        "bullseyeCurrentVersion": str(int(current_version)),
        "bullseyeSegmentId": str(int(be_id)),
        "bullseyeWaitForNewestVersion": "true" if wait_for_newest else "false",
        "marketplaceId": str(int(marketplace_id)),
        "targetingLoadType": load_type
    }

    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.post(base, params=params, data=b"", timeout=timeout)
            if resp.status_code in (401, 403):
                return False, f"AuthFailed ({resp.status_code})"

            rl = _rate_limit_sleep_if_any(resp)
            if rl is not None and attempt < max_attempts:
                time.sleep(rl)
                continue

            if 500 <= resp.status_code < 600 and attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt))
                continue

            if resp.status_code in (409, 400) and attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt))
                continue

            resp.raise_for_status()
            return True, None

        except requests.Timeout as e:
            last_err = f"Timeout: {e}"
            if attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt))
                continue
            return False, last_err
        except requests.RequestException as e:
            last_err = f"HTTP error: {e}"
            if attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt))
                continue
            return False, last_err
        except Exception as e:
            return False, f"Error: {e}"

    return False, (last_err or "Unknown upload error")

def _sonar_get_approval_status(
    session: requests.Session,
    campaign_id: str,
    marketplace_id: int,
    timeout: Tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT)
) -> Tuple[bool, Optional[str], str]:
    body_snippet = ""

    def _try(url: str) -> Tuple[bool, Optional[str], str]:
        nonlocal body_snippet
        try:
            resp = session.get(url, params={"marketplaceId": str(int(marketplace_id))}, timeout=timeout)
            body_snippet = (resp.text or "")[:500]
            if not resp.ok:
                return False, None, body_snippet
            data = _safe_json(resp)
            status_val = None
            if isinstance(data, dict):
                for key in ("status", "approvalStatus", "recipientApprovalStatus"):
                    if key in data and isinstance(data[key], str):
                        status_val = data[key].upper()
                        break
                if not status_val:
                    for v in data.values():
                        if isinstance(v, dict):
                            for key in ("status", "approvalStatus"):
                                if key in v and isinstance(v[key], str):
                                    status_val = v[key].upper()
                                    break
                        if status_val:
                            break
            if not status_val:
                up = body_snippet.upper()
                for token in ("APPROVED", "PENDING", "REQUEST", "REQUESTED"):
                    if token in up:
                        status_val = token
                        break
            return True, status_val, body_snippet
        except Exception:
            return False, None, body_snippet

    base = f"https://{SONAR_DOMAIN}/ajax/campaign/{campaign_id}/approvalRequest"
    ok, st, snip = _try(base)
    if ok:
        return ok, st, snip

    base_fallback = f"https://{SONAR_DOMAIN}/ajax/campaign/{campaign_id}/recipients/approvalRequest"
    return _try(base_fallback)

def _sonar_request_approval(
    session: requests.Session,
    campaign_id: str,
    marketplace_id: int,
    timeout: Tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT),
    max_attempts: int = PENDING_MAX_ATTEMPTS_DEFAULT
) -> Tuple[bool, Optional[str], str]:
    last_err = None
    body_snippet = ""

    def _put(url: str) -> requests.Response:
        return session.put(url, params={"marketplaceId": str(int(marketplace_id)), "status": "PENDING"},
                           data=b"", timeout=timeout)

    for attempt in range(1, max_attempts + 1):
        try:
            for path in (
                f"https://{SONAR_DOMAIN}/ajax/campaign/{campaign_id}/approvalRequest",
                f"https://{SONAR_DOMAIN}/ajax/campaign/{campaign_id}/recipients/approvalRequest",
            ):
                resp = _put(path)
                body_snippet = (resp.text or "")[:400]

                if resp.status_code in (401, 403):
                    return False, f"AuthFailed ({resp.status_code})", body_snippet

                rl = _rate_limit_sleep_if_any(resp)
                if rl is not None:
                    time.sleep(rl); continue

                if resp.status_code in (409, 404, 400):
                    last_err = f"{resp.status_code}"
                    continue

                if 500 <= resp.status_code < 600:
                    last_err = f"{resp.status_code}"
                    continue

                resp.raise_for_status()
                return True, None, body_snippet

            if attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt)); continue
            return False, f"Approval (PENDING) failed: {last_err or 'unknown'}", body_snippet

        except requests.Timeout as e:
            last_err = f"Timeout: {e}"
            if attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt)); continue
            return False, last_err, body_snippet
        except requests.RequestException as e:
            last_err = f"HTTP error: {e}"
            if attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt)); continue
            return False, last_err, body_snippet
        except Exception as e:
            return False, f"Error: {e}", body_snippet

def _sonar_approve_request(
    session: requests.Session,
    campaign_id: str,
    marketplace_id: int,
    timeout: Tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT),
    max_attempts: int = APPROVED_MAX_ATTEMPTS_DEFAULT
) -> Tuple[bool, Optional[str], str]:
    last_err = None
    body_snippet = ""

    def _put(url: str) -> requests.Response:
        return session.put(url, params={"marketplaceId": str(int(marketplace_id)), "status": "APPROVED"},
                           data=b"", timeout=timeout)

    for attempt in range(1, max_attempts + 1):
        try:
            for path in (
                f"https://{SONAR_DOMAIN}/ajax/campaign/{campaign_id}/approvalRequest",
                f"https://{SONAR_DOMAIN}/ajax/campaign/{campaign_id}/recipients/approvalRequest",
            ):
                resp = _put(path)
                body_snippet = (resp.text or "")[:400]

                if resp.status_code in (401, 403):
                    return False, f"AuthFailed ({resp.status_code})", body_snippet

                rl = _rate_limit_sleep_if_any(resp)
                if rl is not None:
                    time.sleep(rl); continue

                if resp.status_code in (409, 404, 400):
                    last_err = f"{resp.status_code}"
                    continue

                if 500 <= resp.status_code < 600:
                    last_err = f"{resp.status_code}"
                    continue

                resp.raise_for_status()
                return True, None, body_snippet

            if attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt)); continue
            return False, f"Approval (APPROVED) failed: {last_err or 'unknown'}", body_snippet

        except requests.Timeout as e:
            last_err = f"Timeout: {e}"
            if attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt)); continue
            return False, last_err, body_snippet
        except requests.RequestException as e:
            last_err = f"HTTP error: {e}"
            if attempt < max_attempts:
                time.sleep(_backoff_seconds(attempt)); continue
            return False, last_err, body_snippet
        except Exception as e:
            return False, f"Error: {e}", body_snippet


# -------------------- Metrics --------------------

def _sonar_get_metrics_summary(
    session: requests.Session,
    campaign_id: str,
    marketplace_id: int,
    requester: str = REQUESTER_USERNAME_DEFAULT,
    timeout: Tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT)
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """
    APPROVED-Seite:
      GET https://{METRICS_DOMAIN}/campaigns/{id}/metricsSummary?marketplaceId=<mp>&requester=<user>
      → { approvedRecipientsCount, uploadedRecipientsCount, ... }
    """
    url = f"https://{METRICS_DOMAIN}/campaigns/{campaign_id}/metricsSummary"
    params = {"marketplaceId": str(int(marketplace_id)), "requester": requester}
    headers = {
        "Origin": f"https://{SONAR_DOMAIN}",
        "Referer": f"https://{SONAR_DOMAIN}/",
        "Accept": "*/*",
    }
    body_snippet = ""
    try:
        resp = session.get(url, params=params, headers=headers, timeout=timeout)
        body_snippet = f"[{resp.status_code}] {(resp.text or '')[:600]}"
        if not resp.ok:
            return False, None, body_snippet
        data = _safe_json(resp)
        if not isinstance(data, dict):
            return False, None, body_snippet
        return True, data, body_snippet
    except Exception:
        return False, None, body_snippet

def _sonar_get_unapproved_metrics(
    session: requests.Session,
    campaign_id: str,
    timeout: Tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT)
) -> Tuple[bool, Dict[str, float], str]:
    """
    UNAPPROVED-Seite (ohne marketplaceId/requester):
      GET https://{METRICS_DOMAIN}/campaigns/{id}/recipientMetricsSummary
        ?recipientMetricTypeList[]=UNAPPROVED_RECIPIENTS_SUBMITTED
        &recipientMetricTypeList[]=UNAPPROVED_RECIPIENTS_SUCCESS
        &recipientMetricTypeList[]=RECIPIENTS_SUBMITTED
        &recipientMetricTypeList[]=RECIPIENTS_SUCCESS
    """
    url = f"https://{METRICS_DOMAIN}/campaigns/{campaign_id}/recipientMetricsSummary"
    params = [
        ("recipientMetricTypeList[]", "UNAPPROVED_RECIPIENTS_SUBMITTED"),
        ("recipientMetricTypeList[]", "UNAPPROVED_RECIPIENTS_SUCCESS"),
        ("recipientMetricTypeList[]", "RECIPIENTS_SUBMITTED"),
        ("recipientMetricTypeList[]", "RECIPIENTS_SUCCESS"),
    ]
    headers = {
        "Origin": f"https://{SONAR_DOMAIN}",
        "Referer": f"https://{SONAR_DOMAIN}/",
        "Accept": "application/json, text/plain, */*",
    }
    body_snippet = ""
    out = {"UNAPPROVED_RECIPIENTS_SUBMITTED": 0.0,
           "UNAPPROVED_RECIPIENTS_SUCCESS": 0.0,
           "RECIPIENTS_SUBMITTED": 0.0,
           "RECIPIENTS_SUCCESS": 0.0}
    try:
        resp = session.get(url, params=params, headers=headers, timeout=timeout)
        body_snippet = (resp.text or "")[:800]
        if not resp.ok:
            return False, out, body_snippet
        data = _safe_json(resp)
        if isinstance(data, dict):
            arr = data.get("campaignRecipientMetrics") or []
            for item in arr:
                t = (item or {}).get("type")
                v = (item or {}).get("value")
                try:
                    out[str(t)] = float(v)
                except Exception:
                    pass
        return True, out, body_snippet
    except Exception:
        return False, out, body_snippet


# -------------------- Core one-pair flow --------------------

def _apply_one_pair_http(
    sessions: Tuple[requests.Session, requests.Session],
    be_id_raw: str,
    sonar_url_raw: str,
    status_callback=None
) -> Dict[str, Any]:
    """
    Neuer Ablauf:
      - Bullseye-Preflight (Version + evtl. MP)
      - Upload
      - WARTE bis UNAPPROVED_RECIPIENTS_SUBMITTED > 0
      - PUT PENDING → kurze Pause → PUT APPROVED
      - WARTE bis approvedRecipientsCount > 0 und uploadedRecipientsCount == 0
        UND UNAPPROVED-Counts == 0
    """
    be_digits = re.findall(r"\d{10}", str(be_id_raw))
    be_id = be_digits[0] if be_digits else ""
    sonar_url = str(sonar_url_raw).strip()
    camp_id = parse_campaign_id_from_url(sonar_url)

    row = {
        "BE ID": be_id or str(be_id_raw),
        "Sonar Campaign ID": camp_id or "",
        "Sonar Campaign URL": sonar_url,
        "Uploading": False,
        "Approved": False,
        "ApprovedRequest": False,
        "Status": "Failed: invalid inputs",
        "Timestamp": _now_ts()
    }
    if not be_id or not (sonar_url.startswith("http") and camp_id):
        return row

    bullseye_session, sonar_session = sessions
    tz_offset = _current_tz_offset_hours()

    # 1) Preflight
    if status_callback:
        status_callback(f"Preflight Bullseye for BE {be_id} …")
    cur_ver, mp_from_be, err = _bullseye_preflight(bullseye_session, be_id, tz_offset)
    if err or cur_ver is None:
        row["Status"] = f"Failed: Bullseye preflight error: {err or 'unknown'}"
        return row

    mp_from_url = parse_marketplace_id_from_sonar_url(sonar_url)
    mp_effective = mp_from_url if isinstance(mp_from_url, int) else mp_from_be
    if mp_effective is None:
        row["Status"] = "Failed: Could not determine marketplaceId"
        return row
    if status_callback:
        status_callback(f"Using marketplaceId={mp_effective} (source: {'SonarURL' if mp_from_url is not None else 'Bullseye'})")

    # 2) Upload
    if status_callback:
        status_callback(f"Uploading BE {be_id} v{cur_ver} to campaign {camp_id} (MP {mp_effective}) …")
    ok_up, err_up = _sonar_upload_segment(sonar_session, camp_id, be_id, cur_ver, mp_effective)
    if not ok_up:
        row["Status"] = f"Failed: Upload error: {err_up or 'unknown'}"
        return row

    # 2a) kurz warten, dann UNAPPROVED prüfen
    time.sleep(WAIT_AFTER_UPLOAD_SECONDS_DEFAULT)
    if status_callback:
        status_callback("Waiting for UNAPPROVED metrics …")
    uploaded_seen = False
    last_unap = None
    for _ in range(METRICS_POLL_MAX_ATTEMPTS_UPLOAD):
        ok_u, met_u, _ = _sonar_get_unapproved_metrics(sonar_session, camp_id)
        last_unap = met_u
        if ok_u:
            if float(met_u.get("UNAPPROVED_RECIPIENTS_SUBMITTED", 0.0)) > 0.0:
                uploaded_seen = True
                break
        time.sleep(METRICS_POLL_INTERVAL_SECONDS)

    if not uploaded_seen:
        row["Status"] = f"Failed: recipients not visible in UNAPPROVED metrics after upload (last={last_unap})"
        return row

    # 3) PENDING
    if status_callback:
        status_callback("Requesting approval (PENDING) …")
    ok_p, errp, body_p = _sonar_request_approval(sonar_session, camp_id, mp_effective)
    if status_callback:
        status_callback(f"PENDING resp: {('OK' if ok_p else 'ERR')} | {(body_p or '')[:120]}")
    if not ok_p:
        row["Status"] = f"Failed: Approval (PENDING) error: {errp or 'unknown'}"
        return row

    # optional: Status anzeigen
    ok_s, st, _ = _sonar_get_approval_status(sonar_session, camp_id, mp_effective)
    if status_callback and ok_s:
        status_callback(f"approvalRequest now reports: {st}")

    row["Uploading"] = True
    row["Approved"] = True

    # 4) kleine Pause vor APPROVED
    time.sleep(APPROVED_INITIAL_DELAY_SECONDS_DEFAULT)

    # 5) APPROVED
    if status_callback:
        status_callback("Approving request (APPROVED) …")
    ok_a, erra, body_a = _sonar_approve_request(sonar_session, camp_id, mp_effective)
    if status_callback:
        status_callback(f"APPROVED resp: {('OK' if ok_a else 'ERR')} | {(body_a or '')[:120]}")
    if not ok_a:
        row["Status"] = f"Failed: Approval (APPROVED) error: {erra or 'unknown'}"
        return row

        # 6) Verifikation: einzig maßgeblich = approvedRecipientsCount > 0
    if status_callback:
        status_callback("Verifying approval via metrics (approvedRecipientsCount > 0) …")
    approved_done = False
    last_metrics2 = None
    for _ in range(METRICS_POLL_MAX_ATTEMPTS_APPROVED):
        ok_m, data_m, _ = _sonar_get_metrics_summary(sonar_session, camp_id, mp_effective)
        if ok_m and isinstance(data_m, dict):
            last_metrics2 = data_m
            apc = int(float(data_m.get("approvedRecipientsCount") or 0))
            if apc > 0:
                approved_done = True
                break
        time.sleep(METRICS_POLL_INTERVAL_SECONDS)

    row["ApprovedRequest"] = approved_done
    row["Status"] = "Success" if approved_done else f"Failed: approvedRecipientsCount did not become > 0 (last={last_metrics2})"
    return row


# -------------------- Core helpers for new 2-phase orchestration --------------------
def _approve_after_upload(
    sonar_session: requests.Session,
    campaign_id: str,
    marketplace_id: int,
    status_callback=None
) -> Tuple[bool, bool, bool, str]:
    """
    Gibt (pending_ok, approved_put_ok, metrics_ok, status_text) zurück.
    """
    pending_ok = False
    approved_put_ok = False
    metrics_ok = False

    # PENDING
    if status_callback:
        status_callback(f"[{campaign_id}] Requesting approval (PENDING)…")
    ok_p, errp, body_p = _sonar_request_approval(sonar_session, campaign_id, marketplace_id)
    pending_ok = bool(ok_p)
    if not ok_p:
        return pending_ok, approved_put_ok, metrics_ok, f"Pending-PUT failed: {errp or ''} | {(body_p or '')[:120]}"

    # kurze Pause vor APPROVED
    time.sleep(APPROVED_INITIAL_DELAY_SECONDS_DEFAULT)

    # APPROVED
    if status_callback:
        status_callback(f"[{campaign_id}] Approving (APPROVED)…")
    ok_a, erra, body_a = _sonar_approve_request(sonar_session, campaign_id, marketplace_id)
    approved_put_ok = bool(ok_a)
    if not ok_a:
        return pending_ok, approved_put_ok, metrics_ok, f"Approved-PUT failed: {erra or ''} | {(body_a or '')[:120]}"

    # Verifikation (metricsSummary)
    last_text = None
    for _ in range(METRICS_POLL_MAX_ATTEMPTS_APPROVED):
        ok_m, data_m, raw = _sonar_get_metrics_summary(sonar_session, campaign_id, marketplace_id)
        last_text = raw if raw else str(data_m)
        if ok_m and isinstance(data_m, dict):
            apc = int(float(data_m.get("approvedRecipientsCount") or 0))
            upl = int(float(data_m.get("uploadedRecipientsCount") or 0))
            if apc > 0:
                metrics_ok = True
                break
        time.sleep(METRICS_POLL_INTERVAL_SECONDS)

    return pending_ok, approved_put_ok, metrics_ok, ("Success" if metrics_ok else f"Metrics not ready (last={last_text!r})")






# -------------------- Public API (used by GUI) --------------------

def apply_segments_to_sonar_pairs(
    pairs: List[Tuple[str, str]],
    status_callback=None,
    progress_callback=None,
    headless: bool = False
) -> Optional[pd.DataFrame]:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    MAX_WORKERS = 5

    t_all_start = time.time()
    if status_callback:
        status_callback("Preparing HTTP sessions (Bullseye & Sonar)…")

    profile_path = get_firefox_profile()
    if not profile_path:
        if status_callback:
            status_callback("No Firefox profile found!")
        return None

    bullseye_sess = _build_bullseye_session(profile_path)
    sonar_sess = _build_sonar_session(profile_path)

    # Merge metrics-domain cookies into sonar-session (falls getrennte Domain-Cookies nötig sind)
    try:
        extra = _load_firefox_cookies_for_domain(profile_path, METRICS_DOMAIN)
        for c in extra:
            sonar_sess.cookies.set_cookie(c)
    except Exception:
        pass

    # ----- Optional: Sonar auth probe -----
    sonar_probe = next((u for _, u in pairs if str(u).startswith("http")), None)
    if sonar_probe:
        try:
            r = sonar_sess.get(sonar_probe, timeout=(CONNECT_TIMEOUT, 15))
            if r.status_code in (401, 403) and status_callback:
                status_callback("Auth failed for Sonar. Refreshing cookies via headless Firefox…")
                new_jar = _selenium_refresh_session_cookies(profile_path, headless=True, url_for_referer=sonar_probe)
                if new_jar:
                    sonar_sess.cookies = new_jar
        except Exception:
            pass

    # ----- Optional: Bullseye auth probe -----
    be_probe = None
    for be_raw, _ in pairs:
        m = re.findall(r"\d{10}", str(be_raw))
        if m:
            be_probe = m[0]; break
    if be_probe:
        try:
            r = bullseye_sess.post(
                BULLSEYE_LOAD_SEGMENT_URL,
                data=json.dumps({"id": int(be_probe), "timeZoneOffset": _current_tz_offset_hours()}),
                timeout=(CONNECT_TIMEOUT, 15),
                headers={"Referer": f"https://{BULLSEYE_DOMAIN}/segment?id={be_probe}"}
            )
            if r.status_code in (401, 403) and status_callback:
                status_callback("Auth failed for Bullseye. Refreshing cookies via headless Firefox…")
                new_jar_be = _selenium_refresh_session_cookies(
                    profile_path, headless=True,
                    url_for_referer=f"https://{BULLSEYE_DOMAIN}/segment?id={be_probe}"
                )
                if new_jar_be:
                    bullseye_sess.cookies = new_jar_be
        except Exception:
            pass

    # ------ ab hier nur Parallelisierung, keine Logikänderungen ------

    def _clone_session(src: requests.Session) -> requests.Session:
        """Headers + Cookies 1:1 in eine neue Session kopieren (thread-safe Nutzung)."""
        s = requests.Session()
        s.headers.update(src.headers.copy())
        jar = requests.cookies.RequestsCookieJar()
        for c in src.cookies:
            jar.set(c.name, c.value, domain=c.domain, path=c.path, secure=getattr(c, "secure", False))
        s.cookies = jar
        return s

    # ===== Neue 2-Phasen-Orchestrierung: =====
    # Phase 1: Preflight + Upload für alle (parallel, aber OHNE Metrics-Wartezeit)
    # Phase 1b: Sentinel-Wait auf UNAPPROVED beim ersten erfolgreichen Upload
    # Phase 2: PENDING→APPROVED für alle (parallel) + Verifikation

    class _Ctx:
        __slots__ = ("idx","be_id","sonar_url","camp_id","mp_id","cur_ver","ok_upload","row","err")
        def __init__(self, idx, be_id, sonar_url, camp_id):
            self.idx = idx
            self.be_id = be_id
            self.sonar_url = sonar_url
            self.camp_id = camp_id
            self.mp_id = None
            self.cur_ver = None
            self.ok_upload = False
            self.row = {
                "BE ID": be_id,
                "Sonar Campaign ID": camp_id or "",
                "Sonar Campaign URL": sonar_url,
                "Uploading": False,
                "Approved": False,
                "ApprovedRequest": False,
                "MetricsApproved": False, 
                "Status": "Pending",
                "Timestamp": _now_ts()
            }
            self.err = None

    ctxs: List[_Ctx] = []
    for i, (be_raw, sonar_raw) in enumerate(pairs):
        be_digits = re.findall(r"\d{10}", str(be_raw))
        be_id = be_digits[0] if be_digits else ""
        camp_id = parse_campaign_id_from_url(str(sonar_raw).strip())
        ctxs.append(_Ctx(i, be_id, str(sonar_raw).strip(), camp_id))

    if status_callback:
        status_callback(f"Phase 1: Uploading all segments for {len(ctxs)} pair(s) in parallel…")

    def _phase1_worker(ctx: _Ctx) -> _Ctx:
        be_sess_local = _clone_session(bullseye_sess)
        sonar_sess_local = _clone_session(sonar_sess)
        if not ctx.be_id or not (ctx.sonar_url.startswith("http") and ctx.camp_id):
            ctx.err = "Invalid inputs"
            ctx.row["Status"] = "Failed: invalid inputs"
            return ctx
        # Preflight
        tz_offset = _current_tz_offset_hours()
        cur_ver, mp_from_be, err = _bullseye_preflight(be_sess_local, ctx.be_id, tz_offset)
        if err or cur_ver is None:
            ctx.err = f"Bullseye preflight error: {err or 'unknown'}"
            ctx.row["Status"] = f"Failed: {ctx.err}"
            return ctx
        mp_from_url = parse_marketplace_id_from_sonar_url(ctx.sonar_url)
        mp_effective = mp_from_url if isinstance(mp_from_url, int) else mp_from_be
        if mp_effective is None:
            ctx.err = "Could not determine marketplaceId"
            ctx.row["Status"] = f"Failed: {ctx.err}"
            return ctx
        ctx.cur_ver = int(cur_ver)
        ctx.mp_id = int(mp_effective)
        # Upload
        if status_callback:
            status_callback(f"[{ctx.camp_id}] Upload BE {ctx.be_id} v{ctx.cur_ver} (MP {ctx.mp_id})…")
        ok_up, err_up = _sonar_upload_segment(sonar_sess_local, ctx.camp_id, ctx.be_id, ctx.cur_ver, ctx.mp_id)
        if not ok_up:
            ctx.err = f"Upload error: {err_up or 'unknown'}"
            ctx.row["Status"] = f"Failed: {ctx.err}"
            return ctx
        ctx.ok_upload = True
        ctx.row["Uploading"] = True
        ctx.row["Status"] = "Uploaded"
        return ctx

    from concurrent.futures import ThreadPoolExecutor, as_completed
    uploaded_ctxs: List[_Ctx] = [None]*len(ctxs)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_phase1_worker, c): c.idx for c in ctxs}
        for fut in as_completed(futs):
            idx = futs[fut]
            uploaded_ctxs[idx] = fut.result()

    # Phase 1b: Sentinel-Wait (erstes erfolgreiches Upload-Context)
    sentinel = next((c for c in uploaded_ctxs if c and c.ok_upload), None)
    if sentinel and status_callback:
        status_callback(f"Phase 1b: Waiting on UNAPPROVED metrics for sentinel campaign {sentinel.camp_id}…")
    if sentinel:
        # kleine Wartezeit wie zuvor
        time.sleep(WAIT_AFTER_UPLOAD_SECONDS_DEFAULT)
        uploaded_seen = False
        last_unap = None
        for _ in range(METRICS_POLL_MAX_ATTEMPTS_UPLOAD):
            ok_u, met_u, _ = _sonar_get_unapproved_metrics(sonar_sess, sentinel.camp_id)
            last_unap = met_u
            if ok_u and float(met_u.get('UNAPPROVED_RECIPIENTS_SUBMITTED', 0.0)) > 0.0:
                uploaded_seen = True
                break
            time.sleep(METRICS_POLL_INTERVAL_SECONDS)
        if not uploaded_seen and status_callback:
            status_callback(f"Warning: Sentinel UNAPPROVED not visible in time (last={last_unap}); proceeding anyway.")
    else:
        if status_callback:
            status_callback("No successful uploads found; skipping approval phase.")

    # Phase 2: PENDING→APPROVED (parallel) für alle erfolgreich hochgeladenen
    if status_callback:
        status_callback("Phase 2: Approving all uploaded segments in parallel…")

    def _phase2_worker(ctx: _Ctx) -> _Ctx:
        if not ctx or not ctx.ok_upload:
            return ctx
        sonar_sess_local = _clone_session(sonar_sess)
        p_ok, a_ok, m_ok, txt = _approve_after_upload(sonar_sess_local, ctx.camp_id, ctx.mp_id, status_callback=status_callback)
        ctx.row["ApprovedRequest"] = p_ok           # PENDING-Transport
        ctx.row["Approved"] = a_ok                  # APPROVED-Transport
        ctx.row["MetricsApproved"] = m_ok           # NEU: verifizierte Metrics
        ctx.row["Status"] = txt
        return ctx

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_phase2_worker, c): c.idx for c in uploaded_ctxs if c}
        for fut in as_completed(futs):
            _ = fut.result()  # rows werden in ctx.row gepflegt

    # Ergebnisse einsammeln (Eingabereihenfolge)
    rows: List[Dict[str, Any]] = []
    success_count = 0
    pair_times: List[float] = []  # nicht mehr pro worker gemessen; optional leer lassen
    for c in uploaded_ctxs:
        if not c:
            continue
        rows.append(c.row)
        if c.row.get("Status") == "Success":
            success_count += 1

    total_time = time.time() - t_all_start
    avg_time_per_pair = (total_time / len(pairs)) if pairs else 0.0
    failed = len(pairs) - success_count
    success_rate = (success_count / len(pairs)) if pairs else 0.0

    if status_callback:
        status_callback("\nPerformance Statistics:")
        status_callback(f"Total processing time: {_fmt(total_time)}")
        status_callback(f"Average batch time: {_fmt(0)}")
        status_callback(f"Success rate: {success_rate*100:.1f}%")
        status_callback(f"Failed segments: {failed}")
        status_callback(f"Average time per segment: {_fmt(avg_time_per_pair)}")
        status_callback("Results will be saved to Excel…")

    df = pd.DataFrame(rows, columns=[
        "BE ID", "Sonar Campaign ID", "Sonar Campaign URL",
        "Uploading", "Approved", "ApprovedRequest","MetricsApproved", "Status", "Timestamp"
    ])
    out_name = f"sonar_apply_results_{_timestamp_for_filename()}.xlsx"
    try:
        df.to_excel(out_name, index=False)
        if status_callback:
            status_callback(f"Results saved to {out_name}")
    except Exception as e:
        if status_callback:
            status_callback(f"Warning: could not write Excel: {e}")

    return df







# -------------------- Minimal manual test --------------------

if __name__ == "__main__":
    pairs = [
        # ("1733939602", "https://sonar-eu.amazon.com/#/3/campaigns/1418903091"),
    ]
    print("Starting sonar apply HTTP test…")
    def _log(msg): print(msg)
    def _prog(i): print(f"Progress idx={i}")
    df = apply_segments_to_sonar_pairs(pairs, _log, _prog, headless=True)
    if df is not None:
        print(df.head())
