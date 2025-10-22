# preview_campaigns.py
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import sqlite3
import shutil
import tempfile
from typing import Dict, Any, List, Tuple, Optional, Callable
import requests

# Selenium-Fallback
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FxService
from selenium.webdriver.firefox.options import Options as FxOptions

from utils import get_firefox_profile  # wie in deinen anderen Files

# -------------------- Konfiguration --------------------

SONAR_WEB_DOMAIN   = "sonar-eu.amazon.com"
SONAR_SERVICE_HOST = "sonar-service-eu-ca-dub.dub.proxy.amazon.com"
SERVICE_BASE       = f"https://{SONAR_SERVICE_HOST}"
WEB_BASE           = f"https://{SONAR_WEB_DOMAIN}"

GECKODRIVER_PATH = "geckodriver.exe" if os.name == "nt" else "geckodriver"

FIXED_PREVIEW_AS_CUSTOMER_ID = 1582381951
FIXED_PREVIEW_TYPE = "SEND_CONTENT_TO_TARGET"

# -------------------- Profile (Alias/CustomerId) --------------------

def _load_app_profile() -> Dict[str, Any]:
    """
    Versucht alias/email/customer_id zu laden.
    Quellen (in dieser Reihenfolge):
      1) utils.get_user_profile()
      2) user_profile.json / profile.json im CWD
      3) Umgebungsvariablen
    Akzeptiert sowohl 'customer_id' als auch 'customerId'.
    """

    def _first(d: Dict[str, Any], *keys: str):
        for k in keys:
            if k in d and d[k] not in (None, ""):
                return d[k]
        return None

    def _to_int_or_none(x: Any) -> Optional[int]:
        if x is None:
            return None
        s = str(x).strip()
        return int(s) if s.isdigit() else None

    # 1) optionaler utils-Hook
    try:
        from utils import get_user_profile  # type: ignore
        prof = get_user_profile() or {}
        if prof:
            return {
                "alias": _first(prof, "alias") or os.environ.get("AMZN_ALIAS") or "",
                "email": _first(prof, "email") or "",
                "customer_id": _to_int_or_none(_first(prof, "customer_id", "customerId")),
            }
    except Exception:
        pass

    # 2) json-Datei im aktuellen Arbeitsverzeichnis
    for fname in ("user_profile.json", "profile.json"):
        p = os.path.join(os.getcwd(), fname)
        if os.path.isfile(p):
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                "alias": _first(data, "alias") or os.environ.get("AMZN_ALIAS") or "",
                "email": _first(data, "email") or "",
                "customer_id": _to_int_or_none(_first(data, "customer_id", "customerId")),
            }

    # 3) Env-Fallbacks
    return {
        "alias": os.environ.get("AMZN_ALIAS") or os.environ.get("USER") or "",
        "email": os.environ.get("AMZN_EMAIL") or "",
        "customer_id": _to_int_or_none(os.environ.get("AMZN_CUSTOMER_ID")),
    }


# -------------------- Cookies/Session (identisch zu deinen Skripten) --------------------

def _copy_sqlite_readonly(src_path: str) -> Tuple[str, str]:
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"cookies.sqlite not found at: {src_path}")
    tmpdir = tempfile.mkdtemp(prefix="ff_cookies_")
    dst = os.path.join(tmpdir, "cookies.sqlite")
    shutil.copy2(src_path, dst)
    return dst, tmpdir

def _load_firefox_cookies_for_suffixes(profile_path: str, suffixes: List[str]) -> requests.cookies.RequestsCookieJar:
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
        wanted = list(suffixes) + [".amazon.com", "amazon.com"]
        seen = set()
        for suf in wanted:
            cur.execute("SELECT name, value, host, path, isSecure FROM moz_cookies WHERE host LIKE ?", (f"%{suf}",))
            for name, value, host, path, isSecure in cur.fetchall():
                key = (name, host, path)
                if key in seen:
                    continue
                seen.add(key)
                jar.set(name, value, domain=host, path=path, secure=bool(isSecure))
    finally:
        conn.close()
        if cleanup_dir and os.path.isdir(cleanup_dir):
            shutil.rmtree(cleanup_dir, ignore_errors=True)
    return jar

def _build_session_from_firefox(profile_path: str) -> requests.Session:
    jar = _load_firefox_cookies_for_suffixes(profile_path, [SONAR_WEB_DOMAIN, SONAR_SERVICE_HOST])
    s = requests.Session()
    s.cookies = jar
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=utf-8",
        "Origin": WEB_BASE,
        "Referer": f"{WEB_BASE}/",
        "Connection": "keep-alive",
    })
    return s

def _cookiejar_from_selenium_cookies(cookies_list):
    jar = requests.cookies.RequestsCookieJar()
    for c in cookies_list:
        jar.set(c.get("name"), c.get("value"), domain=c.get("domain"),
                path=c.get("path", "/"), secure=bool(c.get("secure", False)))
    return jar

def _selenium_refresh_session_cookies(profile_path: str, headless: bool = True, refresh_url: str = WEB_BASE + "/"):
    opts = FxOptions()
    opts.add_argument("-profile"); opts.add_argument(profile_path)
    if headless:
        opts.add_argument("--headless"); opts.add_argument("--width=1920"); opts.add_argument("--height=1080")
    drv = None
    try:
        drv = webdriver.Firefox(service=FxService(GECKODRIVER_PATH), options=opts)
        drv.get(refresh_url); time.sleep(3)
        return _cookiejar_from_selenium_cookies(drv.get_cookies())
    finally:
        if drv is not None:
            try: drv.quit()
            except Exception: pass

def _safe_parse_json(resp: requests.Response) -> dict:
    txt = resp.text or ""
    if not txt.strip():
        return {}
    ct = (resp.headers.get("Content-Type") or "").lower()
    if "json" in ct or txt.strip().startswith(("{", "[")):
        try:
            return resp.json()
        except Exception:
            return json.loads(txt)
    raise RuntimeError(f"HTTP {resp.status_code} non-JSON response")

def _request_json_with_refresh(session: requests.Session, method: str, url: str,
                               payload: Dict | None, params: Dict | None,
                               profile_path: str, headless: bool = True,
                               timeout: Tuple[int, int] = (10, 60)) -> dict:
    try:
        resp = session.request(method.upper(), url, json=payload, params=params, timeout=timeout)
        if resp.status_code in (200, 204) and not (resp.text or "").strip():
            return {}
        if resp.status_code >= 400:
            resp.raise_for_status()
        return _safe_parse_json(resp)
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (401, 403):
            fresh = _selenium_refresh_session_cookies(profile_path, headless=headless)
            if fresh:
                session.cookies = fresh
                resp = session.request(method.upper(), url, json=payload, params=params, timeout=timeout)
                if resp.status_code in (200, 204) and not (resp.text or "").strip():
                    return {}
                resp.raise_for_status()
                return _safe_parse_json(resp)
        raise

# -------------------- Parsing Helpers --------------------

_CAMPAIGN_IN_HASH = re.compile(r"#/(\d+)/(?:campaigns|programs)/(\d+)")
_DIGITS = re.compile(r"\b(\d{6,})\b")

def _parse_campaign_and_mp(ref: str) -> Tuple[int, int | None]:
    """
    Extrahiert (campaign_id, marketplace_id?) aus:
      - https://sonar.../#/<mp>/campaigns/<id>
      - reine ID (mp=None)
    """
    s = (ref or "").strip()
    if not s:
        raise ValueError("empty campaign reference")
    m = _CAMPAIGN_IN_HASH.search(s)
    if m:
        mp = int(m.group(1))
        cid = int(m.group(2))
        return cid, mp
    if s.isdigit():
        return int(s), None
    m2 = _DIGITS.search(s)
    if m2:
        return int(m2.group(1)), None
    raise ValueError(f"cannot parse campaign id from: {s!r}")

# -------------------- Kern-Calls --------------------

def _fetch_variables(session: requests.Session, campaign_id: int, marketplace_id: int, requester_alias: str,
                     profile_path: str, headless: bool) -> List[Dict[str, Any]]:
    """
    GET /campaigns/{id}/content → liefert variables[]. Lässt secondary leer, falls nicht vorhanden.
    """
    url = f"{SERVICE_BASE}/campaigns/{campaign_id}/content"
    params = {"marketplaceId": marketplace_id, "requester": requester_alias or ""}
    data = _request_json_with_refresh(session, "GET", url, None, params, profile_path, headless=headless)
    vars_list = list(data.get("variables") or [])

    # 'supportedSecondaryLanguages' ggf. leer ergänzen
    names = {v.get("name") for v in vars_list}
    if "supportedSecondaryLanguages" not in names:
        vars_list.append({"name": "supportedSecondaryLanguages", "required": None, "value": ""})
    return vars_list

def _build_preview_payload(variables: List[Dict[str, Any]], marketplace_id: int,
                           send_to_customer_id: int) -> Dict[str, Any]:
    return {
        "sendPreviewToCustomerId": int(send_to_customer_id),
        "previewAsCustomerId": FIXED_PREVIEW_AS_CUSTOMER_ID,
        "previewType": FIXED_PREVIEW_TYPE,
        "marketplaceId": int(marketplace_id),
        "campaignOrTreatmentVariables": variables,
    }

def _post_preview(session: requests.Session, campaign_id: int, payload: Dict[str, Any],
                  profile_path: str, headless: bool) -> dict:
    url = f"{WEB_BASE}/ajax/campaign/{campaign_id}/preview/MOBILE_PUSH"
    return _request_json_with_refresh(session, "POST", url, payload, None, profile_path, headless=headless)

# -------------------- Public API --------------------

def plan_preview_batches(jobs: List[Dict[str, Any]]) -> List[int]:
    """
    Ermittelt verfügbare Marketplace-IDs aus den Jobs.
    Job-Formate:
      {"campaign": "<URL oder ID>"}  oder  {"campaign": "...", "marketplaceId": 3}
    Rückgabe: sortierte Liste einmaliger MPs.
    """
    mps = set()
    for j in jobs or []:
        mp = j.get("marketplaceId")
        if mp is None:
            _, mp2 = _parse_campaign_and_mp(str(j.get("campaign", "")))
            if mp2 is not None:
                mp = mp2
        if mp is not None:
            mps.add(int(mp))
    return sorted(mps)


def run_preview_batch_for_marketplace(
    jobs: List[Dict[str, Any]],
    marketplace_id: int,
    *,
    status_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    headless: bool = True,
    min_gap_s: float = 0.5,  # 0,5 s Mindestabstand
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Führt die Previews nur für den angegebenen Marketplace aus.
    Jeder Job braucht mindestens "campaign": "<URL|ID>".
    Rückgabe: (results, log_json_path)
    """
    def tell(msg: str) -> None:
        if status_callback:
            status_callback(msg)

    profile_path = get_firefox_profile()
    if not profile_path or not os.path.isdir(profile_path):
        raise RuntimeError("No Firefox profile found – open Firefox once and sign in.")

    app_profile = _load_app_profile()
    alias = app_profile.get("alias") or ""
    customer_id = app_profile.get("customer_id")
    if not customer_id:
        raise RuntimeError("Customer Id not set in profile. Please add it in the app profile.")

    sess = _build_session_from_firefox(profile_path)

    # Jobs für den MP filtern
    selected_jobs: List[Tuple[int, int]] = []  # (campaign_id, mp)
    for j in jobs or []:
        cid, mp = _parse_campaign_and_mp(str(j.get("campaign", "")))
        mp = int(j.get("marketplaceId") or mp or marketplace_id)
        if mp == int(marketplace_id):
            selected_jobs.append((cid, mp))

    if not selected_jobs:
        tell(f"No jobs for marketplace {marketplace_id}.")
        return [], None

    results: List[Dict[str, Any]] = []
    tell(f"Sending {len(selected_jobs)} preview(s) for marketplace {marketplace_id} …")

    last_start = 0.0
    for idx, (cid, mp) in enumerate(selected_jobs, start=1):
        # Mindestabstand zwischen zwei Send-Starts
        now = time.perf_counter()
        wait = (last_start + min_gap_s) - now
        if wait > 0:
            time.sleep(wait)

        try:
            tell(f"  • Campaign {cid}: fetching variables …")
            vars_list = _fetch_variables(sess, cid, mp, alias, profile_path, headless=headless)

            payload = _build_preview_payload(vars_list, mp, customer_id)
            tell(f"  • Campaign {cid}: sending preview …")
            resp = _post_preview(sess, cid, payload, profile_path, headless=headless)

            results.append({
                "campaignId": cid,
                "marketplaceId": mp,
                "ok": True,
                "response": resp
            })
            if status_callback:
                status_callback(f"Sent preview {idx}/{len(selected_jobs)}")
        except Exception as e:
            results.append({
                "campaignId": cid,
                "marketplaceId": mp,
                "ok": False,
                "error": str(e)
            })
            if status_callback:
                status_callback(f"Failed preview {idx}/{len(selected_jobs)}: {e}")

        last_start = time.perf_counter()
        if progress_callback:
            progress_callback(idx)  # oder idx-1, je nach Erwartung

    # Logdatei schreiben
    out_path: Optional[str] = None
    try:
        ts = time.strftime("%Y%m%d_%H%M%S")
        out_path = os.path.abspath(f"preview_batch_MP{marketplace_id}_{ts}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({"marketplaceId": marketplace_id, "results": results}, f, ensure_ascii=False, indent=2)
        tell(f"Saved log: {out_path}")
    except Exception:
        out_path = None

    tell(f"Finished marketplace {marketplace_id}.")
    return results, out_path