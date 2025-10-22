# approve_sonar.py
# -*- coding: utf-8 -*-
"""
Approve Sonar Campaigns (PENDING -> APPROVED) und Status verifizieren.

Public:
- run_approve_sonar(campaigns, requester_alias, status_callback=None, progress_callback=None,
                    headless=True, parallel=True) -> (list[dict], str)|str
    campaigns: Liste aus IDs oder kompletten Sonar-Links.
    requester_alias: z.B. "nwreth" (für den PENDING-Schritt). Wenn leer/None, wird PENDING übersprungen.
    Rückgabe: (results, xlsx_path)  ODER nur xlsx_path (falls du das einfacher findest in deinem UI).

Excel-Ausgabe (genau diese Spalten):
- Campaign Id
- approvalRequired
- approved

Beispiel:
    results, xlsx = run_approve_sonar(
        ["https://sonar-eu.amazon.com/#/3/campaigns/1415118891", "1415118892"],
        requester_alias="nwreth",
        status_callback=print,
        progress_callback=lambda i: None,
        headless=True,
        parallel=True,
    )
"""

from __future__ import annotations

import os
import re
import time
import json
import csv
import shutil
import sqlite3
import tempfile
from typing import Callable, Dict, Any, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from threading import Lock

# Optionaler Selenium-Fallback zum Cookie-Refresh
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FxService
from selenium.webdriver.firefox.options import Options as FxOptions

# Firefox-Profil (SSO-Cookies)
from utils import get_firefox_profile


# -------------------- Konfiguration --------------------

SONAR_WEB_DOMAIN = "sonar-eu.amazon.com"
SONAR_SERVICE_HOST = "sonar-service-eu-ca-dub.dub.proxy.amazon.com"
SERVICE_BASE = f"https://{SONAR_SERVICE_HOST}"

GECKODRIVER_PATH = "geckodriver.exe" if os.name == "nt" else "geckodriver"



# NEU: sauberes Bool-Coercion
def _coerce_bool(val):
    if isinstance(val, bool):
        return val
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        v = val.strip().lower()
        if v in {"true", "1", "yes", "y"}:
            return True
        if v in {"false", "0", "no", "n"}:
            return False
    # Fallback: Python-Wahrheit vermeiden (z.B. "false" -> True), daher None
    return None



# -------------------- Cookie-Handling --------------------

def _copy_sqlite_readonly(src_path: str) -> Tuple[str, str]:
    """Gesperrte cookies.sqlite in ein Temp-Verzeichnis kopieren, um sie ro zu öffnen."""
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"cookies.sqlite not found at: {src_path}")
    tmpdir = tempfile.mkdtemp(prefix="ff_cookies_")
    dst = os.path.join(tmpdir, "cookies.sqlite")
    shutil.copy2(src_path, dst)
    return dst, tmpdir


def _load_firefox_cookies_for_suffixes(profile_path: str, suffixes: List[str]) -> requests.cookies.RequestsCookieJar:
    """
    Lädt Cookies aus Firefox für alle Host-Suffixe in 'suffixes' + generische amazon.com-Cookies.
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
        wanted = list(suffixes) + [".amazon.com", "amazon.com"]
        seen = set()  # (name, domain, path)
        for suf in wanted:
            cur.execute(
                "SELECT name, value, host, path, isSecure FROM moz_cookies WHERE host LIKE ?",
                (f"%{suf}",)
            )
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
    jar = _load_firefox_cookies_for_suffixes(
        profile_path,
        [SONAR_WEB_DOMAIN, SONAR_SERVICE_HOST]
    )
    s = requests.Session()
    s.cookies = jar
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=utf-8",
        "Origin": f"https://{SONAR_WEB_DOMAIN}",
        "Referer": f"https://{SONAR_WEB_DOMAIN}/",
        "Connection": "keep-alive",
    })
    return s


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


def _selenium_refresh_session_cookies(profile_path: str, headless: bool = True):
    """Öffnet Sonar-Web mit dem Profil und liefert frische Cookies zurück."""
    options = FxOptions()
    options.add_argument("-profile")
    options.add_argument(profile_path)
    if headless:
        options.add_argument("--headless")
        options.add_argument("--width=1920")
        options.add_argument("--height=1080")

    service = FxService(GECKODRIVER_PATH)
    driver = None
    try:
        driver = webdriver.Firefox(service=service, options=options)
        driver.get(f"https://{SONAR_WEB_DOMAIN}/")
        time.sleep(3)
        return _cookiejar_from_selenium_cookies(driver.get_cookies())
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


# -------------------- HTTP Helpers --------------------

def _safe_parse_json(resp: requests.Response) -> dict:
    txt = resp.text or ""
    if not txt.strip():
        return {}
    ct = (resp.headers.get("Content-Type") or "").lower()
    if "json" in ct or txt.strip().startswith(("{", "[")):
        try:
            return resp.json()
        except Exception:
            try:
                return json.loads(txt)
            except Exception:
                pass
    snippet = txt[:500].replace("\n", " ").replace("\r", " ")
    raise RuntimeError(f"HTTP {resp.status_code} non-JSON response. Snippet: {snippet!r}")


def _put_empty(session: requests.Session, url: str, timeout=(10, 60)) -> dict:
    """
    PUT ohne Body (approvalRequest-Endpoint akzeptiert 200/204/empty body).
    """
    resp = session.put(url, timeout=timeout)
    if resp.status_code in (200, 204) and not (resp.text or "").strip():
        return {}
    if resp.status_code >= 400:
        try:
            _ = _safe_parse_json(resp)
        except Exception:
            pass
        resp.raise_for_status()
    return _safe_parse_json(resp)


# NUR ÄNDERUNG: Refresh mit Lock
def _put_empty_with_refresh(session: requests.Session,
                            url: str,
                            profile_path: str,
                            headless: bool = True,
                            timeout=(10, 60)) -> dict:
    try:
        return _put_empty(session, url, timeout=timeout)
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (401, 403):
            with _REFRESH_LOCK:
                fresh = _selenium_refresh_session_cookies(profile_path, headless=headless)
            if fresh:
                # 👇 merge, don't replace
                session.cookies.update(fresh)  # 👈 CHANGE THIS
                return _put_empty(session, url, timeout=timeout)
        raise


# NUR ÄNDERUNG: Refresh mit Lock
def _get_json_with_refresh(session: requests.Session,
                           url: str,
                           profile_path: str,
                           headless: bool = True,
                           timeout=(10, 60)) -> dict:
    try:
        resp = session.get(url, timeout=timeout)
        if resp.status_code >= 400:
            try:
                _ = _safe_parse_json(resp)
            except Exception:
                pass
            resp.raise_for_status()
        return _safe_parse_json(resp)
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (401, 403):
            with _REFRESH_LOCK:
                fresh = _selenium_refresh_session_cookies(profile_path, headless=headless)
            if fresh:
                # 👇 merge, don't replace
                session.cookies.update(fresh)  # 👈 CHANGE THIS
                resp = session.get(url, timeout=timeout)
                if resp.status_code >= 400:
                    try:
                        _ = _safe_parse_json(resp)
                    except Exception:
                        pass
                    resp.raise_for_status()
                return _safe_parse_json(resp)
        raise




# -------------------- Parse Helpers --------------------

_CAMPAIGN_IN_HASH = re.compile(r"#/\d+/(?:campaigns|programs)/(\d+)")
_DIGITS = re.compile(r"\b(\d{6,})\b")

def _to_campaign_id(s: str) -> int:
    """Extrahiert die Campaign-ID aus URL/#/MP/campaigns/<id> oder nimmt eine reine ID."""
    s = (s or "").strip()
    if not s:
        raise ValueError("empty campaign reference")
    if s.isdigit():
        return int(s)
    m = _CAMPAIGN_IN_HASH.search(s)
    if m:
        return int(m.group(1))
    m = _DIGITS.search(s)
    if m:
        return int(m.group(1))
    raise ValueError(f"cannot parse campaign id from: {s!r}")


# -------------------- Core: Approve one --------------------

# ÄNDERUNG: _approve_one nimmt weiter session entgegen – aber der Rückgabeteil coerct sauber
def _approve_one(session: requests.Session,
                 profile_path: str,
                 cid: int,
                 alias: str | None,
                 status_callback: Callable[[str], None] | None,
                 headless: bool = True) -> Dict[str, Any]:
    def tell(msg: str):
        if status_callback:
            status_callback(msg)

    base_ajax = f"https://{SONAR_WEB_DOMAIN}/ajax/campaign/{cid}/qa/approvalRequest"

    if alias:
        pending_url = f"{base_ajax}?response=PENDING&requestedReviewer={alias}"
        try:
            tell(f"[{cid}] Requesting PENDING for reviewer '{alias}' …")
            _put_empty_with_refresh(session, pending_url, profile_path, headless=headless, timeout=(10, 60))
        except Exception as e:
            tell(f"[{cid}] PENDING failed ({e}); will still attempt APPROVED.")

    approve_url = f"{base_ajax}?response=APPROVED"
    tell(f"[{cid}] Approving …")
    _put_empty_with_refresh(session, approve_url, profile_path, headless=headless, timeout=(10, 60))

    get_url = f"{SERVICE_BASE}/campaigns/{cid}"
    tell(f"[{cid}] Verifying approval …")
    data = _get_json_with_refresh(session, get_url, profile_path, headless=headless, timeout=(10, 60))

    approval_required = _coerce_bool(data.get("approvalRequired"))
    approved = _coerce_bool(data.get("approved"))

    return {
        "Campaign Id": cid,
        "approvalRequired": approval_required,
        "approved": approved,
    }



# -------------------- Public API --------------------

def run_approve_sonar(
    campaigns: List[str],
    requester_alias: str,
    status_callback: Callable[[str], None] | None = None,
    progress_callback: Callable[[int], None] | None = None,
    headless: bool = True,
    parallel: bool = True,
):
    # 👇 define tell here
    def tell(msg: str):
        if status_callback:
            status_callback(msg)

    # 👇 start timer
    t0 = time.time()  # 👈 ADD THIS

    tell("Preparing session (Firefox cookies)…")
    profile_path = get_firefox_profile()
    if not profile_path or not os.path.isdir(profile_path):
        raise RuntimeError("No Firefox profile found – open Firefox once and sign in.")

    # Basissession EINMAL aufbauen, danach pro Task kopieren (thread-safe)
    base_session = _build_session_from_firefox(profile_path)
    base_headers = base_session.headers.copy()
    base_cookies = base_session.cookies.copy()

    # IDs parsen wie gehabt …
    parsed_ids: List[int] = []
    for c in campaigns:
        parsed_ids.append(_to_campaign_id(str(c)))

    tell(f"Processing {len(parsed_ids)} campaign(s)…")

    results_list: List[Optional[Dict[str, Any]]] = [None] * len(parsed_ids)

    def make_session() -> requests.Session:
        s = requests.Session()
        s.headers.update(base_headers)
        # WICHTIG: Cookies kopieren, nicht referenzieren
        s.cookies = requests.cookies.RequestsCookieJar()
        for c in base_cookies:
            s.cookies.set(c.name, c.value, domain=c.domain, path=c.path, secure=c.secure)
        return s

    def do_one(idx_cid):
        idx, cid = idx_cid
        sess = make_session()
        try:
            res = _approve_one(sess, profile_path, cid, requester_alias or None, status_callback, headless=headless)
            return (idx, res, None)
        except Exception as e:
            return (idx, {"Campaign Id": cid, "approvalRequired": None, "approved": None}, str(e))

    if parallel and len(parsed_ids) > 1:
        max_workers = min(12, max(2, (os.cpu_count() or 4)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futmap = {ex.submit(do_one, (i, cid)): (i, cid) for i, cid in enumerate(parsed_ids)}
            done_count = 0
            for fut in as_completed(futmap):
                i, _cid = futmap[fut]
                idx, res, err = fut.result()
                if err:
                    tell(f"[{_cid}] Error: {err}")
                results_list[idx] = res
                done_count += 1
                if progress_callback:
                    progress_callback(done_count)
    else:
        for i, cid in enumerate(parsed_ids):
            idx, res, err = do_one((i, cid))
            if err:
                tell(f"[{cid}] Error: {err}")
            results_list[idx] = res
            if progress_callback:
                progress_callback(i + 1)

    # Excel unverändert – Spaltenreihenfolge fixieren
    df = pd.DataFrame(results_list)[["Campaign Id", "approvalRequired", "approved"]]

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = os.path.abspath(f"approve_sonar_{ts}.xlsx")
    try:
        df.to_excel(out_path, index=False)
    except Exception as e:
        tell(f"Could not save Excel: {e}")
        out_path = None

    elapsed = time.time() - t0
    tell(f"Done in {elapsed:.1f}s")

    # Einheitlich immer Tuple zurückgeben (wie bisher)
    return results_list, out_path



# -------------------- CLI / Mini-Test --------------------

if __name__ == "__main__":
    # Beispiel: benötigt sign-in im Firefox-Profil + Netzwerkzugriff
    test_campaigns = [
        "https://sonar-eu.amazon.com/#/3/campaigns/1415118891",
    ]
    def _status(m: str): print(m)
    def _progress(i: int): pass

    try:
        results, path = run_approve_sonar(test_campaigns, requester_alias="nwreth",
                                          status_callback=_status, progress_callback=_progress,
                                          headless=True, parallel=False)
        print("Excel:", path)
        print(json.dumps(results, indent=2))
    except Exception as ex:
        print("Error:", ex)
