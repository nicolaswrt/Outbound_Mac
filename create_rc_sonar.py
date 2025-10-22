# create_rc_sonar.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Backend-Funktion zum Erstellen von Sonar Recurring Campaigns in 2 Steps:
  1) Program anlegen: POST /reoccurringUseCases
  2) Recurring Version anlegen: POST /reoccurringVersions

Mapping (gemäß Spezifikation):
  Step 1 (Program):
    requestContext.marketplaceId  ← Excel "Marketplace" (oder "marketplaceId")
    requestContext.userName       ← alias (aus Profil)
    name                          ← Excel "Name"
    objective                     ← Excel "Description" (Fallback: Name)
    marketplaceId                 ← Excel "Marketplace"
    lobExpression                 ← "0" wenn marketplaceId == 3, sonst aus Template ("lobExpression")
    startDate                     ← heute + 1 (YYYY-MM-DD, Europe/Berlin reicht date.today()+1)
    endDate                       ← startDate + 5 Monate
    ownerBindleName               ← Template "teamBindle"
    businessGroupId               ← Template "businessGroupId" (falls vorhanden)
    managementType                ← Template "managementType"
    channels                      ← Template "channel" / "channels"
    successMetrics                ← alle 0

  Step 2 (Version):
    requestContext                ← wie oben
    name                          ← Excel "Name"
    reoccurringUseCaseId          ← Program-ID (Step 1 Response.id)
    cadenceList                   ← ["MONDAY", ..., "SUNDAY"]
    templatePath                  ← Dialog "TemplatePath" (z. B. /LAYOUT-TEMPLATES/<uuid>)
    bullseyeSegmentId             ← Excel "BE ID"
    refreshableSegment            ← True
    campaignVariables             ← aus Excel-Spalten (notificationTitle, notificationText, primaryButtonText,
                                  primaryButtonCta, url, consolidationKey, hubImage, androidIconImage,
                                  iosImageOrVideo, androidBigPicture) – nur nicht-leere Felder
    treatmentsConfig              ← {}
    channel                       ← aus Template (falls Liste → erstes Element)
    schedule.startDate            ← Excel "Schedule Start Date"
    schedule.endDate              ← Excel "Schedule End Date"
    schedule.campaignStartTime    ← aus Template start/end ("09:00" etc. oder Minutenoffset)
    schedule.campaignEndTime      ← aus Template
    schedule.campaignDuration     ← 1
    schedule.campaignStartDateOffset ← 1
    supportedLanguages            ← Mapping anhand marketplaceId (Minimalbeispiel unten)
    secondaryLanguages            ← []
    status                        ← "ACTIVE"
    pushTopic                     ← "CAFEP"
    optOutIds                     ← []

Output-Excel-Spalten:
  - Program Success (bool; lastUpdated vorhanden)
  - Program ID
  - Program Link (https://prod.sonar-website.outbound.amazon.dev/#/{marketplaceId}/recurring-use-cases/{id})
  - Campaign Success (bool; Version-ID vorhanden)
  - Version ID
  - Error (falls aufgetreten)
"""

import os
import re
import json
import time
import shutil
import sqlite3
import tempfile
import random
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple, Union
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

# Optionaler Selenium-Fallback zum Auffrischen der Cookies
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FxService
from selenium.webdriver.firefox.options import Options as FxOptions

from utils import get_firefox_profile

# ============================ Konfiguration ============================

BASE_URL = "https://sonar-service-eu-ca-dub.dub.proxy.amazon.com"

EP_CREATE_PROGRAM = f"{BASE_URL}/reoccurringUseCases"     # Step 1
EP_CREATE_VERSION = f"{BASE_URL}/reoccurringVersions"     # Step 2

# Für Cookie-Referer/Origin (Web-Host der Sonar UI)
SONAR_WEB_ORIGIN  = "https://prod.sonar-website.outbound.amazon.dev"
SONAR_WEB_REFERER = "https://prod.sonar-website.outbound.amazon.dev/"

# Domains, aus denen wir Cookies ziehen (wir nehmen mehrere, um SSO-Ketten abzudecken)
COOKIE_DOMAINS = [
    "amazon.com",
    "outbound.amazon.dev",
    "dub.proxy.amazon.com",
]

# ============================ Monitoring & Utils ============================

class PerformanceMonitor:
    def __init__(self):
        self.batch_times: List[Dict[str, Any]] = []
        self.segment_results: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def add_batch_time(self, batch_size: int, time_taken: float):
        with self._lock:
            self.batch_times.append({'batch_size': batch_size, 'time': time_taken})

    def add_item_result(self, unique_key: str, time_taken: float, attempts: int, success: bool):
        with self._lock:
            self.segment_results[unique_key] = {
                'time': time_taken,
                'attempts': attempts,
                'success': success
            }


    def get_statistics(self):
        with self._lock:
            batch_times = list(self.batch_times)
            segment_results = dict(self.segment_results)

        avg_batch_time = (sum(b['time'] for b in batch_times) / len(batch_times)) if batch_times else 0.0

        total_items = len(segment_results)
        if total_items:
            times = [s['time'] for s in segment_results.values()]
            attempts = [s['attempts'] for s in segment_results.values()]
            successes = sum(1 for s in segment_results.values() if s['success'])
            failed = total_items - successes
            avg_item_time = sum(times) / total_items
            avg_attempts = sum(attempts) / total_items
            avg_success_rate = successes / total_items
        else:
            failed = 0
            avg_item_time = 0.0
            avg_attempts = 0.0
            avg_success_rate = 0.0

        return {
            "average_batch_time": avg_batch_time,
            "average_item_time": avg_item_time,
            "average_attempts": avg_attempts,
            "average_success_rate": avg_success_rate,
            "failed_segments": failed,
            "total_items": total_items,
        }




def _coerce_iso_date(val: Any) -> Optional[str]:
    """Akzeptiert Pandas Timestamp/Datetime/String und formatiert zu YYYY-MM-DD, sonst None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, pd.Timestamp):
        return val.date().isoformat()
    if isinstance(val, datetime):
        return val.date().isoformat()
    if isinstance(val, date):
        return val.isoformat()
    s = str(val).strip()
    if not s:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    try:
        return pd.to_datetime(s).date().isoformat()
    except Exception:
        return None

def _format_time(seconds: float) -> str:
    return str(timedelta(seconds=round(seconds)))

# ============================ HTTP Session & Cookies ============================

def _copy_sqlite_readonly(src_path: str):
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"cookies.sqlite not found at: {src_path}")
    tmpdir = tempfile.mkdtemp(prefix="ff_cookies_")
    dst = os.path.join(tmpdir, "cookies.sqlite")
    shutil.copy2(src_path, dst)
    return dst, tmpdir

def _load_firefox_cookies_for_domains(profile_path: str, domain_suffixes: List[str]) -> requests.cookies.RequestsCookieJar:
    """Alle Cookies für gegebene Domains laden und in ein gemeinsames Jar packen."""
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
        for suffix in domain_suffixes:
            cur.execute(
                "SELECT name, value, host, path, isSecure FROM moz_cookies WHERE host LIKE ?",
                (f"%{suffix}",)
            )
            for name, value, host, path, isSecure in cur.fetchall():
                jar.set(name, value, domain=host, path=path, secure=bool(isSecure))
    finally:
        conn.close()
        if cleanup_dir and os.path.isdir(cleanup_dir):
            shutil.rmtree(cleanup_dir, ignore_errors=True)
    return jar

def _build_http_session(profile_path: str) -> requests.Session:
    jar = _load_firefox_cookies_for_domains(profile_path, COOKIE_DOMAINS)
    s = requests.Session()
    s.cookies = jar
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": SONAR_WEB_ORIGIN,
        "Referer": SONAR_WEB_REFERER,
        "Connection": "keep-alive",
    })
    return s

# ============================ Selenium Cookie Refresh (Fallback) ============================

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

def _selenium_refresh_session_cookies(profile_path, headless=True, referer_url: Optional[str] = None):
    """Headless Firefox mit Profil starten, Sonar-Web öffnen, frische Cookies holen."""
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
        url = referer_url or SONAR_WEB_REFERER
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

# ============================ Robust HTTP Helpers ============================

def _post_json(session: requests.Session,
               url: str,
               payload: Dict[str, Any],
               timeout=(5, 30),
               max_attempts=4,
               base_backoff=1.6) -> Dict[str, Any]:
    """
    POST mit Retries, Backoff und robuster JSON-Erkennung.
    Hebt bei Auth-/HTML-/leerem Body klare Fehler an.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.post(url, data=json.dumps(payload), timeout=timeout, allow_redirects=True)

            # 1) Auth/Permission direkt erkennen
            if resp.status_code in (401, 403):
                raise PermissionError(f"Unauthorized ({resp.status_code})")

            # 2) Rate limit / 5xx Backoff
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and str(retry_after).isdigit():
                    time.sleep(int(retry_after))
                else:
                    time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue
            if 500 <= resp.status_code < 600:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue

            # 3) Non-OK?
            resp.raise_for_status()

            # 4) Inhalt validieren
            ctype = (resp.headers.get("Content-Type") or "").lower()
            text  = resp.text or ""

            # Leerer Body: akzeptiere als {}, wenn 2xx
            if not text.strip():
                return {}

            # JSON?
            looks_like_json = text.lstrip().startswith("{") or "application/json" in ctype
            if not looks_like_json:
                # Sehr häufig: HTML-Login/SSO-Seite
                snippet = text.strip()[:200]
                if "<html" in text.lower() or "</html>" in text.lower():
                    raise PermissionError(f"Auth required or HTML response from {url}. Snippet: {snippet!r}")
                raise RuntimeError(f"Unexpected non-JSON response ({resp.status_code}, {ctype}). Snippet: {snippet!r}")

            # 5) JSON parse
            try:
                return resp.json()
            except Exception:
                # Fallback manuell
                return json.loads(text)

        except (requests.Timeout, requests.RequestException) as e:
            last_exc = e
            if attempt < max_attempts:
                time.sleep((base_backoff ** attempt) + (random.random() * 0.2))
                continue
            raise
        except PermissionError as e:
            last_exc = e
            # bei Auth-Fehlern nicht automatisch weiter schleifen lassen – Caller macht ggf. Cookie-Refresh/Retry
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("Unknown HTTP error")


# ============================ Helpers: Zeit/Locale/Lang ============================

def _tz_today_berlin() -> date:
    # wir brauchen nur YYYY-MM-DD (lokal Europe/Berlin); date.today() reicht hier
    return date.today()

def _fmt_date(d: date) -> str:
    return d.isoformat()

def _marketplace_to_languages(marketplace_id: int) -> List[str]:
    """
    Minimalbeispiel – bitte bei Bedarf ergänzen.
    """
    mapping = {
        3: ["en_GB"],   # MP 3
        4: ["de_DE"],   # MP 4
        5: ["fr_FR"],   # MP 5
        35691: ["it_IT"],  # MP 35691
        44551: ["es_ES"]
    }
    return mapping.get(int(marketplace_id), ["en_EN"])

def _ensure_hhmm(val: Union[str, int]) -> str:
    """
    Nimmt entweder '09:00' oder Minuten-Offset (z. B. 540) und liefert 'HH:MM'.
    """
    if val is None:
        return "09:00"
    if isinstance(val, str) and ":" in val:
        return val.strip()
    try:
        minutes = int(val)
        h, m = divmod(minutes, 60)
        return f"{h:02d}:{m:02d}"
    except Exception:
        return "09:00"

# ============================ Sonar-Orchestrierung ============================

def _build_program_payload(row: Dict[str, Any], template: Dict[str, Any], alias: str) -> Dict[str, Any]:
    # Excel-Felder
    name = str(row.get("Name") or row.get("Program Name") or row.get("ProgramName") or "").strip()
    description = str(row.get("Description") or "").strip()
    objective = description or name  # Fallback wie gefordert
    marketplace = row.get("Marketplace") or row.get("marketplaceId")
    marketplace_id = int(marketplace) if str(marketplace).strip() else 0

    # lobExpression: IF marketplaceId == 0 → "0", sonst aus Template
    lob_expr = "0" if marketplace_id == 3 else str(template.get("lobExpression", "")).strip()

    # Start/End: heute+1 bzw. +5 Monate
    start = _tz_today_berlin() + timedelta(days=1)
    end   = start + relativedelta(months=5)

    channels = template.get("channel") or template.get("channels") or []
    if isinstance(channels, str):
        channels = [channels]

    payload: Dict[str, Any] = {
        "requestContext": {
            "marketplaceId": marketplace_id,
            "userName": alias
        },
        "name": name or "Unnamed Program",
        "objective": objective or "Automated Objective",
        "marketplaceId": marketplace_id,
        "lobExpression": lob_expr,
        "startDate": _fmt_date(start),
        "endDate": _fmt_date(end),
        "ownerBindleName": str(template.get("teamBindle", "")).strip(),
        "businessGroupId": int(template.get("businessGroupId", 0)) if template.get("businessGroupId") else None,
        "managementType": str(template.get("managementType", "")).strip(),
        "channels": channels,
        "successMetrics": {
            "iopsClickRate": 0,
            "clickThroughRate": 0,
            "optOutClickRate": 0,
            "sentSubmittedRate": 0
        }
    }
    # businessGroupId nur schicken, wenn vorhanden
    if payload["businessGroupId"] is None:
        payload.pop("businessGroupId")
    return payload

CAMPAIGN_KEYS = [
    "notificationTitle",
    "notificationText",
    "primaryButtonText",
    "primaryButtonCta",
    "url",
    "consolidationKey",
    "hubImage",
    "androidIconImage",
    "iosImageOrVideo",
    "androidBigPicture",
]

def _build_version_payload(row: Dict[str, Any], template: Dict[str, Any],
                           template_path: str, alias: str,
                           reoccurring_use_case_id: int) -> Dict[str, Any]:
    # Excel
    name = str(row.get("Name") or row.get("Program Name") or row.get("ProgramName") or "").strip()
    marketplace = row.get("Marketplace") or row.get("marketplaceId")
    marketplace_id = int(marketplace) if str(marketplace).strip() else 0

    be_id = row.get("BE ID") or row.get("BEID") or row.get("be id") or row.get("beid")
    bullseye_segment_id = int(be_id) if str(be_id or "").strip() else None

    # Schedule (aus Excel)
    sched_start = _coerce_iso_date(row.get("Schedule Start Date") or row.get("ScheduleStartDate"))
    sched_end   = _coerce_iso_date(row.get("Schedule End Date") or row.get("ScheduleEndDate"))

    # Zeiten aus Template
    start_time = _ensure_hhmm(
        template.get("startTime") or template.get("campaignStartTime") or template.get("startTimeMinutesOffset")
    )
    end_time   = _ensure_hhmm(
        template.get("endTime")   or template.get("campaignEndTime")   or template.get("endTimeMinutesOffset")
    )

    # campaignVariables aus Excel (nur vorhandene, nicht-leere)
    variables: Dict[str, str] = {}
    for k in CAMPAIGN_KEYS:
        v = row.get(k)
        if v is None:
            v = row.get(k.replace("_", " "))
        if v is not None and str(v).strip():
            variables[k] = str(v).strip()

    langs = _marketplace_to_languages(marketplace_id)
    channel_from_template = template.get("channel") or template.get("channels") or "MOBILE_PUSH"
    if isinstance(channel_from_template, list):
        channel_from_template = channel_from_template[0] if channel_from_template else "MOBILE_PUSH"

    payload: Dict[str, Any] = {
        "requestContext": {
            "marketplaceId": marketplace_id,
            "userName": alias
        },
        "name": name or "Automated Version",
        "reoccurringUseCaseId": int(reoccurring_use_case_id),
        "cadenceList": ["MONDAY","TUESDAY","WEDNESDAY","THURSDAY","FRIDAY","SATURDAY","SUNDAY"],
        "templatePath": template_path,
        "bullseyeSegmentId": bullseye_segment_id,
        "refreshableSegment": True,
        "campaignVariables": variables if variables else {"Variable1": "value1"},
        "treatmentsConfig": {},
        "channel": channel_from_template,
        "schedule": {
            "startDate": sched_start,
            "endDate":   sched_end,
            "campaignStartTime": start_time,
            "campaignEndTime":   end_time,
            "campaignDuration": 1,
            "campaignStartDateOffset": 1
        },
        "supportedLanguages": langs,
        "secondaryLanguages": [],
        "status": "ACTIVE",
        "pushTopic": "CAFEP",
        "optOutIds": []
    }
    return payload

def _create_program(session: requests.Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    return _post_json(session, EP_CREATE_PROGRAM, payload)

def _create_version(session: requests.Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    return _post_json(session, EP_CREATE_VERSION, payload)

# ============================ Public API ============================

def create_remote_configs(
    rows: Union[pd.DataFrame, List[Dict[str, Any]], None] = None,
    df: Union[pd.DataFrame, List[Dict[str, Any]], None] = None,
    template: Dict[str, Any] = None,                 # (/!\) erforderlich
    template_path: str = "",                         # (/!\) erforderlich: /LAYOUT-TEMPLATES/<uuid>
    alias: str = "",                                 # (/!\) erforderlich: requestContext.userName
    out_dir: str = ".",                              # Speicherort Ergebnisdatei
    status_callback=None,
    progress_callback=None,
    headless: bool = True
) -> Tuple[pd.DataFrame, str]:
    """
    Hauptfunktion: erstellt pro Eingabezeile ein Program + eine Recurring Version.
    """

    # ---------- Argument-Normalisierung ----------
    if rows is None and df is not None:
        rows = df
    if isinstance(rows, pd.DataFrame):
        rows = rows.to_dict(orient="records")
    elif rows is not None:
        rows = list(rows)
    if not rows:
        raise TypeError("create_remote_configs benötigt 'rows' (List[dict]) oder 'df' (pandas DataFrame).")
    if template is None:
        raise ValueError("template (dict) fehlt.")
    if not template_path:
        raise ValueError("template_path (str) fehlt.")
    if not alias:
        raise ValueError("alias (str) fehlt.")
    # ---------- Ende Normalisierung ----------

    t_start = time.time()
    perf = PerformanceMonitor()

    if status_callback:
        status_callback("Vorbereitung HTTP-Session…")

    profile_path = get_firefox_profile()
    if not profile_path:
        raise RuntimeError("Kein Firefox-Profil gefunden – bitte in utils.get_firefox_profile prüfen.")


    results: List[Dict[str, Any]] = []
    max_workers = 8
    if status_callback:
        status_callback(f"Erzeuge {len(rows)} Programme + Versionen (workers={max_workers})…")

    def _process_one(idx: int, row: Dict[str, Any]) -> Dict[str, Any]:
        # jede Aufgabe nutzt eine eigene HTTP-Session (threadsicher)
        session = _build_http_session(profile_path) 
        t0 = time.time()
        attempts = 0
        out: Dict[str, Any] = {
            "Row": idx,
            "Program Success": False,
            "Program ID": None,
            "Program Link": None,
            "Campaign Success": False,
            "Version ID": None,
            "Campaign Link": None,
            "Error": None,
        }
        try:
            attempts += 1
            # Step 1: Program (mit einmaligem Auth-Retry)
            p_payload = _build_program_payload(row, template, alias)
            try:
                p_resp = _create_program(session, p_payload)
            except PermissionError:
                # Cookie-Refresh und 1 Retry
                new_jar = _selenium_refresh_session_cookies(profile_path, headless=headless, referer_url=SONAR_WEB_REFERER)
                if new_jar:
                    session.cookies = new_jar
                p_resp = _create_program(session, p_payload)

            prog_id = p_resp.get("id")
            last_updated = p_resp.get("lastUpdated") or p_resp.get("lastUpdate") or p_resp.get("createdAt")
            marketplace_id = p_resp.get("marketplaceId") or p_payload["marketplaceId"]

            out["Program Success"] = bool(last_updated)
            out["Program ID"] = prog_id
            if prog_id and marketplace_id is not None:
                out["Program Link"] = f"https://prod.sonar-website.outbound.amazon.dev/#/{marketplace_id}/recurring-use-cases/{prog_id}"

            if not prog_id:
                out["Error"] = "Program ID fehlt in Response"
                return out

            # Step 2: Version
            v_payload = _build_version_payload(row, template, template_path, alias, prog_id)
            v_resp = _create_version(session, v_payload)
            version_id = v_resp.get("id") or v_resp.get("versionId")

            out["Campaign Success"] = bool(version_id)
            out["Version ID"] = version_id
            out["Version ID"] = version_id
            if version_id and prog_id and marketplace_id is not None:
                out["Campaign Link"] = (
                    f"https://prod.sonar-website.outbound.amazon.dev/#/"
                    f"{marketplace_id}/recurring-use-cases/{prog_id}/versions/{version_id}"
                )
            return out

        except Exception as e:
            out["Error"] = str(e)
            return out
        finally:
            try:
                perf.add_item_result(f"row#{idx}", time.time() - t0, attempts, out["Program Success"] and out["Campaign Success"])
            except Exception:
                pass

    # Parallel: max 8 Threads, stabile Ergebnisreihenfolge wie Input
    results = [None] * len(rows)
    completed = 0
    _prog_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_process_one, i, r): i for i, r in enumerate(rows)}
        for fut in as_completed(future_map):
            i = future_map[fut]
            res = fut.result()
            results[i] = res
            with _prog_lock:
                completed += 1
                if progress_callback:
                    progress_callback(completed)

    stats = perf.get_statistics()
    total_time = time.time() - t_start
    if status_callback:
        status_callback("\nPerformance:")
        status_callback(f"Total: {_format_time(total_time)}")
        status_callback(f"Avg batch time: {_format_time(stats['average_batch_time'])}")
        status_callback(f"Avg item time: {_format_time(stats['average_item_time'])}")
        status_callback(f"Avg attempts: {stats['average_attempts']:.2f}")
        status_callback(f"Success rate: {stats['average_success_rate']*100:.1f}%")
        status_callback(f"Failed: {stats['failed_segments']}")

    df_out = pd.DataFrame(results)
    ts = time.strftime("%Y%m%d_%H%M%S")
    os.makedirs(out_dir or ".", exist_ok=True)
    fname = os.path.join(out_dir or ".", f"sonar_rc_results_{ts}.xlsx")
    df_out.to_excel(fname, index=False)
    return df_out, fname

# ============================ Convenience Alias ============================

def create_rcs(rows_or_df, status_callback=None, progress_callback=None, headless=True):
    """Alias für Kompatibilität mit bestehendem Aufrufschema."""
    return create_remote_configs(
        df=rows_or_df,
        template={"lobExpression": "1", "teamBindle": "", "managementType": "MERCHANDISING", "channels": ["MOBILE_PUSH"]},
        template_path="/LAYOUT-TEMPLATES/<uuid>",
        alias="alias_missing",
        status_callback=status_callback,
        progress_callback=progress_callback,
        headless=headless
    )

# ============================ Minimaler Selbsttest ============================

if __name__ == "__main__":
    # ACHTUNG: Nur zum Smoke-Test – wird 401/403 werfen, wenn keine gültigen Cookies vorliegen.
    demo = [{
        "Name": "Demo Program",
        "Description": "Demo Objective",
        "Marketplace": 4,               # UK
        "BE ID": 1750559802,            # Beispielsegment
        "Schedule Start Date": date.today().isoformat(),
        "Schedule End Date": (date.today() + timedelta(days=5)).isoformat(),
        # optionale campaignVariables:
        "notificationTitle": "Hi!",
        "notificationText": "Welcome back!",
    }]

    # Minimal-Template für Selftest
    template_demo = {
        "lobExpression": "1",
        "teamBindle": "Sonar-EU_SL_Traffic_-_SX",
        "businessGroupId": 162,
        "managementType": "MERCHANDISING",
        "channels": ["MOBILE_PUSH"],
        "startTime": "09:00",
        "endTime": "21:00",
    }

    try:
        df, fname = create_remote_configs(
            rows=demo,
            template=template_demo,
            template_path="/LAYOUT-TEMPLATES/c46dcd21-aba1-4acd-8e58-9440864bc948",
            alias="nwreth",
            out_dir=".",
            status_callback=print,
            progress_callback=lambda n: None,
            headless=True
        )
        print("Saved:", fname)
        print(df)
    except Exception as e:
        print("Selftest error:", e)
