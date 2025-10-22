# create_os_sonar.py
# -*- coding: utf-8 -*-
"""
Create One-Shot Sonar: erstellt Program + Campaign aus vorbereiteten Jobs.

Erwarteter Job-Eintrag (pro Zeile aus deiner Excel):
{
  "program": {
      "name": str,
      "description": str,                # fallback: name
      "marketplaceId": int               # fallback: 4
  },
  "campaign": {
      "name": str,                       # fallback: program.name
      "description": str,                # fallback: program.description
      "startDate": "YYYY-MM-DD",         # endDate = startDate (OneShot)
  },
  "template": {
      "channel": "MOBILE_PUSH|EMAIL",
      "teamBindle": str,
      "lobExpression": str,
      "managementType": str,
      "businessGroupId": int,
      "familyId": int,
      "optOuts": list,                   # wird trotzdem leer übergeben (Vorgabe)
      "startTimeMinutesOffset": int,     # z.B. 540  (09:00)
      "endTimeMinutesOffset": int        # z.B. 1260 (21:00)
  },
  # optional je Job:
  "mp": {
      "displayName": str,
      "emailClientFromField": str,
      "replyQueue": str
  }
}
"""
from __future__ import annotations

import os
import json
import time
import csv
import sqlite3
import tempfile
import shutil
from typing import Callable, Dict, List, Tuple, Optional

import requests

# Optionaler Selenium-Fallback wie in get_sizes
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FxService
from selenium.webdriver.firefox.options import Options as FxOptions

from utils import get_firefox_profile


# -------------------- Konfiguration --------------------

DRY_RUN = False  # True = keine POSTs, nur Payload-Vorschau/Export

# Wir posten gegen den Service-Host (liefert JSON). Cookies kommen vom Web-Login.
SONAR_WEB_DOMAIN = "sonar-eu.amazon.com"
SONAR_SERVICE_HOST = "sonar-service-eu-ca-dub.dub.proxy.amazon.com"
SONAR_SERVICE_BASE = f"https://{SONAR_SERVICE_HOST}"

# Minimal bekannte MP-Defaults (bei EMAIL nützlich; ergänzbar)
MP_DEFAULTS: Dict[int, Dict[str, str]] = {
    4: {  # DE
        "displayName": "Amazon.de",
        "emailClientFromField": "Amazon.de",
        "replyQueue": "promotion5@amazon.de",
    },
    # 3: {...}, 5: {...} etc. – nach Bedarf erweitern
}


# -------------------- Cookies & Session (wie get_sizes) --------------------

def _copy_sqlite_readonly(src_path: str) -> Tuple[str, str]:
    """Gesperrte cookies.sqlite nach /tmp duplizieren, damit wir sie read-only öffnen können."""
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"cookies.sqlite not found at: {src_path}")
    tmpdir = tempfile.mkdtemp(prefix="ff_cookies_")
    dst = os.path.join(tmpdir, "cookies.sqlite")
    shutil.copy2(src_path, dst)
    return dst, tmpdir


def _load_firefox_cookies_for_suffixes(profile_path: str, suffixes: List[str]) -> requests.cookies.RequestsCookieJar:
    """
    Lädt alle Cookies, deren Host LIKE einem der angegebenen Suffixe entspricht.
    Zusätzlich nehmen wir auch generische .amazon.com-Cookies mit.
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


def _build_http_session(profile_path: str) -> requests.Session:
    jar = _load_firefox_cookies_for_suffixes(
        profile_path,
        [SONAR_WEB_DOMAIN, SONAR_SERVICE_HOST]
    )
    s = requests.Session()
    s.cookies = jar
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",         # <— neu
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
    """
    Öffnet headless Firefox mit demselben Profil auf der Web-Seite und holt frische Cookies.
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
        driver.get(f"https://{SONAR_WEB_DOMAIN}/")
        time.sleep(3)  # SSO/Cookies setzen lassen
        cookies = driver.get_cookies()
        return _cookiejar_from_selenium_cookies(cookies)
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


# -------------------- Payload Builder --------------------

def _ensure(val, fallback):
    return val if (val is not None and str(val).strip() != "") else fallback


def _program_payload(job: Dict, requester_alias: str) -> Dict:
    p = job["program"]
    t = job["template"]
    mp = int((p.get("marketplaceId") or 4))

    payload = {
        "type": "MANUAL",
        "familyId": int(t["familyId"]),
        "marketplaceId": mp,
        "name": p["name"],
        "description": (p.get("description") or p["name"]),
        "creator": requester_alias,
        "businessOwner": requester_alias,
        "businessGroupId": int(t["businessGroupId"]),
        "managementType": t["managementType"],
        "teamBindle": t["teamBindle"],
        "topic": "CAFEP",
        "channel": t["channel"],  # wie in deinem cURL
        "inboxManagementWindow": 1,
        "communicationContentType": {"optOutList": []},
    }
    # wichtig: wie in deinem funktionierenden cURL
    if t.get("templateId"):
        payload["templateId"] = int(t["templateId"])
    
    # Für alle anderen MPs ist lobExpression Pflicht.
    lob_expr = (t.get("lobExpression") or "").strip()
    if mp != 3:
        if not lob_expr:
            raise ValueError("template.lobExpression is required for marketplaceId != 3")
        payload["lobExpression"] = lob_expr



    # EMAIL-Felder optional
    mpd = {**MP_DEFAULTS.get(mp, {}), **(job.get("mp") or {})}
    if t["channel"].upper() == "EMAIL":
        if mpd.get("replyQueue"):           payload["replyQueue"] = mpd["replyQueue"]
        if mpd.get("emailClientFromField"): payload["emailClientFromField"] = mpd["emailClientFromField"]
        if mpd.get("displayName"):          payload["displayName"] = mpd["displayName"]

    return payload



def _campaign_payload(job: Dict, owner_alias: str) -> Dict:
    p = job["program"]
    c = job["campaign"]
    t = job["template"]

    name = (c.get("name") or p.get("name"))
    desc = (c.get("description") or p.get("description") or p.get("name"))
    start = c["startDate"]

    return {
        "owner": owner_alias,
        "schedule": {
            "startDate": start,
            "endDate": start,  # OneShot
            "startTimeMinutesOffset": int(t["startTimeMinutesOffset"]),
            "endTimeMinutesOffset": int(t["endTimeMinutesOffset"]),
        },
        "duration": 1,
        "name": name,
        "description": f"{desc}   $Reason : OneShot",
        "reason": "OneShot",
    }



# -------------------- HTTP Helper --------------------

def _safe_parse_json(resp: requests.Response) -> dict:
    txt = resp.text or ""
    ct = (resp.headers.get("Content-Type") or "").lower()
    if "json" in ct or txt.strip().startswith(("{", "[")):
        try:
            return resp.json()
        except Exception:
            try:
                return json.loads(txt)
            except Exception:
                pass
    # Nicht-JSON: aussagekräftigen Fehler werfen
    snippet = txt[:500].replace("\n", " ").replace("\r", " ")
    raise RuntimeError(f"HTTP {resp.status_code} non-JSON response. Snippet: {snippet!r}")

# (neue Funktion einfügen – direkt NACH _safe_parse_json)
def _post_json(session: requests.Session,
               url: str,
               payload: Dict,
               timeout: Tuple[int, int] = (10, 60)) -> dict:
    """
    Einfacher JSON-POST mit Fehlerbehandlung + JSON-Parsing.
    Wirf bei HTTP-Fehlern eine requests.HTTPError mit response.
    """
    resp = session.post(url, json=payload, timeout=timeout)
    # Bei 4xx/5xx direkt HTTPError werfen (für Upstream-Handling)
    if resp.status_code >= 400:
        try:
            # Versuche evtl. Fehlermeldung aus JSON zu ziehen (nur zum Debuggen sinnvoll)
            _ = _safe_parse_json(resp)
        except Exception:
            pass
        # raise_for_status hängt die response an -> upstream kann status_code prüfen
        resp.raise_for_status()

    # Erfolgsfall: JSON (oder RuntimeError, falls kein JSON zurückkam)
    return _safe_parse_json(resp)


# (neue Funktion einfügen – kann NACH _post_json stehen)
def _post_json_with_refresh(session: requests.Session,
                            url: str,
                            payload: Dict,
                            profile_path: str,
                            headless: bool = True,
                            timeout: Tuple[int, int] = (10, 60)) -> dict:
    """
    Wie _post_json, aber mit Auth-Refresh-Fallback über Selenium bei 401/403.
    """
    try:
        return _post_json(session, url, payload, timeout=timeout)
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (401, 403):
            # Cookies via Selenium auffrischen und erneut versuchen
            fresh = _selenium_refresh_session_cookies(profile_path, headless=headless)
            if fresh:
                session.cookies = fresh
                return _post_json(session, url, payload, timeout=timeout)
        # andere Fehler weiterreichen
        raise


def _create_campaign_for_program(session: requests.Session,
                                 program_id: str,
                                 campaign_payload: Dict,
                                 profile_path: str,
                                 headless: bool) -> dict:
    # programId sicherheitshalber auch im Body mitsenden
    payload = dict(campaign_payload)
    payload.setdefault("programId", int(program_id) if program_id.isdigit() else program_id)

    # 1) bevorzugt: /programs/{id}/campaigns
    url1 = f"{SONAR_SERVICE_BASE}/programs/{program_id}/campaigns"
    try:
        return _post_json_with_refresh(session, url1, payload, profile_path, headless, timeout=(10, 60))
    except requests.HTTPError as e:
        # Bei 404/405/501 o.ä. auf die alte Query-Variante fallen
        if getattr(e.response, "status_code", None) in (404, 405, 501):
            pass
        else:
            raise
    except RuntimeError as e:
        # Wenn klar 404/405 im Text steht, fallback probieren
        msg = str(e).lower()
        if "404" in msg or "405" in msg:
            pass
        else:
            raise

    # 2) Fallback: /campaigns?programId=...
    url2 = f"{SONAR_SERVICE_BASE}/campaigns?programId={program_id}"
    return _post_json_with_refresh(session, url2, payload, profile_path, headless, timeout=(10, 60))



# -------------------- Public Runner --------------------

def run_create_os_sonar(
    jobs: List[Dict],
    status_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    headless: bool = True,
    requester_alias: Optional[str] = None,
) -> Tuple[List[Dict], str]:
    """
    Führt die Erstellung Program+Campaign für alle Jobs aus.
    Rückgabe: (results_list, out_path)
      results_list: [{programId, campaignId, programName, campaignName, ok, error, marketplaceId, channel}, ...]
      out_path: Pfad zu Excel/CSV mit der Übersicht
    """
    status = (lambda m: None) if status_callback is None else status_callback
    progress = (lambda i: None) if progress_callback is None else progress_callback

    if not jobs:
        raise ValueError("No jobs provided.")

    # Firefox-Profil (wie get_sizes)
    profile_path = get_firefox_profile()
    if not profile_path or not os.path.isdir(profile_path):
        raise RuntimeError("Kein Firefox-Profil gefunden. Bitte Firefox einmal öffnen/einloggen.")

    # Session aus Profil-Cookies aufbauen (wie get_sizes)
    s = _build_http_session(profile_path)

    results: List[Dict] = []
    ts0 = time.time()

    for idx, job in enumerate(jobs):
        progress(idx)
        pname = job["program"]["name"]
        cname = _ensure(job["campaign"].get("name"), pname)
        mp = int(_ensure(job["program"].get("marketplaceId"), 4))
        channel = job["template"]["channel"]

        try:
            status(f"[{idx+1}/{len(jobs)}] Building payloads…")
            prog_payload = _program_payload(job, requester_alias or "me")


            if DRY_RUN:
                program_id = "DRY_PROG"
                campaign_id = "DRY_CAMP"
            else:
                # Program
                status(f"[{idx+1}/{len(jobs)}] Creating program: {pname}")
                url_prog = f"{SONAR_SERVICE_BASE}/programs"
                prog_resp = _post_json(s, url_prog, prog_payload)
                program_id = str(
                    prog_resp.get("id")
                    or prog_resp.get("programId")
                    or (prog_resp.get("program") or {}).get("id")
                    or ""
                )
                if not program_id:
                    raise RuntimeError("Program creation returned no ID.")

                # Campaign
                status(f"[{idx+1}/{len(jobs)}] Creating campaign for program {program_id}: {cname}")
                url_camp = f"{SONAR_SERVICE_BASE}/campaigns?programId={program_id}"
                camp_payload = _campaign_payload(job, requester_alias or "me")
                camp_resp = _post_json(s, url_camp, camp_payload)
                campaign_id = str(
                    camp_resp.get("campaignId")
                    or camp_resp.get("id")
                    or (camp_resp.get("campaign") or {}).get("id")
                    or ""
                )
                if not campaign_id:
                    raise RuntimeError("Campaign creation returned no ID.")

            results.append({
                "ok": True,
                "error": "",
                "programId": program_id,
                "campaignId": campaign_id,
                "programName": pname,
                "campaignName": cname,
                "marketplaceId": mp,
                "channel": channel,
            })
            status(f"[{idx+1}/{len(jobs)}] Done. IDs: program={program_id}, campaign={campaign_id}")

        except Exception as e:
            results.append({
                "ok": False,
                "error": str(e),
                "programId": None,
                "campaignId": None,
                "programName": pname,
                "campaignName": cname,
                "marketplaceId": mp,
                "channel": channel,
            })
            status(f"[{idx+1}/{len(jobs)}] FAILED: {e}")

    # Fortschritt finalisieren
    if jobs:
        progress(len(jobs) - 1)

    # Ergebnisdatei schreiben
    out_path = None
    ts = int(time.time())

    def _mk_sonar_link(mp, cid):
        try:
            mp_s = str(int(mp))
            cid_s = str(int(cid))
            return f"https://sonar-eu.amazon.com/#/{mp_s}/campaigns/{cid_s}"
        except Exception:
            return ""

    try:
        import pandas as pd  # optional – wenn vorhanden, als .xlsx speichern
        df_rows = []
        for r in results:
            df_rows.append({
                "ok": r["ok"],
                "error": r["error"],
                "programId": r["programId"],
                "campaignId": r["campaignId"],
                "programName": r["programName"],
                "campaignName": r["campaignName"],
                "marketplaceId": r["marketplaceId"],
                "channel": r["channel"],
                # "Sonar Link" bewusst als letztes Feld einfügen
                "Sonar Link": _mk_sonar_link(r["marketplaceId"], r["campaignId"]),
            })
        df = pd.DataFrame(df_rows)
        out_path = os.path.abspath(f"create_os_sonar_results_{ts}.xlsx")
        df.to_excel(out_path, index=False)
    except Exception:
        import csv  # CSV-Fallback
        out_path = os.path.abspath(f"create_os_sonar_results_{ts}.csv")
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ok","error","programId","campaignId","programName","campaignName",
                "marketplaceId","channel","Sonar Link"
            ])
            for r in results:
                w.writerow([
                    r["ok"], r["error"], r["programId"], r["campaignId"],
                    r["programName"], r["campaignName"], r["marketplaceId"], r["channel"],
                    _mk_sonar_link(r["marketplaceId"], r["campaignId"])
                ])

    return results, out_path



# Optional: Standalone-Test (benötigt gültiges Firefox-Login)
if __name__ == "__main__":
    # Minimaler Dry-Run zum Form-Check (setzt DRY_RUN=True für lokalen Test)
    DRY_RUN = True
    sample_jobs = [{
        "program": {"name": "testDE", "description": "testDE", "marketplaceId": 4},
        "campaign": {"name": "testDE", "description": "testDE", "startDate": "2025-10-01"},
        "template": {
            "channel": "MOBILE_PUSH",
            "teamBindle": "yourTeamBindle",
            "lobExpression": "yourLobExpr",
            "managementType": "yourMgmtType",
            "businessGroupId": 1234,
            "familyId": 5678,
            "optOuts": [],
            "startTimeMinutesOffset": 540,
            "endTimeMinutesOffset": 1260,
        },
    }]
    res, path = run_create_os_sonar(sample_jobs, print, lambda i: None, headless=True, requester_alias="me")
    print("Saved:", path)
    print(res)
