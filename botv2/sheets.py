"""
sheets — buying-sheet create + fill.

Sheets are created by the SAME Google Apps Script web app v1 uses (template copy),
so the format Kevin pays off — and the downstream !summary / payments / nightly
reconcile that read these sheets — stay identical. The only v2 change is the fill:
rows come from the portal, so column H is written as the **offer rate** (a number,
portal-sourced) instead of v1's band formula. No band formula, no select-list
stamp, no CardLadder-comp scrape here — the portal already decided.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

from . import certs, config

# Google libs are imported lazily inside the auth helpers below, so `import
# botv2.sheets` (and the whole bot) works in environments where those libs aren't
# installed — sheet I/O only touches them at runtime with real credentials.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
_SUMMARY_SUFFIX = " — summary (KTS Collectibles)"


# ── auth ──────────────────────────────────────────────────────────────────────
def get_credentials():
    from google.oauth2.service_account import Credentials
    if config.GOOGLE_CREDENTIALS_JSON:
        return Credentials.from_service_account_info(
            json.loads(config.GOOGLE_CREDENTIALS_JSON), scopes=SCOPES)
    return Credentials.from_service_account_file(config.GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)


def get_gspread_client():
    import gspread
    return gspread.authorize(get_credentials())


def get_drive_service():
    from googleapiclient.discovery import build
    return build("drive", "v3", credentials=get_credentials())


# ── create (Apps Script + Drive-fallback recovery, ported from v1) ──────────────
def _find_fresh_psa_sheet(username: str, since_dt: datetime, folder_id: str):
    """The Apps Script usually finishes creating the sheet server-side even when
    our HTTP call dies. Find a '<username> discord …' sheet created after we
    started, so a retry doesn't make a duplicate. Exact-prefix match only (a bare
    startswith let 'mark' adopt 'markandjheyson's sheet)."""
    try:
        drive = get_drive_service()
        safe = username.replace("\\", "\\\\").replace("'", "\\'")
        since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%S")
        q = (f"name contains '{safe} discord' and '{folder_id}' in parents "
             f"and trashed=false and createdTime > '{since_iso}'")
        files = drive.files().list(q=q, orderBy="createdTime desc", pageSize=5,
                                   fields="files(id,name,createdTime)").execute().get("files", [])
        want = username.lower() + " discord"
        for f in files:
            fname = f["name"].lower()
            if (fname == want or fname.startswith(want + " ")) and not f["name"].endswith(_SUMMARY_SUFFIX):
                return f
    except Exception as e:
        print(f"[sheets] Drive fallback search failed: {e}")
    return None


def create_psa_sheet(username: str, cert_tokens: list, folder_id: str = None):
    """Create a buying sheet via the Apps Script. Returns (url, name, data).
    Google is slow/flaky under load (a 103-cert sheet took 54s on a healthy run)
    and failed calls usually still created the sheet — so: generous timeout, then
    poll Drive for the sheet Google may have finished anyway (no duplicate), then
    retry the call once."""
    folder_id = folder_id or config.PSA_FOLDER_ID
    certs_str = ",".join(certs.cert_digits(c) for c in cert_tokens)   # column B = bare digits
    params = urllib.parse.urlencode({"username": username, "certs": certs_str, "folder_id": folder_id})
    url = f"{config.APPS_SCRIPT_URL}?{params}"
    started = datetime.utcnow() - timedelta(seconds=30)   # slack for clock skew vs Drive
    last_err = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "KTS-Bot/2.0"})
            with urllib.request.urlopen(req, timeout=150) as resp:
                data = json.loads(resp.read().decode())
            if not data.get("success"):
                raise Exception(data.get("error", "Unknown error from Apps Script"))
            return data["url"], data["name"], data
        except Exception as e:
            last_err = e
            print(f"[sheets] Apps Script call failed (attempt {attempt}/2): {e}")
            for _ in range(6):                      # poll Drive ~60s
                time.sleep(10)
                f = _find_fresh_psa_sheet(username, started, folder_id)
                if f:
                    surl = f"https://docs.google.com/spreadsheets/d/{f['id']}/edit"
                    print(f"[sheets] recovered server-side sheet '{f['name']}' from Drive")
                    return surl, f["name"], {"sheet_id": f["id"], "url": surl,
                                             "name": f["name"], "recovered": True}
    raise last_err


# ── fill (from portal rows) ────────────────────────────────────────────────────
def _sheet_id_from_url(sheet_url: str) -> str:
    # https://docs.google.com/spreadsheets/d/<ID>/edit
    try:
        return sheet_url.split("/d/")[1].split("/")[0]
    except Exception:
        return sheet_url


def save_tracking(sheet_url: str, tracking: str) -> None:
    """Write a tracking number onto the buying sheet via the Apps Script."""
    sid = _sheet_id_from_url(sheet_url) if "/d/" in str(sheet_url) else sheet_url
    data = json.dumps({"action": "update_tracking", "sheet_id": sid,
                       "tracking": tracking}).encode()
    req = urllib.request.Request(config.APPS_SCRIPT_URL, data=data,
                                 headers={"Content-Type": "application/json",
                                          "User-Agent": "KTS-Bot/2.0"})
    urllib.request.urlopen(req, timeout=15)


def fill_buying_sheet(sheet_id: str, rows, sheet_name: str = None) -> list:
    """Write portal rows into the buying sheet the Apps Script just made. Columns:
      A = PSA/CGC   C = CardLadder link   D = name   E = grade   F = category
      G = comp value   H = offer RATE (number, portal-sourced; template I=G*H → payout)
    Returns cert tokens that had no row in column B (caller alerts Kevin)."""
    if not rows:
        return []
    sheet_name = sheet_name or config.SHEET_FORM_NAME
    sheet_id = _sheet_id_from_url(sheet_id) if "/d/" in str(sheet_id) else sheet_id
    gc = get_gspread_client()
    ss = gc.open_by_key(sheet_id)
    try:
        sheet = ss.worksheet(sheet_name)
    except Exception:
        sheet = ss.sheet1

    # Map bare-digit cert -> row from column B (unformatted re-read catches CGC
    # certs a cell's Automatic format turned into 6.19E+09).
    cert_to_row = {}
    for i, val in enumerate(sheet.col_values(2)):
        v = str(val).strip()
        if v.isdigit() and 6 <= len(v) <= 13:
            cert_to_row[v] = i + 1
    want_digits = {certs.cert_digits(r.cert) for r in rows}
    if not want_digits.issubset(cert_to_row.keys()):
        try:
            for i, val in enumerate(sheet.col_values(2, value_render_option="UNFORMATTED_VALUE")):
                v = str(val).strip().split(".")[0]
                if v.isdigit() and v in want_digits and v not in cert_to_row:
                    cert_to_row[v] = i + 1
        except Exception as e:
            print(f"[sheets] unformatted cert re-read failed: {e}")

    updates, missing = [], []
    for r in rows:
        digits = certs.cert_digits(r.cert)
        row = cert_to_row.get(digits)
        if not row:
            missing.append(r.cert)
            continue
        is_cgc = certs.is_cgc_cert(r.cert)
        link = f"https://app.cardladder.com/search?cert={digits}&grader={'cgc' if is_cgc else 'psa'}"
        updates.append({"range": f"A{row}", "values": [["CGC" if is_cgc else "PSA"]]})
        updates.append({"range": f"C{row}", "values": [[f'=HYPERLINK("{link}","🔗 CardLadder")']]})
        if r.card_name:
            updates.append({"range": f"D{row}", "values": [[r.card_name]]})
        grade = f"CGC {r.grade}" if is_cgc else str(r.grade or "").replace("PSA", "").strip()
        if grade:
            updates.append({"range": f"E{row}", "values": [[grade]]})
        if r.category:
            updates.append({"range": f"F{row}", "values": [[r.category.lower().strip()]]})
        # G = comp value; H = offer rate (number). Non-buy rows pay 0; transport
        # errors show the resend mark (customer-visible, never "not found").
        if r.error:
            updates.append({"range": f"G{row}", "values": [[config.SHEET_NOT_LOADED_MARK]]})
            updates.append({"range": f"H{row}", "values": [[0]]})
        elif r.market_value is not None:
            updates.append({"range": f"G{row}", "values": [[r.market_value]]})
            rate = round(r.offer / r.market_value, 6) if (r.is_buy and r.offer and r.market_value) else 0
            updates.append({"range": f"H{row}", "values": [[rate]]})
        else:
            updates.append({"range": f"G{row}", "values": [["not found"]]})
            updates.append({"range": f"H{row}", "values": [[0]]})
    if updates:
        sheet.batch_update(updates, value_input_option="USER_ENTERED")
    return missing
