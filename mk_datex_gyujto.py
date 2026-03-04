import requests
import pandas as pd
import os
import time
import json
import copy
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo
from lxml import etree

# ================================================================
# KONFIGURÁCIÓ – itt állítsd be a repo URL-jét!
# ================================================================

BUDAPEST_TZ  = ZoneInfo("Europe/Budapest")
API_URL      = "https://napphub.kozut.hu/hub-web//datex2/3_3/4a8b2505-df5e-4191-8c96-b98263a771b5/pullSnapshotData"
GITHUB_REPO  = "https://github.com/FELHASZNALO/REPO-NEVE.git"  # <-- ÍRD ÁT!
INTERVAL_SEC = 300  # 5 perc

# Kimeneti mappa: a szkript melletti "kimenet" mappa
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR   = os.path.join(BASE_DIR, "kimenet")
EXCEL_FILE   = os.path.join(OUTPUT_DIR, "Data.xlsx")
JSON_FILE    = os.path.join(OUTPUT_DIR, "data.json")
ANALYSIS_DIR = os.path.join(OUTPUT_DIR, "MK_adat_elemzes")

NS = {
    "s": "http://datex2.eu/schema/3/situation",
    "c": "http://datex2.eu/schema/3/common",
    "l": "http://datex2.eu/schema/3/locationReferencing",
}

COL_ORDER = [
    "situation_record_id", "rekord_id_rovid", "situation_id", "record_version", "xsi_type",
    "situation_version_time", "creation_time", "version_time",
    "overall_start", "overall_end_tervezett",
    "source_name", "road_number",
    "lat_start", "lon_start", "lat_end", "lon_end",
    "comment",
    "statusz", "Rogzites_Ideje", "Lejarva_Ideje", "Korai_lezaras"
]

# ================================================================
# SEGÉDFÜGGVÉNYEK
# ================================================================

def ensure_dirs():
    for d in [OUTPUT_DIR, ANALYSIS_DIR]:
        os.makedirs(d, exist_ok=True)

def init_git():
    """Git repo inicializálása és remote beállítása, ha még nincs."""
    git_dir = os.path.join(OUTPUT_DIR, ".git")
    if not os.path.exists(git_dir):
        subprocess.run(["git", "init"], cwd=OUTPUT_DIR, check=True)
        print("Git repo inicializálva.")
    result = subprocess.run(["git", "remote"], cwd=OUTPUT_DIR, capture_output=True, text=True)
    if "origin" not in result.stdout:
        subprocess.run(["git", "remote", "add", "origin", GITHUB_REPO], cwd=OUTPUT_DIR, check=True)
        print(f"Remote beállítva: {GITHUB_REPO}")

def git_push(ts_label):
    """Commit és push a GitHub repóba."""
    try:
        subprocess.run(["git", "add", "-A"], cwd=OUTPUT_DIR, check=True)
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=OUTPUT_DIR
        )
        if result.returncode == 0:
            print(f"  [{ts_label}] Nincs változás, push kihagyva.")
            return
        subprocess.run(
            ["git", "commit", "-m", f"Automatikus frissítés: {ts_label}"],
            cwd=OUTPUT_DIR, check=True
        )
        subprocess.run(["git", "push", "-u", "origin", "main"], cwd=OUTPUT_DIR, check=True)
        print(f"  [{ts_label}] GitHub push sikeres.")
    except subprocess.CalledProcessError as e:
        print(f"  Git hiba: {e}")

def _parse_dt(s):
    if not s:
        return None
    try:
        dt_aware = datetime.fromisoformat(str(s).replace('Z', '+00:00'))
        return dt_aware.astimezone(BUDAPEST_TZ).replace(tzinfo=None)
    except:
        return None

def _fmt_dt(s):
    dt = _parse_dt(s)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else s

def fetch_api_data():
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/xml"}
    try:
        resp = requests.get(API_URL, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.content
        else:
            print(f"  API hiba: HTTP {resp.status_code}")
    except Exception as e:
        print(f"  Kapcsolódási hiba: {e}")
    return None

def load_json():
    if not os.path.exists(JSON_FILE):
        return None
    try:
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return None

def save_json(records):
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

# ================================================================
# XML PARSOLÁS
# ================================================================

def parse_xml_to_records(xml_bytes):
    try:
        root = etree.fromstring(xml_bytes)
    except Exception as e:
        print(f"  XML parse hiba: {e}")
        return []

    situations = root.xpath(".//s:situation", namespaces=NS)
    records = []

    for sit in situations:
        rec_list = sit.xpath("./s:situationRecord", namespaces=NS)
        if not rec_list:
            continue
        rec_el = rec_list[0]
        full_rec_id = rec_el.get("id", "")
        parts = full_rec_id.split("_")
        short_id = "_".join(parts[2:]) if len(parts) > 2 else full_rec_id

        def get_val(element, path):
            res = element.xpath(path, namespaces=NS)
            return res[0].text.strip() if res and res[0].text else ""

        comment_nodes = rec_el.xpath(
            "./s:generalPublicComment/s:comment/c:values/c:value", namespaces=NS
        )
        comment = comment_nodes[0].text if comment_nodes else ""

        records.append({
            "situation_record_id":    full_rec_id,
            "rekord_id_rovid":        short_id,
            "situation_id":           sit.get("id", ""),
            "record_version":         rec_el.get("version", ""),
            "xsi_type":               (rec_el.get("{http://www.w3.org/2001/XMLSchema-instance}type") or "").split(":")[-1],
            "situation_version_time": _fmt_dt(get_val(sit, "./s:situationVersionTime")),
            "creation_time":          _fmt_dt(get_val(rec_el, "./s:situationRecordCreationTime")),
            "version_time":           _fmt_dt(get_val(rec_el, "./s:situationRecordVersionTime")),
            "overall_start":          _fmt_dt(get_val(rec_el, "./s:validity/c:validityTimeSpecification/c:overallStartTime")),
            "overall_end_tervezett":  _fmt_dt(get_val(rec_el, "./s:validity/c:validityTimeSpecification/c:overallEndTime")),
            "source_name":            "Magyar Közút",
            "road_number":            get_val(rec_el, ".//l:roadNumber"),
            "lat_start":              get_val(rec_el, ".//l:latitude"),
            "lon_start":              get_val(rec_el, ".//l:longitude"),
            "lat_end":                "",
            "lon_end":                "",
            "comment":                comment,
        })

    return records

# ================================================================
# ÖSSZEHASONLÍTÁS ÉS SNAPSHOT
# ================================================================

def compare_and_snapshot(prev_records, curr_records, ts, ts_label):
    prev_map = {r["situation_record_id"]: r for r in prev_records if "situation_record_id" in r}
    curr_map = {r["situation_record_id"]: r for r in curr_records if "situation_record_id" in r}

    prev_ids = set(prev_map.keys())
    curr_ids  = set(curr_map.keys())

    lezart_ids = prev_ids - curr_ids
    uj_ids     = curr_ids - prev_ids

    if not (lezart_ids or uj_ids):
        print(f"  Nincs változás.")
        return False

    snap_dir = os.path.join(ANALYSIS_DIR, ts_label)
    os.makedirs(snap_dir, exist_ok=True)

    # 1. Aktuális JSON mentése
    with open(os.path.join(snap_dir, f"{ts_label}.json"), "w", encoding="utf-8") as f:
        json.dump(curr_records, f, ensure_ascii=False, indent=2)

    # 2. Módosított archív JSON (lezárt rekordok jelölve)
    modositott_archiv = copy.deepcopy(prev_records)
    for rekord in modositott_archiv:
        rid = rekord.get("situation_record_id")
        if rid in lezart_ids:
            rekord["overall_end_tervezett"] = ts
            rekord["statusz"] = "LEZART"

    with open(os.path.join(snap_dir, f"{ts_label}_modosult.json"), "w", encoding="utf-8") as f:
        json.dump(modositott_archiv, f, ensure_ascii=False, indent=2)

    # 3. TXT változásnapló
    txt_path = os.path.join(snap_dir, f"{ts_label}_valtozasok.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"Változásnapló - {ts}\n" + "=" * 50 + "\n")
        f.write(f"ÚJ rekordok ({len(uj_ids)} db):\n")
        for rid in sorted(uj_ids):
            f.write(f"  {rid}\n")
        f.write(f"\nLEZÁRT rekordok ({len(lezart_ids)} db):\n")
        for rid in sorted(lezart_ids):
            f.write(f"  {rid}\n")

    print(f"  Snapshot mentve: {snap_dir}")
    print(f"  Új: {len(uj_ids)} db | Lezárt: {len(lezart_ids)} db")
    return True

# ================================================================
# EXCEL FRISSÍTÉS – Data.xlsx
# ================================================================

def update_excel(curr_records, ts):
    df_new = pd.DataFrame(curr_records)
    df_new["statusz"]        = "AKTIV"
    df_new["Rogzites_Ideje"] = ts
    df_new["Lejarva_Ideje"]  = ""
    df_new["Korai_lezaras"]  = ""

    if not os.path.exists(EXCEL_FILE):
        df_new.reindex(columns=COL_ORDER).to_excel(EXCEL_FILE, index=False)
        print(f"  Excel létrehozva: {EXCEL_FILE}")
        return

    df_old = pd.read_excel(EXCEL_FILE, dtype=str).fillna("")
    curr_ids = set(df_new["situation_record_id"])

    # Lezárás jelölése
    mask = (df_old["statusz"] == "AKTIV") & (~df_old["situation_record_id"].isin(curr_ids))
    df_old.loc[mask, "statusz"]       = "LEZART"
    df_old.loc[mask, "Lejarva_Ideje"] = ts

    # Csak valóban új rekordok hozzáadása
    df_uj = df_new[~df_new["situation_record_id"].isin(set(df_old["situation_record_id"]))]
    df_merged = pd.concat([df_old, df_uj], ignore_index=True)
    df_merged.reindex(columns=COL_ORDER).to_excel(EXCEL_FILE, index=False)
    print(f"  Excel frissítve: +{len(df_uj)} új sor, {mask.sum()} lezárva.")

# ================================================================
# FŐ FUTÁSI LOGIKA
# ================================================================

def futas(is_first=False):
    ts       = datetime.now(BUDAPEST_TZ).strftime("%Y-%m-%d %H:%M:%S")
    ts_label = datetime.now(BUDAPEST_TZ).strftime("%Y%m%d%H%M%S")
    print(f"\n[{ts}] Futás indítása...")

    xml_bytes = fetch_api_data()
    if not xml_bytes:
        print("  API nem elérhető, kihagyva.")
        return

    curr_records = parse_xml_to_records(xml_bytes)
    if not curr_records:
        print("  Üres vagy hibás XML, kihagyva.")
        return

    print(f"  Beolvasva: {len(curr_records)} rekord.")

    prev_records = load_json()
    valtozas = False

    if not is_first and prev_records:
        valtozas = compare_and_snapshot(prev_records, curr_records, ts, ts_label)
    else:
        valtozas = True  # első futásnál mindig push

    save_json(curr_records)
    update_excel(curr_records, ts)

    if valtozas:
        git_push(ts_label)
    else:
        print("  GitHub push kihagyva (nincs változás).")

# ================================================================
# BELÉPÉSI PONT
# ================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Magyar Közút DATEX II adatgyűjtő")
    print(f"Intervallum: {INTERVAL_SEC} mp ({INTERVAL_SEC // 60} perc)")
    print(f"GitHub repo: {GITHUB_REPO}")
    print("=" * 60)

    ensure_dirs()
    init_git()
    futas(is_first=True)

    while True:
        time.sleep(INTERVAL_SEC)
        try:
            futas()
        except Exception as e:
            print(f"  Váratlan hiba: {e}")
