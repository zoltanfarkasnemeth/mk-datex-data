import requests
import pandas as pd
import os
import json
import copy
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from lxml import etree

BUDAPEST_TZ = ZoneInfo("Europe/Budapest")
API_URL     = "https://napphub.kozut.hu/hub-web//datex2/3_3/4a8b2505-df5e-4191-8c96-b98263a771b5/pullSnapshotData"
EXCEL_FILE  = "Data.xlsx"
JSON_FILE   = "data.json"

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

def fetch_api_data(retries=5, delay=10):
    """API lekérés, 5x retry 10 másodperc várakozással."""
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/xml"}
    for attempt in range(1, retries + 1):
        try:
            print(f"  API lekérés ({attempt}/{retries})...")
            resp = requests.get(API_URL, headers=headers, timeout=30)
            print(f"  HTTP státusz: {resp.status_code}")
            if resp.status_code == 200:
                return resp.content
            else:
                print(f"  Hiba: {resp.status_code}")
        except Exception as e:
            print(f"  Kapcsolódási hiba: {e}")
        if attempt < retries:
            print(f"  Várakozás {delay}mp...")
            time.sleep(delay)
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

def parse_xml_to_records(xml_bytes):
    try:
        root = etree.fromstring(xml_bytes)
    except Exception as e:
        print(f"XML parse hiba: {e}")
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

def fetch_and_save():
    ts = datetime.now(BUDAPEST_TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] Adatgyűjtés indítása...")

    xml_bytes = fetch_api_data()
    if not xml_bytes:
        print("API nem elérhető 5 próbálkozás után sem. Leállás.")
        exit(0)  # exit(0) = nem hibás kilépés, git commit kihagyható

    curr_records = parse_xml_to_records(xml_bytes)
    if not curr_records:
        print("Üres vagy hibás XML. Leállás.")
        exit(0)

    print(f"Beolvasva: {len(curr_records)} rekord.")

    # ================================================================
    # ELSŐ FUTÁS
    # ================================================================
    first_run = False
    if not os.path.exists(EXCEL_FILE):
        first_run = True
    else:
        try:
            df_check = pd.read_excel(EXCEL_FILE)
            if df_check.empty or "statusz" not in df_check.columns:
                first_run = True
        except:
            first_run = True

    if first_run:
        print("ELSŐ FUTÁS – összes rekord mentése.")
        df_new = pd.DataFrame(curr_records)
        df_new["statusz"]        = "AKTIV"
        df_new["Rogzites_Ideje"] = ts
        df_new["Lejarva_Ideje"]  = ""
        df_new["Korai_lezaras"]  = ""
        df_new.reindex(columns=COL_ORDER).to_excel(EXCEL_FILE, index=False)
        save_json(curr_records)
        print(f"Elmentve: {len(df_new)} sor.")
        return

    # ================================================================
    # KÖVETKEZŐ FUTÁSOK
    # ================================================================
    df_old = pd.read_excel(EXCEL_FILE, dtype=str).fillna("")
    curr_ids     = set(r["situation_record_id"] for r in curr_records)
    existing_ids = set(df_old["situation_record_id"].astype(str))
    valtozott    = False

    # 1. LEZÁRT: AKTIV volt, de eltűnt az API-ból
    for idx, row in df_old.iterrows():
        if row["statusz"] == "AKTIV" and row["situation_record_id"] not in curr_ids:
            print(f"  LEZÁRT: {row['situation_record_id']}")
            df_old.at[idx, "statusz"]       = "LEZART"
            df_old.at[idx, "Lejarva_Ideje"] = ts
            valtozott = True

    # 2. ÚJ: API-ban van, de Excelben még nincs
    new_ids = curr_ids - existing_ids
    if new_ids:
        df_new = pd.DataFrame([r for r in curr_records if r["situation_record_id"] in new_ids])
        df_new["statusz"]        = "AKTIV"
        df_new["Rogzites_Ideje"] = ts
        df_new["Lejarva_Ideje"]  = ""
        df_new["Korai_lezaras"]  = ""
        print(f"  ÚJ bejegyzések: {len(df_new)} db")
        df_old = pd.concat([df_old, df_new], ignore_index=True)
        valtozott = True

    if not valtozott:
        print("Nincs változás – Excel nem módosul.")
        save_json(curr_records)
        return

    df_old.reindex(columns=COL_ORDER).to_excel(EXCEL_FILE, index=False)
    save_json(curr_records)
    print(f"Excel frissítve. Összes sor: {len(df_old)}")

if __name__ == "__main__":
    fetch_and_save()
