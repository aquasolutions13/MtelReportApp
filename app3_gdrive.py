# app3_gdrive.py
# Analiza prodaje Austrija — lokalno + Google Drive (uz .streamlit/secrets.toml)
# Ne sadrži privatne ključeve — očekuje se GDRIVE_SERVICE_ACCOUNT_JSON i GDRIVE_FOLDER_ID u secrets.

import os
import re
import io
import zipfile
import json
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st

# --- Try importing Google libs (fail with clear error if missing) ---
try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    GOOGLE_AVAILABLE = True
except Exception:
    GOOGLE_AVAILABLE = False

st.set_page_config(page_title="Analiza prodaje Austrija", layout="wide")

# =========================================
# GDRIVE helpers (use st.secrets for credentials)
# =========================================
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def build_drive():
    if not GOOGLE_AVAILABLE:
        st.error("Google paketi nisu instalirani. Pokreni:\n\npip install google-api-python-client google-auth google-auth-httplib2")
        st.stop()

    sa_json = st.secrets.get("GDRIVE_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json:
        st.error("Nedostaje GDRIVE_SERVICE_ACCOUNT_JSON u .streamlit/secrets.toml")
        st.stop()
    try:
        payload = json.loads(sa_json)
    except Exception as e:
        st.error("Ne mogu parsirati GDRIVE_SERVICE_ACCOUNT_JSON iz secrets. Provjeri JSON format.")
        st.stop()
    creds = Credentials.from_service_account_info(payload, scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_list_excel_in_folder(drive, folder_id):
    q = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false"
    items = []
    page_token = None
    pattern = re.compile(r"^ALL_CONTRACT_(20\d{2})_(0[1-9]|1[0-2])\.xlsx$")
    while True:
        resp = drive.files().list(q=q, fields="nextPageToken, files(id, name)", pageToken=page_token).execute()
        for f in resp.get("files", []):
            if pattern.match(f["name"]):
                items.append(f)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    def _key(f):
        m = re.search(r"ALL_CONTRACT_(20\d{2})_(0[1-9]|1[0-2])\.xlsx$", f["name"])
        return (int(m.group(1)), int(m.group(2))) if m else (0, 0)
    return sorted(items, key=_key)

def drive_download_bytes(drive, file_id):
    buf = io.BytesIO()
    request = drive.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    buf.seek(0)
    return buf.read()

# =========================================
# Utility & report logic (same as stable local app)
# =========================================
def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(s).strip().lower())

def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    colmap = {norm(c): c for c in df.columns.astype(str)}
    for k, orig in colmap.items():
        for c in candidates:
            if c == k or c in k:
                return orig
    return None

def parse_excel_datetime(val):
    if pd.isna(val):
        return pd.NaT
    if isinstance(val, pd.Timestamp):
        return val
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        try:
            return pd.to_datetime(val, unit="d", origin="1899-12-30")
        except Exception:
            pass
    s = str(val).strip()
    for fmt in ("%d.%m.%y %H:%M", "%d.%m.%Y %H:%M"):
        try:
            return pd.to_datetime(datetime.strptime(s, fmt))
        except Exception:
            continue
    return pd.to_datetime(s, dayfirst=True, errors="coerce")

def truthy(v) -> bool:
    if pd.isna(v):
        return False
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    return str(v).strip().lower() in {"true", "1", "da", "yes", "y", "t", "x"}

def has_value(v) -> bool:
    if pd.isna(v):
        return False
    if isinstance(v, str):
        s = v.strip().lower()
        return len(s) > 0 and s not in {"nan", "none", "null"}
    return True

def map_pos_tip(pos_val: str) -> str:
    s = "" if pd.isna(pos_val) else str(pos_val)
    su = s.upper()
    if su.startswith("VIP_GCP"): return "VIP_GCP"
    if su.startswith("VIP_LHP"): return "VIP_LHP"
    if su.startswith("VIP_SSA"): return "VIP_SSA"
    if su.startswith("VIP_WHE"): return "VIP_WHE"
    if su.startswith("VIP_WLC"): return "VIP_WLC"
    if su.startswith("VIP_WTR"): return "VIP_WTR"
    if su.startswith("VIP_"):    return "VIP_OTHER"
    if "WMH" in su: return "WMH"
    if "WTH" in su: return "WTH"
    if "WDZ" in su: return "WDZ"
    if "WJH" in su: return "WJH"    
    if "HARTLAUER" in su: return "HARTLAUER"
    if "WEB" in su: return "WEB"
    if "TELESALES" in su: return "TELESALES"
    if "D2" in su: return "D2D"
    return "MULTIBREND"

def services_for_row(r: pd.Series) -> list[str]:
    s = []
    if r.get("_mobile", False):
        s.append("MOBILE")
    if r.get("_tv", False):
        s.append("TV")
    if r.get("_inet_modem", False):
        s.append("INTERNET")
        if r.get("_price_tv", False):
            s.append("TV")
    if r.get("_mobile", False) and not r.get("_tv", False) and r.get("_price_tv", False):
        s.append("TV GO")
    return s

def derive_period_from_filename(name: str):
    m = re.search(r"ALL_CONTRACT_(20\d{2})_(0[1-9]|1[0-2])\.xlsx$", name or "")
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None

def with_totals_pivot(pivot_df: pd.DataFrame) -> pd.DataFrame:
    df = pivot_df.copy()
    df["UKUPNO"] = df.select_dtypes(include=[np.number]).sum(axis=1)
    total_row = pd.DataFrame(df.select_dtypes(include=[np.number]).sum()).T
    total_row.index = ["UKUPNO"]
    out = pd.concat([df, total_row], axis=0)
    for c in out.columns:
        if pd.api.types.is_numeric_dtype(out[c]):
            out[c] = out[c].astype(int)
    return out

def with_total_row(df: pd.DataFrame, value_col: str = "broj", label_col: str | None = None, label: str = "UKUPNO") -> pd.DataFrame:
    total = int(df[value_col].sum()) if value_col in df.columns else 0
    if label_col and label_col in df.columns:
        total_row = pd.DataFrame({label_col: [label], value_col: [total]})
        return pd.concat([df, total_row], ignore_index=True)
    total_row = pd.DataFrame({value_col: [total]})
    total_row.index = [label]
    return pd.concat([df, total_row], ignore_index=False)

# --- Loader za Excel fajlove (popravlja styles.xml ako treba) ---
MIN_STYLES_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font><name val="Calibri"/><family val="2"/></font></fonts>
  <fills count="1"><fill><patternFill patternType="none"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/></border></borders>
  <cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
</styleSheet>
"""

def repair_xlsx(file_like: io.BytesIO) -> io.BytesIO:
    inp = io.BytesIO(file_like.read())
    inp.seek(0)
    out_buf = io.BytesIO()
    with zipfile.ZipFile(inp, "r") as zin, zipfile.ZipFile(out_buf, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            if item.filename.lower() == "xl/styles.xml":
                zout.writestr(item, MIN_STYLES_XML.encode("utf-8"))
            else:
                data = zin.read(item.filename)
                if item.filename.lower().endswith((".xml", ".rels")):
                    try:
                        txt = data.decode("utf-8")
                        txt = re.sub(r"http:\s*//", "http://", txt)
                        txt = re.sub(r"https:\s*//", "https://", txt)
                        data = txt.encode("utf-8")
                    except Exception:
                        pass
                zout.writestr(item, data)
    out_buf.seek(0)
    return out_buf

def load_one_excel(file_bytes: bytes, filename_hint: str) -> pd.DataFrame:
    frames = []
    bio = io.BytesIO(file_bytes)
    try:
        xls = pd.ExcelFile(bio, engine="openpyxl")
        for s in xls.sheet_names:
            d = pd.read_excel(xls, sheet_name=s)
            d["__source_sheet__"] = s
            d["__file__"] = os.path.basename(filename_hint)
            frames.append(d)
    except Exception:
        bio2 = repair_xlsx(io.BytesIO(file_bytes))
        xls = pd.ExcelFile(bio2, engine="openpyxl")
        for s in xls.sheet_names:
            d = pd.read_excel(xls, sheet_name=s)
            d["__source_sheet__"] = s
            d["__file__"] = os.path.basename(filename_hint)
            frames.append(d)
    return pd.concat(frames, ignore_index=True)

def load_many_excels_from_folder(folder: str, pattern=r"^ALL_CONTRACT_(20\d{2})_(0[1-9]|1[0-2])\.xlsx$"):
    if not os.path.isdir(folder):
        return pd.DataFrame(), []
    names = [n for n in os.listdir(folder) if re.match(pattern, n)]
    paths = [os.path.join(folder, n) for n in names]
    all_frames = []
    meta = []
    for p in paths:
        with open(p, "rb") as fh:
            df = load_one_excel(fh.read(), os.path.basename(p))
            all_frames.append(df)
            y, m = derive_period_from_filename(os.path.basename(p))
            meta.append((p, y, m))
    return (pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame(),
            sorted(meta, key=lambda x: (x[1], x[2])))

# =========================================
# Normalizacija i logika
# =========================================
def normalize_df(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    col_mobile         = find_col(raw, ["log_mobiletariff", "mobiletariff"])
    col_tv             = find_col(raw, ["log_tvtariff", "tvtariff"])
    col_price_tv       = find_col(raw, ["price_tv", "tv_price"])
    col_inet_dev       = find_col(raw, ["internetdevice", "internet_device"])
    col_tv_device      = find_col(raw, ["tvdevice"])
    col_mobile_device  = find_col(raw, ["mobiledevice"])
    col_pos            = find_col(raw, ["pos", "pos_name", "shop", "store"])
    col_tariff         = find_col(raw, ["log_tariffname", "tariff", "tarifa", "naziv_tarife", "plan", "paket", "h"])
    col_start          = find_col(raw, ["start"])
    col_contract_type  = find_col(raw, ["contracttype"])

    df = raw.copy()

    df["_start_dt"]   = df[col_start].apply(parse_excel_datetime) if col_start in df.columns else pd.NaT
    df["_mobile"]     = df[col_mobile].map(truthy) if col_mobile in df.columns else False
    df["_tv"]         = df[col_tv].map(truthy) if col_tv in df.columns else False
    df["_price_tv"]   = df[col_price_tv].map(has_value) if col_price_tv in df.columns else False
    df["_inet_modem"] = df[col_inet_dev].astype(str).str.strip().str.upper().eq("MODEM") if col_inet_dev in df.columns else False

    df["POS_TIP"]     = df[col_pos].apply(map_pos_tip) if col_pos in df.columns else "MULTIBREND"
    df["_is_business"]= (df[col_contract_type].astype(str).str.strip().str.upper().eq("BUSINESS")
                         if col_contract_type in df.columns else False)
    df["POS_TIP_BUS"] = np.where(df["_is_business"], "BUSINESS", df["POS_TIP"])

    df["_services"]   = df.apply(services_for_row, axis=1)

    cols = dict(
        mobile=col_mobile, tv=col_tv, price_tv=col_price_tv, inet_dev=col_inet_dev,
        tv_device=col_tv_device, mobile_device=col_mobile_device, pos=col_pos,
        tariff=col_tariff, start=col_start, contract_type=col_contract_type
    )
    return df, cols

def mark_contract_tip_single_file(df: pd.DataFrame) -> pd.DataFrame:
    fname = str(df["__file__"].iloc[0]) if "__file__" in df.columns else ""
    y_hint, m_hint = derive_period_from_filename(fname)
    if not (y_hint and m_hint):
        counts = df["_start_dt"].dropna().dt.to_period("M").value_counts()
        if len(counts) > 0:
            per = counts.index[0]
            y_hint, m_hint = per.year, per.month
        else:
            now = datetime.now(); y_hint, m_hint = now.year, now.month
    df["CONTRACT_TIP"] = np.where(
        (df["_start_dt"].dt.year == y_hint) & (df["_start_dt"].dt.month == m_hint),
        "NOVI", "PRODUŽENJE"
    )
    return df

def mark_contract_tip_multi_file(df: pd.DataFrame) -> pd.DataFrame:
    def _infer(row):
        m = re.search(r"(20\d{2})_(0[1-9]|1[0-2])", str(row.get("__file__", "")))
        if m and not pd.isna(row.get("_start_dt", pd.NaT)):
            y, mo = int(m.group(1)), int(m.group(2))
            return "NOVI" if (row["_start_dt"].year == y and row["_start_dt"].month == mo) else "PRODUŽENJE"
        return "PRODUŽENJE"
    df["CONTRACT_TIP"] = df.apply(_infer, axis=1)
    return df

def tbl_filter(t_tbl: pd.DataFrame, usluga_name: str):
    return (t_tbl["Usluga"] == usluga_name)

# =========================================
# Build reports
# =========================================
def build_reports(df: pd.DataFrame, cols: dict):
    long = df[["CONTRACT_TIP", "POS_TIP_BUS", "POS_TIP", "_services", "_is_business"]].explode("_services").dropna()

    svc_counts = long["_services"].value_counts().to_dict()
    mobile_cnt   = int(svc_counts.get("MOBILE", 0))
    tv_cnt       = int(svc_counts.get("TV", 0))
    internet_cnt = int(svc_counts.get("INTERNET", 0))
    tvgo_cnt     = int(svc_counts.get("TV GO", 0))
    phones_cnt   = int(df[cols["mobile_device"]].notna().sum()) if cols["mobile_device"] in df.columns else 0

    kpi = dict(MOBILE=mobile_cnt, TV=tv_cnt, INTERNET=internet_cnt, TV_GO=tvgo_cnt, PHONES=phones_cnt)

    order = ["MOBILE", "TV", "INTERNET", "TV GO"]
    svc_table = (long["_services"].value_counts().rename_axis("Usluga").reset_index(name="broj"))
    if not svc_table.empty:
        svc_table["Usluga"] = pd.Categorical(svc_table["Usluga"], categories=order, ordered=True)
        svc_table = svc_table.sort_values("Usluga")
        svc_table = with_total_row(svc_table, value_col="broj", label_col="Usluga")

    tariffs_tables = {}
    col_tariff = cols["tariff"]
    if col_tariff in df.columns:
        records = []
        for _, r in df.iterrows():
            tname = r[col_tariff]
            if r["_mobile"]:
                records.append(("MOBILE", tname))
            if r["_tv"]:
                records.append(("TV", tname))
            if r["_inet_modem"]:
                records.append(("INTERNET", tname))
                if r["_price_tv"]:
                    records.append(("TV", "STARNET"))
        if records:
            tl = pd.DataFrame(records, columns=["Usluga", "Tarifa"])
            tl = tl[tl["Usluga"].isin(["MOBILE", "TV", "INTERNET"])]
            t_tbl = (tl.groupby(["Usluga", "Tarifa"]).size().reset_index(name="broj")
                     .sort_values(["Usluga", "broj"], ascending=[True, False]))
            tariffs_tables["MOBILE"]   = with_total_row(t_tbl[tbl_filter(t_tbl, "MOBILE")][["Tarifa","broj"]].reset_index(drop=True), "broj", "Tarifa")
            tariffs_tables["TV"]       = with_total_row(t_tbl[tbl_filter(t_tbl, "TV")][["Tarifa","broj"]].reset_index(drop=True), "broj", "Tarifa")
            tariffs_tables["INTERNET"] = with_total_row(t_tbl[tbl_filter(t_tbl, "INTERNET")][["Tarifa","broj"]].reset_index(drop=True), "broj", "Tarifa")

    phones_pivot = None
    if cols["mobile_device"] in df.columns:
        phones = df.dropna(subset=[cols["mobile_device"]]).copy()
        if not phones.empty:
            phones_pivot = (phones.groupby([cols["mobile_device"], "POS_TIP_BUS"])
                            .size().reset_index(name="broj")
                            .pivot(index=cols["mobile_device"], columns="POS_TIP_BUS", values="broj")
                            .fillna(0).astype(int))
            phones_pivot = with_totals_pivot(phones_pivot)

    stb_pivot = None
    if cols["tv_device"] in df.columns:
        df["_tvdev"] = df[cols["tv_device"]].astype(str).str.upper().str.strip()
        stb_df = df[df["_tvdev"].isin(["STB", "STB2"])]
        if not stb_df.empty:
            stb_pivot = (stb_df.groupby(["_tvdev", "POS_TIP_BUS"])
                         .size().reset_index(name="broj")
                         .pivot(index="_tvdev", columns="POS_TIP_BUS", values="broj")
                         .fillna(0).astype(int))
            stb_pivot = with_totals_pivot(stb_pivot)

    pos_all = (long.groupby(["POS_TIP_BUS", "_services"]).size().reset_index(name="broj")
               .pivot(index="POS_TIP_BUS", columns="_services", values="broj")
               .fillna(0).astype(int))
    for c in ["MOBILE", "TV", "INTERNET", "TV GO"]:
        if c not in pos_all.columns:
            pos_all[c] = 0
    pos_all = pos_all[["MOBILE", "TV", "INTERNET", "TV GO"]]
    pos_all = with_totals_pivot(pos_all)

    long_res = df.loc[~df["_is_business"], ["POS_TIP", "_services"]].explode("_services").dropna()
    if not long_res.empty:
        pos_res = (long_res.groupby(["POS_TIP", "_services"]).size().reset_index(name="broj")
                   .pivot(index="POS_TIP", columns="_services", values="broj")
                   .fillna(0).astype(int))
        for c in ["MOBILE", "TV", "INTERNET", "TV GO"]:
            if c not in pos_res.columns:
                pos_res[c] = 0
        pos_res = pos_res[["MOBILE", "TV", "INTERNET", "TV GO"]]
        pos_res = with_totals_pivot(pos_res)
    else:
        pos_res = None

    long_bus = df.loc[df["_is_business"], ["POS_TIP", "_services"]].explode("_services").dropna()
    if not long_bus.empty:
        pos_bus = (long_bus.groupby(["POS_TIP", "_services"]).size().reset_index(name="broj")
                   .pivot(index="POS_TIP", columns="_services", values="broj")
                   .fillna(0).astype(int))
        for c in ["MOBILE", "TV", "INTERNET", "TV GO"]:
            if c not in pos_bus.columns:
                pos_bus[c] = 0
        pos_bus = pos_bus[["MOBILE", "TV", "INTERNET", "TV GO"]]
        pos_bus = with_totals_pivot(pos_bus)
    else:
        pos_bus = None

    return dict(
        KPI=kpi,
        SVC_TABLE=svc_table if not svc_table.empty else pd.DataFrame(columns=["Usluga","broj"]),
        TARIFFS=tariffs_tables,
        PHONES=phones_pivot,
        STB=stb_pivot,
        POS_USLUGA=pos_all,
        POS_USLUGA_RES=pos_res,
        POS_USLUGA_BUS=pos_bus
    )

# =========================================
# UI & Modes
# =========================================
cols = st.columns([1, 6])
with cols[0]:
    try:
        st.image("mtel.png", use_container_width=True)
    except Exception:
        st.write("")
with cols[1]:
    st.title("Analiza prodaje Austrija — lokalno + Google Drive")
    st.caption("Tri moda: 1) Jedan fajl  2) Više fajlova  3) Poređenje dva mjeseca. Izvor podataka u sidebaru.")

# Data source selection
data_source = st.sidebar.radio("Izvor podataka", ["Google Drive", "Lokalno"])
gdrive_folder_id = st.secrets.get("GDRIVE_FOLDER_ID", "")

mode = st.sidebar.radio("Režim rada:", ["Analiza jednog fajla", "Analiza perioda (više fajlova)", "Poređenje dva mjeseca"])

# -------------------------
# Mode 1: single file
# -------------------------
if mode == "Analiza jednog fajla":
    if data_source == "Google Drive":
        if not gdrive_folder_id:
            st.error("GDRIVE_FOLDER_ID nije postavljen u .streamlit/secrets.toml")
            st.stop()
        drive = build_drive()
        files = drive_list_excel_in_folder(drive, gdrive_folder_id)
        if not files:
            st.error("Nema ALL_CONTRACT_YYYY_MM.xlsx fajlova u zadanom GDrive folderu.")
            st.stop()
        names = [f["name"] for f in files]
        pick = st.selectbox("Odaberi fajl sa Google Drive-a", names, index=len(names)-1)
        chosen = next(f for f in files if f["name"] == pick)
        file_bytes = drive_download_bytes(drive, chosen["id"])
        raw = load_one_excel(file_bytes, chosen["name"])
    else:
        uploaded = st.file_uploader("Učitaj lokalni Excel (XLSX)", type=["xlsx"])
        if not uploaded:
            st.stop()
        raw = load_one_excel(uploaded.read(), uploaded.name)

    df, cols = normalize_df(raw)
    df = mark_contract_tip_single_file(df)
    reports = build_reports(df, cols)

    k1,k2,k3,k4,k5 = st.columns(5)
    with k1: st.metric("📱 MOBILE", reports["KPI"]["MOBILE"])
    with k2: st.metric("📺 TV (uklj. STARNET)", reports["KPI"]["TV"])
    with k3: st.metric("🌐 INTERNET", reports["KPI"]["INTERNET"])
    with k4: st.metric("📲 TV GO", reports["KPI"]["TV_GO"])
    with k5: st.metric("📦 Telefoni", reports["KPI"]["PHONES"])

    st.divider()
    st.subheader("Ugovori po usluzi")
    st.dataframe(reports["SVC_TABLE"].reset_index(drop=True), use_container_width=True)

    st.divider()
    st.subheader("Najprodavanije tarife")
    t1, t2, t3 = st.columns(3)
    with t1:
        st.markdown("**MOBILE**")
        st.dataframe(reports["TARIFFS"].get("MOBILE", pd.DataFrame(columns=["Tarifa","broj"])), use_container_width=True, height=360)
    with t2:
        st.markdown("**TV** *(uklj. STARNET)*")
        st.dataframe(reports["TARIFFS"].get("TV", pd.DataFrame(columns=["Tarifa","broj"])), use_container_width=True, height=360)
    with t3:
        st.markdown("**INTERNET**")
        st.dataframe(reports["TARIFFS"].get("INTERNET", pd.DataFrame(columns=["Tarifa","broj"])), use_container_width=True, height=360)

    st.divider()
    st.subheader("Prodaja telefona po POS tipu (MobileDevice × POS)")
    if reports["PHONES"] is not None:
        st.dataframe(reports["PHONES"], use_container_width=True, height=420)
    else:
        st.info("Nema prodatih telefona.")

    st.divider()
    st.subheader("STB uređaji po POS tipu (STB, STB2)")
    if reports["STB"] is not None:
        st.dataframe(reports["STB"], use_container_width=True, height=280)
    else:
        st.info("Nema STB/STB2 uređaja.")

    st.divider()
    st.subheader("Prodaja po POS tipu (MOBILE, TV, INTERNET, TV GO) — uključuje BUSINESS")
    st.dataframe(reports["POS_USLUGA"], use_container_width=True)

    st.divider()
    st.subheader("Prodaja po POS tipu — SAMO rezidencijala")
    if reports.get("POS_USLUGA_RES") is not None:
        st.dataframe(reports["POS_USLUGA_RES"], use_container_width=True)
    else:
        st.info("Nema rezidencijalnih ugovora u selekciji.")

    st.divider()
    st.subheader("Prodaja po POS tipu — SAMO business")
    if reports.get("POS_USLUGA_BUS") is not None:
        st.dataframe(reports["POS_USLUGA_BUS"], use_container_width=True)
    else:
        st.info("Nema business ugovora u selekciji.")

# -------------------------
# Mode 2: period (many files)
# -------------------------
elif mode == "Analiza perioda (više fajlova)":
    if data_source == "Google Drive":
        if not gdrive_folder_id:
            st.error("GDRIVE_FOLDER_ID nije postavljen u .streamlit/secrets.toml")
            st.stop()
        drive = build_drive()
        files = drive_list_excel_in_folder(drive, gdrive_folder_id)
        if not files:
            st.error("Nema ALL_CONTRACT_YYYY_MM.xlsx fajlova u zadanom GDrive folderu.")
            st.stop()

        months = []
        for f in files:
            y, m = derive_period_from_filename(f["name"])
            if y and m:
                months.append((y, m, f))
        months = sorted(months, key=lambda x: (x[0], x[1]))
    else:
        folder = st.text_input("Putanja do foldera sa fajlovima", "/Users/dzajic/Documents/MtelReportApp/data")
        if not os.path.isdir(folder):
            st.error("Folder ne postoji.")
            st.stop()
        names = [n for n in os.listdir(folder) if re.match(r"^ALL_CONTRACT_(20\d{2})_(0[1-9]|1[0-2])\.xlsx$", n)]
        if not names:
            st.error("Nema fajlova 'ALL_CONTRACT_YYYY_MM.xlsx' u folderu.")
            st.stop()
        months = []
        for n in names:
            y, m = derive_period_from_filename(n)
            if y and m:
                months.append((y, m, os.path.join(folder, n)))
        months = sorted(months, key=lambda x: (x[0], x[1]))

    god_l = sorted({y for y,_,_ in months})
    if not god_l:
        st.error("Nema dostupnih mjeseci.")
        st.stop()
    godina_od = st.selectbox("Godina OD", god_l, index=0)
    godina_do = st.selectbox("Godina DO", god_l, index=len(god_l)-1)
    mj_od = st.slider("Mjesec OD", 1, 12, 1)
    mj_do = st.slider("Mjesec DO", 1, 12, 12)

    sel = []
    for (y,m,ref) in months:
        if ((y>godina_od) or (y==godina_od and m>=mj_od)) and ((y<godina_do) or (y==godina_do and m<=mj_do)):
            sel.append((y,m,ref))

    if not sel:
        st.error("Nema fajlova u izabranom periodu.")
        st.stop()

    frames = []
    if data_source == "Google Drive":
        for (y,m,f) in sel:
            b = drive_download_bytes(drive, f["id"])
            frames.append(load_one_excel(b, f["name"]))
    else:
        for (y,m,p) in sel:
            with open(p, "rb") as fh:
                frames.append(load_one_excel(fh.read(), os.path.basename(p)))

    raw_all = pd.concat(frames, ignore_index=True)
    df, cols = normalize_df(raw_all)
    df = mark_contract_tip_multi_file(df)
    reports = build_reports(df, cols)

    st.subheader("KPI — zbirno za odabrani period")
    k1,k2,k3,k4,k5 = st.columns(5)
    with k1: st.metric("📱 MOBILE", reports["KPI"]["MOBILE"])
    with k2: st.metric("📺 TV (uklj. STARNET)", reports["KPI"]["TV"])
    with k3: st.metric("🌐 INTERNET", reports["KPI"]["INTERNET"])
    with k4: st.metric("📲 TV GO", reports["KPI"]["TV_GO"])
    with k5: st.metric("📦 Telefoni", reports["KPI"]["PHONES"])

    st.divider()
    st.subheader("Ugovori po usluzi — zbirno")
    st.dataframe(reports["SVC_TABLE"].reset_index(drop=True), use_container_width=True)

    st.divider()
    st.subheader("Najprodavanije tarife — zbirno")
    t1, t2, t3 = st.columns(3)
    with t1:
        st.markdown("**MOBILE**")
        st.dataframe(reports["TARIFFS"].get("MOBILE", pd.DataFrame(columns=["Tarifa","broj"])), use_container_width=True, height=360)
    with t2:
        st.markdown("**TV** *(uklj. STARNET)*")
        st.dataframe(reports["TARIFFS"].get("TV", pd.DataFrame(columns=["Tarifa","broj"])), use_container_width=True, height=360)
    with t3:
        st.markdown("**INTERNET**")
        st.dataframe(reports["TARIFFS"].get("INTERNET", pd.DataFrame(columns=["Tarifa","broj"])), use_container_width=True, height=360)

    st.divider()
    st.subheader("Prodaja telefona po POS tipu (MobileDevice × POS) — zbirno")
    if reports["PHONES"] is not None:
        st.dataframe(reports["PHONES"], use_container_width=True, height=420)
    else:
        st.info("Nema prodatih telefona u periodu.")

    st.divider()
    st.subheader("STB uređaji po POS tipu (STB, STB2) — zbirno")
    if reports["STB"] is not None:
        st.dataframe(reports["STB"], use_container_width=True, height=280)
    else:
        st.info("Nema STB/STB2 uređaja u periodu.")

    st.divider()
    st.subheader("Prodaja po POS tipu (MOBILE, TV, INTERNET, TV GO) — uključuje BUSINESS — zbirno")
    st.dataframe(reports["POS_USLUGA"], use_container_width=True)

    st.divider()
    st.subheader("Prodaja po POS tipu — SAMO rezidencijala (zbirno)")
    if reports.get("POS_USLUGA_RES") is not None:
        st.dataframe(reports["POS_USLUGA_RES"], use_container_width=True)
    else:
        st.info("Nema rezidencijalnih ugovora u periodu.")

    st.divider()
    st.subheader("Prodaja po POS tipu — SAMO business (zbirno)")
    if reports.get("POS_USLUGA_BUS") is not None:
        st.dataframe(reports["POS_USLUGA_BUS"], use_container_width=True)
    else:
        st.info("Nema business ugovora u periodu.")

# -------------------------
# Mode 3: compare two months
# -------------------------
else:
    if data_source == "Google Drive":
        if not gdrive_folder_id:
            st.error("GDRIVE_FOLDER_ID nije postavljen u .streamlit/secrets.toml")
            st.stop()
        drive = build_drive()
        files = drive_list_excel_in_folder(drive, gdrive_folder_id)
        if not files or len(files) < 2:
            st.error("Treba najmanje 2 fajla u GDrive folderu.")
            st.stop()
        months = []
        for f in files:
            y, m = derive_period_from_filename(f["name"])
            if y and m:
                months.append((y,m,f))
        months = sorted(months, key=lambda x: (x[0], x[1]))
        label_map = {f"{y}-{m:02d}": f for (y,m,f) in months}
        labels = sorted(label_map.keys())
        c1, c2 = st.columns(2)
        with c1:
            sel_a = st.selectbox("Mjesec A", labels, index=max(0, len(labels)-2))
        with c2:
            sel_b = st.selectbox("Mjesec B", labels, index=len(labels)-1)
        fa = label_map[sel_a]; fb = label_map[sel_b]
        raw_a = load_one_excel(drive_download_bytes(drive, fa["id"]), fa["name"])
        raw_b = load_one_excel(drive_download_bytes(drive, fb["id"]), fb["name"])
    else:
        folder = st.text_input("Putanja do foldera sa fajlovima", "/Users/dzajic/Documents/MtelReportApp/data")
        if not os.path.isdir(folder):
            st.error("Folder ne postoji.")
            st.stop()
        names = [n for n in os.listdir(folder) if re.match(r"^ALL_CONTRACT_(20\d{2})_(0[1-9]|1[0-2])\.xlsx$", n)]
        if not names or len(names) < 2:
            st.error("Potrebna su najmanje dva mjeseca u folderu.")
            st.stop()
        months = []
        for n in names:
            y, m = derive_period_from_filename(n)
            if y and m:
                months.append((y,m,n))
        months = sorted(months, key=lambda x: (x[0], x[1]))
        label_map = {f"{y}-{m:02d}": n for (y,m,n) in months}
        labels = list(label_map.keys())
        c1, c2 = st.columns(2)
        with c1:
            sel_a = st.selectbox("Mjesec A", labels, index=max(0, len(labels)-2))
        with c2:
            sel_b = st.selectbox("Mjesec B", labels, index=len(labels)-1)
        file_a = os.path.join(folder, label_map[sel_a])
        file_b = os.path.join(folder, label_map[sel_b])
        with open(file_a, "rb") as fa:
            raw_a = load_one_excel(fa.read(), os.path.basename(file_a))
        with open(file_b, "rb") as fb:
            raw_b = load_one_excel(fb.read(), os.path.basename(file_b))

    df_a, cols_a = normalize_df(raw_a)
    df_a = mark_contract_tip_single_file(df_a)
    rep_a = build_reports(df_a, cols_a)

    df_b, cols_b = normalize_df(raw_b)
    df_b = mark_contract_tip_single_file(df_b)
    rep_b = build_reports(df_b, cols_b)

    st.subheader(f"Poređenje KPI — {sel_a} vs {sel_b}")
    delta_mobile   = rep_b["KPI"]["MOBILE"]   - rep_a["KPI"]["MOBILE"]
    delta_tv       = rep_b["KPI"]["TV"]       - rep_a["KPI"]["TV"]
    delta_inet     = rep_b["KPI"]["INTERNET"] - rep_a["KPI"]["INTERNET"]
    delta_tvgo     = rep_b["KPI"]["TV_GO"]    - rep_a["KPI"]["TV_GO"]
    delta_phones   = rep_b["KPI"]["PHONES"]   - rep_a["KPI"]["PHONES"]

    k1,k2,k3,k4,k5 = st.columns(5)
    with k1: st.metric("📱 MOBILE", rep_b["KPI"]["MOBILE"], delta_mobile)
    with k2: st.metric("📺 TV (uklj. STARNET)", rep_b["KPI"]["TV"], delta_tv)
    with k3: st.metric("🌐 INTERNET", rep_b["KPI"]["INTERNET"], delta_inet)
    with k4: st.metric("📲 TV GO", rep_b["KPI"]["TV_GO"], delta_tvgo)
    with k5: st.metric("📦 Telefoni", rep_b["KPI"]["PHONES"], delta_phones)

    st.divider()
    st.subheader("Ugovori po usluzi — uporedno")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**{sel_a}**")
        st.dataframe(rep_a["SVC_TABLE"].reset_index(drop=True), use_container_width=True)
    with col2:
        st.markdown(f"**{sel_b}**")
        st.dataframe(rep_b["SVC_TABLE"].reset_index(drop=True), use_container_width=True)

    st.divider()
    st.subheader("Najprodavanije tarife — uporedno")
    for svc in ["MOBILE", "TV", "INTERNET"]:
        st.markdown(f"**{svc}**")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"{sel_a}")
            st.dataframe(rep_a["TARIFFS"].get(svc, pd.DataFrame(columns=["Tarifa","broj"])), use_container_width=True, height=320)
        with c2:
            st.markdown(f"{sel_b}")
            st.dataframe(rep_b["TARIFFS"].get(svc, pd.DataFrame(columns=["Tarifa","broj"])), use_container_width=True, height=320)

    st.divider()
    st.subheader("Telefoni po POS tipu — uporedno")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"{sel_a}")
        if rep_a["PHONES"] is not None:
            st.dataframe(rep_a["PHONES"], use_container_width=True, height=380)
        else:
            st.info("Nema prodatih telefona.")
    with c2:
        st.markdown(f"{sel_b}")
        if rep_b["PHONES"] is not None:
            st.dataframe(rep_b["PHONES"], use_container_width=True, height=380)
        else:
            st.info("Nema prodatih telefona.")

    st.divider()
    st.subheader("STB uređaji po POS tipu — uporedno")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"{sel_a}")
        if rep_a["STB"] is not None:
            st.dataframe(rep_a["STB"], use_container_width=True, height=240)
        else:
            st.info("Nema STB/STB2 uređaja.")
    with c2:
        st.markdown(f"{sel_b}")
        if rep_b["STB"] is not None:
            st.dataframe(rep_b["STB"], use_container_width=True, height=240)
        else:
            st.info("Nema STB/STB2 uređaja.")

    st.divider()
    st.subheader("Prodaja po POS tipu (MOBILE, TV, INTERNET, TV GO) — uporedno")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"{sel_a}")
        st.dataframe(rep_a["POS_USLUGA"], use_container_width=True)
    with c2:
        st.markdown(f"{sel_b}")
        st.dataframe(rep_b["POS_USLUGA"], use_container_width=True)

    st.divider()
    st.subheader("Prodaja po POS tipu — SAMO rezidencijala — uporedno")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"{sel_a}")
        if rep_a.get("POS_USLUGA_RES") is not None:
            st.dataframe(rep_a["POS_USLUGA_RES"], use_container_width=True)
        else:
            st.info("Nema rezidencijalnih ugovora.")
    with c2:
        st.markdown(f"{sel_b}")
        if rep_b.get("POS_USLUGA_RES") is not None:
            st.dataframe(rep_b["POS_USLUGA_RES"], use_container_width=True)
        else:
            st.info("Nema rezidencijalnih ugovora.")

    st.divider()
    st.subheader("Prodaja po POS tipu — SAMO business — uporedno")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"{sel_a}")
        if rep_a.get("POS_USLUGA_BUS") is not None:
            st.dataframe(rep_a["POS_USLUGA_BUS"], use_container_width=True)
        else:
            st.info("Nema business ugovora.")
    with c2:
        st.markdown(f"{sel_b}")
        if rep_b.get("POS_USLUGA_BUS") is not None:
            st.dataframe(rep_b["POS_USLUGA_BUS"], use_container_width=True)
        else:
            st.info("Nema business ugovora.")

st.caption("© Stabilna lokalna + Google Drive verzija. Podrška: MOBILE/TV/INTERNET/TV GO, STARNET, POS/BUSINESS, NOVI/PRODUŽENJE.")
