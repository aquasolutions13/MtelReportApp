# app.py — Analiza prodaje Austrija (Google Drive)
# Razvijeno od strane *gennaro* ✨

import io
import os
import re
import json
import tempfile
from datetime import datetime
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# ---------------------------
# UI & Branding
# ---------------------------
st.set_page_config(page_title="Analiza prodaje Austrija", layout="wide", page_icon="📈")
logo_path = os.path.join(os.getcwd(), "mtel.png")
c0, c1 = st.columns([1, 5])
with c0:
    if os.path.exists(logo_path):
        st.image(logo_path, use_container_width=True)
with c1:
    st.title("Analiza prodaje Austrija")
    st.caption("Razvijeno od strane *gennaro*")

# ---------------------------
# Google Drive helpers
# ---------------------------

def _gdrive():
    """Autentikacija na Google Drive preko service accounta (PyDrive2)."""
    try:
        from pydrive2.auth import GoogleAuth
        from pydrive2.drive import GoogleDrive

        ga = GoogleAuth()
        ga.auth_method = 'service'

        creds = json.loads(st.secrets["gdrive"]["SERVICE_ACCOUNT_JSON"])
        sa_email = creds.get("client_email")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as tf:
            tf.write(json.dumps(creds))
            tf.flush()
            ga.settings['service_config'] = {
                'client_json_file_path': tf.name,
                'client_user_email': sa_email,
            }
            ga.ServiceAuth()

        return GoogleDrive(ga)
    except Exception as e:
        st.error(f"❌ GDrive autentikacija nije uspjela: {e}")
        st.stop()


def gdrive_list_reports() -> List[str]:
    """Vrati nazive ALL_CONTRACT_YYYY_MM.xlsx fajlova iz definisanog GDrive foldera."""
    try:
        drv = _gdrive()
        folder_id = st.secrets["gdrive"]["FOLDER_ID"]
        q = (
            f"'{folder_id}' in parents and "
            "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and "
            "trashed=false"
        )
        file_list = drv.ListFile({'q': q}).GetList()
        rx = re.compile(r"^ALL_CONTRACT_(20\d{2})_(0[1-9]|1[0-2])\.xlsx$")
        names = [f['title'] for f in file_list if rx.match(f['title'])]
        return sorted(set(names))
    except Exception as e:
        st.error(f"❌ Ne mogu listati fajlove na Google Drive-u: {e}")
        st.info("Provjeri: Secrets (FOLDER_ID/SERVICE_ACCOUNT_JSON), share na service account email i nazive fajlova (ALL_CONTRACT_YYYY_MM.xlsx).")
        st.stop()


def gdrive_download(name: str) -> bytes:
    """Preuzmi XLSX kao RAW bajtove (isti tok kao drag&drop)."""
    drv = _gdrive()
    folder_id = st.secrets["gdrive"]["FOLDER_ID"]
    q = (
        f"'{folder_id}' in parents and "
        f"title = '{name}' and "
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and "
        "trashed=false"
    )
    lst = drv.ListFile({'q': q}).GetList()
    if not lst:
        raise FileNotFoundError(f"Nema fajla '{name}' u Drive folderu.")
    f = lst[0]
    # RAW bytes bez lokalnog snimanja
    try:
        content = f.GetContentBinary()
        if isinstance(content, str):
            content = content.encode("latin-1", errors="ignore")
        return content
    except Exception:
        # fallback preko temp fajla
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp_path = tmp.name
        try:
            f.GetContentFile(tmp_path)
            with open(tmp_path, "rb") as fh:
                data = fh.read()
            return data
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

# ---------------------------
# Excel reading (ultimate ZIP/XML fallback — bez stilova)
# ---------------------------

def _read_excel_any(xls_bytes_or_path) -> pd.DataFrame:
    """
    Ultimate fallback za čitanje .xlsx/.xlsm:
    - Ne koristi pandas.read_excel niti openpyxl stilove.
    - Čita direktno iz ZIP-a (sharedStrings + prvi worksheet).
    - Ignoriše sve stilove/makroe/conditional formatting.
    - Prvi red tretira kao header ako izgleda tekstualno.
    """
    import zipfile
    import xml.etree.ElementTree as ET

    if isinstance(xls_bytes_or_path, (bytes, bytearray)):
        bio = io.BytesIO(xls_bytes_or_path)
    elif hasattr(xls_bytes_or_path, "read"):
        bio = io.BytesIO(xls_bytes_or_path.read())
    else:
        bio = open(xls_bytes_or_path, "rb")

    try:
        with zipfile.ZipFile(bio) as z:
            # sharedStrings
            shared = []
            if "xl/sharedStrings.xml" in z.namelist():
                xml_ss = ET.parse(z.open("xl/sharedStrings.xml")).getroot()
                for si in xml_ss.findall("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si"):
                    text_elems = si.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
                    text = "".join([(t.text or "") for t in text_elems])
                    shared.append(text)

            # prvi sheet (sheet1.xml ili najmanji sheetN.xml)
            sheet_name = "xl/worksheets/sheet1.xml" if "xl/worksheets/sheet1.xml" in z.namelist() else None
            if sheet_name is None:
                candidates = [n for n in z.namelist() if n.startswith("xl/worksheets/sheet") and n.endswith(".xml")]
                sheet_name = sorted(candidates)[0] if candidates else None
            if not sheet_name:
                raise RuntimeError("Nema sheet XML-a u XLSX fajlu.")

            xml_ws = ET.parse(z.open(sheet_name)).getroot()
            ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

            rows = []
            for row in xml_ws.findall("a:sheetData/a:row", ns):
                vals = []
                for c in row.findall("a:c", ns):
                    t = c.attrib.get("t")
                    v = c.find("a:v", ns)
                    if v is None:
                        vals.append(None)
                    elif t == "s":  # shared string
                        try:
                            vals.append(shared[int(v.text)])
                        except Exception:
                            vals.append(v.text)
                    else:
                        vals.append(v.text)
                rows.append(vals)

            if not rows:
                return pd.DataFrame()

            header_candidate = rows[0]
            if all((isinstance(x, str) or x is None) for x in header_candidate):
                cols = [x if (x is not None and str(x).strip() != "") else f"Col_{i+1}" for i, x in enumerate(header_candidate)]
                df = pd.DataFrame(rows[1:], columns=cols)
            else:
                df = pd.DataFrame(rows)

            df = df.dropna(how="all").reset_index(drop=True)
            df.columns = [str(c).strip() for c in df.columns]
            return df

    except Exception as e:
        raise RuntimeError(f"Ne mogu pročitati Excel (fallback): {e}")

# ---------------------------
# Column detection
# ---------------------------

def find_col(df: pd.DataFrame, candidates: List[str], fallback_letter: Optional[str]=None) -> Optional[str]:
    cols = {str(c).strip(): c for c in df.columns}
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        if cand in cols:
            return cols[cand]
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    if fallback_letter:
        def excel_letter_to_idx(s):
            s = s.strip().upper()
            val = 0
            for ch in s:
                val = val*26 + (ord(ch)-64)
            return val-1
        idx = excel_letter_to_idx(fallback_letter)
        if 0 <= idx < df.shape[1]:
            return df.columns[idx]
    return None


def detect_pos_col(df: pd.DataFrame) -> Optional[str]:
    pos_keywords = ["vip_", "hartlauer", "web", "telesales", "wmh", "wth", "wdz", "d2"]
    for name in df.columns:
        lname = str(name).lower()
        if any(k in lname for k in ["pos", "shop", "salespoint", "channel", "store"]):
            return name
    object_cols = [c for c in df.columns if df[c].dtype == object]
    for c in object_cols:
        series = df[c].dropna().astype(str).str.lower()
        if series.str.contains("|".join(pos_keywords), regex=True).any():
            return c
    if "POS" in df.columns:
        return "POS"
    return None

# ---------------------------
# POS Mapping rules
# ---------------------------

VIP_SPECIAL = {"VIP_GCP", "VIP_LHP", "VIP_SSA", "VIP_WHE", "VIP_WLC", "VIP_WTR"}

def map_pos_type(raw_name: str) -> str:
    if not isinstance(raw_name, str) or not raw_name.strip():
        return "MULTIBREND"
    s = raw_name.strip()
    s_upper = s.upper()

    for direct in ["WMH", "WTH", "WDZ"]:
        if s_upper.startswith(direct):
            return direct

    if s_upper.startswith("VIP_"):
        if s_upper in VIP_SPECIAL:
            return s_upper
        return s_upper

    if "BUSINESS" in s_upper:
        return "BUSINESS"

    if "D2" in s_upper:
        return "D2D"

    if "HARTLAUER" in s_upper:
        return "HARTLAUER"
    if s_upper == "WEB" or " WEB" in s_upper or s_upper.startswith("WEB"):
        return "WEB"
    if "TELESALES" in s_upper:
        return "TELESALES"

    return "MULTIBREND"

# ---------------------------
# Helpers
# ---------------------------

def parse_month_from_filename(name: str) -> Optional[Tuple[int,int]]:
    m = re.match(r'^ALL_CONTRACT_(20\d{2})_(0[1-9]|1[0-2])\.xlsx$', name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def to_datetime_safe(s) -> Optional[datetime]:
    if pd.isna(s):
        return None
    if isinstance(s, datetime):
        return s
    for fmt in ("%d.%m.%y %H:%M", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(s), fmt)
        except Exception:
            pass
    try:
        return pd.to_datetime(s, dayfirst=True, errors="coerce")
    except Exception:
        return None

# ---------------------------
# ENRICH (glavna logika — identično kao jutros)
# ---------------------------

def enrich(df: pd.DataFrame, file_year: Optional[int]=None, file_month: Optional[int]=None) -> pd.DataFrame:
    col_start = find_col(df, ["Start"], "D")
    col_contract_type = find_col(df, ["ContractType"], "I")
    col_tariff = find_col(df, ["Tariff", "TariffName", "NazivTarife"], "H")
    col_mobile_tar = find_col(df, ["log_mobiletariff"], "Q")
    col_tv_tar = find_col(df, ["log_tvtariff"], "R")
    col_price_tv = find_col(df, ["Price_TV"], "T")
    col_mdevice = find_col(df, ["MobileDevice"], "AA")
    col_tvdevice = find_col(df, ["TVDevice"], "AD")
    col_idevice = find_col(df, ["InternetDevice"], "AE")
    col_pos = detect_pos_col(df) or "POS"

    ren = {}
    if col_start: ren[col_start] = "_Start"
    if col_contract_type: ren[col_contract_type] = "_ContractType"
    if col_tariff: ren[col_tariff] = "_Tariff"
    if col_mobile_tar: ren[col_mobile_tar] = "_log_mobiletariff"
    if col_tv_tar: ren[col_tv_tar] = "_log_tvtariff"
    if col_price_tv: ren[col_price_tv] = "_Price_TV"
    if col_mdevice: ren[col_mdevice] = "_MobileDevice"
    if col_tvdevice: ren[col_tvdevice] = "_TVDevice"
    if col_idevice: ren[col_idevice] = "_InternetDevice"
    if col_pos: ren[col_pos] = "_POS_RAW"

    df = df.rename(columns=ren).copy()

    # POS tip i Business flag
    df["_POS_TIP"] = df.get("_POS_RAW", pd.Series([""]*len(df))).astype(str).map(map_pos_type)
    df["_BUSINESS"] = df.get("_ContractType", "").astype(str).str.upper().eq("BUSINESS")

    # Detekcija (year, month) za NOVE ugovore
    if file_year is None or file_month is None:
        dt = df.get("_Start")
        if dt is not None and not pd.Series(dt).isna().all():
            any_dt = to_datetime_safe(pd.Series(dt).dropna().iloc[0])
            if isinstance(any_dt, (pd.Timestamp, datetime)):
                file_year, file_month = any_dt.year, any_dt.month

    df["_StartDT"] = df.get("_Start").map(to_datetime_safe) if "_Start" in df.columns else pd.NaT
    df["_IS_NEW"] = False
    if file_year and file_month:
        df["_IS_NEW"] = df["_StartDT"].apply(
            lambda x: isinstance(x, (pd.Timestamp, datetime)) and x.year == file_year and x.month == file_month
        )

    # NORMALIZACIJA Q/R NA "TRUE"/"FALSE" (da ostane stara logika)
    def _to_TRUE_FALSE(x):
        s = str(x).strip().upper()
        if s in ("1", "TRUE", "T"):
            return "TRUE"
        if s in ("0", "FALSE", "F", "", "NONE", "NAN", "NULL"):
            return "FALSE"
        return s

    if "_log_mobiletariff" in df.columns:
        df["_log_mobiletariff"] = df["_log_mobiletariff"].apply(_to_TRUE_FALSE)
    if "_log_tvtariff" in df.columns:
        df["_log_tvtariff"] = df["_log_tvtariff"].apply(_to_TRUE_FALSE)

    # STARA PROVJERENA LOGIKA
    is_mobile   = df.get("_log_mobiletariff", False).astype(str).str.upper().eq("TRUE")
    is_tv       = df.get("_log_tvtariff",    False).astype(str).str.upper().eq("TRUE")
    is_internet = df.get("_InternetDevice",  "").astype(str).str.upper().eq("MODEM")

    has_tv_price = df.get("_Price_TV")
    if has_tv_price is not None:
        has_tv_price = pd.to_numeric(has_tv_price, errors="coerce").fillna(0) != 0
    else:
        has_tv_price = pd.Series([False]*len(df))

    df["_SERV_MOBILE"]   = is_mobile
    df["_SERV_INTERNET"] = is_internet
    df["_SERV_TV"]       = (is_tv) | (is_internet & has_tv_price)
    df["_SERV_TVGO"]     = (is_mobile & (~is_tv) & has_tv_price)

    # TV uređaji
    tvdev = df.get("_TVDevice", "").astype(str).str.upper()
    df["_STB"]  = tvdev.eq("STB").astype(int)
    df["_STB2"] = tvdev.eq("STB2").astype(int)

    # MobileDevice — samo očisti prazno/"-"; računamo identično kao ranije (.notna())
    df["_MobileDevice"] = (
        df.get("_MobileDevice", np.nan)
          .astype(str).str.strip()
          .replace({"": np.nan, "-": np.nan, "N/A": np.nan, "None": np.nan, "NONE": np.nan, "nan": np.nan})
    )

    # Tarife — TV tarife iz Internet paketa = STARNET
    df["_Tariff"] = df.get("_Tariff", np.nan).astype(str).replace({"nan": np.nan})
    df["_Tariff_TV"] = np.where((is_internet & has_tv_price), "STARNET", np.where(is_tv, df["_Tariff"], np.nan))
    df["_Tariff_MOBILE"] = np.where(is_mobile, df["_Tariff"], np.nan)
    df["_Tariff_INTERNET"] = np.where(is_internet, df["_Tariff"], np.nan)

    df["_POS_TIP_BUS"] = np.where(df["_BUSINESS"], df["_POS_TIP"] + " (BUSINESS)", df["_POS_TIP"] + " (RES)")
    return df

# ---------------------------
# Aggregations & Views
# ---------------------------

def kpi_counts(df: pd.DataFrame):
    total_mobile = int(df["_SERV_MOBILE"].sum())
    total_tv     = int(df["_SERV_TV"].sum())
    total_inet   = int(df["_SERV_INTERNET"].sum())
    total_tvgo   = int(df["_SERV_TVGO"].sum())
    total_phones = int(df["_MobileDevice"].notna().sum())  # identično kao ranije
    return total_mobile, total_tv, total_inet, total_tvgo, total_phones


def pivot_services_by_pos(df: pd.DataFrame, business_only=None):
    d = df.copy()
    if business_only is True:
        d = d[d["_BUSINESS"]]
    elif business_only is False:
        d = d[~d["_BUSINESS"]]

    cols = ["_SERV_MOBILE", "_SERV_TV", "_SERV_INTERNET", "_SERV_TVGO"]
    pivot = d.groupby("_POS_TIP")[cols].sum().astype(int)
    pivot = pivot.rename(columns={
        "_SERV_MOBILE": "MOBILE",
        "_SERV_TV": "TV",
        "_SERV_INTERNET": "INTERNET",
        "_SERV_TVGO": "TV GO",
    }).sort_index()
    pivot.loc["Ukupno"] = pivot.sum(numeric_only=True)
    return pivot


def phones_by_model_pos(df: pd.DataFrame):
    d = df[df["_MobileDevice"].notna()].copy()  # isto kao ranije
    if d.empty:
        return pd.DataFrame()
    p = pd.pivot_table(d, index="_MobileDevice", columns="_POS_TIP", values="_POS_TIP", aggfunc="count", fill_value=0)
    p = p.sort_index()
    p.loc["Ukupno"] = p.sum(numeric_only=True)
    return p


def stb_by_pos(df: pd.DataFrame):
    p = df.groupby("_POS_TIP")[["_STB", "_STB2"]].sum().astype(int).sort_index()
    p.loc["Ukupno"] = p.sum(numeric_only=True)
    return p


def tariffs_sold(df: pd.DataFrame):
    res = {}
    t_m = df["_Tariff_MOBILE"].dropna()
    res["MOBILE"] = t_m.value_counts().to_frame("Count")

    t_tv = df["_Tariff_TV"].dropna()
    res["TV"] = t_tv.value_counts().to_frame("Count")

    t_i = df["_Tariff_INTERNET"].dropna()
    res["INTERNET"] = t_i.value_counts().to_frame("Count")
    return res


def only_new(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["_IS_NEW"]]


def monthly_trend(dfs: List[Tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    rows = []
    for name, d in dfs:
        ym = parse_month_from_filename(name)
        if not ym:
            continue
        y, m = ym
        dn = only_new(d)
        M, T, I, TG, _ = kpi_counts(dn)
        rows.append({"year": y, "month": m, "MOBILE": M, "TV": T, "INTERNET": I, "TV GO": TG})
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).sort_values(["year", "month"])
    out["YM"] = out.apply(lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1)
    return out[["YM", "MOBILE", "TV", "INTERNET", "TV GO"]]


def quarterly_trend(monthly: pd.DataFrame) -> pd.DataFrame:
    if monthly.empty:
        return monthly
    tmp = monthly.copy()
    tmp["year"] = tmp["YM"].str.slice(0,4).astype(int)
    tmp["month"] = tmp["YM"].str.slice(5,7).astype(int)
    tmp["Q"] = ((tmp["month"]-1)//3 + 1).astype(int)
    qdf = tmp.groupby(["year", "Q"])[["MOBILE","TV","INTERNET","TV GO"]].sum().reset_index()
    qdf["YQ"] = qdf.apply(lambda r: f"{int(r['year'])} Q{int(r['Q'])}", axis=1)
    return qdf[["YQ","MOBILE","TV","INTERNET","TV GO"]]


def compare_two(dfa: pd.DataFrame, dfb: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    resA = pivot_services_by_pos(dfa, business_only=False)
    resB = pivot_services_by_pos(dfb, business_only=False)
    busA = pivot_services_by_pos(dfa, business_only=True)
    busB = pivot_services_by_pos(dfb, business_only=True)
    return resA, resB, busA, busB


def diff_with_pct(a: pd.DataFrame, b: pd.DataFrame, label_new="Noviji", label_old="Stariji") -> pd.DataFrame:
    cols = sorted(set(a.columns).union(b.columns))
    idx = sorted(set(a.index).union(b.index))
    A = a.reindex(index=idx, columns=cols).fillna(0)
    B = b.reindex(index=idx, columns=cols).fillna(0)
    D = A - B
    pct_df = pd.DataFrame(index=idx, columns=[c+" %Δ" for c in cols], dtype=float)

    def pct(x, y):
        with np.errstate(divide='ignore', invalid='ignore'):
            return np.where(y==0, np.nan, (x/y - 1.0) * 100.0)

    for c in cols:
        pct_df[c+" %Δ"] = pct(A[c].values, B[c].values)

    out = pd.concat([A.add_prefix(f"{label_new} "),
                     B.add_prefix(f"{label_old} "),
                     D.add_prefix("Δ "),
                     pct_df], axis=1)
    return out

# ---------------------------
# File loading (single & multi)
# ---------------------------

def load_one_from_bytes(x: bytes, filename_for_period: Optional[str]=None) -> pd.DataFrame:
    df = _read_excel_any(x)
    y, m = (None, None)
    if filename_for_period:
        pm = parse_month_from_filename(filename_for_period)
        if pm:
            y, m = pm
    return enrich(df, y, m)


def load_from_gdrive(name: str) -> pd.DataFrame:
    y, m = (None, None)
    pm = parse_month_from_filename(name)
    if pm:
        y, m = pm
    data = gdrive_download(name)  # RAW bytes
    df = _read_excel_any(data)
    return enrich(df, y, m)

# ---------------------------
# Sidebar — režimi
# ---------------------------

mode = st.sidebar.radio(
    "Izaberi režim:",
    [
        "Analiza pojedinačnog fajla",
        "Analiza perioda (više fajlova)",
        "Uporedna analiza (2 mjeseca)",
    ],
    index=0
)

# ---------------------------
# MODE 1 — Single file
# ---------------------------

if mode == "Analiza pojedinačnog fajla":
    st.subheader("📄 Analiza pojedinačnog fajla")
    src = st.radio("Izvor podataka:", ["Upload (lokalno)", "Google Drive"], horizontal=True)

    df_single = None
    selected_name = None

    if src == "Upload (lokalno)":
        up = st.file_uploader("Učitaj ALL_CONTRACT_YYYY_MM.xlsx", type=["xlsx"])
        if up is not None:
            selected_name = up.name
            try:
                df_single = load_one_from_bytes(up.read(), filename_for_period=selected_name)
            except Exception as e:
                st.error(f"Ne mogu učitati fajl: {e}")

    else:
        names = gdrive_list_reports()
        if not names:
            st.warning("Nema fajlova 'ALL_CONTRACT_YYYY_MM.xlsx' u Drive folderu.")
        else:
            selected_name = st.selectbox("Izaberi fajl sa GDrive:", names, index=len(names)-1)
            if selected_name:
                with st.spinner("Čitam sa Google Drive-a..."):
                    df_single = load_from_gdrive(selected_name)

    if df_single is not None and not df_single.empty:
        # KPI
        M, T, I, TG, PH = kpi_counts(df_single)
        k1,k2,k3,k4,k5 = st.columns(5)
        k1.metric("MOBILE", M)
        k2.metric("TV", T)
        k3.metric("INTERNET", I)
        k4.metric("TV GO", TG)
        k5.metric("Telefoni", PH)

        st.markdown("---")

        # Prodaja po POS tipu (svi)
        st.markdown("### Prodaja po POS tipu — svi ugovori")
        st.dataframe(pivot_services_by_pos(df_single))

        # Business posebno
        st.markdown("### Prodaja po POS tipu — samo BUSINESS")
        st.dataframe(pivot_services_by_pos(df_single, business_only=True))

        # Rezidencijala posebno
        st.markdown("### Prodaja po POS tipu — REZIDENCIJALA (bez BUSINESS)")
        st.dataframe(pivot_services_by_pos(df_single, business_only=False))

        st.markdown("---")
        st.markdown("### Prodaja tarifa")
        tabs = st.tabs(["MOBILE", "TV (uklj. STARNET uz internet)", "INTERNET"])
        tariffs = tariffs_sold(df_single)
        with tabs[0]:
            st.dataframe(tariffs["MOBILE"])
        with tabs[1]:
            st.dataframe(tariffs["TV"])
        with tabs[2]:
            st.dataframe(tariffs["INTERNET"])

        st.markdown("---")
        st.markdown("### Telefoni po POS tipu")
        ph = phones_by_model_pos(df_single)
        if ph.empty:
            st.info("Nema prodatih telefona u ovom periodu.")
        else:
            st.dataframe(ph)

        st.markdown("---")
        st.markdown("### STB uređaji po POS-u (STB / STB2)")
        st.dataframe(stb_by_pos(df_single))

# ---------------------------
# MODE 2 — Multi files (period)
# ---------------------------

elif mode == "Analiza perioda (više fajlova)":
    st.subheader("🗓 Analiza perioda (više fajlova)")

    names = gdrive_list_reports()
    if not names:
        st.warning("Nema fajlova 'ALL_CONTRACT_YYYY_MM.xlsx' u Drive folderu.")
        st.stop()

    years = sorted({int(n[13:17]) for n in names})
    months_by_year = lambda y: sorted({int(n[18:20]) for n in names if int(n[13:17])==y})

    c1,c2 = st.columns(2)
    with c1:
        y_from = st.selectbox("Godina od", years, index=0)
        m_from = st.selectbox("Mjesec od", months_by_year(y_from), index=0)
    with c2:
        y_to = st.selectbox("Godina do", years, index=len(years)-1)
        m_to = st.selectbox("Mjesec do", months_by_year(y_to), index=len(months_by_year(y_to))-1)

    def ym_key(s):
        y, m = parse_month_from_filename(s)
        return (y, m)

    y1, m1, y2, m2 = y_from, m_from, y_to, m_to
    if (y1, m1) > (y2, m2):
        st.error("Period je invertovan (OD > DO). Ispravi izbor.")
        st.stop()

    sel = [n for n in names if (y1, m1) <= ym_key(n) <= (y2, m2)]
    st.caption(f"Izabrano fajlova: {len(sel)}")

    data_list = []
    for n in sel:
        with st.spinner(f"Učitavam {n}..."):
            data_list.append((n, load_from_gdrive(n)))

    if not data_list:
        st.warning("Nema podataka za izabrani period.")
        st.stop()

    df_all = pd.concat([d.assign(__file=n) for n,d in data_list], ignore_index=True)
    df_new = only_new(df_all)
    M, T, I, TG, PH = kpi_counts(df_new)
    k1,k2,k3,k4,k5 = st.columns(5)
    k1.metric("MOBILE (Novi)", M)
    k2.metric("TV (Novi)", T)
    k3.metric("INTERNET (Novi)", I)
    k4.metric("TV GO (Novi)", TG)
    k5.metric("Telefoni", PH)

    st.markdown("---")
    st.markdown("### Prodaja po POS tipu — Svi ugovori (period)")
    st.dataframe(pivot_services_by_pos(df_all))

    st.markdown("### Prodaja po POS tipu — samo BUSINESS")
    st.dataframe(pivot_services_by_pos(df_all, business_only=True))

    st.markdown("### Prodaja po POS tipu — REZIDENCIJALA (bez BUSINESS)")
    st.dataframe(pivot_services_by_pos(df_all, business_only=False))

    st.markdown("---")
    st.markdown("### Trend po mjesecima (samo NOVE ugovore)")
    monthly = monthly_trend(data_list)
    if monthly.empty:
        st.info("Nema mjesečnih podataka.")
    else:
        st.dataframe(monthly.set_index("YM"))
        fig, ax = plt.subplots(figsize=(10,4))
        monthly.plot(x="YM", y=["MOBILE","TV","INTERNET","TV GO"], ax=ax)
        ax.set_xlabel("Mjesec")
        ax.set_ylabel("Broj ugovora (Novi)")
        ax.set_title("Trend po mjesecima")
        st.pyplot(fig)

    st.markdown("---")
    st.markdown("### Trend po kvartalima (samo NOVE ugovore)")
    qdf = quarterly_trend(monthly) if 'monthly' in locals() and not monthly.empty else pd.DataFrame()
    if qdf.empty:
        st.info("Nema kvartalnih podataka.")
    else:
        st.dataframe(qdf.set_index("YQ"))
        fig2, ax2 = plt.subplots(figsize=(10,4))
        qplot = qdf.set_index("YQ")[["MOBILE","TV","INTERNET","TV GO"]]
        qplot.plot(kind="barh", ax=ax2)
        ax2.set_xlabel("Broj ugovora (Novi)")
        ax2.set_ylabel("Kvartal")
        ax2.set_title("Trend po kvartalima")
        st.pyplot(fig2)

# ---------------------------
# MODE 3 — Compare two months
# ---------------------------

elif mode == "Uporedna analiza (2 mjeseca)":
    st.subheader("🔁 Uporedna analiza (2 mjeseca) — KPI na Novim ugovorima")

    names = gdrive_list_reports()
    if not names:
        st.warning("Nema fajlova na Google Drive-u.")
        st.stop()

    c1,c2 = st.columns(2)
    with c1:
        a_name = st.selectbox("Noviji mjesec", names, index=len(names)-1)
    with c2:
        b_name = st.selectbox("Stariji mjesec", names, index=max(0, len(names)-2))

    if a_name and b_name:
        da = load_from_gdrive(a_name)
        db = load_from_gdrive(b_name)

        da_new = only_new(da)
        db_new = only_new(db)

        M_a, T_a, I_a, TG_a, PH_a = kpi_counts(da_new)
        M_b, T_b, I_b, TG_b, PH_b = kpi_counts(db_new)

        st.markdown("#### KPI — Novi ugovori")
        ka,kb,kc,kd,ke = st.columns(5)
        ka.metric(f"MOBILE ({a_name})", M_a, f"{M_a - M_b:+}")
        kb.metric(f"TV ({a_name})", T_a, f"{T_a - T_b:+}")
        kc.metric(f"INTERNET ({a_name})", I_a, f"{I_a - I_b:+}")
        kd.metric(f"TV GO ({a_name})", TG_a, f"{TG_a - TG_b:+}")
        ke.metric(f"Telefoni ({a_name})", PH_a, f"{PH_a - PH_b:+}")

        st.markdown("---")
        st.markdown("### REZIDENCIJALA — po POS tipu")
        resA, resB, busA, busB = compare_two(da, db)
        resid_diff = diff_with_pct(resA, resB, label_new=a_name, label_old=b_name)
        st.dataframe(resid_diff)

        st.markdown("### BUSINESS — po POS tipu")
        bus_diff = diff_with_pct(busA, busB, label_new=a_name, label_old=b_name)
        st.dataframe(bus_diff)

# ---------------------------
# Footer
# ---------------------------
st.markdown("---")
st.caption("© Izvještaj generisan prema poslovnim pravilima klijenta. Razvijeno od strane *gennaro*.")