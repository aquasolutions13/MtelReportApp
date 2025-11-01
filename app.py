# -*- coding: utf-8 -*-
# Analiza prodaje Austrija — razvijeno od strane *gennaro* :)

import io
import os
import re
import json
import zipfile
import tempfile
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
from xlsx2csv import Xlsx2csv

# =========================
# OSNOVNA PODEŠAVANJA
# =========================
APP_TITLE = "Analiza prodaje Austrija"
APP_LOGO = "mtel.png"  # stavi logo PNG pored app.py ako ga želiš prikazati
DEFAULT_GDRIVE_FOLDER_ID = "1kcOvSblQ19VwhmR4qcS_C5e7Rj2cscoc"

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.caption("© Izvještaj razvijen od strane *gennaro*")

# =========================
# ROBUSNO ČITANJE EXCELA
# =========================

def _strip_xlsx_styles(raw: bytes) -> bytes:
    """Ukloni styles.xml/theme1.xml iz .xlsx da bi openpyxl mogao da pročita korumpirane fajlove."""
    src = io.BytesIO(raw)
    dst = io.BytesIO()
    with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            if item.filename in ("xl/styles.xml", "xl/theme/theme1.xml"):
                continue
            zout.writestr(item, zin.read(item.filename))
    return dst.getvalue()

def _xlsx2csv_first_sheet(raw: bytes) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as fx:
        fx.write(raw); fx.flush()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as fc:
            Xlsx2csv(fx.name, outputencoding="utf-8").convert(fc.name)
            fc.flush()
            # xlsx2csv ne čuva tipove; sve kao tekst → pokušaj konverzije kasnije po potrebi
            return pd.read_csv(fc.name)

def safe_read_excel_bytes(raw: bytes) -> pd.DataFrame:
    # 1) openpyxl
    try:
        return pd.read_excel(io.BytesIO(raw), engine="openpyxl")
    except Exception:
        pass
    # 2) strip + openpyxl
    try:
        return pd.read_excel(io.BytesIO(_strip_xlsx_styles(raw)), engine="openpyxl")
    except Exception:
        pass
    # 3) xlsx2csv
    try:
        return _xlsx2csv_first_sheet(_strip_xlsx_styles(raw))
    except Exception as e3:
        raise RuntimeError(f"Ne mogu pročitati Excel: {e3}")

def safe_read_excel(path_or_file) -> pd.DataFrame:
    """Radi i za UploadedFile i za putanju/bytes niz."""
    if hasattr(path_or_file, "read"):  # streamlit uploader
        raw = path_or_file.read()
        return safe_read_excel_bytes(raw)
    if isinstance(path_or_file, (bytes, bytearray)):
        return safe_read_excel_bytes(path_or_file)
    if isinstance(path_or_file, str) and os.path.exists(path_or_file):
        with open(path_or_file, "rb") as f:
            return safe_read_excel_bytes(f.read())
    # fallback: možda su već bytes-like
    return safe_read_excel_bytes(bytes(path_or_file))

# =========================
# DATUMI & KPI (NOVI/PRODUŽETCI)
# =========================

def infer_period_from_filename(name: str):
    """ALL_CONTRACT_2025_10.xlsx -> (2025, 10)"""
    m = re.search(r"ALL\s*[_\- ]?CONTRACT\s*[_\- ]?(\d{4})[_\- ](\d{2})", str(name), re.I)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))

def robust_to_datetime(series: pd.Series) -> pd.Series:
    s = series
    # Ako je već datetime64
    if np.issubdtype(s.dtype, np.datetime64):
        return pd.to_datetime(s, errors="coerce")

    # numerički: Excel serijski broj
    if np.issubdtype(s.dtype, np.number):
        s_num = s.astype("float64")
        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
        mask = s_num.between(10000, 60000, inclusive="both")
        if mask.any():
            out.loc[mask] = pd.to_datetime(s_num.loc[mask], unit="d", origin="1899-12-30", errors="coerce")
        rest = ~mask
        if rest.any():
            out.loc[rest] = pd.to_datetime(s.loc[rest].astype(str), errors="coerce", dayfirst=True)
        return out

    # string + pokušaji formata
    s_str = s.astype(str).str.replace("\u00A0", " ", regex=False).str.strip()
    dt = pd.to_datetime(s_str, errors="coerce", dayfirst=True)

    def try_fmt(curr, fmt):
        miss = curr.isna()
        if miss.any():
            parsed = pd.to_datetime(s_str.loc[miss], format=fmt, errors="coerce")
            curr.loc[miss] = parsed
        return curr

    for fmt in ("%d.%m.%y %H:%M", "%d.%m.%y %H:%M:%S", "%d.%m.%Y %H:%M",
                "%Y-%m-%d %H:%M:%S", "%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d"):
        dt = try_fmt(dt, fmt)

    # još jedan pokušaj Excel serijskog
    miss = dt.isna()
    if miss.any():
        s_num = pd.to_numeric(s_str.loc[miss], errors="coerce")
        mask = s_num.between(10000, 60000, inclusive="both")
        if mask.any():
            dt.loc[s_num[mask].index] = pd.to_datetime(s_num[mask], unit="d", origin="1899-12-30", errors="coerce")
    return dt

def calc_new_vs_extension(df: pd.DataFrame, file_name: str):
    """NOVI = Start ∈ (YYYY,MM) iz imena; PRODUŽETAK = ostalo (validni Start)."""
    yy, mm = infer_period_from_filename(file_name)
    if yy is None or "Start" not in df.columns:
        return {"novi": 0, "produzetak": 0, "valid": 0, "period": (yy, mm)}

    s = robust_to_datetime(df["Start"])
    valid = s.notna()
    is_new = valid & (s.dt.year == yy) & (s.dt.month == mm)
    return {
        "novi": int(is_new.sum()),
        "produzetak": int(valid.sum() - is_new.sum()),
        "valid": int(valid.sum()),
        "period": (yy, mm),
    }

# =========================
# POS & SERVISI
# =========================

VIP_LIST = {"VIP_GCP", "VIP_LHP", "VIP_SSA", "VIP_WHE", "VIP_WLC", "VIP_WTR"}
VIP_PREFIX = "VIP_"

def map_pos_tip(raw_pos: str, contract_type: str) -> str:
    if raw_pos is None or str(raw_pos).strip() == "":
        return "Multibrend"
    p = str(raw_pos).strip().upper()

    # BUSINESS poseban POS
    if p in {"BUSINESS", "MTELBUSINESS"} or (contract_type and str(contract_type).strip().lower() == "business" and "BUSINESS" in p):
        return "BUSINESS"

    # MTELWMH/WTH/WDZ
    if p.startswith("MTELWMH"):
        return "MTELWMH"
    if p.startswith("MTELWTH"):
        return "MTELWTH"
    if p.startswith("MTELWDZ"):
        return "MTELWDZ"

    # VIP_* — sve izdvojiti, sa specijalnim ako se poklapa
    if p.startswith(VIP_PREFIX):
        for v in VIP_LIST:
            if p.startswith(v):
                return v
        return p.split()[0]

    if "HARTLAUER" in p:
        return "HARTLAUER"
    if "WEB" in p:
        return "WEB"
    if "TELESALES" in p or ("TELE" in p and "SALES" in p):
        return "TELESALES"
    if "D2" in p:
        return "D2D"
    return "Multibrend"

def classify_services(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def col(name, default=None):
        return out[name] if name in out.columns else default

    q_mobile = col("log_mobiletariff", pd.Series([False]*len(out)))
    r_tv = col("log_tvtariff", pd.Series([False]*len(out)))
    price_tv = col("Price_TV", pd.Series([np.nan]*len(out)))
    internet_dev = col("InternetDevice", pd.Series([""]*len(out)))
    tv_device = col("TVDevice", pd.Series([""]*len(out)))
    mobile_dev = col("MobileDevice", pd.Series([""]*len(out)))
    pos_raw = col("POS", pd.Series([""]*len(out)))
    contract_type = col("ContractType", pd.Series([""]*len(out)))

    # POS tip + BUSINESS flag
    out["POS_TIP"] = [map_pos_tip(p, c) for p, c in zip(pos_raw, contract_type)]
    out["IS_BUSINESS"] = (contract_type.astype(str).str.lower() == "business").astype(int)

    services = []
    starnet = []

    def to_float(x):
        try:
            return float(str(x).replace(",", "."))
        except:
            return np.nan

    for i in range(len(out)):
        tags = []
        # MOBILE
        if str(q_mobile.iloc[i]).strip().upper() == "TRUE":
            tags.append("MOBILE")
        # INTERNET
        if str(internet_dev.iloc[i]).strip().upper() == "MODEM":
            tags.append("INTERNET")
        # TV
        if str(r_tv.iloc[i]).strip().upper() == "TRUE":
            tags.append("TV")
        # TV GO
        price_val = to_float(price_tv.iloc[i])
        qv = str(q_mobile.iloc[i]).strip().upper() == "TRUE"
        rv = str(r_tv.iloc[i]).strip().upper() == "TRUE"
        if qv and (not rv) and pd.notna(price_val) and price_val > 0:
            tags.append("TV GO")
        services.append(tags)

        # STARNET: Internet + Price_TV > 0
        is_modem = str(internet_dev.iloc[i]).strip().upper() == "MODEM"
        is_t_price = (pd.notna(price_val) and price_val > 0)
        starnet.append(bool(is_modem and is_t_price))

    out["_services"] = services
    out["_starnet"] = starnet
    out["_has_phone"] = mobile_dev.astype(str).str.strip().ne("").astype(int)

    td = tv_device.astype(str).str.upper()
    out["_STB"] = (td == "STB").astype(int)
    out["_STB2"] = (td == "STB2").astype(int)
    return out

def pos_pivot(df: pd.DataFrame, only_business: bool | None = None) -> pd.DataFrame:
    d = df.copy()
    if only_business is True:
        d = d[d["IS_BUSINESS"] == 1]
    elif only_business is False:
        d = d[d["IS_BUSINESS"] == 0]

    long = d[["POS_TIP", "_services", "_starnet"]].explode("_services")
    long = long[long["_services"].notna()]

    pt = pd.pivot_table(long, index="POS_TIP", columns="_services",
                        values="_services", aggfunc="count", fill_value=0)

    stn = d.groupby("POS_TIP")["_starnet"].sum().rename("STARNET").to_frame()

    # TV total = TV + STARNET
    tv_col = pt["TV"] if "TV" in pt.columns else pd.Series(0, index=pt.index)
    tv_total = tv_col.add(stn["STARNET"].reindex(pt.index, fill_value=0), fill_value=0)
    pt["TV"] = tv_total
    pt = pt.join(stn, how="left").fillna(0).astype(int)

    for col in ["MOBILE", "TV", "INTERNET", "TV GO", "STARNET"]:
        if col not in pt.columns:
            pt[col] = 0
    pt = pt[["MOBILE", "TV", "INTERNET", "TV GO", "STARNET"]].sort_index()

    total = pd.DataFrame([pt.sum(numeric_only=True)], index=["Ukupno"])
    pt = pd.concat([pt, total], axis=0)
    return pt

def top_tariffs(df: pd.DataFrame) -> dict:
    def norm_true(x): return str(x).strip().upper() == "TRUE"
    name_col = "log_tariffname"
    if name_col not in df.columns:
        return {"MOBILE": pd.DataFrame(), "INTERNET": pd.DataFrame(), "TV": pd.DataFrame(), "STARNET": pd.DataFrame()}

    # MOBILE
    mob = df[["log_mobiletariff", name_col]].copy()
    mob = mob[mob["log_mobiletariff"].apply(norm_true)]
    top_mob = mob.groupby(name_col).size().sort_values(ascending=False).rename("Broj").to_frame()

    # INTERNET
    itn = df[["InternetDevice", name_col]].copy()
    itn = itn[itn["InternetDevice"].astype(str).str.upper() == "MODEM"]
    top_int = itn.groupby(name_col).size().sort_values(ascending=False).rename("Broj").to_frame()

    # TV
    tv = df[["log_tvtariff", name_col]].copy()
    tv = tv[tv["log_tvtariff"].apply(norm_true)]
    top_tv = tv.groupby(name_col).size().sort_values(ascending=False).rename("Broj").to_frame()

    # STARNET
    def has_price(v):
        try: return float(str(v).replace(",", ".")) > 0
        except: return False
    stn = df[["InternetDevice", "Price_TV", name_col]].copy()
    stn = stn[(stn["InternetDevice"].astype(str).str.upper()=="MODEM") & (stn["Price_TV"].apply(has_price))]
    top_stn = stn.groupby(name_col).size().sort_values(ascending=False).rename("Broj").to_frame()

    def add_total_row(df_table: pd.DataFrame, label="Ukupno"):
        if df_table.empty: return df_table
        total = pd.DataFrame([df_table.sum(numeric_only=True)], index=[label])
        return pd.concat([df_table, total], axis=0)

    return {
        "MOBILE": add_total_row(top_mob),
        "INTERNET": add_total_row(top_int),
        "TV": add_total_row(top_tv),
        "STARNET": add_total_row(top_stn),
    }

def phones_by_pos(df: pd.DataFrame) -> pd.DataFrame:
    if "MobileDevice" not in df.columns: return pd.DataFrame()
    d = df.copy()
    d["MobileDevice"] = d["MobileDevice"].astype(str).str.strip()
    d = d[d["MobileDevice"].notna() & (d["MobileDevice"] != "nan") & (d["MobileDevice"] != "None")]

    if d.empty: return pd.DataFrame()
    if "POS_TIP" not in d.columns: d["POS_TIP"] = "NEPOZNAT"

    tab = (
        d.assign(cnt=1)
         .groupby(["MobileDevice", "POS_TIP"], dropna=False)["cnt"]
         .sum()
         .unstack(fill_value=0)
    )
    tab["Ukupno"] = tab.sum(axis=1)
    tab = tab.sort_values("Ukupno", ascending=False)
    return tab

def stb_by_pos(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "TVDevice" not in d.columns:
        return pd.DataFrame()
    d["TVDevice"] = d["TVDevice"].astype(str).str.strip().str.upper()

    d["_STB"] = (d["TVDevice"] == "STB").astype(int)
    d["_STB2"] = (d["TVDevice"] == "STB2").astype(int)
    if "POS_TIP" not in d.columns:
        d["POS_TIP"] = "NEPOZNAT"

    grp = d.groupby("POS_TIP")[["_STB", "_STB2"]].sum().T
    grp.index = ["STB", "STB2"]
    grp["Ukupno"] = grp.sum(axis=1)
    return grp

# =========================
# GDRIVE AUTENTIFIKACIJA (PyDrive2)
# =========================

def _secrets_sa_dict() -> dict:
    """Vrati service_account_json kao dict (radi i za JSON string i za TOML mapu)."""
    g = st.secrets.get("gdrive", {})
    raw = g.get("service_account_json")
    if raw is None:
        raise RuntimeError("Nedostaje gdrive.service_account_json u secrets.")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        return json.loads(raw)
    raise TypeError(f"service_account_json neočekivan tip: {type(raw)}")

@st.cache_resource(show_spinner=False)
def _gdrive_client():
    from pydrive2.auth import GoogleAuth
    from pydrive2.drive import GoogleDrive

    svc_dict = _secrets_sa_dict()  # uvijek dict
    folder_id = st.secrets.get("gdrive", {}).get("folder_id", DEFAULT_GDRIVE_FOLDER_ID)

    settings = {
        "client_config_backend": "service",
        "service_config": {
            # KLJUČNO: dict ide u client_json_dict
            "client_json_dict": svc_dict
        }
    }

    ga = GoogleAuth(settings=settings)
    ga.ServiceAuth()
    drive = GoogleDrive(ga)
    return drive, folder_id

def gdrive_list_reports():
    drive, folder_id = _gdrive_client()
    q = (
        f"'{folder_id}' in parents and trashed=false "
        f"and title contains 'ALL_CONTRACT_' and title contains '.xlsx'"
    )
    file_list = drive.ListFile({
        'q': q,
        'maxResults': 1000,
        'supportsAllDrives': True,
        'includeItemsFromAllDrives': True
    }).GetList()

    rows = []
    for f in file_list:
        nm = f.get("title") or f.get("name")
        rows.append({"id": f["id"], "name": nm, "mime": f.get("mimeType","")})
    df = pd.DataFrame(rows)
    if not df.empty:
        df["name"] = df["name"].astype(str)
        df = df.sort_values(by="name")
    return df

def gdrive_download_bytes(file_id: str) -> bytes:
    drive, _ = _gdrive_client()
    f = drive.CreateFile({'id': file_id})
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        f.GetContentFile(tmp.name)
        tmp.flush()
        with open(tmp.name, "rb") as r:
            data = r.read()
    try: os.unlink(tmp.name)
    except: pass
    return data

# =========================
# UI — SIDEBAR
# =========================

if os.path.exists(APP_LOGO):
    st.sidebar.image(APP_LOGO, use_container_width=False, width=150)
st.sidebar.markdown("### 🧭 Navigacija")

mode = st.sidebar.radio(
    "Izaberi način rada:",
    ["Analiza jednog fajla (GDrive)", "Analiza više fajlova (GDrive)", "Uporedna analiza (GDrive)", "Lokalni upload"]
)

# =========================
# 1) ANALIZA JEDNOG FAJLA — GDRIVE
# =========================

if mode == "Analiza jednog fajla (GDrive)":
    st.subheader("📁 Analiza jednog fajla — Google Drive")
    try:
        df_files = gdrive_list_reports()
    except Exception as e:
        st.error(f"❌ GDrive autentikacija/čitanje nije uspjelo: {e}")
        df_files = pd.DataFrame()

    if df_files.empty:
        st.info("Nema dostupnih fajlova (ALL_CONTRACT_YYYY_MM.xlsx) u Drive folderu.")
    else:
        names = df_files["name"].tolist()
        pick = st.selectbox("Odaberi fajl", names, index=len(names)-1 if names else 0)
        if pick:
            file_id = df_files.loc[df_files["name"] == pick, "id"].iloc[0]
            raw = gdrive_download_bytes(file_id)
            df_raw = safe_read_excel_bytes(raw)
            df = classify_services(df_raw)

            st.caption(f"Učitano: **{pick}** — redova: {len(df)}")

            # KPI NOVI/PRODUŽECI
            res_np = calc_new_vs_extension(df_raw, pick)
            k1, k2, k3, k4, k5 = st.columns(5)
            # Osnovni KPI (MOBILE, TV, INTERNET, TV GO, Telefoni)
            k1.metric("📱 MOBILE", int((df["_services"].apply(lambda x: "MOBILE" in x)).sum()))
            k2.metric("📺 TV", int((df["_services"].apply(lambda x: "TV" in x)).sum()))
            k3.metric("🌐 INTERNET", int((df["_services"].apply(lambda x: "INTERNET" in x)).sum()))
            k4.metric("▶️ TV GO", int((df["_services"].apply(lambda x: "TV GO" in x)).sum()))
            k5.metric("📦 Telefoni", int(df["_has_phone"].sum()))

            a1, a2, a3 = st.columns(3)
            a1.metric("✅ NOVI", f'{res_np["novi"]:,}'.replace(",", " "))
            a2.metric("🔁 PRODUŽECI", f'{res_np["produzetak"]:,}'.replace(",", " "))
            a3.metric("📅 Validni START", f'{res_np["valid"]:,}'.replace(",", " "))

            st.divider()

            st.markdown("### Prodaja po POS tipu — SVI ugovori")
            st.dataframe(pos_pivot(df), use_container_width=True, height=420)

            st.markdown("### Prodaja po POS tipu — samo BUSINESS ugovori")
            st.dataframe(pos_pivot(df, only_business=True), use_container_width=True, height=420)

            st.markdown("### Prodaja po POS tipu — REZIDENCIJALA (bez Business)")
            st.dataframe(pos_pivot(df, only_business=False), use_container_width=True, height=420)

            st.divider()
            st.markdown("### Najprodavanije tarife (bez TV GO) + STARNET (TV uz Internet)")
            tops = top_tariffs(df_raw)
            c1, c2, c3 = st.columns(3)
            with c1:
                st.subheader("MOBILE")
                st.dataframe(tops["MOBILE"], use_container_width=True, height=360)
            with c2:
                st.subheader("INTERNET")
                st.dataframe(tops["INTERNET"], use_container_width=True, height=360)
            with c3:
                st.subheader("TV")
                st.dataframe(tops["TV"], use_container_width=True, height=360)
            st.caption("STARNET (TV uz Internet)")
            st.dataframe(tops["STARNET"], use_container_width=True, height=260)

            st.divider()
            st.markdown("### Prodaja telefona po POS-u")
            ph = phones_by_pos(df)
            if ph.empty:
                st.info("Nema prodatih telefona u ovom periodu.")
            else:
                st.dataframe(ph, use_container_width=True, height=480)

            st.divider()
            st.markdown("### STB uređaji po POS-u (STB, STB2)")
            stb = stb_by_pos(df)
            if stb.empty:
                st.info("Nema STB uređaja evidentiranih u ovom periodu.")
            else:
                st.dataframe(stb, use_container_width=True, height=280)

# =========================
# 2) ANALIZA VIŠE FAJLOVA — GDRIVE
# =========================

elif mode == "Analiza više fajlova (GDrive)":
    st.subheader("📂 Analiza više fajlova — Google Drive")
    try:
        df_files = gdrive_list_reports()
    except Exception as e:
        st.error(f"❌ GDrive autentikacija/čitanje nije uspjelo: {e}")
        df_files = pd.DataFrame()

    if df_files.empty:
        st.info("Nema dostupnih fajlova.")
    else:
        df_files["PERIOD"] = df_files["name"].str.extract(r"(\d{4}_\d{2})", expand=False)
        years = sorted({ int(x.split("_")[0]) for x in df_files["PERIOD"].dropna() })
        y_sel = st.multiselect("Godina", years, default=years)

        mask = df_files["PERIOD"].apply(lambda p: (p is not None) and (int(p.split("_")[0]) in y_sel))
        pick_names = df_files[mask]["name"].tolist()

        chosen = st.multiselect("Odaberi fajlove", pick_names, default=pick_names)
        if chosen:
            rows = []
            for nm in sorted(chosen):
                file_id = df_files.loc[df_files["name"] == nm, "id"].iloc[0]
                raw = gdrive_download_bytes(file_id)
                df_raw = safe_read_excel_bytes(raw)
                res = calc_new_vs_extension(df_raw, nm)
                rows.append({
                    "Fajl": nm,
                    "Period": f"{res['period'][0]}-{res['period'][1]:02d}",
                    "NOVI": res["novi"],
                    "PRODUŽECI": res["produzetak"],
                    "VALID": res["valid"]
                })
            tab = pd.DataFrame(rows).sort_values(by="Period")
            st.markdown("### KPI: NOVI / PRODUŽECI po mjesecima")
            st.dataframe(tab, use_container_width=True, height=420)

            st.markdown("#### Trend novih ugovora po mjesecima")
            if not tab.empty:
                trend = tab.set_index("Period")[["NOVI"]]
                st.line_chart(trend)

# =========================
# 3) UPOREDNA ANALIZA — GDRIVE
# =========================

elif mode == "Uporedna analiza (GDrive)":
    st.subheader("🔄 Uporedna analiza dva mjeseca — Google Drive")
    try:
        df_files = gdrive_list_reports()
    except Exception as e:
        st.error(f"❌ GDrive autentikacija/čitanje nije uspjelo: {e}")
        df_files = pd.DataFrame()

    if df_files.empty:
        st.info("Nema fajlova.")
    else:
        names = df_files["name"].tolist()
        c1, c2 = st.columns(2)
        with c1:
            a_name = st.selectbox("Stariji period (Fajl A)", names, index=max(0, len(names)-2))
        with c2:
            b_name = st.selectbox("Noviji period (Fajl B)", names, index=len(names)-1)

        if a_name and b_name:
            a_id = df_files.loc[df_files["name"] == a_name, "id"].iloc[0]
            b_id = df_files.loc[df_files["name"] == b_name, "id"].iloc[0]
            a_raw = gdrive_download_bytes(a_id)
            b_raw = gdrive_download_bytes(b_id)
            df_a_raw = safe_read_excel_bytes(a_raw)
            df_b_raw = safe_read_excel_bytes(b_raw)
            df_a = classify_services(df_a_raw)
            df_b = classify_services(df_b_raw)

            # NOVI/PRODUŽECI KPI
            ra = calc_new_vs_extension(df_a_raw, a_name)
            rb = calc_new_vs_extension(df_b_raw, b_name)
            st.markdown("#### NOVI / PRODUŽECI — KPI")
            k1, k2, k3 = st.columns(3)
            k1.metric(f"NOVI ({a_name})", f'{ra["novi"]:,}'.replace(",", " "))
            k2.metric(f"NOVI ({b_name})", f'{rb["novi"]:,}'.replace(",", " "))
            try:
                delta = (rb["novi"] - ra["novi"]) / max(ra["novi"], 1) * 100
                k3.metric("Promjena (%)", f"{delta:.2f}%")
            except ZeroDivisionError:
                k3.metric("Promjena (%)", "—")

            st.divider()

            st.markdown("### Rezidencijala — Prodaja po POS tipu")
            pA = pos_pivot(df_a, only_business=False)
            pB = pos_pivot(df_b, only_business=False)
            c3, c4 = st.columns(2)
            c3.subheader(a_name); c3.dataframe(pA, use_container_width=True, height=420)
            c4.subheader(b_name); c4.dataframe(pB, use_container_width=True, height=420)

            st.markdown("### Business — Prodaja po POS tipu")
            pA_b = pos_pivot(df_a, only_business=True)
            pB_b = pos_pivot(df_b, only_business=True)
            c5, c6 = st.columns(2)
            c5.subheader(a_name); c5.dataframe(pA_b, use_container_width=True, height=420)
            c6.subheader(b_name); c6.dataframe(pB_b, use_container_width=True, height=420)

# =========================
# 4) LOKALNI UPLOAD (TEST)
# =========================

elif mode == "Lokalni upload":
    st.subheader("🧪 Lokalni upload (drag & drop)")
    up = st.file_uploader("Izaberi .xlsx fajl", type=["xlsx"])
    if up:
        df_raw = safe_read_excel(up)
        df = classify_services(df_raw)
        st.caption(f"Učitano: **{up.name}** — redova: {len(df)}")

        res = calc_new_vs_extension(df_raw, up.name)
        a, b, c = st.columns(3)
        a.metric("✅ NOVI", f'{res["novi"]:,}'.replace(",", " "))
        b.metric("🔁 PRODUŽECI", f'{res["produzetak"]:,}'.replace(",", " "))
        c.metric("📅 VALID START", f'{res["valid"]:,}'.replace(",", " "))

        st.divider()
        st.dataframe(pos_pivot(df), use_container_width=True, height=420)

st.caption("© Izvještaj generisan prema poslovnim pravilima MTEL AT — *gennaro*")