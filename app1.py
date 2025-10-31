import os
import re
import io
import zipfile
from datetime import datetime
import pandas as pd
import numpy as np
import streamlit as st
import matplotlib.pyplot as plt  # koristimo matplotlib (stabilno na Py 3.14)

# =========================
#   PAGE LAYOUT & HEADER
# =========================
st.set_page_config(page_title="Analiza prodaje Austrija", layout="wide")

cols = st.columns([1, 6])
with cols[0]:
    try:
        st.image("mtel.png", use_container_width=True)
    except Exception:
        st.write("")
with cols[1]:
    st.title("Analiza prodaje Austrija")
    st.caption(
        "Analiza ugovora po uslugama (MOBILE, TV, INTERNET, TV GO), POS tipovima, Business/Rezidencijala, "
        "tarife (TV=STARNET uz internet), telefoni, STB/STB2, KPI. Trendovi po mjesecima i kvartalima (samo NOVI). "
        "Zaseban ekran: Uporedna analiza 2 mjeseca sa % razlikama."
    )

# =========================
#   HELPER FUNKCIJE
# =========================
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

def services_for_row(r: pd.Series) -> list[str]:
    s = []
    # MOBILE
    if r.get("_mobile", False):
        s.append("MOBILE")
    # TV (direktno)
    if r.get("_tv", False):
        s.append("TV")
    # INTERNET (+ dodatni TV ako ima Price_TV)
    if r.get("_inet_modem", False):
        s.append("INTERNET")
        if r.get("_price_tv", False):
            s.append("TV")  # TV uz internet -> brojimo i TV
    # TV GO
    if r.get("_mobile", False) and (not r.get("_tv", False)) and r.get("_price_tv", False):
        s.append("TV GO")
    return s

def map_pos_tip(pos_val: str) -> str:
    s = "" if pd.isna(pos_val) else str(pos_val)
    su = s.upper()
    # VIP podgrupe (specifični kodovi)
    if su.startswith("VIP_GCP"): return "VIP_GCP"
    if su.startswith("VIP_LHP"): return "VIP_LHP"
    if su.startswith("VIP_SSA"): return "VIP_SSA"
    if su.startswith("VIP_WHE"): return "VIP_WHE"
    if su.startswith("VIP_WLC"): return "VIP_WLC"
    if su.startswith("VIP_WTR"): return "VIP_WTR"
    if su.startswith("VIP_"):    return "VIP_OTHER"
    # Ostali kanali
    if "WMH" in su:               return "WMH"
    if "WTH" in su:               return "WTH"
    if "WDZ" in su:               return "WDZ"
    if "HARTLAUER" in su:         return "HARTLAUER"
    if "WEB" in su:               return "WEB"
    if "TELESALES" in su:         return "TELESALES"
    if "D2" in su:                return "D2D"
    return "MULTIBREND"

def derive_period_from_filename(name: str) -> tuple[int | None, int | None]:
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

# ============ XLSX load helpers ============
def repair_xlsx(file_like: io.BytesIO) -> io.BytesIO:
    inp = io.BytesIO(file_like.read())
    inp.seek(0)
    out_buf = io.BytesIO()
    with zipfile.ZipFile(inp, "r") as zin, zipfile.ZipFile(out_buf, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            if item.filename.lower() in {"xl/styles.xml", "xl/theme/theme1.xml"}:
                continue
            data = zin.read(item.filename)
            if item.filename.lower().endswith((".xml", ".rels")):
                try:
                    text = data.decode("utf-8")
                    text = re.sub(r"http:\s*//", "http://", text)
                    text = re.sub(r"https:\s*//", "https://", text)
                    data = text.encode("utf-8")
                except Exception:
                    pass
            zout.writestr(item, data)
    out_buf.seek(0)
    return out_buf

def load_one_excel(file_bytes: bytes, filename_hint: str) -> pd.DataFrame:
    frames = []
    fbytes = io.BytesIO(file_bytes)
    fbytes.seek(0)
    try:
        xls = pd.ExcelFile(fbytes, engine="openpyxl")
        for s in xls.sheet_names:
            d = pd.read_excel(xls, sheet_name=s)
            d["__source_sheet__"] = s
            d["__file__"] = os.path.basename(filename_hint)
            frames.append(d)
    except Exception:
        cleaned = repair_xlsx(io.BytesIO(file_bytes))
        xls = pd.ExcelFile(cleaned, engine="openpyxl")
        for s in xls.sheet_names:
            d = pd.read_excel(xls, sheet_name=s)
            d["__source_sheet__"] = s
            d["__file__"] = os.path.basename(filename_hint)
            frames.append(d)
    return pd.concat(frames, ignore_index=True)

# ===== Helperi za uporednu analizu (2 mjeseca) =====
def preprocess_month_df(raw_month: pd.DataFrame) -> pd.DataFrame:
    col_mobile         = find_col(raw_month, ["log_mobiletariff", "mobiletariff"])
    col_tv             = find_col(raw_month, ["log_tvtariff", "tvtariff"])
    col_price_tv       = find_col(raw_month, ["price_tv", "tv_price"])
    col_inet_dev       = find_col(raw_month, ["internetdevice", "internet_device"])
    col_pos            = find_col(raw_month, ["pos", "pos_name", "shop", "store"])
    col_start          = find_col(raw_month, ["start"])
    col_contract_type  = find_col(raw_month, ["contracttype"])

    dfm = raw_month.copy()
    dfm["_start_dt"] = dfm[col_start].apply(parse_excel_datetime) if col_start in dfm.columns else pd.NaT
    dfm["_is_business"] = (
        dfm[col_contract_type].astype(str).str.strip().str.upper().eq("BUSINESS")
        if col_contract_type in dfm.columns else False
    )
    dfm["_mobile"]     = dfm[col_mobile].map(truthy) if col_mobile in dfm.columns else False
    dfm["_tv"]         = dfm[col_tv].map(truthy) if col_tv in dfm.columns else False
    dfm["_price_tv"]   = dfm[col_price_tv].map(has_value) if col_price_tv in dfm.columns else False
    dfm["_inet_modem"] = dfm[col_inet_dev].astype(str).str.strip().str.upper().eq("MODEM") if col_inet_dev in dfm.columns else False
    dfm["_services"]   = dfm.apply(services_for_row, axis=1)
    dfm["POS_TIP"]     = dfm[col_pos].apply(map_pos_tip) if col_pos in dfm.columns else "MULTIBREND"
    return dfm

def pos_pivot_by_business(dfm: pd.DataFrame):
    long = dfm[["_services", "POS_TIP", "_is_business"]].explode("_services").dropna()
    bus = long[long["_is_business"]]
    res = long[~long["_is_business"]]

    def _pivot(sub):
        p = (sub.groupby(["POS_TIP", "_services"]).size().reset_index(name="broj")
               .pivot(index="POS_TIP", columns="_services", values="broj")
               .fillna(0).astype(int))
        for c in ["MOBILE", "TV", "INTERNET", "TV GO"]:
            if c not in p.columns:
                p[c] = 0
        return p[["MOBILE", "TV", "INTERNET", "TV GO"]]

    return _pivot(bus), _pivot(res)

def pct_change_table(new_df: pd.DataFrame, old_df: pd.DataFrame) -> pd.DataFrame:
    all_idx = sorted(set(old_df.index) | set(new_df.index))
    all_cols = ["MOBILE", "TV", "INTERNET", "TV GO"]
    new_al = new_df.reindex(index=all_idx, columns=all_cols).fillna(0)
    old_al = old_df.reindex(index=all_idx, columns=all_cols).fillna(0)
    diff = new_al - old_al
    with np.errstate(divide="ignore", invalid="ignore"):
        pct = (diff / old_al.replace(0, np.nan)) * 100
    pct = pct.fillna(0)
    pct[(old_al == 0) & (new_al > 0)] = np.inf
    pct = pct.round(1)
    return pct

# =========================
#   REŽIMI & UČITAVANJE
# =========================
st.sidebar.header("⚙️ Režim analize")
mode = st.sidebar.radio(
    "Izaberi režim:",
    [
        "Analiza pojedinačnog fajla",
        "Analiza perioda (više fajlova)",
        "Uporedna analiza (2 mjeseca)",
    ]
)

# default za poređenje
compare_folder = "/Users/dzajic/Documents/MtelReportApp/data"
idx_old = None
idx_new = None

if mode == "Analiza pojedinačnog fajla":
    uploaded = st.file_uploader("Uploaduj Excel (XLSX)", type=["xlsx"])
    if not uploaded:
        st.stop()
    raw = load_one_excel(uploaded.read(), getattr(uploaded, "name", "uploaded.xlsx"))

elif mode == "Analiza perioda (više fajlova)":
    folder = st.sidebar.text_input("Putanja do foldera", "/Users/dzajic/Documents/MtelReportApp/data")
    pattern = re.compile(r"^ALL_CONTRACT_(2024|2025)_(0[1-9]|1[0-2])\.xlsx$")
    entries = [n for n in os.listdir(folder) if pattern.match(n)]
    ym_rows = []
    for n in entries:
        m = pattern.match(n)
        ym_rows.append((int(m.group(1)), int(m.group(2)), os.path.join(folder, n)))
    df_ym = pd.DataFrame(ym_rows, columns=["godina", "mjesec", "putanja"]).sort_values(["godina", "mjesec"])
    if df_ym.empty:
        st.warning("Nije pronađen nijedan fajl u formatu ALL_CONTRACT_YYYY_MM.xlsx u izabranom folderu.")
        st.stop()
    god_l = sorted(df_ym["godina"].unique())
    godina_od = st.sidebar.selectbox("Godina OD", god_l, index=0)
    godina_do = st.sidebar.selectbox("Godina DO", god_l, index=len(god_l)-1)
    mj_od = st.sidebar.slider("Mjesec OD", 1, 12, 1)
    mj_do = st.sidebar.slider("Mjesec DO", 1, 12, 12)
    mask = (
        ((df_ym["godina"] > godina_od) | ((df_ym["godina"] == godina_od) & (df_ym["mjesec"] >= mj_od))) &
        ((df_ym["godina"] < godina_do) | ((df_ym["godina"] == godina_do) & (df_ym["mjesec"] <= mj_do)))
    )
    chosen = df_ym.loc[mask].sort_values(["godina", "mjesec"])
    if chosen.empty:
        st.warning("Nema fajlova u izabranom opsegu.")
        st.stop()
    frames = []
    for _, row in chosen.iterrows():
        with open(row["putanja"], "rb") as fh:
            frames.append(load_one_excel(fh.read(), row["putanja"]))
    raw = pd.concat(frames, ignore_index=True)

elif mode == "Uporedna analiza (2 mjeseca)":
    compare_folder = st.sidebar.text_input("Folder sa mjesečnim fajlovima", "/Users/dzajic/Documents/MtelReportApp/data")
    file_rx_sb = re.compile(r"^ALL_CONTRACT_(20\d{2})_(0[1-9]|1[0-2])\.xlsx$")
    month_files_sb = [f for f in os.listdir(compare_folder) if file_rx_sb.match(f)]

    if not month_files_sb:
        st.warning("Nema mjesečnih fajlova u izabranom folderu (ALL_CONTRACT_YYYY_MM.xlsx).")
        st.stop()

    month_keys_sb = []
    for f in month_files_sb:
        m = file_rx_sb.match(f)
        y, mo = int(m.group(1)), int(m.group(2))
        month_keys_sb.append((y, mo, f))
    month_keys_sb.sort()
    labels_sb = [f"{y}-{mo:02d}" for (y, mo, f) in month_keys_sb]
    files_sb  = [f for (_, _, f) in month_keys_sb]

    idx_old = st.sidebar.selectbox("Stariji mjesec", list(range(len(labels_sb))),
                                   format_func=lambda i: labels_sb[i],
                                   index=max(0, len(labels_sb)-2), key="cmp_old_top")
    idx_new = st.sidebar.selectbox("Noviji mjesec", list(range(len(labels_sb))),
                                   format_func=lambda i: labels_sb[i],
                                   index=len(labels_sb)-1, key="cmp_new_top")

    # placeholder 'raw' da ostatak skripte (helperi) ostane sretan
    raw = pd.DataFrame({"__file__": []})

# =========================
#   PRIPREMA I KALKULACIJE (za režime 1 i 2)
# =========================
if mode in ("Analiza pojedinačnog fajla", "Analiza perioda (više fajlova)"):
    col_mobile         = find_col(raw, ["log_mobiletariff", "mobiletariff"])
    col_tv             = find_col(raw, ["log_tvtariff", "tvtariff"])
    col_price_tv       = find_col(raw, ["price_tv", "tv_price"])
    col_inet_dev       = find_col(raw, ["internetdevice", "internet_device"])
    col_tv_device      = find_col(raw, ["tvdevice"])
    col_mobile_device  = find_col(raw, ["mobiledevice"])
    col_pos            = find_col(raw, ["pos", "pos_name", "shop", "store"])
    col_tariff         = find_col(raw, ["log_tariffname", "tariff", "tarifa", "naziv_tarife", "plan", "paket"])
    col_start          = find_col(raw, ["start"])
    col_contract_type  = find_col(raw, ["contracttype"])

    df = raw.copy()

    # Datumi
    df["_start_dt"] = df[col_start].apply(parse_excel_datetime) if col_start in df.columns else pd.NaT

    # Business flag
    df["_is_business"] = (
        df[col_contract_type].astype(str).str.strip().str.upper().eq("BUSINESS")
        if col_contract_type in df.columns else False
    )

    # Servisni flagovi
    df["_mobile"]     = df[col_mobile].map(truthy) if col_mobile in df.columns else False
    df["_tv"]         = df[col_tv].map(truthy) if col_tv in df.columns else False
    df["_price_tv"]   = df[col_price_tv].map(has_value) if col_price_tv in df.columns else False
    df["_inet_modem"] = df[col_inet_dev].astype(str).str.strip().str.upper().eq("MODEM") if col_inet_dev in df.columns else False

    # Servisi & POS
    df["_services"]   = df.apply(services_for_row, axis=1)
    df["POS_TIP"]     = df[col_pos].apply(map_pos_tip) if col_pos in df.columns else "MULTIBREND"
    df["POS_TIP_BUS"] = np.where(df["_is_business"], "BUSINESS", df["POS_TIP"])

    # --- NOVI vs PRODUŽENJE ---
    if mode == "Analiza pojedinačnog fajla":
        fname = str(df["__file__"].iloc[0]) if "__file__" in df.columns else ""
        y_hint, m_hint = derive_period_from_filename(fname)
        if not (y_hint and m_hint):
            counts = df["_start_dt"].dropna().dt.to_period("M").value_counts()
            if len(counts) > 0:
                per = counts.index[0]
                y_hint, m_hint = per.year, per.month
            else:
                now = datetime.now()
                y_hint, m_hint = now.year, now.month
        df["CONTRACT_TIP"] = np.where(
            (df["_start_dt"].dt.year == y_hint) & (df["_start_dt"].dt.month == m_hint),
            "NOVI", "PRODUŽENJE"
        )
    else:
        def _infer_contract_tip(row):
            m = re.search(r"(20\d{2})_(0[1-9]|1[0-2])", str(row.get("__file__", "")))
            if m and not pd.isna(row.get("_start_dt", pd.NaT)):
                y, mo = int(m.group(1)), int(m.group(2))
                return "NOVI" if (row["_start_dt"].year == y and row["_start_dt"].month == mo) else "PRODUŽENJE"
            return "PRODUŽENJE"
        df["CONTRACT_TIP"] = df.apply(_infer_contract_tip, axis=1)

    # =========================
    #   KPI + TABELE
    # =========================
    long_all = df[["CONTRACT_TIP", "POS_TIP_BUS", "_services"]].explode("_services").dropna()

    svc_counts   = long_all["_services"].value_counts().to_dict()
    mobile_cnt   = int(svc_counts.get("MOBILE", 0))
    tv_cnt       = int(svc_counts.get("TV", 0))
    internet_cnt = int(svc_counts.get("INTERNET", 0))
    tvgo_cnt     = int(svc_counts.get("TV GO", 0))
    phones_cnt   = 0
    col_mobile_device = find_col(raw, ["mobiledevice"])
    if col_mobile_device and col_mobile_device in df.columns:
        phones_cnt = int(df[col_mobile_device].notna().sum())
    biz_cnt      = int(df["_is_business"].sum())

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1: st.metric("📱 MOBILE", mobile_cnt)
    with k2: st.metric("📺 TV", tv_cnt)
    with k3: st.metric("🌐 INTERNET", internet_cnt)
    with k4: st.metric("📲 TV GO", tvgo_cnt)
    with k5: st.metric("📦 Telefoni", phones_cnt)
    with k6: st.metric("🏢 BUSINESS", biz_cnt)

    st.divider()
    st.subheader("Ugovori po usluzi × tip (NOVI / PRODUŽENJE)")
    svc_tip = (
        long_all.groupby(["_services", "CONTRACT_TIP"])
            .size().reset_index(name="broj")
            .pivot(index="_services", columns="CONTRACT_TIP", values="broj")
            .fillna(0).astype(int)
    )
    st.dataframe(with_totals_pivot(svc_tip), use_container_width=True)

    # ============== TARIFE ==============
    st.divider()
    st.subheader("Prodaja tarifa (MOBILE / TV / INTERNET)")
    col_tariff = find_col(raw, ["log_tariffname", "tariff", "tarifa", "naziv_tarife", "plan", "paket"])
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
                    records.append(("TV", "STARNET"))  # TV uz internet -> STARNET
        tariffs_long = pd.DataFrame(records, columns=["_services", "tarifa"])
        tariffs_long = tariffs_long[tariffs_long["_services"].isin(["MOBILE", "TV", "INTERNET"])]
        sales_by_tariff = (
            tariffs_long.groupby(["_services", "tarifa"]).size().reset_index(name="broj")
            .sort_values(["_services", "broj"], ascending=[True, False])
        )
        tabs = st.tabs(["MOBILE", "TV", "INTERNET"])
        for svc, tab in zip(["MOBILE", "TV", "INTERNET"], tabs):
            with tab:
                tdf = sales_by_tariff[sales_by_tariff["_services"] == svc].drop(columns=["_services"]).reset_index(drop=True)
                st.dataframe(with_total_row(tdf, value_col="broj", label_col="tarifa"), use_container_width=True)
    else:
        st.info("Nema kolone sa tarifama (H).")

    # ============== TELEFONI ==============
    st.divider()
    st.subheader("Prodaja telefona po POS tipu (MobileDevice × POS)")
    if col_mobile_device and col_mobile_device in df.columns:
        phones = (
            df.dropna(subset=[col_mobile_device])
              .groupby([col_mobile_device, "POS_TIP_BUS"])
              .size().reset_index(name="broj")
        )
        phones_pivot = phones.pivot(index=col_mobile_device, columns="POS_TIP_BUS", values="broj").fillna(0).astype(int)
        st.dataframe(with_totals_pivot(phones_pivot), use_container_width=True)
    else:
        st.info("Kolona MobileDevice (AA) nije pronađena.")

    # ============== STB ==============
    st.divider()
    st.subheader("STB uređaji po POS tipu (STB/STB2)")
    col_tv_device = find_col(raw, ["tvdevice"])
    if col_tv_device and col_tv_device in df.columns:
        df["_tvdev"] = df[col_tv_device].astype(str).str.upper().str.strip()
        stb_df = df[df["_tvdev"].isin(["STB", "STB2"])]
        stb = stb_df.groupby(["_tvdev", "POS_TIP_BUS"]).size().reset_index(name="broj")
        stb_pivot = stb.pivot(index="_tvdev", columns="POS_TIP_BUS", values="broj").fillna(0).astype(int)
        st.dataframe(with_totals_pivot(stb_pivot), use_container_width=True)
    else:
        st.info("Kolona TVDevice (AD) nije pronađena.")

    # ============== POS × USLUGA (ukupno) ==============
    st.divider()
    st.subheader("Prodaja po POS tipu (MOBILE, TV, INTERNET, TV GO) — uključuje BUSINESS")
    pos_pivot_all = (
        long_all.groupby(["POS_TIP_BUS", "_services"]).size().reset_index(name="broj")
           .pivot(index="POS_TIP_BUS", columns="_services", values="broj")
           .fillna(0).astype(int)
    )
    for c in ["MOBILE", "TV", "INTERNET", "TV GO"]:
        if c not in pos_pivot_all.columns:
            pos_pivot_all[c] = 0
    pos_pivot_all = pos_pivot_all[["MOBILE", "TV", "INTERNET", "TV GO"]]
    st.dataframe(with_totals_pivot(pos_pivot_all), use_container_width=True)

    # ============== POS × USLUGA — samo BUSINESS ==============
    st.divider()
    st.subheader("Prodaja po POS tipu — samo BUSINESS ugovori")
    df_bus = df[df["_is_business"]].copy()
    if df_bus.empty:
        st.info("Nema BUSINESS ugovora u odabranom periodu.")
    else:
        long_bus = df_bus[["_services", "POS_TIP"]].explode("_services").dropna()
        pos_bus_pivot = (
            long_bus.groupby(["POS_TIP", "_services"]).size().reset_index(name="broj")
                    .pivot(index="POS_TIP", columns="_services", values="broj")
                    .fillna(0).astype(int)
        )
        for c in ["MOBILE", "TV", "INTERNET", "TV GO"]:
            if c not in pos_bus_pivot.columns:
                pos_bus_pivot[c] = 0
        pos_bus_pivot = pos_bus_pivot[["MOBILE", "TV", "INTERNET", "TV GO"]]
        st.dataframe(with_totals_pivot(pos_bus_pivot), use_container_width=True)

    # ============== POS × USLUGA — REZIDENCIJALA ==============
    st.subheader("Prodaja po POS tipu — REZIDENCIJALA (bez BUSINESS)")
    df_res = df[~df["_is_business"]].copy()
    if df_res.empty:
        st.info("Nema REZIDENCIJALNIH ugovora u odabranom periodu.")
    else:
        long_res = df_res[["_services", "POS_TIP"]].explode("_services").dropna()
        pos_res_pivot = (
            long_res.groupby(["POS_TIP", "_services"]).size().reset_index(name="broj")
                    .pivot(index="POS_TIP", columns="_services", values="broj")
                    .fillna(0).astype(int)
        )
        for c in ["MOBILE", "TV", "INTERNET", "TV GO"]:
            if c not in pos_res_pivot.columns:
                pos_res_pivot[c] = 0
        pos_res_pivot = pos_res_pivot[["MOBILE", "TV", "INTERNET", "TV GO"]]
        st.dataframe(with_totals_pivot(pos_res_pivot), use_container_width=True)

    # ============== TRENDS (više fajlova) — SAMO NOVI ==============
    if mode == "Analiza perioda (više fajlova)":
        st.divider()
        st.subheader("📈 Trend po mjesecima – samo NOVI (MOBILE / TV / INTERNET / TV GO)")
        df_novi = df[df["CONTRACT_TIP"] == "NOVI"].copy()
        df_novi["YEAR_MONTH"] = df_novi["__file__"].str.extract(r"(20\d{2})_(0[1-9]|1[0-2])").agg("_".join, axis=1)
        long_m = df_novi[["YEAR_MONTH", "_services"]].explode("_services").dropna()
        monthly = (
            long_m.groupby(["YEAR_MONTH", "_services"]).size().reset_index(name="broj")
                  .pivot(index="YEAR_MONTH", columns="_services", values="broj")
                  .fillna(0).astype(int).sort_index()
        )
        for c in ["MOBILE", "TV", "INTERNET", "TV GO"]:
            if c not in monthly.columns:
                monthly[c] = 0
        monthly = monthly[["MOBILE", "TV", "INTERNET", "TV GO"]]

        _monthly = monthly.copy()
        try:
            _monthly.index = pd.to_datetime(_monthly.index.str.replace("_", "-") + "-01", format="%Y-%m-%d")
        except Exception:
            pass
        fig1, ax1 = plt.subplots()
        _monthly.plot(ax=ax1)
        ax1.set_title("Trend po mjesecima – samo NOVI (MOBILE/TV/INTERNET/TV GO)")
        ax1.set_xlabel("Mjesec")
        ax1.set_ylabel("Broj ugovora (NOVI)")
        ax1.grid(True, which="both", axis="both", linestyle="--", linewidth=0.5)
        fig1.autofmt_xdate()
        st.pyplot(fig1, use_container_width=True)

        st.subheader("📈 Ukupno po mjesecima – samo NOVI")
        monthly_total = monthly.sum(axis=1).to_frame(name="UKUPNO").sort_index()
        _monthly_total = monthly_total.copy()
        try:
            _monthly_total.index = pd.to_datetime(_monthly_total.index.str.replace("_", "-") + "-01", format="%Y-%m-%d")
        except Exception:
            pass
        fig2, ax2 = plt.subplots()
        _monthly_total.plot(ax=ax2)
        ax2.set_title("Ukupno po mjesecima – samo NOVI")
        ax2.set_xlabel("Mjesec")
        ax2.set_ylabel("Broj ugovora (NOVI)")
        ax2.grid(True, which="both", axis="both", linestyle="--", linewidth=0.5)
        fig2.autofmt_xdate()
        st.pyplot(fig2, use_container_width=True)

        st.subheader("📊 Trend po kvartalima – samo NOVI")
        df_novi["_QTR"] = df_novi["_start_dt"].dt.to_period("Q").astype(str)
        long_q = df_novi[["_QTR", "_services"]].explode("_services").dropna()
        quarterly = (
            long_q.groupby(["_QTR", "_services"]).size().reset_index(name="broj")
                  .pivot(index="_QTR", columns="_services", values="broj")
                  .fillna(0).astype(int).sort_index()
        )
        for c in ["MOBILE", "TV", "INTERNET", "TV GO"]:
            if c not in quarterly.columns:
                quarterly[c] = 0
        quarterly = quarterly[["MOBILE", "TV", "INTERNET", "TV GO"]]
        quarterly = quarterly.loc[quarterly.sum(axis=1) > 0]

        available_services = [c for c in ["MOBILE", "TV", "INTERNET", "TV GO"] if quarterly[c].sum() > 0]
        selected_services = st.multiselect(
            "Prikaži usluge na kvartalnom grafikonu",
            options=available_services,
            default=available_services,
        )
        show_values = st.checkbox("Prikaži vrijednosti na tačkama (kvartali)", value=True)

        q_plot = quarterly.copy()
        try:
            q_plot.index = pd.PeriodIndex(q_plot.index, freq="Q").to_timestamp(how="start")
        except Exception:
            pass

        if selected_services:
            figq, axq = plt.subplots()
            q_plot[selected_services].plot(ax=axq)
            axq.set_title("Trend po kvartalima – samo NOVI")
            axq.set_xlabel("Kvartal")
            axq.set_ylabel("Broj ugovora (NOVI)")
            axq.grid(True, which="both", axis="both", linestyle="--", linewidth=0.5)
            figq.autofmt_xdate()
            if show_values:
                for col in selected_services:
                    for x, y in zip(q_plot.index, q_plot[col].values):
                        axq.annotate(f"{int(y)}", (x, y), textcoords="offset points",
                                     xytext=(0, 4), ha="center", fontsize=8)
            st.pyplot(figq, use_container_width=True)
        else:
            st.info("Odaberi najmanje jednu uslugu za prikaz.")

        st.subheader("📊 Ukupno po kvartalima – samo NOVI")
        q_total = quarterly.sum(axis=1).to_frame(name="UKUPNO")
        q_total_plot = q_total.copy()
        try:
            q_total_plot.index = pd.PeriodIndex(q_total_plot.index, freq="Q").to_timestamp(how="start")
        except Exception:
            pass
        figqt, axqt = plt.subplots()
        q_total_plot.plot(ax=axqt)
        axqt.set_title("Ukupno po kvartalima – samo NOVI")
        axqt.set_xlabel("Kvartal")
        axqt.set_ylabel("Broj ugovora (NOVI)")
        axqt.grid(True, which="both", axis="both", linestyle="--", linewidth=0.5)
        figqt.autofmt_xdate()
        if show_values:
            for x, y in zip(q_total_plot.index, q_total_plot["UKUPNO"].values):
                axqt.annotate(f"{int(y)}", (x, y), textcoords="offset points",
                              xytext=(0, 4), ha="center", fontsize=8)
        st.pyplot(figqt, use_container_width=True)

# ===== Uporedna analiza (2 mjeseca) — poseban ekran =====
if mode == "Uporedna analiza (2 mjeseca)":
    st.header("🔁 Uporedna analiza (2 mjeseca)")

    file_rx = re.compile(r"^ALL_CONTRACT_(20\d{2})_(0[1-9]|1[0-2])\.xlsx$")
    month_files = [f for f in os.listdir(compare_folder) if file_rx.match(f)]

    if not month_files:
        st.info("Nema mjesečnih fajlova u izabranom folderu.")
        st.stop()

    if idx_old is None or idx_new is None or idx_old == idx_new:
        st.warning("Odaberi dva različita mjeseca u lijevom meniju.")
        st.stop()

    month_keys = []
    for f in month_files:
        m = file_rx.match(f)
        y, mo = int(m.group(1)), int(m.group(2))
        month_keys.append((y, mo, f))
    month_keys.sort()
    labels = [f"{y}-{mo:02d}" for (y, mo, f) in month_keys]
    files   = [f for (_, _, f) in month_keys]

    path_old = os.path.join(compare_folder, files[idx_old])
    path_new = os.path.join(compare_folder, files[idx_new])

    with open(path_old, "rb") as fh:
        raw_old = load_one_excel(fh.read(), path_old)
    with open(path_new, "rb") as fh:
        raw_new = load_one_excel(fh.read(), path_new)

    df_old = preprocess_month_df(raw_old)
    df_new = preprocess_month_df(raw_new)

    bus_old, res_old = pos_pivot_by_business(df_old)
    bus_new, res_new = pos_pivot_by_business(df_new)

    bus_pct = pct_change_table(bus_new, bus_old)
    res_pct = pct_change_table(res_new, res_old)

    st.subheader("🏠 Rezidencijala — Prodaja po POS (stari vs. novi)")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**{labels[idx_old]} — REZIDENCIJALA**")
        st.dataframe(with_totals_pivot(res_old), use_container_width=True)
    with c2:
        st.markdown(f"**{labels[idx_new]} — REZIDENCIJALA**")
        st.dataframe(with_totals_pivot(res_new), use_container_width=True)
    st.markdown("**Δ% (novi vs. stari) — REZIDENCIJALA**  \n*(∞ znači da u starom nije bilo prodaje, a u novom ima)*")
    st.dataframe(res_pct.replace(np.inf, "∞"), use_container_width=True)

    st.subheader("🏢 Business — Prodaja po POS (stari vs. novi)")
    c3, c4 = st.columns(2)
    with c3:
        st.markdown(f"**{labels[idx_old]} — BUSINESS**")
        st.dataframe(with_totals_pivot(bus_old), use_container_width=True)
    with c4:
        st.markdown(f"**{labels[idx_new]} — BUSINESS**")
        st.dataframe(with_totals_pivot(bus_new), use_container_width=True)
    st.markdown("**Δ% (novi vs. stari) — BUSINESS**  \n*(∞ znači da u starom nije bilo prodaje, a u novom ima)*")
    st.dataframe(bus_pct.replace(np.inf, "∞"), use_container_width=True)

    st.subheader("📊 Ukupan procenat razlike po uslugama (novi vs. stari)")
    old_totals = (res_old.sum(axis=0) + bus_old.sum(axis=0)).reindex(["MOBILE", "TV", "INTERNET", "TV GO"]).fillna(0).astype(int)
    new_totals = (res_new.sum(axis=0) + bus_new.sum(axis=0)).reindex(["MOBILE", "TV", "INTERNET", "TV GO"]).fillna(0).astype(int)
    diff_tot   = new_totals - old_totals

    with np.errstate(divide="ignore", invalid="ignore"):
        pct_values = (diff_tot / old_totals.replace(0, np.nan) * 100).round(1)

    def fmt_pct(name):
        old_v = old_totals[name]
        new_v = new_totals[name]
        if old_v == 0 and new_v > 0:
            return "∞"
        val = pct_values[name]
        if pd.isna(val):
            return "0.0%"
        return f"{val:.1f}%"

    summary = pd.DataFrame({
        "STARI": old_totals,
        "NOVI":  new_totals,
        "Δ":     diff_tot,
        "Δ%":    [fmt_pct(s) for s in ["MOBILE", "TV", "INTERNET", "TV GO"]]
    }, index=["MOBILE", "TV", "INTERNET", "TV GO"])
    st.dataframe(summary, use_container_width=True)

    st.stop()

st.caption("© Izvještaj generisan prema poslovnim pravilima i podacima iz odabranih Excel fajlova.")