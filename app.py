# -*- coding: utf-8 -*-
# Analiza prodaje Austrija — by gennaro (2025)

import os
import io
import json
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st

# Fallback konverzija xlsx -> csv ako openpyxl padne
try:
    from xlsx2csv import Xlsx2csv
except Exception:
    Xlsx2csv = None

# Google Drive (PyDrive2)
try:
    from pydrive2.auth import GoogleAuth
    from pydrive2.drive import GoogleDrive
except Exception:
    GoogleAuth = None
    GoogleDrive = None

# -------------------------
# Osnovna konfiguracija UI
# -------------------------
st.set_page_config(page_title="Analiza prodaje Austrija", layout="wide")
APP_TITLE = "Analiza prodaje Austrija"
APP_LOGO = "mtel.png"

st.title(APP_TITLE)
st.caption("© Izvještaj razvijen od strane *gennaro*")

if os.path.exists(APP_LOGO):
    st.sidebar.image(APP_LOGO, width=150)
st.sidebar.markdown("### 🧭 Navigacija")

# -------------------------
# Helperi za čitanje Excel-a
# -------------------------
def safe_read_excel(file_like):
    """Pokušava pročitati .xlsx pomoću openpyxl, fallback na xlsx2csv."""
    import io, pandas as pd
    try:
        file_like.seek(0)
        return pd.read_excel(file_like, engine="openpyxl")
    except Exception as e1:
        last = e1

    # Fallback na xlsx2csv
    try:
        from xlsx2csv import Xlsx2csv
        file_like.seek(0)
        csv_io = io.StringIO()
        Xlsx2csv(file_like).convert(csv_io)
        csv_io.seek(0)
        return pd.read_csv(csv_io)
    except Exception as e2:
        raise RuntimeError(f"Ne mogu pročitati Excel: {last} | xlsx2csv: {e2}")

# -------------------------
# Google Drive helpers
# -------------------------
def _load_gdrive_secrets():
    """
    Čita [gdrive] iz st.secrets. Podržava:
      - JSON string
      - dict / AttrDict (Streamlit-ov)
    Vraća: (folder_id: str, sac: dict)
    """
    if "gdrive" not in st.secrets:
        raise RuntimeError("Nema [gdrive] sekcije u Streamlit secrets.")

    cfg = st.secrets["gdrive"]

    # folder_id
    folder_id = cfg.get("folder_id")
    if not folder_id:
        raise RuntimeError("Nedostaje gdrive.folder_id u secrets.")

    # service_account_json: može biti string (JSON) ili dict/AttrDict
    sac_raw = cfg.get("service_account_json")
    if sac_raw is None:
        raise RuntimeError("Nedostaje gdrive.service_account_json u secrets.")

    # Normalize → dict
    if isinstance(sac_raw, str):
        try:
            sac = json.loads(sac_raw)
        except Exception as e:
            raise RuntimeError(f"service_account_json (string) nije validan JSON: {e}") from e
    else:
        # AttrDict/dict → pravi dict
        sac = json.loads(json.dumps(sac_raw))

    # Minimalna provjera
    if "client_email" not in sac or "private_key" not in sac:
        raise RuntimeError("service_account_json nema client_email/private_key.")

    return folder_id, sac

def _make_gdrive():
    if GoogleAuth is None or GoogleDrive is None:
        raise RuntimeError("PyDrive2 nije instaliran (requirements.txt: pydrive2).")

    folder_id, sac = _load_gdrive_secrets()

    # PyDrive2 ponekad očekuje client_user_email — ubacimo ga iz client_email
    if "client_email" in sac and "client_user_email" not in sac:
        sac["client_user_email"] = sac["client_email"]

    ga = GoogleAuth()
    ga.settings = {
        "client_config_backend": "service",
        "service_config": {
            "client_json_dict": sac   # NAJSTABILNIJE — bez \n problema
        }
    }
    ga.ServiceAuth()
    drv = GoogleDrive(ga)
    return folder_id, drv

def gdrive_list_files(folder_id, drive):
    # v2 API: koristi 'title' umjesto 'name'
    q = f"'{folder_id}' in parents and trashed=false and title contains '.xlsx'"
    files = drive.ListFile({'q': q}).GetList()
    return sorted([(f['title'], f['id']) for f in files], key=lambda x: x[0].lower())

def load_from_gdrive(file_id, drive) -> pd.DataFrame:
    f = drive.CreateFile({'id': file_id})
    data = io.BytesIO(f.GetContentBinary())
    return safe_read_excel(data)

# -------------------------
# Logika datuma / perioda
# -------------------------
def to_datetime_series(s):
    return pd.to_datetime(s, errors="coerce", dayfirst=True)

def parse_period_from_filename(name: str):
    """
    Iz imena fajla izvuci (YYYY, MM). Radi za ALL_CONTRACT_YYYY_MM.xlsx.
    """
    base = os.path.basename(name)
    parts = [p for p in base.replace(".", "_").split("_") if p.isdigit()]
    y, m = None, None
    for i, p in enumerate(parts):
        if len(p) == 4 and i + 1 < len(parts) and len(parts[i + 1]) in (1, 2):
            y = int(p)
            m = int(parts[i + 1])
            break
    return (y, m) if y and m else (None, None)

def period_label_from_filename(name: str) -> str:
    y, m = parse_period_from_filename(name)
    return f"{y}-{m:02d}" if y and m else name

# -------------------------
# POS i SERVISI
# -------------------------
VIP_PREFIX = "VIP_"

def pos_tip(s: str) -> str:
    """
    POS pravila:
      - svi 'VIP_*' izdvojeno (svaki shop posebno)
      - MTELWMH / MTELWTH / MTELWDZ
      - HARTLAUER / WEB / TELESALES / (ako sadrži 'D2' → D2D)
      - ako sadrži 'BUSINESS' → BUSINESS_POS
      - ostalo → MULTIBREND
    """
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return "MULTIBREND"
    x = str(s).strip()
    ux = x.upper()
    if ux.startswith(VIP_PREFIX):
        return x  # ostavi original (svaki VIP shop posebno)
    if "MTELWMH" in ux:
        return "MTELWMH"
    if "MTELWTH" in ux:
        return "MTELWTH"
    if "MTELWDZ" in ux:
        return "MTELWDZ"
    if "HARTLAUER" in ux:
        return "HARTLAUER"
    if "TELESALES" in ux:
        return "TELESALES"
    if "WEB" in ux:
        return "WEB"
    if "D2" in ux:
        return "D2D"
    if "BUSINESS" in ux:
        return "BUSINESS_POS"
    return "MULTIBREND"

def classify_services(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dodaje kolone:
      - MOBILE (Q: log_mobiletariff=True)
      - TV (R: log_tvtariff=True)
      - INTERNET (AE: InternetDevice=='MODEM')
      - TV_GO (Q=True, R=False i Price_TV>0)
      - STARNET (INTERNET & Price_TV>0) -> ulazi i u TV total
    """
    d = df.copy()

    def as_bool(x):
        if pd.isna(x): return False
        if isinstance(x, (bool, np.bool_)): return bool(x)
        if isinstance(x, (int, np.integer)): return x != 0
        s = str(x).strip().lower()
        return s in ("true", "1", "da", "yes", "y")

    Q = d.get("log_mobiletariff")
    R = d.get("log_tvtariff")
    AE = d.get("InternetDevice")
    T = d.get("Price_TV")

    mobile = Q.map(as_bool) if Q is not None else pd.Series(False, index=d.index)
    tv = R.map(as_bool) if R is not None else pd.Series(False, index=d.index)
    internet = (AE.astype(str).str.upper() == "MODEM") if AE is not None else pd.Series(False, index=d.index)
    price_tv = pd.to_numeric(T, errors="coerce") if T is not None else pd.Series(np.nan, index=d.index)

    tv_go = mobile & (~tv) & (price_tv.fillna(0) > 0)
    starnet = internet & (price_tv.fillna(0) > 0)

    d["MOBILE"] = mobile.astype(int)
    d["TV"] = tv.astype(int)
    d["INTERNET"] = internet.astype(int)
    d["TV_GO"] = tv_go.astype(int)
    d["STARNET"] = starnet.astype(int)
    return d

def add_pos_columns(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    pos_col = "POS" if "POS" in d.columns else d.columns[11] if len(d.columns) > 11 else None
    if pos_col:
        d["POS_TIP"] = d[pos_col].apply(pos_tip)
    else:
        d["POS_TIP"] = "MULTIBREND"

    ctype_col = "ContractType" if "ContractType" in d.columns else None
    if ctype_col:
        d["CONTRACT_TIP"] = np.where(
            d[ctype_col].astype(str).str.strip().str.upper() == "BUSINESS", "BUSINESS", "REZ"
        )
    else:
        d["CONTRACT_TIP"] = "REZ"
    return d

def pivot_pos_services(d: pd.DataFrame) -> pd.DataFrame:
    """
    POS_TIP × [MOBILE, TV (TV+STARNET), INTERNET, TV GO, STARNET], plus red "Ukupno"
    """
    d = d.copy()
    d["TV_TOTAL"] = d["TV"].astype(int) + d["STARNET"].astype(int)
    tab = pd.pivot_table(
        d,
        index="POS_TIP",
        values=["MOBILE", "TV_TOTAL", "INTERNET", "TV_GO", "STARNET"],
        aggfunc="sum",
        fill_value=0,
        observed=True,
    ).astype(int).sort_index()
    total = pd.DataFrame(tab.sum(axis=0)).T
    total.index = ["Ukupno"]
    tab = pd.concat([tab, total], axis=0)
    tab.columns = ["MOBILE", "TV", "INTERNET", "TV GO", "STARNET"]
    return tab

def phones_by_pos(d: pd.DataFrame) -> pd.DataFrame:
    if "MobileDevice" not in d.columns:
        return pd.DataFrame()
    sub = d.loc[d["MobileDevice"].notna() & (d["MobileDevice"].astype(str).str.strip() != "")]
    if sub.empty:
        return pd.DataFrame()
    tab = pd.pivot_table(
        sub,
        index="MobileDevice",
        columns="POS_TIP",
        values="MobileDevice",
        aggfunc="count",
        fill_value=0,
        observed=True,
    )
    tot = pd.DataFrame(tab.sum(axis=1), columns=["Ukupno"])
    tab = pd.concat([tab, tot], axis=1)
    return tab.sort_values("Ukupno", ascending=False)

def stb_by_pos(d: pd.DataFrame) -> pd.DataFrame:
    if "TVDevice" not in d.columns:
        return pd.DataFrame()
    sub = d[d["TVDevice"].astype(str).str.upper().isin(["STB", "STB2"])].copy()
    if sub.empty:
        return pd.DataFrame()
    sub["_kind"] = sub["TVDevice"].astype(str).str.upper()
    sub["_val"] = 1
    tab = pd.pivot_table(
        sub,
        index="POS_TIP",
        columns="_kind",
        values="_val",
        aggfunc="sum",
        fill_value=0,
        observed=True,
    ).astype(int)
    for c in ["STB", "STB2"]:
        if c not in tab.columns:
            tab[c] = 0
    tab = tab[["STB", "STB2"]].sort_index()
    total = pd.DataFrame(tab.sum(axis=0)).T
    total.index = ["Ukupno"]
    tab = pd.concat([tab, total], axis=0)
    return tab

def top_tariffs(d: pd.DataFrame, service: str) -> pd.DataFrame:
    name_col = "log_tariffname"
    if name_col not in d.columns:
        return pd.DataFrame()
    svc = service.upper()
    mask = d[svc] == 1
    sub = d.loc[mask & d[name_col].notna()].copy()
    if sub.empty:
        return pd.DataFrame()
    tab = (
        sub[name_col].astype(str).str.strip()
        .value_counts()
        .rename_axis("Tarifa")
        .reset_index(name="Komada")
        .sort_values("Komada", ascending=False)
    )
    return tab

def kpi_new_ext(df: pd.DataFrame, file_name: str):
    """
    Ako postoji DatumProduzenja: NOVI = (Start.date == DatumProduzenja.date)
    Inače: iz imena fajla (Start ∈ taj mjesec) → NOVI, ostalo PRODUŽECI
    """
    start = to_datetime_series(df.get("Start"))
    if "DatumProduzenja" in df.columns:
        end = to_datetime_series(df["DatumProduzenja"])
        valid = start.notna() & end.notna()
        novi = (start.dt.date == end.dt.date) & valid
        prod = valid & (start.dt.date != end.dt.date)
        return int(novi.sum()), int(prod.sum()), int(valid.sum())
    y, m = parse_period_from_filename(file_name)
    if not y or not m:
        return 0, 0, int(start.notna().sum())
    valid = start.notna().sum()
    novi = ((start.dt.year == y) & (start.dt.month == m)).sum()
    return int(novi), int(valid - novi), int(valid)

# -------------------------
# Uporedna analiza helpers
# -------------------------
def _service_col_map():
    # kolona u pivotu koja predstavlja total za service
    return {"MOBILE": "MOBILE", "TV": "TV", "INTERNET": "INTERNET", "TV GO": "TV GO"}

def _compare_tables_by_service(df_a: pd.DataFrame, df_b: pd.DataFrame, service: str,
                               label_a: str, label_b: str) -> pd.DataFrame:
    """
    Vrati tabelu po POS:
      POS_TIP | label_a | label_b | Δ | Δ% | Trend
    na osnovu pivot_pos_services (koji već računa TV total = TV + STARNET)
    """
    p_a = pivot_pos_services(df_a)
    p_b = pivot_pos_services(df_b)
    col = _service_col_map()[service]

    # ujednači indexe
    all_idx = sorted(set(p_a.index).union(set(p_b.index)), key=lambda x: (x != "Ukupno", x))
    a = p_a.reindex(all_idx).fillna(0).astype(int)
    b = p_b.reindex(all_idx).fillna(0).astype(int)

    out = pd.DataFrame(index=all_idx)
    out[label_a] = a[col]
    out[label_b] = b[col]
    out["Δ"] = out[label_b] - out[label_a]
    # Δ%
    def pct(row):
        base = row[label_a]
        if base == 0:
            return np.nan if row[label_b] == 0 else 100.0
        return (row[label_b] - base) / base * 100.0
    out["Δ%"] = out.apply(pct, axis=1)

    # Strelice
    def arrow(row):
        if row["Δ"] > 0: return "⬆️"
        if row["Δ"] < 0: return "⬇️"
        return "➡️"
    out["Trend"] = out.apply(arrow, axis=1)

    # Format Δ%
    out["Δ%"] = out["Δ%"].map(lambda v: "" if pd.isna(v) else f"{v:.1f}%")
    return out

# -------------------------
# Trend (više fajlova) – samo NOVI
# -------------------------
def monthly_trend_new(files_with_names, drive=None):
    """
    Prima listu tuple-ova: (label_name, loader)
    gdje je loader callable -> pd.DataFrame
    Vraća DataFrame: index=Period (YYYY-MM), kolone=[MOBILE, TV, INTERNET, TV GO]
    računa SAMO NOVE (Start ∈ Period).
    """
    rows = []
    for fname, loader in files_with_names:
        try:
            df = loader()
        except Exception:
            continue
        d = classify_services(df)
        y, m = parse_period_from_filename(fname)
        if not y or not m:
            continue
        start = to_datetime_series(df.get("Start"))
        mask_new = (start.dt.year == y) & (start.dt.month == m)
        sub = d.loc[mask_new]

        tv_total = int((sub["TV"] + sub["STARNET"]).sum())
        rows.append({
            "Period": f"{y}-{m:02d}",
            "MOBILE": int(sub["MOBILE"].sum()),
            "TV": tv_total,
            "INTERNET": int(sub["INTERNET"].sum()),
            "TV GO": int(sub["TV_GO"].sum()),
        })
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).groupby("Period", as_index=True).sum().sort_index()
    return out

# -------------------------
# Sidebar – Glavni meni
# -------------------------
section = st.sidebar.radio(
    "📊 Odaberi sekciju:",
    ["Analiza jednog fajla", "Analiza više fajlova", "Uporedna analiza (2 mjeseca)"]
)

# -------------------------
# Sekcija: Analiza jednog fajla
# -------------------------
if section == "Analiza jednog fajla":
    source = st.sidebar.radio("Izvor podataka:", ["Google Drive", "Lokalni upload"])
    df = None
    selected_file_label = None

    if source == "Google Drive":
        try:
            folder_id, drive = _make_gdrive()
            files = gdrive_list_files(folder_id, drive)
            if not files:
                st.warning("⚠️ Nema .xlsx fajlova u GDrive folderu.")
            else:
                names = [t for t, _ in files]
                selected_file_label = st.sidebar.selectbox("Odaberi fajl (GDrive):", names, index=0)
                if selected_file_label:
                    file_id = dict(files)[selected_file_label]
                    df = load_from_gdrive(file_id, drive)
                    st.success(f"📂 Učitan: {selected_file_label}")
        except Exception as e:
            st.error(f"❌ GDrive autentikacija/čitanje nije uspjelo: {e}")
    else:
        uploaded = st.sidebar.file_uploader("📁 Učitaj lokalni Excel (.xlsx)", type=["xlsx"])
        if uploaded:
            selected_file_label = uploaded.name
            try:
                df = safe_read_excel(uploaded)
                st.success(f"📄 Učitan lokalni fajl: {selected_file_label}")
            except Exception as e:
                st.error(f"❌ Ne mogu pročitati Excel: {e}")

    if df is not None:
        raw = df.copy()
        d = classify_services(raw)
        d = add_pos_columns(d)

        # KPI
        k_mobile = int(d["MOBILE"].sum())
        k_tv = int((d["TV"] + d["STARNET"]).sum())
        k_inet = int(d["INTERNET"].sum())
        k_tvgo = int(d["TV_GO"].sum())
        k_phones = int(d["MobileDevice"].notna().sum() if "MobileDevice" in d.columns else 0)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("MOBILE", k_mobile)
        c2.metric("TV (uklj. STARNET)", k_tv)
        c3.metric("INTERNET", k_inet)
        c4.metric("TV GO", k_tvgo)
        c5.metric("Prodati telefoni", k_phones)

        novi, produzetak, valid = kpi_new_ext(raw, selected_file_label or "")
        n1, n2, n3 = st.columns(3)
        n1.metric("NOVI ugovori", novi)
        n2.metric("PRODUŽECI", produzetak)
        n3.metric("Validnih Start zapisa", valid)

        st.divider()

        st.subheader("🧾 Prodaja po POS (SVE) — MOBILE, TV, INTERNET, TV GO (TV uključuje STARNET)")
        st.dataframe(pivot_pos_services(d), width="stretch", height=380)

        st.subheader("🏢 Prodaja po POS — samo BUSINESS ugovori")
        sub_b = d[d["CONTRACT_TIP"] == "BUSINESS"]
        if not sub_b.empty:
            st.dataframe(pivot_pos_services(sub_b), width="stretch", height=320)
        else:
            st.info("Nema BUSINESS ugovora u ovom fajlu.")

        st.subheader("🏠 Prodaja po POS — REZIDENCIJALA (bez BUSINESS)")
        sub_r = d[d["CONTRACT_TIP"] == "REZ"]
        if not sub_r.empty:
            st.dataframe(pivot_pos_services(sub_r), width="stretch", height=320)
        else:
            st.info("Nema REZIDENCIJALNIH ugovora u ovom fajlu.")

        st.divider()

        st.subheader("📱 Prodaja telefona po POS (MobileDevice × POS)")
        ph = phones_by_pos(d)
        if not ph.empty:
            st.dataframe(ph, width="stretch", height=420)
        else:
            st.info("Nema prodatih telefona u ovom fajlu.")

        st.subheader("📦 STB uređaji po POS (STB, STB2)")
        stb = stb_by_pos(d)
        if not stb.empty:
            st.dataframe(stb, width="stretch", height=300)
        else:
            st.info("Nema STB uređaja (STB/STB2) u ovom fajlu.")

        st.divider()

        st.subheader("🏆 Najprodavanije tarife")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**MOBILE tarife**")
            tm = top_tariffs(d, "MOBILE")
            st.dataframe(tm, width="stretch", height=350) if not tm.empty else st.info("Nema MOBILE tarifa.")
        with c2:
            st.markdown("**INTERNET tarife**")
            ti = top_tariffs(d, "INTERNET")
            st.dataframe(ti, width="stretch", height=350) if not ti.empty else st.info("Nema INTERNET tarifa.")
        with c3:
            st.markdown("**TV tarife** (TV total uključuje STARNET)")
            tt = top_tariffs(d, "TV")
            st.dataframe(tt, width="stretch", height=350) if not tt.empty else st.info("Nema TV tarifa.")

# -------------------------
# Sekcija: Analiza više fajlova (trend)
# -------------------------
elif section == "Analiza više fajlova":
    st.header("📊 Analiza više fajlova — trend (samo NOVI ugovori po mjesecima)")
    source = st.sidebar.radio("Izvor:", ["Google Drive", "Lokalni upload (više fajlova)"])

    files_with_names = []

    if source == "Google Drive":
        try:
            folder_id, drive = _make_gdrive()
            files = gdrive_list_files(folder_id, drive)
            if not files:
                st.warning("⚠️ Nema .xlsx fajlova u GDrive folderu.")
            else:
                names = [t for t, _ in files]
                picks = st.multiselect("Odaberi fajlove (2–15):", names, default=names[:min(6, len(names))])
                for name in picks:
                    fid = dict(files)[name]
                    files_with_names.append((name, (lambda fid=fid, d=drive: load_from_gdrive(fid, d))))
        except Exception as e:
            st.error(f"❌ GDrive: {e}")

    else:
        upl = st.file_uploader("📁 Učitaj više .xlsx fajlova", type=["xlsx"], accept_multiple_files=True)
        if upl:
            for f in upl:
                files_with_names.append((f.name, (lambda f=f: safe_read_excel(f))))

    if files_with_names:
        trend = monthly_trend_new(files_with_names)
        if trend.empty:
            st.info("Nema dovoljno podataka za trend (provjeri imena fajlova i kolonu Start).")
        else:
            st.dataframe(trend, width="stretch", height=420)
            st.subheader("Grafikon (Novi ugovori po mjesecima)")
            st.line_chart(trend, x=None, y=trend.columns.tolist())

# -------------------------
# Sekcija: Uporedna analiza (2 mjeseca)
# -------------------------
elif section == "Uporedna analiza (2 mjeseca)":
    st.header("📈 Uporedna analiza (dva mjeseca) — po POS i servisima")
    source = st.sidebar.radio("Izvor:", ["Google Drive", "Lokalno (2 fajla)"])

    df_a = df_b = None
    label_a = label_b = "A"

    if source == "Google Drive":
        try:
            folder_id, drive = _make_gdrive()
            files = gdrive_list_files(folder_id, drive)
            if not files:
                st.warning("⚠️ Nema .xlsx fajlova u GDrive folderu.")
            else:
                names = [t for t, _ in files]
                a_name = st.selectbox("Period A:", names, index=0, key="cmpA")
                b_name = st.selectbox("Period B:", names, index=min(1, len(names)-1), key="cmpB")
                if a_name and b_name:
                    df_a = classify_services(load_from_gdrive(dict(files)[a_name], drive))
                    df_a = add_pos_columns(df_a)
                    df_b = classify_services(load_from_gdrive(dict(files)[b_name], drive))
                    df_b = add_pos_columns(df_b)
                    label_a = period_label_from_filename(a_name)
                    label_b = period_label_from_filename(b_name)
        except Exception as e:
            st.error(f"❌ GDrive: {e}")

    else:
        a = st.file_uploader("📄 Fajl A", type=["xlsx"], key="locA")
        b = st.file_uploader("📄 Fajl B", type=["xlsx"], key="locB")
        if a and b:
            df_a = classify_services(safe_read_excel(a))
            df_a = add_pos_columns(df_a)
            df_b = classify_services(safe_read_excel(b))
            df_b = add_pos_columns(df_b)
            label_a = period_label_from_filename(a.name)
            label_b = period_label_from_filename(b.name)

    if df_a is not None and df_b is not None:
        services = ["MOBILE", "TV", "INTERNET", "TV GO"]
        for svc in services:
            st.subheader(f"🔹 {svc}")
            tbl = _compare_tables_by_service(df_a, df_b, svc, label_a, label_b)
            st.dataframe(tbl, width="stretch", height=360)