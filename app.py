import json
from urllib.parse import urljoin
import re
import os

import duckdb
import pandas as pd
import requests
import streamlit as st

# ---------- Page config ----------
st.set_page_config(page_title="ARMS Performance Entity Viewer", layout="wide")

# ---------- Theme + CSS ----------
ACCENT = "#78D1FA"
BG_MAIN = "#272733"
BG_CARD = "#12161C"
TEXT = "#E8EEF2"
SIDEBAR_W = 480  # desktop width for expanded sidebar

st.markdown(f"""
<style>
:root {{ --sidebar-width: {SIDEBAR_W}px; }}

/* Fix half-collapsed sidebar: apply width only when expanded */
section[data-testid="stSidebar"][aria-expanded="true"] {{
  min-width: var(--sidebar-width);
  max-width: var(--sidebar-width);
}}
section[data-testid="stSidebar"][aria-expanded="false"] {{
  min-width: 0 !important;
  max-width: 0 !important;
}}

/* Logo spacing */
section[data-testid="stSidebar"] img:first-of-type {{
  display:block; margin-bottom:20px;
}}

/* Mobile tweaks */
@media (max-width: 768px) {{
  /* Sidebar acts like an overlay drawer on phones */
  section[data-testid="stSidebar"][aria-expanded="true"] {{
    position: fixed;
    z-index: 1000;
    min-width: 85vw !important;
    max-width: 85vw !important;
  }}
  section[data-testid="stSidebar"][aria-expanded="false"] {{
    min-width: 0 !important;
    max-width: 0 !important;
  }}

  /* Reduce main content side padding so the table uses the width */
  .block-container {{
    padding-left: 0.75rem;
    padding-right: 0.75rem;
  }}

  /* Title size */
  h1, h2 {{ font-size: 1.35rem; }}

  /* Make buttons and inputs a bit taller for touch */
  button[kind="primary"], .stButton button, .stTextInput input {{
    min-height: 40px;
  }}
}}
</style>
""", unsafe_allow_html=True)

st.markdown(
    f"<h1 style='margin:0.2rem 0 1rem 0; color:{ACCENT};'>ARMS Performance Entity Viewer</h1>",
    unsafe_allow_html=True
)

# ---------- Helpers ----------
def build_url(site: str, endpoint: str) -> str:
    site = (site or "").strip().rstrip("/")
    if not site.startswith("http"):
        site = "https://" + site
    return urljoin(site + "/", endpoint.lstrip("/"))

def to_df(payload):
    if isinstance(payload, list):
        return pd.json_normalize(payload)
    if isinstance(payload, dict):
        for k in ("data", "results", "items", "value", "Response"):
            v = payload.get(k)
            if isinstance(v, list):
                return pd.json_normalize(v)
        return pd.json_normalize(payload)
    return pd.DataFrame()

def add_player_name_col(df: pd.DataFrame) -> pd.DataFrame:
    f = next((c for c in df.columns if c.lower() == "firstname"), None)
    l = next((c for c in df.columns if c.lower() == "lastname"), None)
    if f and l:
        pn = (
            df[f].astype(str).fillna("").str.strip()
            + " "
            + df[l].astype(str).fillna("").str.strip()
        ).str.replace(r"\s+", " ", regex=True).str.strip()
        if "Player Name" in df.columns:
            df = df.drop(columns=["Player Name"])
        df.insert(0, "Player Name", pn)
    return df

def ensure_duck(df: pd.DataFrame):
    if "duck" not in st.session_state or st.session_state.get("duck_closed", False):
        st.session_state.duck = duckdb.connect(database=":memory:")
        st.session_state.duck_closed = False
    con: duckdb.DuckDBPyConnection = st.session_state.duck
    try:
        con.unregister("api_data")
    except Exception:
        pass
    con.register("api_data", df)
    return con

def quote_ident(col: str) -> str:
    return '"' + col.replace('"', '""') + '"'

def reset_state():
    for k in (
        "url", "data", "df",
        "cols_to_show", "last_nonempty_cols",
        "ct_filter", "player_ms", "player_free",
        "player_like", "player_like_ms"
    ):
        st.session_state.pop(k, None)

# Compatibility for rerun across Streamlit versions
try:
    RERUN = st.rerun
except AttributeError:
    RERUN = st.experimental_rerun  # older Streamlit

# ---------- Known clients ----------
CLIENTS = [
    "afcbournemouth9456.edge10online.co.uk",
    "newcastleunited7703.edge10online.co.uk",
    "amazulufc4646.edge10online.co.uk",
    "qpr1882.edge10online.co.uk",
    "oxfordunited1893.edge10online.co.uk",
    "northamptontownfc.edge10online.co.uk",
    "cheltenhamtownfc.edge10online.co.uk",
    "solihullmoors2007.edge10online.co.uk",
]

# ---------- Sidebar ----------
with st.sidebar:
    logo_path = "ARMS_Performance_Logo_White_Alt.png"
    if os.path.exists(logo_path):
        st.image(logo_path, width=160)

    st.markdown(
        f"<h3 style='color:{ACCENT}; font-weight:800; margin:.25rem 0;'>CONNECTION</h3>",
        unsafe_allow_html=True
    )

    client_choice = st.selectbox(
        "Client",
        options=CLIENTS + ["Other (enter below)"],
        help="Pick a client from the list or choose Other to type a new site"
    )

    if client_choice == "Other (enter below)":
        site = st.text_input(
            "Site name",
            placeholder="club.edge10online.co.uk",
            help="Enter the full host, for example club.edge10online.co.uk",
        )
    else:
        site = st.text_input(
            "Site name",
            value=client_choice,
            disabled=True,
            help="Option to open if Other selected in Client"
        )

    endpoint = st.text_input(
        "Endpoint path",
        value="api/entity/",
        help="Examples: api/entity/ OR api/template OR api/entity/groups OR for a full list of API calls check out sitename/swagger",
    )

    st.markdown(
        f"<h3 style='color:{ACCENT}; font-weight:800; margin:1rem 0 .25rem;'>LOGIN</h3>",
        unsafe_allow_html=True
    )
    user = st.text_input("Username")
    pwd = st.text_input("Password", type="password")

    c1, c2 = st.columns(2)
    with c1:
        run = st.button("Fetch", type="primary")
    with c2:
        clear = st.button("Reset")

if clear:
    reset_state()
    RERUN()

# ---------- Fetch ----------
if run:
    if not site or not endpoint or not user or not pwd:
        st.error("Please fill site name, endpoint, username, and password")
        st.stop()

    url = build_url(site, endpoint)
    prog = st.progress(0, text="Starting")
    try:
        with st.status("Fetching data...", expanded=True) as status:
            status.write("Sending request")
            prog.progress(30, text="Sending request")
            r = requests.get(
                url,
                auth=requests.auth.HTTPBasicAuth(user, pwd),
                headers={"Accept": "application/json"},
                timeout=60,
            )
            if r.status_code >= 400:
                status.update(label=f"HTTP {r.status_code}", state="error")
                st.error(f"HTTP {r.status_code}: {r.text[:500]}")
                st.stop()

            status.write("Parsing JSON")
            prog.progress(60, text="Parsing JSON")
            data = r.json()

            status.write("Normalizing table")
            prog.progress(85, text="Normalizing table")
            df = to_df(data)
            df = add_player_name_col(df)

            prog.progress(100, text="Done")
            status.update(label="Fetch complete", state="complete")

        if df.empty:
            st.warning("No rows returned")
            st.json(data)
        else:
            st.session_state.url = url
            st.session_state.data = data
            st.session_state.df = df

            # Default visible columns (limit to 8 initially for small screens)
            if "cols_to_show" not in st.session_state:
                all_cols = df.columns.tolist()
                st.session_state.cols_to_show = all_cols[: min(8, len(all_cols))]
                st.session_state.last_nonempty_cols = st.session_state.cols_to_show

            ct_col = next((c for c in df.columns if c.lower() == "contacttype"), None)
            if ct_col and "ct_filter" not in st.session_state:
                uniq_ct = sorted(df[ct_col].dropna().astype(str).unique().tolist())
                st.session_state.ct_filter = ["1"] if "1" in uniq_ct else []

    except requests.exceptions.RequestException as e:
        st.error(f"Request error: {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
    finally:
        try:
            prog.empty()
        except Exception:
            pass

# ---------- Render ----------
if "df" in st.session_state:
    df_base = st.session_state.df.copy()
    st.caption(f"GET {st.session_state.url}")

    like_opts = []
    # Collapse by default (nicer on mobile)
    filt_exp = st.expander("Filters", expanded=False)
    with filt_exp:
        ct_col = next((c for c in df_base.columns if c.lower() == "contacttype"), None)
        if ct_col:
            uniq_ct = sorted(df_base[ct_col].dropna().astype(str).unique().tolist())
            st.multiselect(
                "contactType",
                options=uniq_ct,
                key="ct_filter",
                help="Contact Type = 1 : Player | Contact Type = 2 : Staff"
            )

        if "Player Name" in df_base.columns:
            player_opts = sorted(df_base["Player Name"].dropna().unique().tolist())
            st.multiselect(
                "Players (exact match, multi select)",
                options=player_opts,
                key="player_ms",
            )

            st.text_input(
                "Player contains (LIKE search). Example: Rol, dia",
                key="player_like",
                placeholder="Rol, dia"
            )
            like_tokens = [t.strip() for t in re.split(r"[,\n ]+", st.session_state.get("player_like", "")) if len(t.strip()) >= 2]

            if like_tokens:
                con_all = ensure_duck(df_base)
                placeholders = " OR ".join(['"Player Name" ILIKE ?' for _ in like_tokens])
                sql_like = f'SELECT DISTINCT "Player Name" AS name FROM api_data WHERE {placeholders} ORDER BY 1 LIMIT 500'
                params = [f"%{t}%" for t in like_tokens]
                like_df = con_all.execute(sql_like, params).fetchdf()
                like_opts = like_df["name"].tolist()

                st.multiselect(
                    "LIKE matches. Pick to narrow, or leave empty to include all matches.",
                    options=like_opts,
                    key="player_like_ms"
                )

            st.text_input(
                "Or paste names (comma or newline separated)",
                key="player_free",
            )

    # Apply filters
    df_filt = df_base.copy()
    if ct_col and st.session_state.get("ct_filter"):
        df_filt = df_filt[df_filt[ct_col].astype(str).isin(st.session_state.ct_filter)]

    allowed = set(st.session_state.get("player_ms", []) or [])
    like_query_present = bool(st.session_state.get("player_like"))
    like_selected = set(st.session_state.get("player_like_ms", []) or [])
    if like_query_present:
        allowed |= (like_selected if like_selected else set(like_opts))

    pasted = st.session_state.get("player_free", "") or ""
    if pasted:
        pasted_set = {x.strip() for x in re.split(r"[,\n]", pasted) if x.strip()}
        existing = set(df_base["Player Name"].dropna().unique().tolist())
        missing = sorted([p for p in pasted_set if p not in existing])
        found = pasted_set - set(missing)
        if missing:
            st.warning("Not found: " + ", ".join(missing))
        allowed |= found

    if "Player Name" in df_base.columns and allowed:
        df_filt = df_filt[df_filt["Player Name"].isin(allowed)]

    # Choose columns
    all_cols = df_filt.columns.tolist()
    cols_to_show = st.multiselect(
        "Choose cols to show",
        options=all_cols,
        key="cols_to_show",
        help="Controls which columns are visible and downloaded below."
    )
    cols_render = cols_to_show if cols_to_show else (
        st.session_state.get("last_nonempty_cols") or all_cols[: min(8, len(all_cols))]
    )
    if cols_to_show:
        st.session_state.last_nonempty_cols = cols_to_show

    # DuckDB projection
    con = ensure_duck(df_filt)
    sql = f"SELECT {', '.join(quote_ident(c) for c in cols_render)} FROM api_data" if cols_render else "SELECT * FROM api_data"
    df_show = con.execute(sql).fetchdf()

    st.success(f"Rows: {len(df_filt)}  Cols: {len(df_filt.columns)}  |  Showing {len(df_show.columns)} columns")
    st.dataframe(df_show, use_container_width=True)

    # Downloads
    st.subheader("Downloads")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("Download visible table CSV", df_show.to_csv(index=False).encode("utf-8"), "api_data_visible.csv", "text/csv")
    with c2:
        st.download_button("Download filtered full table CSV", df_filt.to_csv(index=False).encode("utf-8"), "api_data_filtered.csv", "text/csv")
    with c3:
        st.download_button("Download raw JSON", json.dumps(st.session_state.data, indent=2).encode("utf-8"), "api_raw.json", "application/json")
