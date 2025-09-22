import json
from urllib.parse import urljoin
import re
import os
import datetime as dt

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

# ---------- Write helpers ----------
VALID_TITLES = {"mr", "mrs", "ms", "miss", "dr", "prof", "mx"}
TITLE_CASE = {"mr": "Mr", "mrs": "Mrs", "ms": "Ms", "miss": "Miss", "dr": "Dr", "prof": "Prof", "mx": "Mx"}

def iso_date_seconds(d: dt.date) -> str:
    return dt.datetime(d.year, d.month, d.day, 0, 0, 0).isoformat(timespec="seconds")

def normalize_title(s: str) -> str | None:
    s = (s or "").strip()
    if not s:
        return None
    low = s.lower()
    if low in VALID_TITLES:
        return TITLE_CASE[low]
    return None

def clean_payload(d: dict) -> dict:
    return {k: v for k, v in d.items() if v not in ("", None, {}, [])}

def to_contact_type(value: str | int) -> int:
    if isinstance(value, int):
        return 1 if value != 2 else 2
    v = (value or "").strip().lower()
    if v in {"2", "staff", "s"}:
        return 2
    return 1

def case_insensitive_col(df: pd.DataFrame, name: str):
    return next((c for c in df.columns if c.lower() == name.lower()), None)

def try_prefill_from_filters(df: pd.DataFrame) -> tuple[str, str, dt.date | None, int]:
    """Derive first, last, dob, contactType default from current filters."""
    first = ""
    last = ""
    dob = None
    # contact type
    ct_default = 1
    if st.session_state.get("ct_filter"):
        sel = st.session_state["ct_filter"]
        if sel == ["2"]:
            ct_default = 2
        elif sel == ["1"]:
            ct_default = 1
    # name prefill if exactly one player selected
    selected_names = st.session_state.get("player_ms") or []
    if len(selected_names) == 1 and not df.empty and "Player Name" in df.columns:
        name = selected_names[0]
        row = df[df["Player Name"] == name].head(1)
        if not row.empty:
            fcol = case_insensitive_col(df, "firstName")
            lcol = case_insensitive_col(df, "lastName")
            dcol = case_insensitive_col(df, "dateOfBirth")
            if fcol and lcol:
                first = str(row.iloc[0][fcol] or "").strip()
                last = str(row.iloc[0][lcol] or "").strip()
            else:
                # fallback split
                parts = name.split()
                if parts:
                    first = parts[0]
                    last = " ".join(parts[1:]) if len(parts) > 1 else ""
            if dcol:
                try:
                    # handle possible timestamp strings
                    dob_str = str(row.iloc[0][dcol])
                    # keep only date part
                    dob_date = pd.to_datetime(dob_str, errors="coerce").date() if pd.notna(dob_str) else None
                    dob = dob_date
                except Exception:
                    dob = None
    return first, last, dob, ct_default

def find_duplicates(df: pd.DataFrame, first: str, last: str, dob: dt.date | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    fcol = case_insensitive_col(df, "firstName")
    lcol = case_insensitive_col(df, "lastName")
    dcol = case_insensitive_col(df, "dateOfBirth")
    if not fcol or not lcol:
        return pd.DataFrame()
    mask = (df[fcol].astype(str).str.strip().str.lower() == (first or "").strip().lower()) & \
           (df[lcol].astype(str).str.strip().str.lower() == (last or "").strip().lower())
    if dob and dcol:
        # compare date only
        try:
            dob_series = pd.to_datetime(df[dcol], errors="coerce").dt.date
            mask &= (dob_series == dob)
        except Exception:
            pass
    return df.loc[mask]

def post_create_subject(base_url: str, auth: requests.auth.HTTPBasicAuth, payload: dict, timeout=45):
    url = build_url(base_url, "/api/entity/subject")
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    return requests.post(url, auth=auth, headers=headers, json=payload, timeout=timeout)

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
    "ChicagoWhiteSox1901.edge10online.com",
    "clevelandguardians0501.edge10online.com",
    "CsiCalgary0763.edge10online.com",
    "dallasmavericks7336.edge10online.com",
    "EquineCanada3223.edge10online.com",
    "lachargers1959.edge10online.com",
    "Nebraska7798.edge10online.com",
    "NewEnglandPatriots9907.edge10online.com",
    "p27baseballacademy7227.edge10online.com"
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
                timeout=(15,180),
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

# =========================================================
#                         TABS
# =========================================================
tab_view, tab_write = st.tabs(["VIEW", "CREATE PLAYER"])

# ---------- Tab: View data ----------
with tab_view:
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
    else:
        st.info("Fetch data on the View tab using the sidebar, then come back here.")

# ---------- Tab: Write data ----------
with tab_write:
    # Context chips from filters
    if "df" in st.session_state:
        df_ctx = st.session_state.df
    else:
        df_ctx = pd.DataFrame()

    st.caption("Write to: /api/entity/subject")
    chip_texts = []
    if st.session_state.get("ct_filter"):
        chip_texts.append(f"contactType: {', '.join(st.session_state.ct_filter)}")
    if st.session_state.get("player_ms"):
        chip_texts.append(f"Selected players: {len(st.session_state.player_ms)}")
    if st.session_state.get("player_like"):
        chip_texts.append(f"LIKE: {st.session_state.player_like}")
    if st.session_state.get("player_like_ms"):
        chip_texts.append(f"LIKE picks: {len(st.session_state.player_like_ms)}")
    if st.session_state.get("player_free"):
        chip_texts.append("Pasted list present")
    if chip_texts:
        st.write("**Context**  " + "  •  ".join(chip_texts))

    # Prefills from current filters and df
    pre_first, pre_last, pre_dob, ct_default = try_prefill_from_filters(df_ctx)

    # Basic config from sidebar
    base_site = (st.session_state.get("site") or "") if "site" in st.session_state else None
    # In this app, we keep the site in the variable `site` above, so use that
    base_site = site

    st.subheader("Create player")
    with st.form("create_player_form", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            first = st.text_input("First name", value=pre_first or "")
            last  = st.text_input("Last name", value=pre_last or "")
            dob_val = pre_dob or dt.date(2001, 1, 1)
            dob   = st.date_input("Date of birth", value=dob_val)
            gender = st.selectbox("Gender", ["unknown", "male", "female"], index=0)
            contact = st.radio("Contact type", options=["Player", "Staff"], index=0 if ct_default == 1 else 1, horizontal=True)
            title_in = st.selectbox("Title (optional)", ["", "Mr", "Mrs", "Ms", "Miss", "Dr", "Prof", "Mx"], index=0)
        with c2:
            email = st.text_input("Email (optional)", "")
            mobile = st.text_input("Mobile (optional)", "")
            username = st.text_input("Username (optional)", "")
            group_ids_raw = st.text_area("Group IDs (comma separated GUIDs, optional)", "")

            # Live payload preview
            preview_payload = clean_payload({
                "contactType": 1 if contact == "Player" else 2,
                "dateOfBirth": iso_date_seconds(dob),
                "title": normalize_title(title_in),
                "gender": gender.lower(),
                "firstName": first,
                "lastName": last,
                "emailAddress": email,
                "mobileNumber": mobile,
                "username": username,
                "profile": {"customID": None},
                "groupIds": [g.strip() for g in group_ids_raw.split(",") if g.strip()],
            })
            st.code(json.dumps(preview_payload, indent=2), language="json")

        c3, c4 = st.columns([1,2])
        with c3:
            dry_run = st.toggle("Test mode (do not write)", value=False, help="If on, it will not POST, only show the payload.")
        with c4:
            confirm = st.checkbox("I understand this writes to production", value=False)

        submit = st.form_submit_button("Create")

    if submit:
        # Validations
        if not base_site or not user or not pwd:
            st.error("Please fill site, username, and password in the sidebar.")
            st.stop()

        if not first or not last:
            st.error("First and last name are required.")
            st.stop()

        payload = preview_payload  # from above box
        auth = requests.auth.HTTPBasicAuth(user, pwd)

        # Duplicate check against current df (if available)
        dupes = find_duplicates(df_ctx, first, last, dob)
        if not dupes.empty:
            st.warning(f"Possible duplicate found: {len(dupes)} match in current data.")

        if dry_run:
            st.info("Test mode is ON. No write performed.")
            st.code(json.dumps(payload, indent=2), language="json")
        else:
            if not confirm:
                st.warning("Please tick the confirmation to proceed.")
                st.stop()

            with st.status("Creating player...", expanded=False) as status:
                try:
                    r = post_create_subject(base_site, auth, payload, timeout=45)
                except requests.RequestException as e:
                    status.update(state="error")
                    st.error(f"Request failed: {e}")
                    st.stop()

                st.write("Status:", r.status_code)
                if not r.ok:
                    status.update(label=f"HTTP {r.status_code}", state="error")
                    try:
                        st.error(json.dumps(r.json(), indent=2))
                    except Exception:
                        st.error(r.text)
                    st.stop()

                created = r.json()
                status.update(label="Created", state="complete")
                st.success(f"Created subject id: {created.get('id')}")
                st.code(json.dumps(created, indent=2), language="json")

                # Optional read back
                try:
                    read_url = build_url(base_site, f"/api/entity/subject/{created.get('id')}")
                    rr = requests.get(read_url, auth=auth, headers={"Accept": "application/json"}, timeout=30)
                    if rr.ok:
                        st.caption("Read back")
                        st.code(json.dumps(rr.json(), indent=2), language="json")
                except requests.RequestException:
                    pass
