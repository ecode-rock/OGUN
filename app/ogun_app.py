"""
ogun_app.py — OGUN Race Visualizer
Streamlit app connecting to baseball_db PostgreSQL database.
"""

import time
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OGUN Race",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── DB connection ─────────────────────────────────────────────────────────────
_FALLBACK_URL = "postgresql://postgres.pepkzpdjebituvxzamfn:PBMlApFSKUMVxJw2@aws-1-ca-central-1.pooler.supabase.com:6543/postgres"
DB_URL = st.secrets.get("DATABASE_URL", _FALLBACK_URL).replace(
    "postgresql://", "postgresql+psycopg2://"
)

@st.cache_resource
def get_engine():
    return create_engine(DB_URL, future=True)

@st.cache_data(ttl=60)
def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)

# ── Team colours (all 30 + neutral fallback) ──────────────────────────────────
TEAM_COLORS = {
    "ARI": "#A71930", "AZ":  "#A71930",
    "ATH": "#003831", "OAK": "#003831",
    "ATL": "#CE1141",
    "BAL": "#DF4601",
    "BOS": "#BD3039",
    "CHC": "#0E3386",
    "CWS": "#27251F",
    "CIN": "#C6011F",
    "CLE": "#00385D",
    "COL": "#333366",
    "DET": "#0C2340",
    "HOU": "#EB6E1F",
    "KC":  "#004687",
    "LAA": "#BA0021",
    "LAD": "#005A9C",
    "MIA": "#00A3E0",
    "MIL": "#12284B",
    "MIN": "#002B5C",
    "NYM": "#002D72",
    "NYY": "#003087",
    "PHI": "#E81828",
    "PIT": "#FDB827",
    "SD":  "#2F241D",
    "SEA": "#0C2C56",
    "SF":  "#FD5A1E",
    "STL": "#C41E3A",
    "TB":  "#092C5C",
    "TEX": "#003278",
    "TOR": "#134A8E",
    "WSH": "#AB0003",
}
NEUTRAL = "#888888"

def team_color(team: str | None) -> str:
    return TEAM_COLORS.get(team or "", NEUTRAL)

# ── View states ────────────────────────────────────────────────────────────────
VIEW_OPTIONS = ["ALL CONTACT", "HITS ONLY", "AGGREGATE"]
VIEW_LABELS  = {"ALL CONTACT": "ALL", "HITS ONLY": "HITS", "AGGREGATE": "AGG"}
HIT_EVENTS   = {"single", "double", "triple", "home run"}  # matches events col (title case lowered)

def apply_view_filter(df: pd.DataFrame, view: str) -> pd.DataFrame:
    """Filter df to hit events only when view == 'HITS ONLY'. Other views unfiltered."""
    if df.empty or view != "HITS ONLY":
        return df
    mask = df["events"].str.lower().isin(HIT_EVENTS)
    return df[mask].reset_index(drop=True)

# ── Batted ball outcome classification ────────────────────────────────────────
OUTCOME_LABELS = ["HR", "3B", "2B", "1B", "FLY", "POP", "LINE", "GROUND", "OTHER"]

def classify_batted_ball(evt: str, la: float | None) -> str | None:
    """Classify an at-bat event to an outcome label. Returns None for non-contact events."""
    e = (evt or "").lower()
    if e == "home run":    return "HR"
    if e == "triple":      return "3B"
    if e == "double":      return "2B"
    if e == "single":      return "1B"
    if e in ("pop out", "bunt pop out"):                          return "POP"
    if e in ("flyout", "sac fly", "sac fly double play"):         return "FLY"
    if e == "lineout":                                            return "LINE"
    if e in ("groundout", "forceout", "gidp", "double play",
             "bunt groundout", "sac bunt"):                       return "GROUND"
    if e in ("field error", "fielders choice", "fielders choice out"): return "OTHER"
    # Statcast API format fallback — classify field_out by launch angle
    if e in ("field_out", "field out"):
        if la is None:   return "FLY"
        if la >= 50:     return "POP"
        if la >= 25:     return "FLY"
        if la >= 10:     return "LINE"
        return "GROUND"
    if e in ("force_out", "double_play"):   return "GROUND"
    if e == "home_run":                     return "HR"
    return None   # strikeout, walk, HBP, caught stealing, etc.

def calc_outcome_counts(df: pd.DataFrame) -> dict:
    """Count batted ball outcomes in df. Returns {label: count} for all OUTCOME_LABELS."""
    counts = {lbl: 0 for lbl in OUTCOME_LABELS}
    if df.empty:
        return counts
    for _, row in df.iterrows():
        evt = row.get("events") or ""
        la  = row.get("launch_angle")
        la_f = float(la) if (la is not None and pd.notna(la)) else None
        lbl = classify_batted_ball(evt, la_f)
        if lbl is not None:
            counts[lbl] += 1
    return counts

# ── OGUN formula ──────────────────────────────────────────────────────────────
def calc_ogun(avg_dist: float, avg_ev: float, avg_la: float, optimum: float = 29) -> float | None:
    if not avg_ev:
        return None
    mult = np.cos(np.radians(abs(avg_la - optimum))) ** 2
    return (avg_dist / avg_ev) * mult

def ogun_color(score: float | None) -> str:
    if score is None:
        return "#888888"
    if score >= 2.0:
        return "#00C851"
    if score >= 1.75:
        return "#9ACD32"
    if score >= 1.5:
        return "#FFA500"
    return "#FF4444"

def ogun_label(score: float | None) -> str:
    if score is None:
        return "N/A"
    if score >= 2.0:
        return "ELITE"
    if score >= 1.75:
        return "ABOVE AVG"
    if score >= 1.5:
        return "BELOW AVG"
    return "POOR"

# ── DB helpers ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=120)
def get_teams() -> list[str]:
    df = query("SELECT DISTINCT team_batting FROM pitches WHERE team_batting IS NOT NULL ORDER BY team_batting")
    return df["team_batting"].tolist()

@st.cache_data(ttl=120)
def get_players() -> list[str]:
    df = query("SELECT DISTINCT batter_name FROM pitches WHERE batter_name IS NOT NULL ORDER BY batter_name")
    return df["batter_name"].tolist()

@st.cache_data(ttl=120)
def get_date_range() -> tuple:
    df = query("SELECT MIN(game_date) as mn, MAX(game_date) as mx FROM pitches")
    return df["mn"].iloc[0], df["mx"].iloc[0]

@st.cache_data(ttl=120)
def get_available_dates() -> list:
    df = query("SELECT DISTINCT game_date FROM pitches ORDER BY game_date")
    return df["game_date"].tolist()

@st.cache_data(ttl=120)
def get_games_on_date(game_date) -> pd.DataFrame:
    df = query(
        "SELECT DISTINCT game_pk, home_team, away_team FROM pitches "
        "WHERE game_date = :d ORDER BY game_pk",
        {"d": str(game_date)},
    )
    return df

@st.cache_data(ttl=60)
def fetch_abs(mode: str, selector: str, date_mode: str,
              start_date=None, end_date=None, game_pk: int | None = None) -> pd.DataFrame:
    """
    Fetch last-pitch rows for one panel.
    mode: 'TEAM' | 'PLAYER'
    selector: team abbrev or batter_name
    date_mode: 'DATE RANGE' | 'SINGLE GAME'
    Returns empty DataFrame if required params are missing.
    """
    if not selector:
        return pd.DataFrame()
    if date_mode == "DATE RANGE" and (start_date is None or end_date is None):
        return pd.DataFrame()
    if date_mode == "SINGLE GAME" and game_pk is None:
        return pd.DataFrame()

    base = """
        SELECT ab_number, inning, game_date, game_pk,
               batter_name, team_batting,
               hit_distance, launch_speed, launch_angle,
               events, is_barrel,
               game_total_pitches
        FROM pitches
        WHERE type = 'pitch'
          AND is_last_pitch = TRUE
    """
    params: dict = {}

    if mode == "TEAM":
        base += " AND team_batting = :sel"
    else:
        base += " AND batter_name = :sel"
    params["sel"] = selector

    if date_mode == "SINGLE GAME":
        base += " AND game_pk = :gpk"
        params["gpk"] = int(game_pk)
    else:
        # Pass date objects directly — SQLAlchemy handles the conversion.
        # Never use str() here: str(None) = "None" which breaks the query.
        base += " AND game_date BETWEEN :sd AND :ed"
        params["sd"] = start_date
        params["ed"] = end_date

    base += " ORDER BY game_date, game_pk, game_total_pitches::float"
    df = query(base, params)
    return df

# ── Arc drawing ───────────────────────────────────────────────────────────────
def make_arc(x_start: float, dist: float, angle: float,
             color: str, opacity: float, name: str) -> list:
    """
    Draw a quadratic Bezier arc from x_start to x_start+dist.
    Peak height is driven by launch angle (clamped 0–90°).
    Returns a list of traces (fill + line).
    """
    angle_clamped = max(0.0, min(90.0, angle if angle is not None else 20.0))
    # height = fraction of distance, scaled by sin of launch angle
    peak_h = dist * np.sin(np.radians(angle_clamped)) * 0.35

    n = 40
    t = np.linspace(0, 1, n)
    # Quadratic Bezier: P0=(x_start,0), P1=(x_start+dist/2, peak_h), P2=(x_start+dist,0)
    x0, y0_pt = x_start, 0.0
    x1, y1 = x_start + dist / 2, peak_h
    x2, y2 = x_start + dist, 0.0
    bx = (1 - t) ** 2 * x0 + 2 * (1 - t) * t * x1 + t ** 2 * x2
    by = (1 - t) ** 2 * y0_pt + 2 * (1 - t) * t * y1 + t ** 2 * y2

    fill_trace = go.Scatter(
        x=np.concatenate([bx, bx[::-1]]),
        y=np.concatenate([by, np.zeros(n)]),
        fill="toself",
        fillcolor=color,
        opacity=opacity * 0.25,
        line=dict(width=0),
        showlegend=False,
        hoverinfo="skip",
        name=name,
    )
    line_trace = go.Scatter(
        x=bx, y=by,
        mode="lines",
        line=dict(color=color, width=2),
        opacity=opacity,
        showlegend=False,
        hovertemplate=(
            f"<b>{name}</b><br>"
            f"Dist: {dist:.0f} ft<br>"
            f"Angle: {angle:.1f}°<br>"
            "<extra></extra>"
        ),
        name=name,
    )
    return [fill_trace, line_trace]

def build_race_figure(contact_abs: pd.DataFrame, color: str,
                      current_idx: int, shared_xmax: float) -> go.Figure:
    """
    Build one race lane figure from a DataFrame of contact at-bats
    up to and including current_idx.
    contact_abs: rows with launch_speed NOT NULL, sorted chronologically.
    current_idx: how many ABs to show (0 = none).
    """
    fig = go.Figure()

    if contact_abs.empty or current_idx == 0:
        fig.update_layout(**_lane_layout(shared_xmax))
        return fig

    visible = contact_abs.iloc[:current_idx]
    n = len(visible)
    x_cursor = 0.0

    for i, (_, row) in enumerate(visible.iterrows()):
        dist = float(row["hit_distance"] or 0)
        angle = float(row["launch_angle"]) if row["launch_angle"] is not None else 20.0
        is_last = (i == n - 1)
        opacity = (0.35 + 0.65 * (i / max(n - 1, 1))) if n > 1 else 1.0
        arc_color = color if is_last else color
        traces = make_arc(x_cursor, dist, angle, arc_color, opacity,
                          f"{row['batter_name']} – {row['events']}")
        for t in traces:
            fig.add_trace(t)

        if is_last:
            # Meeple icon at arc start
            fig.add_trace(go.Scatter(
                x=[x_cursor],
                y=[0],
                mode="text",
                text=["🏏"],
                textfont=dict(size=18),
                showlegend=False,
                hoverinfo="skip",
            ))

            # Dashed position line
            peak_h = dist * np.sin(np.radians(max(0, angle))) * 0.35
            fig.add_shape(
                type="line",
                x0=x_cursor + dist, x1=x_cursor + dist,
                y0=0, y1=max(peak_h * 1.5, 20),
                line=dict(color=color, width=1.5, dash="dash"),
            )

            # Optimum angle guide from last arc start
            opt_angle = 29.0
            guide_len = dist * 1.2 if dist > 0 else 100
            guide_x = x_cursor + guide_len * np.cos(np.radians(opt_angle))
            guide_y = guide_len * np.sin(np.radians(opt_angle)) * 0.35
            fig.add_trace(go.Scatter(
                x=[x_cursor, guide_x],
                y=[0, guide_y],
                mode="lines",
                line=dict(color="rgba(255,255,255,0.25)", width=1, dash="dot"),
                showlegend=False,
                hoverinfo="skip",
            ))

        x_cursor += dist

    fig.update_layout(**_lane_layout(shared_xmax))
    return fig

def build_aggregate_figure(contact_abs: pd.DataFrame, color: str,
                           shared_xmax: float) -> go.Figure:
    """
    Draw a single thick arc using aggregate (avg) stats from contact_abs.
    contact_abs must already be filtered/cleaned (launch_speed & hit_distance non-null, > 0).
    """
    fig = go.Figure()
    if contact_abs.empty:
        fig.update_layout(**_lane_layout(shared_xmax))
        return fig

    avg_dist = float(contact_abs["hit_distance"].mean())
    avg_la   = float(contact_abs["launch_angle"].mean()) \
               if contact_abs["launch_angle"].notna().any() else 20.0
    avg_ev   = float(contact_abs["launch_speed"].mean()) \
               if contact_abs["launch_speed"].notna().any() else 0.0
    n_contact = len(contact_abs)

    angle_clamped = max(0.0, min(90.0, avg_la))
    peak_h = avg_dist * np.sin(np.radians(angle_clamped)) * 0.35
    n = 40
    t = np.linspace(0, 1, n)
    bx = (1-t)**2 * 0 + 2*(1-t)*t*(avg_dist/2) + t**2*avg_dist
    by = (1-t)**2 * 0 + 2*(1-t)*t*peak_h        + t**2*0

    hover = (
        f"<b>Aggregate · {n_contact} contact ABs</b><br>"
        f"Avg Dist: {avg_dist:.0f} ft<br>"
        f"Avg LA: {avg_la:.1f}°<br>"
        f"Avg EV: {avg_ev:.1f} mph<br>"
        "<extra></extra>"
    )
    fig.add_trace(go.Scatter(
        x=np.concatenate([bx, bx[::-1]]),
        y=np.concatenate([by, np.zeros(n)]),
        fill="toself", fillcolor=color, opacity=0.35,
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=bx, y=by, mode="lines",
        line=dict(color=color, width=4),
        opacity=1.0, showlegend=False,
        hovertemplate=hover, name="Aggregate",
    ))
    fig.add_shape(
        type="line",
        x0=avg_dist, x1=avg_dist,
        y0=0, y1=max(peak_h * 1.5, 20),
        line=dict(color=color, width=1.5, dash="dash"),
    )
    fig.update_layout(**_lane_layout(shared_xmax))
    return fig

def _lane_layout(xmax: float) -> dict:
    pad = xmax * 0.2 if xmax > 0 else 200
    return dict(
        height=210,
        margin=dict(l=0, r=0, t=0, b=28),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#0d1117",
        xaxis=dict(
            range=[0, xmax + pad],
            showgrid=True,
            gridcolor="rgba(255,255,255,0.07)",
            tickfont=dict(color="#888", size=10),
            title=dict(text="Cumulative Distance (ft)", font=dict(color="#888", size=10)),
            zeroline=False,
        ),
        yaxis=dict(
            visible=False,
            range=[0, None],
        ),
        showlegend=False,
    )

# ── Stats calculation ─────────────────────────────────────────────────────────
def calc_stats(df: pd.DataFrame, up_to: int) -> dict:
    """
    df = all AB rows for the panel (including non-contact).
    up_to = number of ABs to include.
    """
    subset = df.iloc[:up_to] if up_to > 0 else df.iloc[0:0]
    total_abs = len(subset)
    contact = subset.dropna(subset=["launch_speed"])
    contact_abs = len(contact)

    avg_dist = contact["hit_distance"].mean() if contact_abs else None
    avg_ev   = contact["launch_speed"].mean() if contact_abs else None
    avg_la   = contact["launch_angle"].mean() if contact_abs else None
    total_dist = contact["hit_distance"].sum() if contact_abs else 0.0

    ogun = calc_ogun(avg_dist, avg_ev, avg_la) if (avg_dist and avg_ev and avg_la is not None) else None
    contact_rate = contact_abs / total_abs if total_abs else None

    return dict(
        total_dist=total_dist,
        avg_dist=avg_dist,
        avg_ev=avg_ev,
        avg_la=avg_la,
        ogun=ogun,
        contact_rate=contact_rate,
        total_abs=total_abs,
        contact_abs=contact_abs,
    )

# ── CSS ───────────────────────────────────────────────────────────────────────
def inject_css():
    st.markdown("""
    <style>
    /* Dark background */
    .stApp { background-color: #0d1117; color: #e6edf3; }
    [data-testid="stSidebar"] { background: #161b22; }

    /* Title */
    .ogun-title {
        font-family: 'Arial Black', sans-serif;
        font-size: 1.7rem;
        font-weight: 900;
        letter-spacing: 0.12em;
        color: #e6edf3;
        margin-bottom: 0;
        line-height: 1.1;
    }
    .ogun-subtitle {
        font-size: 0.8rem;
        letter-spacing: 0.22em;
        color: #8b949e;
        margin-top: 0;
        text-transform: uppercase;
    }

    /* Panel header */
    .panel-header {
        display: flex;
        align-items: center;
        gap: 14px;
        padding: 10px 0 6px 0;
        border-bottom: 1px solid #21262d;
        margin-bottom: 8px;
    }
    .panel-name {
        font-size: 1.15rem;
        font-weight: 700;
        letter-spacing: 0.06em;
    }

    /* OGUN badge */
    .ogun-badge {
        display: inline-flex;
        flex-direction: column;
        align-items: center;
        padding: 4px 14px;
        border-radius: 8px;
        min-width: 80px;
    }
    .ogun-score { font-size: 1.5rem; font-weight: 900; line-height: 1.1; }
    .ogun-lbl   { font-size: 0.6rem; letter-spacing: 0.15em; opacity: 0.85; }
    .ogun-view-lbl { font-size: 0.55rem; letter-spacing: 0.12em; color: #8b949e; margin-top: 1px; }

    /* Stats bar */
    .stats-bar {
        display: flex;
        gap: 0;
        background: #161b22;
        border-radius: 6px;
        overflow: hidden;
        margin: 6px 0 4px 0;
    }
    .stat-cell {
        flex: 1;
        padding: 6px 4px;
        text-align: center;
        border-right: 1px solid #21262d;
    }
    .stat-cell:last-child { border-right: none; }
    .stat-val { font-size: 1.05rem; font-weight: 700; color: #e6edf3; }
    .stat-lbl { font-size: 0.6rem; color: #8b949e; letter-spacing: 0.1em; text-transform: uppercase; }

    /* Controls */
    .control-row {
        display: flex;
        align-items: center;
        gap: 12px;
        background: #161b22;
        padding: 10px 16px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .pos-display {
        background: #21262d;
        border-radius: 6px;
        padding: 4px 12px;
        font-size: 0.85rem;
        color: #8b949e;
        white-space: nowrap;
    }

    /* At-bat log table */
    .ab-log-wrap {
        max-height: 280px;
        overflow-y: auto;
        background: #0d1117;
        border: 1px solid #21262d;
        border-radius: 8px;
    }
    .ab-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.8rem;
    }
    .ab-table th {
        background: #161b22;
        color: #8b949e;
        text-align: left;
        padding: 6px 10px;
        position: sticky;
        top: 0;
        z-index: 1;
        letter-spacing: 0.08em;
        font-size: 0.7rem;
        text-transform: uppercase;
        border-bottom: 1px solid #21262d;
    }
    .ab-table td { padding: 5px 10px; border-bottom: 1px solid #161b22; }
    .ab-table tr:last-child td { border-bottom: none; }
    .ab-row-current td { background: #1c2128 !important; }
    .ab-row-xbh td { color: #f0b429 !important; }
    .ab-row-hr td { color: #f0b429 !important; font-weight: 700; }

    /* Divider */
    .panel-divider {
        border: none;
        border-top: 1px solid #21262d;
        margin: 16px 0;
    }

    /* Speed pill labels */
    .speed-labels {
        display: flex;
        justify-content: space-between;
        font-size: 0.65rem;
        color: #8b949e;
        margin-top: -4px;
        padding: 0 4px;
    }

    /* Empty state */
    .empty-state {
        text-align: center;
        padding: 40px;
        color: #8b949e;
        font-size: 0.95rem;
    }

    /* Streamlit widget tweaks */
    div[data-testid="stRadio"] label { font-size: 0.82rem !important; }
    div[data-testid="stSelectbox"] label { font-size: 0.78rem !important; color: #8b949e !important; }
    div[data-testid="stDateInput"] label { font-size: 0.78rem !important; color: #8b949e !important; }
    button[kind="primary"] { font-weight: 700 !important; }

    /* Outcome breakdown */
    .outcome-panel {
        padding: 8px 6px;
        background: #161b22;
        border-radius: 6px;
    }
    .outcome-hdr {
        color: #8b949e;
        font-size: 0.6rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        font-weight: 700;
        margin-bottom: 5px;
        padding-bottom: 4px;
        border-bottom: 1px solid #21262d;
    }
    .outcome-row {
        display: flex;
        align-items: center;
        gap: 4px;
        margin-bottom: 3px;
        min-height: 15px;
    }
    .outcome-row-zero .outcome-lbl,
    .outcome-row-zero .outcome-cnt { color: #3d444d !important; }
    .outcome-lbl {
        width: 38px;
        color: #c9d1d9;
        font-size: 0.63rem;
        letter-spacing: 0.04em;
        font-weight: 600;
        flex-shrink: 0;
    }
    .outcome-bar-wrap {
        flex: 1;
        background: #21262d;
        border-radius: 2px;
        height: 7px;
        overflow: hidden;
        min-width: 10px;
    }
    .outcome-bar { height: 100%; border-radius: 2px; }
    .outcome-cnt {
        width: 18px;
        text-align: right;
        color: #e6edf3;
        font-size: 0.63rem;
        flex-shrink: 0;
    }
    .outcome-sep {
        border-top: 1px solid #21262d;
        margin: 4px 0 4px 0;
    }
    .outcome-total {
        color: #8b949e;
        font-size: 0.6rem;
        letter-spacing: 0.08em;
        margin-top: 5px;
        padding-top: 4px;
        border-top: 1px solid #21262d;
    }
    </style>
    """, unsafe_allow_html=True)

# ── HTML helpers ──────────────────────────────────────────────────────────────
def render_ogun_badge(score: float | None, color: str, view_label: str = "") -> str:
    bg = ogun_color(score)
    lbl = ogun_label(score)
    val = f"{score:.3f}" if score is not None else "—"
    view_tag = f'<span class="ogun-view-lbl">{view_label}</span>' if view_label else ""
    return f"""
    <div class="ogun-badge" style="background:{bg}22; border:2px solid {bg};">
        <span class="ogun-score" style="color:{bg};">{val}</span>
        <span class="ogun-lbl" style="color:{bg};">{lbl}</span>
        {view_tag}
    </div>"""

def render_stats_bar(stats: dict) -> str:
    def fmt(v, fmt_str):
        return fmt_str.format(v) if v is not None else "—"

    td  = fmt(stats["total_dist"],  "{:.0f} ft")
    ad  = fmt(stats["avg_dist"],    "{:.1f} ft")
    ev  = fmt(stats["avg_ev"],      "{:.1f}")
    la  = fmt(stats["avg_la"],      "{:.1f}°")
    cr  = fmt(stats["contact_rate"],"{:.0%}")
    return f"""
    <div class="stats-bar">
        <div class="stat-cell"><div class="stat-val">{td}</div><div class="stat-lbl">Total Dist</div></div>
        <div class="stat-cell"><div class="stat-val">{ad}</div><div class="stat-lbl">Avg Dist</div></div>
        <div class="stat-cell"><div class="stat-val">{ev}</div><div class="stat-lbl">Avg EV</div></div>
        <div class="stat-cell"><div class="stat-val">{la}</div><div class="stat-lbl">Avg LA</div></div>
        <div class="stat-cell"><div class="stat-val">{cr}</div><div class="stat-lbl">Contact %</div></div>
    </div>"""

def render_ab_log(df: pd.DataFrame, current_idx: int, mode: str,
                  view: str = "ALL CONTACT") -> str:
    """Build the at-bat log HTML table for rows 0..current_idx."""
    XBH = {"Double", "Triple", "Home Run", "Single"}
    HR  = {"Home Run"}

    rows_html = []
    visible = df.iloc[:current_idx] if current_idx > 0 else df.iloc[0:0]

    # HITS ONLY: filter visible rows to hit events
    if view == "HITS ONLY" and not visible.empty:
        visible = visible[visible["events"].str.lower().isin(HIT_EVENTS)]

    team_col_lbl = "TEAM" if mode == "TEAM" else "PLAYER"
    filter_tag = " ★" if view == "HITS ONLY" else (" ◈" if view == "AGGREGATE" else "")

    for i, (_, row) in enumerate(visible.iterrows()):
        is_current = (i == len(visible) - 1)
        evt = row.get("events") or ""
        is_hr  = evt in HR
        is_xbh = evt in XBH and not is_hr

        row_class = ""
        if is_current:
            row_class += " ab-row-current"
        if is_hr:
            row_class += " ab-row-hr"
        elif is_xbh:
            row_class += " ab-row-xbh"

        dist = f"{row['hit_distance']:.0f}" if pd.notna(row.get("hit_distance")) else ""
        ev_v = f"{row['launch_speed']:.1f}" if pd.notna(row.get("launch_speed")) else ""
        la_v = f"{row['launch_angle']:.1f}°" if pd.notna(row.get("launch_angle")) else ""
        barrel = "✓" if row.get("is_barrel") else ""
        team_or_player = row.get("team_batting", "") if mode == "TEAM" else row.get("batter_name", "")
        inning = row.get("inning", "")
        batter = row.get("batter_name", "")
        result = evt or ""

        rows_html.append(f"""
        <tr class="{row_class.strip()}">
            <td>{inning}</td>
            <td>{team_or_player}</td>
            <td>{batter}</td>
            <td>{result}</td>
            <td>{dist}</td>
            <td>{ev_v}</td>
            <td>{la_v}</td>
            <td style="text-align:center;">{barrel}</td>
        </tr>""")

    rows_str = "\n".join(rows_html) if rows_html else \
        '<tr><td colspan="8" style="text-align:center;color:#8b949e;padding:20px;">No at-bats yet</td></tr>'

    return f"""
    <div class="ab-log-wrap">
    <table class="ab-table">
        <thead>
            <tr>
                <th>INN</th><th>{team_col_lbl}</th><th>BATTER</th>
                <th>RESULT{filter_tag}</th><th>DIST</th><th>EV</th><th>LA</th><th>BARREL</th>
            </tr>
        </thead>
        <tbody>
            {rows_str}
        </tbody>
    </table>
    </div>"""

def render_outcome_breakdown(counts: dict, color: str) -> str:
    """Render the vertical batted ball outcome breakdown column as HTML."""
    total = sum(counts.values())
    max_cnt = max(counts.values()) if total > 0 else 1
    # Convert hex color → rgba for bars
    c = color.lstrip("#")
    r, g, b = int(c[0:2], 16), int(c[2:4], 16), int(c[4:6], 16)
    bar_color = f"rgba({r},{g},{b},0.6)"

    rows = []
    for i, lbl in enumerate(OUTCOME_LABELS):
        if i == 4:   # separator between hits and outs
            rows.append('<div class="outcome-sep"></div>')
        cnt = counts[lbl]
        bar_pct = int(cnt / max_cnt * 100) if cnt > 0 else 0
        zero_cls = " outcome-row-zero" if cnt == 0 else ""
        bar_style = (f"width:{bar_pct}%;background:{bar_color};"
                     if cnt > 0 else "width:0%;")
        cnt_str = str(cnt) if cnt > 0 else "·"
        rows.append(
            f'<div class="outcome-row{zero_cls}">'
            f'<span class="outcome-lbl">{lbl}</span>'
            f'<div class="outcome-bar-wrap"><div class="outcome-bar" style="{bar_style}"></div></div>'
            f'<span class="outcome-cnt">{cnt_str}</span>'
            f'</div>'
        )

    return (
        '<div class="outcome-panel">'
        '<div class="outcome-hdr">BATTED BALLS</div>'
        + "".join(rows)
        + f'<div class="outcome-total">TOTAL&nbsp;&nbsp;{total}</div>'
        + '</div>'
    )

# ── Session state init ────────────────────────────────────────────────────────
def init_state():
    # Only initialise keys that are NOT widget keys.
    # Widget keys (team/player selectors, date inputs) must NOT be pre-set to
    # None here — doing so would cause Streamlit to override the widget's own
    # default value with None on the first render.
    non_widget_defaults = {
        "playing": False,
        "ab_pos": 0,
        "abs_per_sec": 5,   # 1–100 ABs per second
        "last_tick": 0.0,
        "data_sig": "",     # detects selector/date changes to auto-show full data
    }
    for k, v in non_widget_defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

# ── Panel selector UI ─────────────────────────────────────────────────────────
def panel_selector(panel_id: str, teams: list, players: list):
    """Render the team/player selector for one panel. Returns (mode, selector)."""
    label = "Panel 1" if panel_id == "A" else "Panel 2"
    mode_key   = f"p{panel_id}_mode"
    team_key   = f"p{panel_id}_team"
    player_key = f"p{panel_id}_player"

    col_toggle, col_pick = st.columns([1, 3])
    with col_toggle:
        mode = st.radio(
            f"{label} type",
            ["TEAM", "PLAYER"],
            key=mode_key,
            horizontal=True,
            label_visibility="collapsed",
        )

    with col_pick:
        if mode == "TEAM":
            cur = st.session_state.get(team_key)
            default_idx = teams.index(cur) if cur in teams else 0
            sel = st.selectbox(
                "Team",
                teams,
                index=default_idx,
                key=team_key,
                label_visibility="collapsed",
            )
        else:
            cur = st.session_state.get(player_key)
            default_idx = players.index(cur) if cur in players else 0
            sel = st.selectbox(
                "Player",
                players,
                index=default_idx,
                key=player_key,
                label_visibility="collapsed",
            )
    return mode, sel

# ── Date/game selector UI ─────────────────────────────────────────────────────
def date_game_selector(date_min, date_max, avail_dates: list):
    """Render shared date controls. Returns (date_mode, start, end, game_pk, home_team, away_team)."""
    date_mode = st.radio(
        "Date mode",
        ["DATE RANGE", "SINGLE GAME"],
        key="date_mode",
        horizontal=True,
        label_visibility="collapsed",
    )

    start_date = end_date = game_pk = None
    home_team = away_team = None

    if date_mode == "DATE RANGE":
        c1, c2 = st.columns(2)
        with c1:
            start_date = st.date_input(
                "From",
                value=st.session_state.get("start_date") or date_min,
                min_value=date_min,
                max_value=date_max,
                key="start_date",
            )
        with c2:
            end_date = st.date_input(
                "To",
                value=st.session_state.get("end_date") or date_max,
                min_value=date_min,
                max_value=date_max,
                key="end_date",
            )
    else:
        c1, c2 = st.columns(2)
        with c1:
            avail_strs = [str(d) for d in avail_dates]
            default_date_str = str(st.session_state.get("game_date_single") or avail_dates[-1])
            if default_date_str not in avail_strs:
                default_date_str = avail_strs[-1]
            chosen_date_str = st.selectbox(
                "Date",
                avail_strs,
                index=avail_strs.index(default_date_str),
                key="game_date_single",
                label_visibility="visible",
            )
        with c2:
            games_df = get_games_on_date(chosen_date_str)
            if games_df.empty:
                st.info("No games on that date.")
            else:
                game_options = {
                    f"{row['away_team']} @ {row['home_team']}": int(row["game_pk"])
                    for _, row in games_df.iterrows()
                }
                game_teams = {
                    f"{row['away_team']} @ {row['home_team']}": (row["away_team"], row["home_team"])
                    for _, row in games_df.iterrows()
                }
                chosen_label = st.selectbox(
                    "Game",
                    list(game_options.keys()),
                    key="game_pk_single_label",
                    label_visibility="visible",
                )
                game_pk = game_options[chosen_label]
                away_team, home_team = game_teams[chosen_label]
                st.session_state["game_pk_single"] = game_pk
        start_date = end_date = None

    return date_mode, start_date, end_date, game_pk, home_team, away_team

# ── Panel renderer ────────────────────────────────────────────────────────────
def render_panel(
    panel_id: str,
    mode: str,
    selector: str,
    all_abs: pd.DataFrame,
    current_idx: int,
    shared_xmax: float,
):
    """Render one race lane panel (header + badge + view toggle + chart + stats bar)."""
    color = team_color(selector if mode == "TEAM" else None)

    if all_abs.empty:
        st.markdown(f'<div class="empty-state">No at-bats found for <b>{selector}</b></div>',
                    unsafe_allow_html=True)
        return

    # ── View toggle (above chart) ──────────────────────────────────────────────
    view_key = f"p{panel_id}_view"
    panel_num = "1" if panel_id == "A" else "2"

    # Header row: name + OGUN badge
    hcol1, hcol2 = st.columns([5, 1])
    with hcol1:
        st.html(
            f'<div class="panel-header">'
            f'<span class="panel-name" style="color:{color};">&#9612; {selector}</span>'
            f'<span style="color:#8b949e;font-size:0.75rem;">Panel {panel_num}</span>'
            f'</div>'
        )

    # ── Apply view filter ──────────────────────────────────────────────────────
    # view is set by the radio widget rendered below; read from session_state so
    # it is available immediately even before the widget renders on first run.
    view = st.session_state.get(view_key, "ALL CONTACT")

    view_df = apply_view_filter(all_abs, view)

    # view-aware current index: for HITS ONLY, count how many hit-ABs have
    # occurred within the first current_idx total ABs.
    if view == "HITS ONLY" and not all_abs.empty:
        slice_up_to = all_abs.iloc[:current_idx]
        view_current_idx = len(apply_view_filter(slice_up_to, "HITS ONLY"))
    else:
        view_current_idx = current_idx  # AGGREGATE ignores index anyway

    # Stats: AGGREGATE always uses full data; others use view_current_idx
    if view == "AGGREGATE":
        stats = calc_stats(view_df, len(view_df))
    else:
        stats = calc_stats(view_df, view_current_idx)

    with hcol2:
        st.html(render_ogun_badge(stats["ogun"], color, VIEW_LABELS[view]))

    # ── View toggle radio + breakdown toggle ──────────────────────────────────
    view_ctrl_col, bd_toggle_col = st.columns([4, 1])
    with view_ctrl_col:
        st.radio(
            "View",
            VIEW_OPTIONS,
            key=view_key,
            horizontal=True,
            label_visibility="collapsed",
        )
    with bd_toggle_col:
        bd_key = f"p{panel_id}_breakdown"
        show_bd = st.toggle("Breakdown", key=bd_key, value=True)

    # ── Chart ──────────────────────────────────────────────────────────────────
    contact_abs = view_df.dropna(subset=["launch_speed", "hit_distance"]).reset_index(drop=True)
    contact_abs = contact_abs[contact_abs["hit_distance"] > 0].reset_index(drop=True)

    if view == "AGGREGATE":
        fig = build_aggregate_figure(contact_abs, color, shared_xmax)
    else:
        subset_view = view_df.iloc[:view_current_idx] if view_current_idx > 0 \
                      else view_df.iloc[0:0]
        contact_so_far = subset_view.dropna(subset=["launch_speed", "hit_distance"])
        contact_so_far = contact_so_far[contact_so_far["hit_distance"] > 0]
        n_contact = len(contact_so_far)
        fig = build_race_figure(contact_abs, color, n_contact, shared_xmax)

    # ── Outcome counts (respects view) ─────────────────────────────────────────
    if view == "AGGREGATE":
        count_df = all_abs
    elif view == "HITS ONLY":
        count_df = apply_view_filter(
            all_abs.iloc[:current_idx] if current_idx > 0 else all_abs.iloc[0:0],
            "HITS ONLY",
        )
    else:
        count_df = all_abs.iloc[:current_idx] if current_idx > 0 else all_abs.iloc[0:0]
    counts = calc_outcome_counts(count_df)

    # ── Chart + optional outcome breakdown ─────────────────────────────────────
    if show_bd:
        chart_col, breakdown_col = st.columns([3, 1])
        with chart_col:
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False},
                            key=f"race_chart_{panel_id}")
            st.html(render_stats_bar(stats))
        with breakdown_col:
            st.html(render_outcome_breakdown(counts, color))
    else:
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False},
                        key=f"race_chart_{panel_id}")
        st.html(render_stats_bar(stats))

# ── Playback controls ─────────────────────────────────────────────────────────
def render_playback(max_abs: int, disabled: bool = False):
    st.markdown('<hr class="panel-divider">', unsafe_allow_html=True)

    c_play, c_back, c_step, c_reset, c_speed, c_pos = st.columns([1, 1, 1, 1, 3, 2])

    with c_play:
        play_label = "⏸ PAUSE" if st.session_state.playing else "▶ PLAY"
        if st.button(play_label, use_container_width=True, type="primary",
                     disabled=disabled):
            new_playing = not st.session_state.playing
            st.session_state.playing = new_playing
            if new_playing:
                st.session_state.last_tick = time.time()
            st.rerun()

    with c_back:
        if st.button("← BACK", use_container_width=True, disabled=disabled):
            st.session_state.playing = False
            if st.session_state.ab_pos > 0:
                st.session_state.ab_pos -= 1
            st.rerun()

    with c_step:
        if st.button("⏭ STEP", use_container_width=True, disabled=disabled):
            st.session_state.playing = False
            if st.session_state.ab_pos < max_abs:
                st.session_state.ab_pos += 1
            st.rerun()

    with c_reset:
        if st.button("↺ RESET", use_container_width=True, disabled=disabled):
            st.session_state.playing = False
            st.session_state.ab_pos = 0
            st.rerun()

    with c_speed:
        st.slider(
            "Speed",
            min_value=1,
            max_value=100,
            value=max(1, min(100, st.session_state.abs_per_sec)),
            step=1,
            label_visibility="collapsed",
            key="abs_per_sec",
        )
        st.markdown(
            '<div class="speed-labels">'
            '<span>SLOW (1/s)</span><span>MED</span><span>FAST (100/s)</span>'
            '</div>',
            unsafe_allow_html=True,
        )

    with c_pos:
        pos = st.session_state.ab_pos
        pct = int(pos / max_abs * 100) if max_abs else 0
        st.markdown(
            f'<div class="pos-display">AB {pos} / {max_abs} &nbsp;·&nbsp; {pct}%</div>',
            unsafe_allow_html=True,
        )
        st.progress(pct / 100)

# ── Auto-advance playback ─────────────────────────────────────────────────────
def maybe_advance(max_abs: int, agg_mode: bool = False):
    if agg_mode:
        return
    if not st.session_state.playing:
        return
    if st.session_state.ab_pos >= max_abs:
        st.session_state.playing = False
        return

    abs_per_sec = max(1, st.session_state.abs_per_sec)
    now = time.time()
    elapsed = now - st.session_state.last_tick
    interval = 1.0 / abs_per_sec   # seconds between each single AB advance

    # How many full AB-intervals have passed since last advance?
    n_advance = int(elapsed / interval)

    if n_advance >= 1:
        st.session_state.ab_pos = min(max_abs, st.session_state.ab_pos + n_advance)
        # Shift last_tick forward by exactly the consumed time to preserve fractional remainder
        st.session_state.last_tick += n_advance * interval
        st.rerun()
    else:
        # Not time yet — sleep until the next interval boundary (cap at 250 ms)
        remaining = interval - elapsed
        time.sleep(max(0.01, min(remaining, 0.25)))
        st.rerun()

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    inject_css()
    init_state()

    # ── Title bar ─────────────────────────────────────────────────────────────
    st.html(
        '<p class="ogun-title">&#9918; OGUN RACE</p>'
        '<p class="ogun-subtitle">Offensive Game Unifying Number</p>'
    )

    # ── Load reference data ───────────────────────────────────────────────────
    try:
        teams   = get_teams()
        players = get_players()
        date_min, date_max = get_date_range()
        avail_dates = get_available_dates()
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.stop()

    if not teams:
        st.info("Database is empty. Load pitch data first.")
        st.stop()

    # ── Shared date/game selector ─────────────────────────────────────────────
    date_mode, start_date, end_date, game_pk, game_home_team, game_away_team = date_game_selector(
        date_min, date_max, avail_dates
    )

    # In SINGLE GAME mode, auto-populate panels with the game's teams
    if date_mode == "SINGLE GAME" and game_pk is not None and game_away_team and game_home_team:
        st.session_state["pA_mode"] = "TEAM"
        st.session_state["pB_mode"] = "TEAM"
        st.session_state["pA_team"] = game_away_team   # Panel 1 = away team
        st.session_state["pB_team"] = game_home_team   # Panel 2 = home team

    # ── Selector controls ─────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### Select Competitors")

    col_p1, col_div, col_p2 = st.columns([5, 0.2, 5])

    with col_p1:
        st.markdown("**Panel 1**")
        modeA, selA = panel_selector("A", teams, players)

    with col_div:
        st.markdown('<div style="border-left:1px solid #21262d;height:80px;margin:auto;width:1px;"></div>',
                    unsafe_allow_html=True)

    with col_p2:
        st.markdown("**Panel 2**")
        modeB, selB = panel_selector("B", teams, players)

    # ── Fetch data for both panels ────────────────────────────────────────────
    try:
        abs_A = fetch_abs(modeA, selA, date_mode, start_date, end_date, game_pk)
        abs_B = fetch_abs(modeB, selB, date_mode, start_date, end_date, game_pk)
    except Exception as e:
        st.error(f"Query error: {e}")
        st.stop()

    max_abs = max(len(abs_A), len(abs_B))

    # Detect selector / date changes so we auto-show the full picture on new data.
    # RESET only zeroes ab_pos without touching data_sig, so it genuinely empties the panels.
    data_sig = f"{modeA}:{selA}:{modeB}:{selB}:{date_mode}:{game_pk}:{start_date}:{end_date}"
    if data_sig != st.session_state.data_sig:
        st.session_state.data_sig = data_sig
        st.session_state.ab_pos = max_abs
        st.session_state.playing = False
    elif st.session_state.ab_pos > max_abs:
        st.session_state.ab_pos = max_abs

    current_idx = st.session_state.ab_pos

    # ── View states (read before rendering panels so xmax can use them) ───────
    viewA = st.session_state.get("pA_view", "ALL CONTACT")
    viewB = st.session_state.get("pB_view", "ALL CONTACT")
    agg_mode = (viewA == "AGGREGATE") or (viewB == "AGGREGATE")

    if agg_mode and st.session_state.playing:
        st.session_state.playing = False

    # ── Shared x-axis scale ───────────────────────────────────────────────────
    def panel_xmax(all_df: pd.DataFrame, view: str, up_to: int) -> float:
        """Compute effective total contact distance for the given view up to position."""
        if all_df.empty:
            return 0.0
        view_df = apply_view_filter(all_df, view)
        if view == "AGGREGATE":
            contact = view_df.dropna(subset=["launch_speed", "hit_distance"])
            contact = contact[contact["hit_distance"] > 0]
            return float(contact["hit_distance"].mean()) * 2 if not contact.empty else 100.0
        if view == "HITS ONLY":
            slice_df = apply_view_filter(all_df.iloc[:up_to], "HITS ONLY")
        else:
            slice_df = all_df.iloc[:up_to] if up_to > 0 else all_df.iloc[0:0]
        contact = slice_df.dropna(subset=["launch_speed", "hit_distance"])
        contact = contact[contact["hit_distance"] > 0]
        return float(contact["hit_distance"].sum()) if not contact.empty else 0.0

    xmax_A = panel_xmax(abs_A, viewA, min(current_idx, len(abs_A)))
    xmax_B = panel_xmax(abs_B, viewB, min(current_idx, len(abs_B)))
    shared_xmax = max(xmax_A, xmax_B, 100.0)

    # ── Panel 1 ───────────────────────────────────────────────────────────────
    st.markdown("---")
    render_panel(
        "A", modeA, selA,
        abs_A,
        min(current_idx, len(abs_A)),
        shared_xmax,
    )

    # ── Panel 2 ───────────────────────────────────────────────────────────────
    st.markdown('<hr class="panel-divider">', unsafe_allow_html=True)
    render_panel(
        "B", modeB, selB,
        abs_B,
        min(current_idx, len(abs_B)),
        shared_xmax,
    )

    # ── At-bat log ────────────────────────────────────────────────────────────
    st.markdown('<hr class="panel-divider">', unsafe_allow_html=True)
    st.markdown("#### At-Bat Log")

    log_col_A, log_col_B = st.columns(2)
    with log_col_A:
        st.markdown(f"**{selA}**")
        st.html(render_ab_log(abs_A, min(current_idx, len(abs_A)), modeA, viewA))
    with log_col_B:
        st.markdown(f"**{selB}**")
        st.html(render_ab_log(abs_B, min(current_idx, len(abs_B)), modeB, viewB))

    # ── Playback controls ─────────────────────────────────────────────────────
    render_playback(max_abs, disabled=agg_mode)

    # ── Auto-advance ──────────────────────────────────────────────────────────
    maybe_advance(max_abs, agg_mode=agg_mode)


if __name__ == "__main__":
    main()
