#!/usr/bin/env python3
"""
fetch_range.py
Baseball Savant Data Pipeline — Phase 1 through Phase 4

Fetches all games for a date range, cleans data using the confirmed
column whitelist from DATA_FIELD_REFERENCE.docx, sorts by
game_pk / game_total_pitches, and loads into baseball_db on localhost.

Default range: September 1 – September 30, 2025

Usage:
    python fetch_range.py
"""

import json
import logging
import os
import time
from datetime import date, timedelta

import pandas as pd
import requests
from sqlalchemy import create_engine, text

# ── Connection ─────────────────────────────────────────────────────────────────
_FALLBACK_URL = "postgresql://postgres.pepkzpdjebituvxzamfn:PBMlApFSKUMVxJw2@aws-1-ca-central-1.pooler.supabase.com:6543/postgres"
DATABASE_URL  = os.environ.get("DATABASE_URL", _FALLBACK_URL)
TABLE         = "pitches"

# ── Date Range ─────────────────────────────────────────────────────────────────
START_DATE = date(2025, 9, 1)
END_DATE   = date(2025, 9, 30)

# ── API ────────────────────────────────────────────────────────────────────────
SCHEDULE_URL     = "https://baseballsavant.mlb.com/schedule?date={year}-{month}-{day}"
GAME_URL         = "https://baseballsavant.mlb.com/gf?game_pk={game_pk}"
SCHEDULE_DELAY   = 0.5   # seconds between schedule calls
GAME_DELAY       = 1.5   # seconds between /gf calls (be polite)

# ── Column Whitelist ───────────────────────────────────────────────────────────
# Source: DATA_FIELD_REFERENCE.docx — all KEEP + INVESTIGATE + PENDING VERIFY fields.
# CONFIRMED DROP fields are simply absent from this list.
WHITELIST = [
    # ── Group 1: Game Identity ─────────────────────────────────────────────────
    # game_date / home_team / away_team injected from /gf top-level
    "game_pk", "game_date", "home_team", "away_team", "type", "play_id",

    # ── Group 2: At-Bat Context ────────────────────────────────────────────────
    "inning", "ab_number", "cap_index", "outs",
    "batter", "stand", "batter_name",
    "pitcher", "p_throws", "pitcher_name",

    # ── Group 3: Team Identity ─────────────────────────────────────────────────
    "team_batting", "team_fielding", "team_batting_id", "team_fielding_id",

    # ── Group 4: Result Description ────────────────────────────────────────────
    "result", "des", "events", "contextMetrics",

    # ── Group 5: Count / Call ──────────────────────────────────────────────────
    "strikes", "balls", "pre_strikes", "pre_balls",
    "call", "call_name", "pitch_call", "is_strike_swinging",
    "result_code",          # PENDING VERIFY: may equal call

    # ── Group 6: Pitch Identity ────────────────────────────────────────────────
    "pitch_type", "pitch_name", "description",

    # ── Group 7: Velocity ──────────────────────────────────────────────────────
    "start_speed", "end_speed",

    # ── Group 8: Strike Zone (sz_depth / sz_width CONFIRMED DROP — always 17) ─
    "sz_top", "sz_bot",

    # ── Group 9: Pitch Movement ────────────────────────────────────────────────
    "extension", "plateTime", "zone", "spin_rate",
    "breakX", "inducedBreakZ",
    "breakZ",               # PENDING VERIFY: less meaningful than inducedBreakZ
    "px", "pz",             # PENDING VERIFY: may equal plate_x / plate_z
    "pfxX", "pfxZ",         # PENDING VERIFY: older pfx variants
    "pfxZWithGravity", "pfxXWithGravity", "pfxXNoAbs",  # PENDING VERIFY
    "plateTimeSZDepth",     # PENDING VERIFY

    # ── Group 12: Zone / Swing Flags ──────────────────────────────────────────
    "savantIsInZone", "isInZone", "isSword", "is_bip_out", "is_abs_challenge",

    # ── Group 13: Plate Location (preferred over px/pz) ───────────────────────
    "plate_x", "plate_z",

    # ── Group 14: Pitch Counting / Sequencing ─────────────────────────────────
    "pitch_number",
    "player_total_pitches",
    "player_total_pitches_pitch_types",
    "pitcher_pa_number",
    "pitcher_time_thru_order",
    "game_total_pitches",   # PRIMARY SORT KEY (whole int = pitch, decimal = no_pitch)

    # ── Group 16: Batted Ball ─────────────────────────────────────────────────
    "batSpeed",
    "hit_distance",
    "xba",
    "is_barrel",
    "hc_x_ft", "hc_y_ft",
    "hit_speed",            # PENDING VERIFY: may equal launch_speed
    "hit_angle",            # PENDING VERIFY: may equal launch_angle

    # ── Group 17: Launch Data (preferred naming) ───────────────────────────────
    "launch_speed",
    "launch_angle",

    # ── Group 18: Base State ───────────────────────────────────────────────────
    "runnerOn1B", "runnerOn2B", "runnerOn3B",

    # ── Group 19: Pitch Result (is_last_pitch COMPUTED — not from API) ─────────
    "is_last_pitch",

    # ── Doubleheader metadata (injected from schedule endpoint) ───────────────
    "double_header",
    "game_number",
]

# ── Numeric columns (pd.to_numeric coerce) ────────────────────────────────────
NUMERIC_COLS = [
    "game_pk", "inning", "ab_number", "cap_index", "outs",
    "batter", "pitcher", "team_batting_id", "team_fielding_id",
    "strikes", "balls", "pre_strikes", "pre_balls",
    "start_speed", "end_speed", "sz_top", "sz_bot",
    "extension", "plateTime", "zone", "spin_rate",
    "breakX", "inducedBreakZ", "breakZ",
    "px", "pz",
    "pfxX", "pfxZ", "pfxZWithGravity", "pfxXWithGravity", "pfxXNoAbs",
    "plateTimeSZDepth",
    "plate_x", "plate_z",
    "pitch_number",
    "player_total_pitches", "player_total_pitches_pitch_types",
    "pitcher_pa_number", "pitcher_time_thru_order", "game_total_pitches",
    "batSpeed", "hit_distance", "hit_speed", "hit_angle",
    "hc_x_ft", "hc_y_ft",
    "launch_speed", "launch_angle",
    "game_number",
]

# ── Float rounding ─────────────────────────────────────────────────────────────
ROUND_2 = [
    "start_speed", "end_speed", "sz_top", "sz_bot",
    "extension", "plateTime", "spin_rate",
    "breakX", "inducedBreakZ", "breakZ",
    "pfxX", "pfxZ", "pfxZWithGravity", "pfxXWithGravity", "pfxXNoAbs",
    "plateTimeSZDepth",
    "batSpeed", "hit_distance", "hit_speed", "hit_angle",
    "hc_x_ft", "hc_y_ft",
    "launch_speed", "launch_angle",
    "xba",
]
ROUND_4 = ["plate_x", "plate_z", "px", "pz"]

# ── Boolean columns from API (True/False objects) ─────────────────────────────
BOOL_COLS = [
    "is_strike_swinging", "savantIsInZone", "isInZone", "isSword",
    "is_abs_challenge",
]

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler("edge_cases.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — Fetch
# ══════════════════════════════════════════════════════════════════════════════

def date_range(start: date, end: date):
    """Yield every date from start to end inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def fetch_schedule(d: date) -> list[dict]:
    """
    Fetch the MLB schedule for date d.
    Returns a list of dicts for every Final game:
        {game_pk, double_header, game_number}
    Logs non-Final games to edge_cases.log.
    """
    url = SCHEDULE_URL.format(year=d.year, month=d.month, day=d.day)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.error("SCHEDULE_FETCH_ERROR  date=%s  %s", d, exc)
        return []

    # API returns [] (empty list) when there are no games for the date
    if not isinstance(data, dict):
        log.info("NO_GAMES  date=%s  (API returned non-dict response)", d)
        return []

    dates = data.get("schedule", {}).get("dates", [])
    if not dates:
        log.info("NO_GAMES  date=%s", d)
        return []

    games = dates[0].get("games", [])
    if not games:
        log.info("NO_GAMES  date=%s", d)
        return []

    final_games = []
    for g in games:
        game_pk  = g.get("gamePk")
        status   = g.get("status", {}).get("detailedState", "")
        dh       = g.get("doubleHeader", "N")
        gnum     = g.get("gameNumber", 1)
        teams    = g.get("teams", {})
        home_abbr = teams.get("home", {}).get("team", {}).get("abbreviation", "?")
        away_abbr = teams.get("away", {}).get("team", {}).get("abbreviation", "?")
        tag = f"{away_abbr}@{home_abbr}"

        if status == "Final":
            final_games.append({
                "game_pk":       int(game_pk),
                "double_header": dh,
                "game_number":   int(gnum),
                "home_team":     home_abbr,   # pass abbrev so /gf parsing doesn't rely on team_home string
                "away_team":     away_abbr,
            })
        elif status == "Postponed":
            log.warning("POSTPONED   date=%s  game_pk=%s  %s", d, game_pk, tag)
        elif status == "Cancelled":
            log.warning("CANCELLED   date=%s  game_pk=%s  %s", d, game_pk, tag)
        elif "Progress" in status or "Live" in status:
            log.warning("INCOMPLETE  date=%s  game_pk=%s  %s  status=%s", d, game_pk, tag, status)
        # Pre-game / Scheduled / Warmup → silent skip

    return final_games


def fetch_game_pitches(game_meta: dict) -> list[dict]:
    """
    Fetch /gf for one game and return a flat list of pitch-row dicts.
    Injects game_date, home_team, away_team, double_header, game_number
    into every row.
    Skips per-pitcher aggregate items (those lack 'play_id').
    """
    game_pk = game_meta["game_pk"]
    url = GAME_URL.format(game_pk=game_pk)

    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.error("GAME_FETCH_ERROR  game_pk=%s  %s", game_pk, exc)
        return []

    game_date = data.get("game_date", "")

    # NOTE: For September 2025 games, data["team_home"] and data["team_away"] are flat
    # pitch lists, NOT string abbreviations. Use abbreviations from schedule (game_meta).
    home_team = game_meta["home_team"]
    away_team = game_meta["away_team"]

    rows = []
    for side in ("home_pitchers", "away_pitchers"):
        pitcher_dict = data.get(side, {})
        if not isinstance(pitcher_dict, dict):
            continue
        for pitcher_id, pitch_list in pitcher_dict.items():
            if not isinstance(pitch_list, list):
                continue
            for item in pitch_list:
                if not isinstance(item, dict):
                    continue
                # Each real pitch/no_pitch event has a play_id UUID.
                # Skip rows without it (they are per-pitcher summary aggregates).
                if "play_id" not in item:
                    continue

                row = dict(item)
                # Inject game-level metadata not present in pitch rows
                row["game_date"]     = game_date
                row["home_team"]     = home_team
                row["away_team"]     = away_team
                row["double_header"] = game_meta["double_header"]
                row["game_number"]   = game_meta["game_number"]
                # Normalize game_pk to int (API is inconsistent — sometimes string)
                row["game_pk"] = int(row.get("game_pk", game_pk))

                rows.append(row)

    log.info("  game_pk=%-8s  %s@%s  %d rows", game_pk, away_team, home_team, len(rows))
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — Clean
# ══════════════════════════════════════════════════════════════════════════════

def _context_metrics_to_str(val) -> str | None:
    """Convert contextMetrics: empty dict → None, non-empty → JSON string."""
    if val is None:
        return None
    if isinstance(val, dict):
        return None if not val else json.dumps(val)
    s = str(val).strip()
    return None if s in ("", "{}", "None") else s


def compute_is_last_pitch(df: pd.DataFrame) -> pd.Series:
    """
    For each (game_pk, ab_number), the row with the highest game_total_pitches
    is the terminal pitch → is_last_pitch = True.
    no_pitch rows get None (not False) per the reference doc.
    """
    gtp = pd.to_numeric(df["game_total_pitches"], errors="coerce")
    max_gtp = gtp.groupby([df["game_pk"], df["ab_number"]]).transform("max")
    is_last = (gtp == max_gtp)

    # Set None on no_pitch rows
    if "type" in df.columns:
        no_pitch_mask = df["type"].eq("no_pitch")
        is_last = is_last.where(~no_pitch_mask, other=pd.NA)

    return is_last


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the full cleaning pipeline:
      1. Compute is_last_pitch (before column filtering)
      2. Convert contextMetrics {} → NULL
      3. Apply column whitelist
      4. Normalize data types (numeric, boolean, date)
      5. Round floats
    """
    log.info("  Computing is_last_pitch...")
    df["is_last_pitch"] = compute_is_last_pitch(df)

    log.info("  Converting contextMetrics...")
    if "contextMetrics" in df.columns:
        df["contextMetrics"] = df["contextMetrics"].apply(_context_metrics_to_str)

    # ── Whitelist ──────────────────────────────────────────────────────────────
    cols_available = [c for c in WHITELIST if c in df.columns]
    missing = [c for c in WHITELIST if c not in df.columns]
    if missing:
        log.warning("  Whitelist columns absent from data: %s", missing)
    df = df[cols_available].copy()

    # ── Numeric coercions ──────────────────────────────────────────────────────
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # game_pk → non-nullable int (Int64 = nullable integer in pandas)
    if "game_pk" in df.columns:
        df["game_pk"] = df["game_pk"].astype("Int64")

    # game_number → Int64
    if "game_number" in df.columns:
        df["game_number"] = df["game_number"].astype("Int64")

    # game_date → Python date object
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date

    # xba: stored as string like '.200' in API → float
    if "xba" in df.columns:
        df["xba"] = pd.to_numeric(df["xba"], errors="coerce")

    # is_barrel: 0/1 integer in API → keep as Int64 (0=False, 1=True)
    if "is_barrel" in df.columns:
        df["is_barrel"] = pd.to_numeric(df["is_barrel"], errors="coerce").astype("Int64")

    # is_bip_out: 'Y'/'N' string → boolean
    if "is_bip_out" in df.columns:
        df["is_bip_out"] = df["is_bip_out"].map(
            {"Y": True, "N": False, True: True, False: False}
        )

    # Standard boolean cols (True/False objects from API)
    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = df[col].map({True: True, False: False})

    # is_last_pitch: boolean with NA
    if "is_last_pitch" in df.columns:
        df["is_last_pitch"] = df["is_last_pitch"].map(
            {True: True, False: False, pd.NA: None}
        )

    # runnerOn1B/2B/3B: API returns True or None
    # Store as boolean (True = runner present, None/False = no runner)
    for col in ("runnerOn1B", "runnerOn2B", "runnerOn3B"):
        if col in df.columns:
            df[col] = df[col].map({True: True, False: False, None: None})

    # ── Float rounding ─────────────────────────────────────────────────────────
    for col in ROUND_2:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    for col in ROUND_4:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

    return df


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — Sort
# ══════════════════════════════════════════════════════════════════════════════

def sort_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Primary sort: game_pk ASC, game_total_pitches ASC.
    (DATA_FIELD_REFERENCE.docx Section 2 — confirmed method.)
    game_total_pitches: whole integer = real pitch, decimal = no_pitch event.
    """
    return df.sort_values(
        ["game_pk", "game_total_pitches"],
        ascending=True,
        na_position="last",
    ).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4 — Load into PostgreSQL
# ══════════════════════════════════════════════════════════════════════════════

def load_to_postgres(df: pd.DataFrame, engine) -> None:
    """
    Drop + recreate pitches table, load data, create indexes, verify.
    Uses if_exists='replace' for this initial historical batch.
    """
    log.info("  Dropping existing pitches table (if any)...")
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS pitches CASCADE"))
        conn.commit()

    log.info("  Loading %d rows into %s.%s ...", len(df), DB_NAME, TABLE)
    df.to_sql(
        TABLE, engine,
        if_exists="replace",
        index=False,
        chunksize=500,
        method="multi",
    )
    log.info("  Load complete.")

    log.info("  Creating indexes...")
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_game_date   ON pitches(game_date)",
        "CREATE INDEX IF NOT EXISTS idx_game_pk     ON pitches(game_pk)",
        "CREATE INDEX IF NOT EXISTS idx_pitcher     ON pitches(pitcher_name)",
        "CREATE INDEX IF NOT EXISTS idx_batter      ON pitches(batter_name)",
        "CREATE INDEX IF NOT EXISTS idx_team        ON pitches(team_batting)",
        "CREATE INDEX IF NOT EXISTS idx_pitch_type  ON pitches(pitch_type)",
        "CREATE INDEX IF NOT EXISTS idx_type        ON pitches(type)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_play_id ON pitches(play_id)",
    ]
    with engine.connect() as conn:
        for sql in indexes:
            conn.execute(text(sql))
        conn.commit()
    log.info("  Indexes created.")


def verify_load(engine) -> None:
    """Run the three verification queries from PROJECT_STEPS.md Step 4.5."""
    sep = "-" * 60
    print(f"\n{sep}")
    print("[VERIFY] Load verification queries")
    print(sep)

    with engine.connect() as conn:
        # 1. Row count
        count = conn.execute(text("SELECT COUNT(*) FROM pitches")).scalar()
        print(f"\n[1] Row count: {count:,}")

        # 2. Sample pitchers
        rows = conn.execute(text(
            "SELECT DISTINCT pitcher_name, team_fielding "
            "FROM pitches "
            "ORDER BY pitcher_name "
            "LIMIT 20"
        )).fetchall()
        print(f"\n[2] Sample pitchers (first 20):")
        print(f"    {'Pitcher':<30}  Team")
        print(f"    {'─'*30}  ────")
        for r in rows:
            print(f"    {str(r[0]):<30}  {r[1]}")

        # 3. Average velocity by pitch type
        rows = conn.execute(text(
            "SELECT pitch_type, pitch_name, "
            "  ROUND(AVG(start_speed)::numeric, 1) AS avg_mph, "
            "  COUNT(*) AS pitches "
            "FROM pitches "
            "WHERE start_speed IS NOT NULL AND type = 'pitch' "
            "GROUP BY pitch_type, pitch_name "
            "ORDER BY avg_mph DESC"
        )).fetchall()
        print(f"\n[3] Average velocity by pitch type:")
        print(f"    {'Pitch Name':<25}  {'Type':<5}  {'Avg MPH':>7}  {'Count':>7}")
        print(f"    {'─'*25}  {'─'*5}  {'─'*7}  {'─'*7}")
        for r in rows:
            print(f"    {str(r[1]):<25}  {str(r[0]):<5}  {str(r[2]):>7}  {str(r[3]):>7}")

    print(f"\n{sep}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("Baseball Savant Pipeline  |  %s → %s", START_DATE, END_DATE)
    log.info("=" * 60)

    # ── Phase 1: Collect Final game PKs ───────────────────────────────────────
    log.info("PHASE 1 — Fetching schedules...")
    all_games: list[dict] = []

    for d in date_range(START_DATE, END_DATE):
        log.info("  Schedule: %s", d)
        games = fetch_schedule(d)
        all_games.extend(games)
        time.sleep(SCHEDULE_DELAY)

    log.info("Total Final games found: %d", len(all_games))

    if not all_games:
        log.info("No Final games in range. Exiting.")
        return

    # ── Phase 1 cont: Fetch pitch data for each game ──────────────────────────
    log.info("PHASE 1 (cont) — Fetching game data...")
    all_rows: list[dict] = []

    for i, game_meta in enumerate(all_games, 1):
        log.info("[%d/%d] Fetching game_pk=%s", i, len(all_games), game_meta["game_pk"])
        rows = fetch_game_pitches(game_meta)
        all_rows.extend(rows)
        time.sleep(GAME_DELAY)

    log.info("Total raw pitch rows: %d", len(all_rows))

    if not all_rows:
        log.info("No pitch data collected. Exiting.")
        return

    df = pd.DataFrame(all_rows)
    log.info("DataFrame shape before cleaning: %s", df.shape)

    # ── Phase 2: Clean ────────────────────────────────────────────────────────
    log.info("PHASE 2 — Cleaning data...")
    df = clean_dataframe(df)
    log.info("DataFrame shape after cleaning: %s", df.shape)

    # ── Phase 3: Sort ─────────────────────────────────────────────────────────
    log.info("PHASE 3 — Sorting by game_pk, game_total_pitches...")
    df = sort_dataframe(df)
    log.info("Sort complete. Final shape: %s", df.shape)

    # ── Phase 4: Load into PostgreSQL ─────────────────────────────────────────
    log.info("PHASE 4 — Connecting to PostgreSQL...")
    engine = create_engine(
        DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://"),
        future=True,
    )

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info("  Database connection OK.")

    load_to_postgres(df, engine)
    verify_load(engine)

    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
