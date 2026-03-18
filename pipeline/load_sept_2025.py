#!/usr/bin/env python3
"""
load_sept_2025.py

Drops and recreates the pitches table in baseball_db, then loads
sept_2025_pitches.csv with explicitly-typed columns — same dtype contract
as load_sept_sample.py.

Rows are sorted by (game_date, game_pk, game_total_pitches) ascending
before loading so chronological order is correct regardless of CSV order.
"""

import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Boolean, Float, Integer, Date, Text

# ── Connection settings ────────────────────────────────────────────────────────
DB_URL  = "postgresql+psycopg2://postgres:Manipura1@localhost:5432/baseball_db"
TABLE   = "pitches"
CSV_PATH = r"C:\OGUN\sept_2025_pitches.csv"

# ── Column type lists (same contract as load_sept_sample.py) ──────────────────
BOOL_COLS = [
    "is_last_pitch",
    "is_barrel",
    "is_strike_swinging",
    "isSword",
    "is_bip_out",
    "is_abs_challenge",
]

FLOAT_COLS = [
    "start_speed", "end_speed", "launch_speed", "launch_angle",
    "hit_distance", "spin_rate", "breakX", "inducedBreakZ",
    "plate_x", "plate_z", "xba", "sz_top", "sz_bot",
    "extension", "plateTime", "batSpeed", "hc_x_ft", "hc_y_ft",
]

INT_COLS = [
    "game_pk", "inning", "ab_number", "pitch_number",
    "batter", "pitcher", "outs", "balls", "strikes",
    "pre_balls", "pre_strikes", "zone",
    "player_total_pitches", "pitcher_pa_number",
    "pitcher_time_thru_order", "team_batting_id", "team_fielding_id",
]

DATE_COL = "game_date"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_bool(s: pd.Series) -> pd.Series:
    mapping = {
        "True": True,  "False": False,
        "true": True,  "false": False,
        "TRUE": True,  "FALSE": False,
        "T": True,     "F": False,
        "Y": True,     "N": False,
        "1": True,     "0": False,
    }
    return s.map(mapping)


def load_csv() -> pd.DataFrame:
    print(f"Reading: {CSV_PATH}")
    df = pd.read_csv(
        CSV_PATH,
        dtype=str,
        encoding="utf-8-sig",
        low_memory=False,
    )
    df.columns = df.columns.str.strip()
    print(f"  {len(df):,} rows  x  {len(df.columns)} columns")

    # Boolean coercions
    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = _to_bool(df[col])

    # Float coercions
    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Integer coercions (nullable Int64 keeps NULLs as NULL)
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Date coercion
    if DATE_COL in df.columns:
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce").dt.date

    # Sort chronologically before loading
    # game_total_pitches is TEXT in PG; coerce to numeric just for sorting
    print("  Sorting by game_date, game_pk, game_total_pitches ...")
    sort_gtp = pd.to_numeric(df["game_total_pitches"], errors="coerce").fillna(0)
    df = df.assign(_sort_gtp=sort_gtp) \
           .sort_values(["game_date", "game_pk", "_sort_gtp"]) \
           .drop(columns=["_sort_gtp"]) \
           .reset_index(drop=True)
    print("  Sort complete.")

    return df


def build_dtype_map(df: pd.DataFrame) -> dict:
    dtype_map: dict = {}
    for col in BOOL_COLS:
        if col in df.columns:
            dtype_map[col] = Boolean()
    for col in FLOAT_COLS:
        if col in df.columns:
            dtype_map[col] = Float()
    for col in INT_COLS:
        if col in df.columns:
            dtype_map[col] = Integer()
    if DATE_COL in df.columns:
        dtype_map[DATE_COL] = Date()
    for col in df.columns:
        if col not in dtype_map:
            dtype_map[col] = Text()
    return dtype_map


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    engine = create_engine(DB_URL, future=True)

    # 1. Load and coerce
    df = load_csv()
    dtype_map = build_dtype_map(df)

    # 2. Drop existing table
    print(f"\nDropping '{TABLE}' table if it exists ...")
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE} CASCADE"))
    print("  Done.")

    # 3. Load with explicit types
    print(f"Writing {len(df):,} rows to baseball_db.{TABLE} ...")
    df.to_sql(
        TABLE,
        engine,
        if_exists="fail",
        index=False,
        chunksize=500,
        method="multi",
        dtype=dtype_map,
    )
    print("  Done.\n")

    # 4. Validation
    sep = "=" * 65
    print(sep)
    print("VALIDATION")
    print(sep)

    with engine.connect() as conn:

        # [1] Row count
        count = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
        print(f"\n[1] Total row count:          {count:,}")

        # [2] Distinct game count
        games = conn.execute(
            text(f"SELECT COUNT(DISTINCT game_pk) FROM {TABLE}")
        ).scalar()
        print(f"[2] Distinct games:           {games:,}")

        # [3] Date range
        row = conn.execute(
            text(f"SELECT MIN(game_date), MAX(game_date) FROM {TABLE}")
        ).fetchone()
        print(f"[3] Date range:               {row[0]}  →  {row[1]}")

        # [4] is_last_pitch boolean check
        ilp = conn.execute(
            text(f"SELECT COUNT(*) FROM {TABLE} WHERE is_last_pitch = TRUE")
        ).scalar()
        print(f"[4] is_last_pitch = TRUE:     {ilp:,}")

        # [5] NULL check on key columns
        print(f"\n[5] NULL check (should be 0):")
        for col in ["game_pk", "game_date", "type", "is_last_pitch"]:
            nulls = conn.execute(
                text(f"SELECT COUNT(*) FROM {TABLE} WHERE {col} IS NULL")
            ).scalar()
            flag = "  OK" if nulls == 0 else "  *** HAS NULLS ***"
            print(f"      {col:<20} {nulls:>6,} NULLs{flag}")

        # [6] Balls in play (launch_speed NOT NULL)
        bip = conn.execute(
            text(f"SELECT COUNT(*) FROM {TABLE} WHERE launch_speed IS NOT NULL")
        ).scalar()
        print(f"\n[6] launch_speed NOT NULL:    {bip:,}  (balls in play)")

        # [7] OGUN query — all teams
        print(f"\n[7] OGUN scores by team (September 2025):")
        ogun_sql = text("""
            SELECT
                team_batting,
                COUNT(*)                                                        AS balls_in_play,
                ROUND(AVG(hit_distance)::numeric, 1)                            AS avg_distance,
                ROUND(AVG(launch_speed)::numeric, 1)                            AS avg_exit_velo,
                ROUND(AVG(launch_angle)::numeric, 1)                            AS avg_launch_angle,
                ROUND(
                    ((AVG(hit_distance) / AVG(launch_speed)) *
                     POWER(COS(RADIANS(ABS(AVG(launch_angle) - 29))), 2))::numeric,
                3)                                                              AS ogun
            FROM pitches
            WHERE type = 'pitch'
              AND launch_speed IS NOT NULL
              AND is_last_pitch = TRUE
            GROUP BY team_batting
            ORDER BY ogun DESC
        """)
        rows = conn.execute(ogun_sql).fetchall()

        hdr = f"  {'Team':<6} {'BIP':>6} {'AvgDist':>8} {'AvgEV':>7} {'AvgLA':>7} {'OGUN':>7}"
        div = "  " + "-" * (len(hdr) - 2)
        print(hdr)
        print(div)
        print("  --- TOP 5 ---")
        for r in rows[:5]:
            print(f"  {r[0]:<6} {r[1]:>6,} {r[2]:>8} {r[3]:>7} {r[4]:>7} {r[5]:>7}")
        print("  ...")
        print("  --- BOTTOM 5 ---")
        for r in rows[-5:]:
            print(f"  {r[0]:<6} {r[1]:>6,} {r[2]:>8} {r[3]:>7} {r[4]:>7} {r[5]:>7}")

    print(f"\n{sep}")
    print("Load and validation complete.")


if __name__ == "__main__":
    main()
