#!/usr/bin/env python3
"""
load_scrape3.py

Loads scrape_3.csv into the baseball_db PostgreSQL database, then runs
three validation queries:
  1. Row count
  2. All pitchers in the dataset
  3. Average pitch velocity by pitch type (desc)

Usage:
  Set the PGPASSWORD environment variable to your postgres password, or
  edit DB_PASS directly below.
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# ── Connection settings ───────────────────────────────────────────────────────
DB_USER  = "postgres"
DB_PASS  = os.environ.get("PGPASSWORD", "")   # set env var or paste password here
DB_HOST  = "localhost"
DB_PORT  = 5432
DB_NAME  = "baseball_db"
TABLE    = "pitches"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH   = os.path.join(SCRIPT_DIR, "scrape_3.csv")

# Columns to coerce to numeric
NUMERIC_COLS = [
    "game_pk", "inning", "ab_number", "cap_index", "outs",
    "batter", "pitcher", "team_batting_id", "team_fielding_id",
    "strikes", "balls", "pre_strikes", "pre_balls",
    "start_speed", "end_speed",
    "sz_top", "sz_bot", "sz_depth", "sz_width",
    "extension", "plateTime", "zone", "spin_rate",
    "breakX", "inducedBreakZ", "breakZ", "px", "pz",
    "x0", "y0", "z0", "ax", "ay", "az", "vx0", "vy0", "vz0",
    "pfxX", "pfxZ", "pfxZWithGravity", "pfxXWithGravity", "pfxXNoAbs",
    "plateTimeSZDepth", "plateXPoly", "plateYPoly", "plateZPoly",
    "pitch_number", "plate_x", "plate_z",
    "player_total_pitches", "pitcher_pa_number",
    "pitcher_time_thru_order", "game_total_pitches", "index",
    "batSpeed", "hit_speed_round", "hit_speed", "hit_distance",
    "xba", "hit_angle", "is_barrel",
    "hc_x", "hc_x_ft", "hc_y", "hc_y_ft",
    "launch_speed", "launch_angle",
]

# Columns to coerce to boolean (stored as True/False strings in CSV)
BOOL_COLS = [
    "is_strike_swinging", "savantIsInZone", "isInZone", "isSword",
    "is_abs_challenge", "is_last_pitch",
    "runnerOn1B", "runnerOn2B", "runnerOn3B",
]


def load_csv() -> pd.DataFrame:
    print(f"Reading: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8", low_memory=False)
    print(f"  {len(df)} rows  ×  {len(df.columns)} columns")

    # Numeric coercions
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Boolean coercions
    bool_map = {"True": True, "False": False, "true": True, "false": False}
    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = df[col].map(bool_map)

    # game_date as date
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date

    return df


def main():
    if not DB_PASS:
        print("ERROR: No database password found.")
        print("  Option A: set the PGPASSWORD environment variable")
        print("  Option B: paste your password into DB_PASS in this script")
        sys.exit(1)

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        future=True,
    )

    # ── Load ─────────────────────────────────────────────────────────────────
    df = load_csv()

    print(f"\nWriting to {DB_NAME}.{TABLE} (replacing if it exists) ...")
    df.to_sql(
        TABLE,
        engine,
        if_exists="replace",
        index=False,
        chunksize=500,
        method="multi",
    )
    print("  Done.")

    # ── Validation queries ────────────────────────────────────────────────────
    sep = "-" * 60
    print(f"\n{sep}")

    with engine.connect() as conn:

        # 1. Row count
        count = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
        print(f"\n[1] Row count in '{TABLE}': {count}")

        # 2. Sample of pitchers
        print(f"\n[2] Pitchers in the dataset:")
        rows = conn.execute(text(
            f"SELECT DISTINCT pitcher_name, team_fielding "
            f"FROM {TABLE} "
            f"ORDER BY pitcher_name"
        )).fetchall()
        print(f"    {'Pitcher':<30} Team")
        print(f"    {'-'*30} ----")
        for r in rows:
            print(f"    {str(r[0]):<30} {r[1]}")

        # 3. Average velocity by pitch type
        print(f"\n[3] Average pitch velocity by pitch type:")
        rows = conn.execute(text(
            f"SELECT pitch_name, pitch_type, "
            f"  ROUND(AVG(start_speed)::numeric, 1) AS avg_mph, "
            f"  COUNT(*) AS pitches "
            f"FROM {TABLE} "
            f"WHERE start_speed IS NOT NULL "
            f"GROUP BY pitch_name, pitch_type "
            f"ORDER BY avg_mph DESC"
        )).fetchall()
        print(f"    {'Pitch Name':<28} {'Type':<6} {'Avg MPH':>8} {'Count':>7}")
        print(f"    {'-'*28} {'-'*6} {'-'*8} {'-'*7}")
        for r in rows:
            print(f"    {str(r[0]):<28} {str(r[1]):<6} {str(r[2]):>8} {str(r[3]):>7}")

    print(f"\n{sep}")
    print("All validation queries passed.")


if __name__ == "__main__":
    main()
