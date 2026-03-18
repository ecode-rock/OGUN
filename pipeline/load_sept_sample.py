#!/usr/bin/env python3
"""
load_sept_sample.py

Drops and recreates the pitches table in baseball_db, then loads
"sept sample.csv" with explicitly-typed columns.

Column type contract
--------------------
BOOLEAN  : is_last_pitch, is_barrel, is_strike_swinging, isSword,
           is_bip_out, is_abs_challenge
FLOAT    : start_speed, end_speed, launch_speed, launch_angle,
           hit_distance, spin_rate, breakX, inducedBreakZ, plate_x,
           plate_z, xba, sz_top, sz_bot, extension, plateTime,
           batSpeed, hc_x_ft, hc_y_ft
INTEGER  : game_pk, inning, ab_number, pitch_number, batter, pitcher,
           outs, balls, strikes, pre_balls, pre_strikes, zone,
           player_total_pitches, pitcher_pa_number,
           pitcher_time_thru_order, team_batting_id, team_fielding_id
DATE     : game_date
TEXT     : everything else

Usage
-----
  Set PGPASSWORD env var or paste password into DB_PASS below, then:
    python load_sept_sample.py
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Boolean, Float, Integer, Date, Text

# ── Connection settings ────────────────────────────────────────────────────────
DB_USER = "postgres"
DB_PASS = os.environ.get("PGPASSWORD", "")   # set env var or paste password
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "baseball_db"
TABLE   = "pitches"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH   = os.path.join(SCRIPT_DIR, "sept sample.csv")

# ── Column type lists ──────────────────────────────────────────────────────────
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


# ── Helpers ────────────────────────────────────────────────────────────────────

def _to_bool(s: pd.Series) -> pd.Series:
    """
    Map common truthy/falsy string variants to Python bool.
    Unrecognised values (including empty strings) become NaN → NULL in PG.
    """
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
        encoding="utf-8-sig",   # handles BOM character if present
        low_memory=False,
    )
    df.columns = df.columns.str.strip()   # remove accidental whitespace
    print(f"  {len(df):,} rows  x  {len(df.columns)} columns")

    # Boolean coercions
    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = _to_bool(df[col])

    # Float coercions  (errors='coerce' → NaN for blanks / bad values)
    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Integer coercions — use nullable Int64 so missing values stay NULL,
    # not coerced to float.
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Date coercion
    if DATE_COL in df.columns:
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce").dt.date

    return df


def build_dtype_map(df: pd.DataFrame) -> dict:
    """
    Return a SQLAlchemy dtype dict for every column in df.
    Typed columns get their declared PG type; everything else gets Text.
    """
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

    # All remaining columns → TEXT (prevents SQLAlchemy from guessing)
    for col in df.columns:
        if col not in dtype_map:
            dtype_map[col] = Text()

    return dtype_map


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    if not DB_PASS:
        print("ERROR: No database password found.")
        print("  Option A: set the PGPASSWORD environment variable")
        print("  Option B: paste your password into DB_PASS in this script")
        sys.exit(1)

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        future=True,
    )

    # ── 1. Load CSV and coerce types ───────────────────────────────────────────
    df = load_csv()
    dtype_map = build_dtype_map(df)

    # ── 2. Drop existing table ─────────────────────────────────────────────────
    print(f"\nDropping '{TABLE}' table if it exists ...")
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE} CASCADE"))
    print("  Done.")

    # ── 3. Create table + load data with explicit types ────────────────────────
    print(f"Writing {len(df):,} rows to {DB_NAME}.{TABLE} ...")
    df.to_sql(
        TABLE,
        engine,
        if_exists="fail",   # table was just dropped; fail loudly if it exists
        index=False,
        chunksize=500,
        method="multi",
        dtype=dtype_map,
    )
    print("  Done.")

    # ── 4. Validation ──────────────────────────────────────────────────────────
    sep = "-" * 62
    print(f"\n{sep}")

    typed_cols = BOOL_COLS + FLOAT_COLS + INT_COLS + [DATE_COL]

    with engine.connect() as conn:

        # Row count
        count = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
        print(f"\n[1] Row count in '{TABLE}': {count:,}")

        # Verify PG column types via information_schema
        print(f"\n[2] PostgreSQL column types for explicitly-typed columns:")
        rows = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name   = :tbl
              AND table_schema = 'public'
              AND column_name  = ANY(:cols)
            ORDER BY ordinal_position
        """), {"tbl": TABLE, "cols": typed_cols}).fetchall()

        print(f"    {'Column':<38} PG Type")
        print(f"    {'-'*38} --------------------")
        for col_name, pg_type in rows:
            print(f"    {col_name:<38} {pg_type}")

        # Non-null counts for key columns
        print(f"\n[3] Non-null row counts for key columns:")
        spot_check = [
            "game_pk", "game_date", "start_speed",
            "is_last_pitch", "is_barrel", "is_bip_out",
        ]
        for col in spot_check:
            n = conn.execute(
                text(f"SELECT COUNT(*) FROM {TABLE} WHERE {col} IS NOT NULL")
            ).scalar()
            print(f"    {col:<30} {n:>8,} non-null")

    print(f"\n{sep}")
    print("Load complete.")


if __name__ == "__main__":
    main()
