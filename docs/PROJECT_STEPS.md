# Baseball Savant Pipeline — Granular Project Steps
### Current Status: PostgreSQL running locally, scrape_3.csv loaded, single game tested

---

## PHASE 1 — Expand the Sample Data

### Step 1.1 — Define the Date Range
- Confirmed date range: **September 1, 2025 to September 30, 2025**
- Expected volume: approximately 800-900 games, 75,000-80,000 pitches
- API date format required: `YYYY-M-D` with no zero padding — so September 1 = `2025-9-1` not `2025-09-01`
- At 1.5 second delay between game fetches, expect the full month fetch to take approximately 20-25 minutes to complete

### Step 1.2 — Write the Date Range Fetcher in Claude Code
- Claude Code writes a Python script called `fetch_range.py`
- Script accepts `start_date` and `end_date` as arguments
- Script generates every date in the range as a list
- For each date, calls `https://baseballsavant.mlb.com/schedule?date=YYYY-M-D`
- Parses the `schedule.dates[0].games` array
- Extracts `gamePk`, `status.detailedState`, `doubleHeader`, `gameNumber` for every game

### Step 1.3 — Filter Games by Status
- Keep only games where `status.detailedState == "Final"`
- Log all non-Final games to `edge_cases.log` with date, gamePk, teams, and reason
  - Postponed → log as POSTPONED
  - Cancelled → log as CANCELLED
  - In Progress → log as INCOMPLETE (game may not be done yet)
- Result is a clean list of gamePks that are safe to process

### Step 1.4 — Fetch Game Data for Each gamePk
- For each valid gamePk, call `https://baseballsavant.mlb.com/gf?game_pk=GAMEPK`
- Add a 1.5 second delay between each call to avoid hammering the server
- Parse the returned JSON
- Extract pitch-level data from `home_pitchers` and `away_pitchers` arrays
- Extract game metadata from `scoreboard` and `boxscore` objects

### Step 1.5 — Handle Doubleheaders
- When `doubleHeader == "Y"`, both `gameNumber: 1` and `gameNumber: 2` will appear
- Process both games normally
- Tag every row in the dataset with `game_number` column (value 1 or 2)
- This allows filtering doubleheader games separately later if needed

---

## PHASE 2 — Data Cleaning Block

### Step 2.1 — Combine All Game Data into One DataFrame
- As each game is fetched and parsed, append its rows to a master DataFrame
- All games from all dates accumulate into one structure before any cleaning

### Step 2.2 — Drop Unwanted Columns
- Keep only the columns on the confirmed whitelist below — drop everything else
- This is one pandas operation: `df = df[columns_to_keep]`
- After loading, run a NULL-safe verification check: confirm `hit_speed == launch_speed` and `hit_angle == launch_angle`. If confirmed identical, drop `hit_speed` and `hit_angle` — keep only `launch_speed` and `launch_angle`

**Confirmed Column Whitelist (63 fields):**
```
game_pk, game_date, home_team, away_team, type, play_id,
inning, ab_number, outs, batter, stand, batter_name, pitcher, pitcher_name,
team_batting, team_fielding, team_batting_id, team_fielding_id,
result, des, events,
strikes, balls, pre_strikes, pre_balls, call, pitch_call, is_strike_swinging,
pitch_type, pitch_name, description,
start_speed, end_speed,
sz_top, sz_bot,
extension, plateTime, zone, spin_rate, breakX, inducedBreakZ,
savantIsInZone, isInZone, isSword, is_bip_out, is_abs_challenge,
plate_x, plate_z,
pitch_number, player_total_pitches, player_total_pitches_pitch_types,
pitcher_pa_number, pitcher_time_thru_order, game_total_pitches,
batSpeed, hit_distance, xba, is_barrel, hc_x_ft, hc_y_ft,
hit_speed, hit_angle, launch_speed, launch_angle,
runnerOn1B, runnerOn2B, runnerOn3B,
is_last_pitch
```

### Step 2.3 — Sort the Data
- Sort by exactly two columns in this order:
  1. `game_pk` ascending — groups all pitches from the same game together
  2. `game_total_pitches` ascending — correct chronological sequence within each game
- This is one pandas operation: `df.sort_values(['game_pk', 'game_total_pitches'])`
- NOTE: `game_total_pitches` is a whole integer for real pitches and a decimal (e.g. 88.02) for non-pitch events such as pitch clock violations. This single field correctly sequences everything in true game order without needing inning, ab_number, or pitch_number as sort keys

### Step 2.4 — Handle NULL Values
- Do NOT replace NULLs with 0 — 0 is a valid value for many baseball fields
- Leave non-contact pitch rows as NULL for hit-related columns
- This is critical for the custom metric calculation accuracy
- Specifically: `launch_speed`, `launch_angle`, `hit_distance`, `xba`, `is_barrel` stay NULL on non-contact pitches

### Step 2.5 — Round Floating Point Values
- Several columns have excessive decimal precision (e.g., `-8.299999999999997`)
- Round to 2 decimal places for: `start_speed`, `end_speed`, `hit_distance`, `launch_angle`, `launch_speed`, `xba`, `breakX`, `inducedBreakZ`, `spin_rate`, `extension`
- Round to 4 decimal places for plate location columns: `plate_x`, `plate_z`, `sz_top`, `sz_bot`

### Step 2.6 — Normalize Data Types
- Ensure `game_pk` is stored as integer throughout (API returns it as both string and integer inconsistently)
- Ensure `game_date` is stored as a proper date type, not a string
- Ensure boolean columns (`is_barrel`, `is_strike_swinging`, etc.) are stored as boolean not string

---

## PHASE 3 — Custom Metric Column

### The Metric: OGUN — Offensive Game Unifying Number

OGUN is a proprietary contact quality metric that combines three Statcast inputs into a single number representing how well a team or player hit the ball. Higher is better.

**Formula:**
```
OGUN = (avg_distance / avg_exit_velo) * cos²(|avg_launch_angle - 29|)
```

**Inputs:**
- `hit_distance` — projected distance in feet, normalized for weather and altitude
- `launch_speed` — exit velocity in mph
- `launch_angle` — vertical angle off bat in degrees. Optimum = 29 degrees

**Required filter before calculation:**
```sql
WHERE type = 'pitch' AND launch_speed IS NOT NULL AND is_last_pitch = TRUE
```

**Interpretation scale:**
| Score | Contact Quality |
|-------|----------------|
| Above 2.0 | Elite |
| 1.75 — 2.0 | Above average |
| 1.50 — 1.75 | Below average |
| Below 1.50 | Poor |

**How it works:**
- `avg_distance / avg_exit_velo` measures contact efficiency — feet of distance per unit of exit velocity. Rewards good launch angles implicitly.
- `cos²(|avg_launch_angle - 29|)` is a multiplier that equals 1.0 at exactly 29 degrees and shrinks proportionally as the average launch angle moves away from optimum in either direction. Squaring the cosine amplifies the penalty for poor launch angles.

**SQL implementation:**
```sql
ROUND(
    ((AVG(hit_distance) / AVG(launch_speed)) * 
    POWER(COS(RADIANS(ABS(AVG(launch_angle) - 29))), 2))::numeric
, 3) as OGUN
```

**Future metric — OGUN+:**
OGUN+ will incorporate bat speed (`batSpeed`) as a fourth input for 2024 and beyond when that data is consistently populated. Keep as a separate metric — do not replace OGUN. OGUN remains the historically consistent baseline. OGUN+ is the modern enhanced version. bat speed should be displayed as a context column in Streamlit alongside OGUN scores in the meantime.

### Step 3.1 — Filter to Balls in Play Only
- Create a filtered subset of rows where the ball was actually put in play
- Condition: `launch_speed` IS NOT NULL AND `hit_distance` IS NOT NULL
- This naturally excludes strikeouts, walks, hit by pitch, and all non-contact pitches
- Do not modify the main DataFrame — work on the subset for calculation

### Step 3.2 — Apply the OGUN Formula
- Implement OGUN as a standalone Python function so it can be easily modified
- Function signature: `def calculate_ogun(avg_distance, avg_exit_velo, avg_launch_angle, optimum_angle=29)`
- Rows where ball was not in play get NULL in the OGUN column, not 0
- Calculate at display time in Streamlit during development phase
- When validated against a full season of data, move calculation to pipeline and store in database

### Step 3.3 — Keep the Formula Modifiable
- OGUN is defined but still experimental at the team aggregation level
- Write as a standalone Python function — changing the formula means editing one function
- The optimum launch angle of 29 degrees should be a parameter, not hardcoded

---

## PHASE 4 — Load into PostgreSQL

### Step 4.1 — Confirm Database Connection
- Verify PostgreSQL service is running (should auto-start on Windows boot)
- Connection string: `postgresql://postgres:YOURPASSWORD@localhost:5432/baseball_db`
- Test connection before attempting to load data

### Step 4.2 — Create the Pitches Table
- Claude Code writes the `CREATE TABLE` SQL statement based on the confirmed whitelist
- Set appropriate data types for each column (INTEGER, FLOAT, VARCHAR, BOOLEAN, DATE)
- Use `play_id` as the unique key to prevent duplicate rows — it is a UUID that uniquely identifies every pitch across all of MLB
- This means re-running the pipeline on the same date range will not create duplicate rows
- Confirm PostgreSQL database uses UTF-8 encoding before creating the table — required for accented player names

### Step 4.3 — Load the Cleaned DataFrame into PostgreSQL
- Use `df.to_sql('pitches', engine, if_exists='append', index=False)`
- `if_exists='append'` means new data adds to existing data rather than replacing it
- This is how the daily pipeline will work in production — always append, never overwrite

### Step 4.4 — Create Indexes
- After loading, create indexes on the columns that will be used for filtering in the web app
- At minimum:
  ```sql
  CREATE INDEX idx_game_date ON pitches(game_date);
  CREATE INDEX idx_pitcher ON pitches(pitcher_name);
  CREATE INDEX idx_batter ON pitches(batter_name);
  CREATE INDEX idx_team ON pitches(team_batting);
  CREATE INDEX idx_pitch_type ON pitches(pitch_type);
  ```
- Indexes make filter queries feel instant even with hundreds of thousands of rows

### Step 4.5 — Verify the Load
- Run row count query: `SELECT COUNT(*) FROM pitches;`
- Run sample query: `SELECT DISTINCT pitcher_name FROM pitches LIMIT 20;`
- Run aggregation test: `SELECT pitch_type, AVG(start_speed) FROM pitches GROUP BY pitch_type;`
- All three should return sensible results

---

## PHASE 5 — Build the Streamlit App

### Step 5.1 — Create the App File
- Claude Code creates `app.py` — this is the entire website in one file
- Streamlit app connects to the same PostgreSQL database using the same connection string

### Step 5.2 — Build the Date Filter
- Dropdown or date picker allowing user to select a date or date range
- Queries the database for games played on selected date(s)

### Step 5.3 — Build the Game Filter
- Once date is selected, show a dropdown of games played that day
- Displayed as "Away Team vs Home Team" for readability
- Selecting a game filters all subsequent views to that game

### Step 5.4 — Build the Team Filter
- Dropdown to filter by team within the selected game or across all games

### Step 5.5 — Build the Player Filter
- Dropdown to filter by individual pitcher or batter
- Populated dynamically based on game and team selection

### Step 5.6 — Display the Results Table
- Show filtered pitch data in a clean table
- Include the custom metric column

### Step 5.7 — Display OGUN
- Calculate OGUN at display time in Streamlit using the confirmed formula
- Show OGUN score per team alongside avg_distance, avg_exit_velo, avg_launch_angle, and balls_in_play
- Display bat speed as a context column alongside OGUN — do not incorporate into formula yet
- Color code or rank teams by OGUN score using the interpretation scale: Elite (2.0+), Above Average (1.75-2.0), Below Average (1.50-1.75), Poor (below 1.50)
- This is where OGUN gets validated against real game results

### Step 5.8 — Run the App Locally
- Command: `streamlit run app.py`
- Opens automatically in browser at `http://localhost:8501`
- This is your local working demo

---

## PHASE 6 — Polling Test (Live Game Scrape)

### Step 6.1 — Write the Polling Script
- Claude Code writes `poll_games.py` — a script that runs on a loop
- Configurable interval (start with every 15 minutes for testing)
- On each cycle: fetch today's schedule, check game statuses, fetch any newly Final games

### Step 6.2 — Status Check Logic
- For each game in today's schedule:
  - If `status == "Final"` AND not already in database → fetch and load
  - If `status == "Final"` AND already in database → skip (duplicate prevention)
  - If `status == "In Progress"` → skip, log, check again next cycle
  - If `status == "Postponed"` or `"Cancelled"` → log to edge_cases.log, stop checking

### Step 6.3 — Run the Test
- Start the polling script during a live game day
- Watch `edge_cases.log` and console output to confirm it's behaving correctly
- Confirm that completed games appear in the database after the next polling cycle
- Confirm that in-progress games are skipped cleanly

### Step 6.4 — Confirm Streamlit Updates
- With the polling script running and app.py open in browser
- Complete a game → polling script fetches it → refresh Streamlit → new data appears
- This proves the end-to-end pipeline works in real time

---

## PHASE 7 — Git Version Control (Local)

### Step 7.1 — Initialize a Local Git Repository
- In your project folder, run: `git init`
- This creates a local version history — no GitHub account needed yet

### Step 7.2 — Create a .gitignore File
- Claude Code creates this file
- It tells Git what NOT to track:
  - Your PostgreSQL password / connection string
  - Large raw data files
  - Python cache files
  - Any local config files with credentials

### Step 7.3 — Make the First Commit
- `git add .`
- `git commit -m "Initial pipeline — fetch, clean, load, Streamlit demo"`
- From this point forward, every meaningful change gets its own commit
- If something breaks, you can roll back to any previous commit

---

## PHASE 8 — GitHub and Deployment (When Ready)

### Step 8.1 — Create a GitHub Repository
- Create a free account at github.com if not already done
- Create a new repository called `baseball-savant-pipeline` (or similar)
- Push your local repository to GitHub

### Step 8.2 — Choose a Hosting Platform
- Recommended starting point: **Supabase** for the database + **Streamlit Cloud** for the app
- Both have free tiers sufficient for this project at this stage
- Both connect directly to GitHub

### Step 8.3 — Migrate the Database to Supabase
- Export local PostgreSQL data
- Create a new PostgreSQL database on Supabase
- Import data to Supabase
- Change one line in all scripts: update the connection string from localhost to Supabase URL

### Step 8.4 — Deploy the Streamlit App
- Connect Streamlit Cloud to your GitHub repository
- Point it at `app.py`
- Set the database connection string as a secret environment variable (never hardcode passwords)
- Streamlit Cloud auto-deploys every time you push a new commit to GitHub

### Step 8.5 — Deploy the Pipeline Script
- The morning download script needs to run on a schedule in the cloud
- Options: GitHub Actions (free, runs on a cron schedule), Railway, or Render
- Configure it to run once daily at your chosen morning time

### Step 8.6 — Test the Live Deployment
- Confirm the website is accessible at the public Streamlit URL
- Confirm the pipeline runs on schedule and new game data appears on the site
- Confirm edge cases (doubleheaders, postponed games) are logged correctly in production

---

## Decisions Still Pending

| Decision | Status |
|----------|--------|
| Column whitelist | **CONFIRMED — 63 fields, see Step 2.2** |
| Custom metric formula finalized | **CONFIRMED — OGUN formula. See Phase 3** |
| Custom metric column name | **CONFIRMED — OGUN (Offensive Game Unifying Number)** |
| Date range for historical sample | **CONFIRMED — September 1 to September 30, 2025** |
| Cloud hosting platform confirmed | Supabase + Streamlit Cloud recommended |
| Number of seasons to store | To be decided |

---

## Reference — Key API Endpoints

| Purpose | URL |
|---------|-----|
| Get all games for a date | `https://baseballsavant.mlb.com/schedule?date=YYYY-M-D` |
| Get all data for one game | `https://baseballsavant.mlb.com/gf?game_pk=GAMEPK` |
| Known good test game | `https://baseballsavant.mlb.com/gf?game_pk=776661` (Blue Jays vs Pirates, 8/20/2025) |

---

## Reference — Tech Stack Decisions Made

| Layer | Tool | Decision |
|-------|------|----------|
| Data fetch | Python requests | Confirmed |
| Data cleaning | Python pandas | Confirmed |
| Database | PostgreSQL | Confirmed, running locally |
| Custom metric | Python (Streamlit display time for now) | Experimental phase |
| Web app | Streamlit | Path A confirmed |
| Version control | Git → GitHub | Local first, GitHub when ready |
| Cloud database | Supabase | Recommended, not yet implemented |
| Cloud hosting | Streamlit Cloud | Recommended, not yet implemented |
| Scheduling | GitHub Actions or similar | To be decided at deployment |
