# Baseball Savant Data Pipeline — Project README
### For Claude Code: Read this first before touching anything.

---

## What This Project Is

An automated daily data pipeline that pulls game data from Baseball Savant (MLB's Statcast platform), combines it into clean CSVs, and ultimately powers a regularly updated website. The goal is one scheduled morning download that captures all completed games from the previous day.

---

## What Has Already Been Done (Do Not Repeat This Work)

### Phase 1 — Discovery (Completed in Claude in Chrome)

We reverse-engineered the Baseball Savant website using browser network inspection. The key finding:

**No browser scraping is needed. Everything is available via two clean JSON API endpoints.**

#### Endpoint 1 — Daily Schedule (get all games for a date):
```
https://baseballsavant.mlb.com/schedule?date=YYYY-M-D
```
Returns every game for the date with game status, team names, gamePk IDs, doubleheader flags, and game numbers. This is the entry point for the pipeline — it tells us which games to process.

#### Endpoint 2 — Full Game Data (one call per game):
```
https://baseballsavant.mlb.com/gf?game_pk=GAMEPK
```
Returns a single large JSON object containing ALL game data for one game:
- Scoreboard / linescore (runs per inning, final score)
- Box score (team and player stats)
- Every individual pitch with full Statcast metrics (velocity, pitch type, spin, location, exit velocity, launch angle, hit distance, etc.)
- Win probability per at-bat
- Lineup data

The three tabs on the Baseball Savant UI (Scoreboard, Box Score, Pitch Velocity) all draw from this same single endpoint. There is no need to click through tabs or scrape rendered HTML.

---

## The Four Files in This Folder

| File | What It Is |
|------|-----------|
| `README.md` | This file — project context and goals |
| `Baseball_Savant_Website.txt` | Original notes on the target website elements (Scoreboard, Box Score, Pitch Velocity tabs, gamePk URL structure) |
| `scrape_1.txt` | Raw JSON captured from the schedule endpoint for 8/20/2025 — minified single line |
| `scrape_2.txt` | Same data as scrape_1 but pretty-printed — use this for readability |

### What the Scrape Files Contain
The scrape files are from `https://baseballsavant.mlb.com/schedule?date=2025-8-20` and contain:
- **`wpa` array** — Win probability added per at-bat, per game, for all 14 games played that day
- **`schedule` object** — Full MLB StatsAPI schedule response including game metadata, linescore, team info, status, venue, series info, and deeply nested player objects

These files are provided so Claude Code can understand the raw data structure and design the extraction/cleaning logic correctly.

---

## Known Data Quality Issues (Already Identified)

The raw API response is extremely verbose — approximately 70% is noise. Key problems:

1. **Player biographical bloat** — Every player object contains birthDate, birthCity, height, weight, gender, draftYear, mlbDebutDate, and more. None of this is needed for game analysis.
2. **Redundant name fields** — Each player has 8+ name format variations (nameFirstLast, fullFMLName, lastInitName, etc.). We only need one — `fullName`.
3. **Live game-state cursors** — Fields like `onDeck`, `inHole` are mid-game position trackers. Meaningless post-game.
4. **API link fields** — Every object has `/api/v1/...` href references. Not needed.
5. **Operational metadata** — Fields like `reverseHomeAwayStatus`, `inningBreakLength`, `tiebreaker`, `recordSource`, `ifNecessaryDescription`, `calendarEventID` are MLB internal ops data, not useful for analysis.
6. **Floating point noise** — Some probability values have excessive decimal precision (e.g., `-8.299999999999997`). Should be rounded.

---

## Edge Cases to Handle (Critical — Do Not Skip)

These were flagged early and must be built in from day one, not added later:

| Situation | How to Handle |
|-----------|--------------|
| **Doubleheader Game 1** | `doubleHeader: "Y"`, `gameNumber: 1` — process normally, tag in CSV |
| **Doubleheader Game 2** | `doubleHeader: "Y"`, `gameNumber: 2` — process normally, tag in CSV |
| **Postponed game** | `status.detailedState: "Postponed"` — log to edge_cases.log, skip, do not error |
| **Cancelled game** | `status.detailedState: "Cancelled"` — log to edge_cases.log, skip, do not error |
| **Rain delay (completed)** | Status will be "Final" — no special handling needed, process normally |
| **No games today** | Schedule returns empty dates array — log and exit cleanly |
| **Partial data / missing fields** | Any field extraction should use `.get()` with a None default — never assume a field exists |

---

## Target Data — What We Want to Keep

The pipeline should produce clean CSVs with (at minimum) the following categories of data. **The exact column schema still needs to be finalized** — this is the next task for Claude Code.

### Game-Level Data (one row per game)
- gamePk, gameDate, homeTeam, awayTeam, venue
- finalScore (home runs, away runs)
- hits and errors (home and away)
- gameStatus (Final / Postponed / etc.)
- doubleHeader flag, gameNumber
- dayNight, seriesDescription

### Inning-Level Data (one row per half-inning)
- gamePk, inning, half (top/bottom)
- runs, hits, errors, leftOnBase

### Pitch-Level Data (one row per pitch — from /gf endpoint)
- gamePk, inning, ab_number, pitch sequence number
- batter_name, pitcher_name, team_batting, team_fielding
- pitch_type, pitch_name, start_speed, end_speed
- call, call_name, description (ball/strike/hit etc.)
- balls, strikes, outs (count before pitch)
- result, events (strikeout / home run / etc.)
- Statcast: exit_velocity, launch_angle, hit_distance
- Plate location: plate_x, plate_z, sz_top, sz_bot
- spin_rate, extension, release_x, release_z (if available)

### Win Probability Data (one row per at-bat)
- gamePk, atBatIndex
- homeTeamWinProbability, awayTeamWinProbability
- homeTeamWinProbabilityAdded (WPA swing)

---

## Custom Metric — OGUN (Offensive Game Unifying Number)

OGUN is a proprietary contact quality metric that unifies three Statcast inputs into a single number representing how well a team or player hit the ball on any given day.

**Formula:**
```
OGUN = (avg_distance / avg_exit_velo) * cos²(|avg_launch_angle - 29|)
```

**Inputs:**
- `hit_distance` — projected distance in feet, normalized for weather and altitude
- `launch_speed` — exit velocity in mph (same measurement as exit velocity)
- `launch_angle` — vertical angle off bat in degrees. Optimum = 29 degrees

**Required filter:**
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

**SQL implementation:**
```sql
ROUND(
    ((AVG(hit_distance) / AVG(launch_speed)) * 
    POWER(COS(RADIANS(ABS(AVG(launch_angle) - 29))), 2))::numeric
, 3) as OGUN
```

**Future — OGUN+:**
OGUN+ will incorporate `batSpeed` as a fourth input for 2024 and beyond. Keep as a separate metric alongside OGUN. Display bat speed as a context column in Streamlit in the meantime. Do not incorporate bat speed into OGUN itself.

---

```
[Morning Scheduler / Cron]
        |
        v
[1. Fetch schedule for yesterday]
  GET /schedule?date=YYYY-M-D
  → Extract list of gamePks with status="Final"
  → Log non-Final games to edge_cases.log
        |
        v
[2. For each gamePk, fetch game data]
  GET /gf?game_pk=GAMEPK
  → Parse JSON
  → Extract pitch data, scoreboard, boxscore, WPA
        |
        v
[3. Clean and flatten data]
  → Apply field whitelist (only keep wanted columns)
  → Round floats
  → Normalize names
        |
        v
[4. Write CSVs]
  → pitches_YYYYMMDD.csv
  → games_YYYYMMDD.csv
  → innings_YYYYMMDD.csv
  → wpa_YYYYMMDD.csv
        |
        v
[5. Website reads latest CSVs and displays]
```

---

## What Claude Code Should Do Next

1. **Read the scrape files** to fully understand the raw JSON structure of both endpoints
2. **Write a Python script** (`fetch_games.py`) that:
   - Accepts a date argument (defaults to yesterday)
   - Hits the schedule endpoint and filters for Final games
   - Loops through gamePks and hits the `/gf` endpoint for each
   - Extracts and flattens only the wanted fields
   - Writes the four CSV types listed above
   - Logs edge cases (postponed, cancelled, doubleheaders) to `edge_cases.log`
3. **Propose and confirm the final column schema** by generating sample CSVs from the existing scrape data before hitting live endpoints
4. **Test against at least one real gamePk** (776661 = Blue Jays vs Pirates, 8/20/2025 is known good)
5. **Do not worry about the website layer yet** — pipeline and clean data first

---

## Tech Stack Preferences
- **Language:** Python 3
- **HTTP:** `requests` library
- **Data:** `pandas` for CSV handling preferred
- **Scheduling:** To be determined (cron, Windows Task Scheduler, or cloud — TBD based on user environment)
- **No browser automation needed** — pure API calls only

---

## Notes and Constraints
- Baseball Savant does not require authentication for these endpoints (as of 8/20/2025 testing)
- The `/gf` endpoint can be large (hundreds of KB per game) — handle accordingly
- The schedule endpoint uses `YYYY-M-D` format (no zero-padding on month/day)
- The WPA data in the schedule response and the pitch data in `/gf` use different `gamePk` formats — schedule uses string, `/gf` uses integer. Normalize to integer.
- Do not hammer the API — add a short delay (1-2 seconds) between `/gf` calls when looping through multiple games
- Season runs roughly April through October — the morning script should handle the off-season gracefully (no games = clean exit, not an error)
