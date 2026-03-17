import time
import requests
import sqlite3
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import queue
from datetime import date
from typing import List, Optional
import argparse
import sys
from tqdm import tqdm
# Use a workspace-local DB at BigDB/data/DB/highscore.db
DB_DIR = Path(__file__).resolve().parent / "data" / "DB"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "highscore.db"


def get_sqlite_conn():
    # allow other threads/processes to read while writing using WAL and give
    # a generous busy timeout so brief locks don't fail operations
    conn = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA busy_timeout = 30000;")
    except Exception:
        pass
    return conn


API_AIRPORTS = "https://api.skycards.oldapes.com/airports"
API_HIGHSCORE_TEMPLATE = "https://api.skycards.oldapes.com/highscore/airport/{airport_id}"

# Token from previous script — replace with env var or secure store as needed
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIwMTljMmVmMi0xMDEyLTcwNjEtYWUzNS0wNjlmMThhZTQyYjIiLCJqdGkiOiJIaVdiZlVFM0lnMjBONkhqYnVWT2pfeGFZa2VlNDRaSVZHcFpXX2htSGVNIiwiaWF0IjoxNzcwODI1MzM3LCJleHAiOjQ5MjY1ODUzMzd9.-fOLPEtuvKqOuLnk2EZT8f_Lf-ymMIp4_vjnVgqzNEo"

HEADERS = {
    "Authorization": f"Bearer {token}",
    "accept": "application/json",
    "accept-encoding": "gzip",
    "content-type": "application/json",
    "Host": "api.skycards.oldapes.com",
    "User-Agent": "okhttp/4.12.0",
    "x-client-version": "2.0.24",
}


def fetch_airports():
    resp = requests.get(API_AIRPORTS, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # API may return {"rows": [...] } or a bare list
    airports = []
    if isinstance(data, dict):
        airports = data.get("rows") or data.get("airports") or []
    elif isinstance(data, list):
        airports = data

    # Normalize to contain 'id' as airport id and keep iata/icao if present
    normalized = []
    for a in airports:
        aid = a.get("id") or a.get("airportID")
        if aid is None:
            # try nested
            aid = a.get("airportId") or a.get("airport_id")
        if aid is not None:
            a["airportID"] = int(aid)
            normalized.append(a)

    return normalized
def weeks_since_start_of_year():
    today = date.today()
    year = today.year
    last_week = today.isocalendar()[1]
    return [f"{year}{w:02d}" for w in range(1, last_week + 1)]
def get_last_n_weeks(n: int = 3):
    """Return list of the last n ISO weeks as strings 'YYYYWW', oldest-first.
    Handles year boundaries."""
    today = date.today()
    cyear, cweek, _ = today.isocalendar()
    weeks = []
    year = cyear
    week = cweek
    for i in range(n - 1, -1, -1):
        w = week - i
        y = year
        # adjust for year wrap-around
        while w <= 0:
            y -= 1
            # last ISO week of previous year
            last_w_prev = date(y, 12, 28).isocalendar()[1]
            w += last_w_prev
        # if w exceeds last week of year, roll forward (unlikely here)
        last_w = date(y, 12, 28).isocalendar()[1]
        if w > last_w:
            w = last_w
        weeks.append(f"{y}{w:02d}")
    return weeks


def ensure_table(conn):
    # Primary key is only userId as requested (this will dedupe across airports/weeks)
    desired_cols = ["userId", "userName", "userXP"]
    cur = conn.cursor()

    # If table doesn't exist, create it
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='airport_highscore'")
    if not cur.fetchone():
        cur.executescript(
            """
            CREATE TABLE airport_highscore (
                userId TEXT PRIMARY KEY,
                userName TEXT,
                userXP INTEGER,
                aircraftCount INTEGER,
                destinations INTEGER,
                battleWins INTEGER
            )
            """
        )
        conn.commit()
        cur.close()
        # ensure achievement columns exist on the user table
        try:
            ensure_achievement_columns(conn)
        except Exception:
            pass
        try:
            drop_legacy_ach_tables(conn)
        except Exception:
            pass
        return

    # Table exists: inspect columns
    cur.execute("PRAGMA table_info(airport_highscore)")
    existing_info = cur.fetchall()
    existing_cols = [r[1] for r in existing_info]

    # If all desired columns present, ensure stat columns exist; add missing
    if set(desired_cols).issubset(set(existing_cols)):
        stat_cols = {
            "aircraftCount": "INTEGER",
            "destinations": "INTEGER",
            "battleWins": "INTEGER",
        }
        for col, ctype in stat_cols.items():
            if col not in existing_cols:
                cur.execute(f"ALTER TABLE airport_highscore ADD COLUMN {col} {ctype}")
        conn.commit()
        # ensure achievements tables exist for existing DBs too
        try:
            ensure_achievement_columns(conn)
        except Exception:
            pass
        cur.close()
        return

    print("Migrating airport_highscore schema to include missing columns...")

    # Create a new table with desired schema
    cur.executescript(
        """
        CREATE TABLE airport_highscore_new (
            userId TEXT PRIMARY KEY,
            userName TEXT,
            userXP INTEGER,
            aircraftCount INTEGER,
            destinations INTEGER,
            battleWins INTEGER
        )
        """
    )

    # Copy rows from old table into new table, mapping available columns
    select_cols = existing_cols
    col_placeholders = ",".join([f'"{c}"' for c in select_cols])
    cur.execute(f"SELECT {col_placeholders} FROM airport_highscore")

    rows = cur.fetchall()
    insert_sql = "INSERT OR REPLACE INTO airport_highscore_new (userId, userName, userXP, aircraftCount, destinations, battleWins) VALUES (?, ?, ?, ?, ?, ?)"

    for r in rows:
        row_dict = {col: r[idx] for idx, col in enumerate(select_cols)}
        vals = [
            row_dict.get("userId") or row_dict.get("userid") or None,
            row_dict.get("userName") or row_dict.get("username") or None,
            row_dict.get("userXP") or row_dict.get("userxp") or row_dict.get("xp") or None,
            row_dict.get("aircraftCount") or row_dict.get("aircraft_count") or row_dict.get("numAircraftModels") or None,
            row_dict.get("destinations") or row_dict.get("destinationsCount") or row_dict.get("numDestinations") or None,
            row_dict.get("battleWins") or row_dict.get("battlesWon") or row_dict.get("numBattleWins") or None,
        ]
        try:
            cur.execute(insert_sql, vals)
        except Exception as e:
            # If insert fails, try to salvage by inserting minimal data
            try:
                cur.execute(insert_sql, [vals[0], vals[1], vals[2] or 0, vals[3] or None, vals[4] or None, vals[5] or None])
            except Exception:
                pass

    conn.commit()

    # Drop old table and rename new
    cur.executescript(
        """
        DROP TABLE airport_highscore;
        ALTER TABLE airport_highscore_new RENAME TO airport_highscore;
        """
    )
    conn.commit()
    # ensure achievement columns exist after migration
    try:
        ensure_achievement_columns(conn)
    except Exception:
        pass
    try:
        drop_legacy_ach_tables(conn)
    except Exception:
        pass
    cur.close()


def ensure_ach_tables(conn):
    """No-op: canonical achievements tables removed by user request."""
    return


def drop_legacy_ach_tables(conn):
    """Remove legacy achievements tables if present."""
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS user_achievements")
        cur.execute("DROP TABLE IF EXISTS achievements")
        conn.commit()
        cur.close()
    except Exception:
        pass



def seed_default_achievements(conn, total=35):
    """Ensure there are at least `total` achievements in the achievements table.
    If the API provides real achievements they will be inserted elsewhere; this
    seeds placeholders so the site always shows 35 trophies.
    """
    # canonical achievements table removed; no-op
    return


def achievement_keys(total=35):
    """Return canonical achievement keys for columns."""
    return [f"trophy_{i:02d}" for i in range(1, total + 1)]


def ensure_achievement_columns(conn, total=35):
    """Ensure achievement columns exist for canonical achievement keys.

    Do NOT create trophy_XX placeholders. Columns are only created for
    canonical achievements present in the `achievements` table or when a
    explicit set of keys is provided by callers.
    """
    cur = conn.cursor()
    # gather keys from achievements table
    keys = set()
    try:
        cur.execute("SELECT ach_key FROM achievements")
        for r in cur.fetchall():
            if not r or not r[0]:
                continue
            raw = str(r[0])
            # normalize to safe column name (alnum and underscore)
            col = raw if raw.isidentifier() else ''.join(c if c.isalnum() or c == '_' else '_' for c in raw)
            if col == 'trophy_nb':
                continue
            keys.add(col)
    except Exception:
        # no achievements table yet; nothing to create
        cur.close()
        return

    if not keys:
        cur.close()
        return

    # check existing columns and add any missing
    cur.execute("PRAGMA table_info(airport_highscore)")
    cols = {r[1] for r in cur.fetchall()}

    for k in sorted(keys):
        if k not in cols:
            try:
                cur.execute(f'ALTER TABLE airport_highscore ADD COLUMN "{k}" BOOLEAN DEFAULT 0')
            except Exception:
                pass
    conn.commit()
    cur.close()


def writer_thread_fn(in_q: "queue.Queue", stop_event: threading.Event):
    conn = get_sqlite_conn()
    ensure_table(conn)
    # canonical achievements tables removed — we operate directly on airport_highscore columns
    cur = conn.cursor()
    batch = []
    BATCH_SIZE = 200

    while not stop_event.is_set() or not in_q.empty():
        try:
            item = in_q.get(timeout=1)
        except queue.Empty:
            item = None

        if item is None:
            # timeout handling: flush if batch has items
            if batch:
                cur.executemany(
                    "INSERT INTO airport_highscore (userId, userName, userXP, aircraftCount, destinations, battleWins) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(userId) DO UPDATE SET userName=excluded.userName, userXP=excluded.userXP, aircraftCount=excluded.aircraftCount, destinations=excluded.destinations, battleWins=excluded.battleWins;",
                    batch,
                )
                conn.commit()
                batch = []
            continue

        if item == "__SENTINEL__":
            break
        # item can be one of:
        #  - tuple (userId,userName,userXP,aircraftCount,destinations,battleWins)
        #  - ('__ACH__', userId, {col:val, ...})
        if isinstance(item, tuple) and len(item) == 6:
            batch.append(item)
            if len(batch) >= BATCH_SIZE:
                    try:
                        cur.executemany(
                            "INSERT INTO airport_highscore (userId, userName, userXP, aircraftCount, destinations, battleWins) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(userId) DO UPDATE SET userName=excluded.userName, userXP=excluded.userXP, aircraftCount=excluded.aircraftCount, destinations=excluded.destinations, battleWins=excluded.battleWins;",
                            batch,
                        )
                        conn.commit()
                    except Exception as e:
                        print("DB write error:", e)
                    batch = []
        elif isinstance(item, tuple) and len(item) == 3 and item[0] == '__ACHV2__':
            # new achievement update format: ('__ACHV2__', userId, [(raw_key, bool), ...])
            try:
                _tag, uid, items = item
                # operate directly on airport_highscore columns (no canonical tables)
                # For each item, create/update a boolean column on airport_highscore
                # using the achievement id as the column name (sanitized).
                try:
                    # get current columns once
                    cur.execute("PRAGMA table_info(airport_highscore)")
                    existing_cols = {r[1] for r in cur.fetchall()}
                except Exception:
                    existing_cols = set()

                for raw_key, raw_val in items:
                    try:
                        ach_key = str(raw_key)
                        # choose display name/title when available
                        ach_name = None
                        if isinstance(raw_val, dict):
                            for f in ('title', 'name'):
                                if f in raw_val and raw_val.get(f):
                                    ach_name = str(raw_val.get(f))
                                    break

                        # canonical achievements table removed — skip inserting metadata

                        # determine owned value: prefer explicit isAchieved-like keys
                        owned_val = None
                        if isinstance(raw_val, dict):
                            for cand in ('isAchieved', 'is_achieved', 'isachieved', 'achieved', 'owned'):
                                if cand in raw_val:
                                    owned_val = raw_val.get(cand)
                                    break
                            if owned_val is None:
                                # case-insensitive search
                                for k, v in raw_val.items():
                                    if k.lower() in ('isachieved', 'is_achieved', 'achieved', 'owned'):
                                        owned_val = v
                                        break
                            # if no explicit flag, assume present dict means achieved
                            if owned_val is None:
                                owned_val = True
                        else:
                            owned_val = bool(raw_val)

                        owned = 1 if bool(owned_val) else 0

                        # per-user canonical table removed — store flags directly on airport_highscore

                        # sanitize column name (prefer the raw ach_key if valid)
                        col = ach_key if ach_key.isidentifier() else ''.join(c if c.isalnum() or c == '_' else '_' for c in ach_key)
                        if col == 'trophy_nb':
                            continue

                        # ensure column exists on airport_highscore
                        if col not in existing_cols:
                            try:
                                cur.execute(f'ALTER TABLE airport_highscore ADD COLUMN "{col}" BOOLEAN DEFAULT 0')
                                existing_cols.add(col)
                            except Exception:
                                pass

                        # upsert the single column for this user (preserve other columns)
                        try:
                            cur.execute(f'INSERT INTO airport_highscore (userId, "{col}") VALUES (?, ?) ON CONFLICT(userId) DO UPDATE SET "{col}"=excluded."{col}"', (str(uid), owned))
                        except Exception:
                            pass
                    except Exception:
                        continue
                conn.commit()
            except Exception as e:
                print("ACHV2 write error:", e)
        else:
            # unknown item type: ignore or log
            print("Unknown queue item type:", type(item), item)

    # final flush
    if batch:
        try:
            cur.executemany(
                "INSERT INTO airport_highscore (userId, userName, userXP, aircraftCount, destinations, battleWins) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(userId) DO UPDATE SET userName=excluded.userName, userXP=excluded.userXP, aircraftCount=excluded.aircraftCount, destinations=excluded.destinations, battleWins=excluded.battleWins;",
                batch,
            )
            conn.commit()
        except Exception as e:
            print("DB final write error:", e)

    cur.close()
    conn.close()


def process_all_airports(max_workers: int = 12, weeks: Optional[List[str]] = None):
    airports = fetch_airports()
    print(f"Fetched {len(airports)} airports")

    if weeks is None:
        weeks = get_last_n_weeks(3)
    if not weeks:
        print("No weeks to process.")
        return
    print(f"Fetching {len(weeks)} weeks per airport: {weeks[0]}..{weeks[-1]}")

    q: "queue.Queue" = queue.Queue()
    stop_event = threading.Event()

    writer = threading.Thread(target=writer_thread_fn, args=(q, stop_event), daemon=True)
    writer.start()

    session_factory = requests.Session

    def worker(a, iso_week):
        airport_id = a.get("airportID")
        if not airport_id:
            return 0
        s = session_factory()
        saved = 0
        url = API_HIGHSCORE_TEMPLATE.format(airport_id=airport_id)
        try:
            r = s.get(url, headers=HEADERS, params={"isoYearWeek": iso_week}, timeout=20)
            if r.status_code != 200:
                return 0
            payload = r.json()
            highscores = payload.get("highscore") or payload.get("rows") or payload.get("data") or []
            for entry in highscores:
                uid = entry.get("userId") or entry.get("id") or entry.get("playerId")
                if not uid:
                    continue
                uname = entry.get("userName") or entry.get("displayName") or entry.get("name")
                xp = entry.get("userXP") or entry.get("xp") or entry.get("score") or 0

                aircraft_ct = None
                dest_ct = None
                battle_wins = None
                user_xp = xp
                items = []
                try:
                    user_url = f"https://api.skycards.oldapes.com/users/pub/{uid}"
                    ur = s.get(user_url, headers=HEADERS, timeout=10)
                    if ur.status_code == 200:
                        udata = ur.json()
                        user_xp = udata.get("userXP") or udata.get("xp") or user_xp
                        aircraft_ct = (
                            udata.get("aircraftCount")
                            or udata.get("aircraft_count")
                            or udata.get("aircrafts")
                            or udata.get("aircraftsCount")
                            or udata.get("numAircraftModels")
                        )
                        dest_ct = (
                            udata.get("destinations")
                            or udata.get("destinationsCount")
                            or udata.get("destinationCount")
                            or udata.get("numDestinations")
                        )
                        battle_wins = (
                            udata.get("battleWins")
                            or udata.get("battlesWon")
                            or udata.get("wins")
                            or udata.get("numBattleWins")
                        )

                        # detect achievement structures
                        ach_candidates = None
                        for k in ("achievements", "trophies", "trophiesUnlocked", "badges", "unlockedAchievements", "unlocks", "awards"):
                            if k in udata and udata.get(k) is not None:
                                ach_candidates = udata.get(k)
                                break

                        if ach_candidates is not None:
                            if isinstance(ach_candidates, dict):
                                for k, v in ach_candidates.items():
                                    items.append((k, v))
                            elif isinstance(ach_candidates, list):
                                for it in ach_candidates:
                                    if isinstance(it, (str, int)):
                                        items.append((str(it), True))
                                    elif isinstance(it, dict):
                                        ak = None
                                        for f in ("key", "id", "slug", "name", "title"):
                                            if f in it:
                                                ak = str(it.get(f))
                                                break
                                        if not ak:
                                            ak = json.dumps(it, sort_keys=True)
                                        # preserve full dict so writer can read isAchieved/title/description
                                        items.append((ak, it))
                            else:
                                items.append((str(ach_candidates), True))
                except Exception:
                    pass

                # enqueue base stats row for writer thread
                q.put((str(uid), uname, int(user_xp or 0), aircraft_ct, dest_ct, battle_wins))

                # enqueue achievement update for writer thread (new format)
                if items:
                    try:
                        # send raw achievement key and boolean; writer will normalize and upsert into canonical tables
                        q.put(("__ACHV2__", str(uid), items))
                    except Exception:
                        pass

                saved += 1
            # polite pacing
            time.sleep(0.01)
        except Exception:
            return 0
        return saved

    total_saved = 0
    # Iterate weeks outermost, process all airports for one week concurrently
    for iso in weeks:
        print(f"Starting week {iso} (processing {len(airports)} airports)")
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(worker, a, iso): a for a in airports}
            with tqdm(total=len(futures), desc=f"Week {iso}", unit="airport") as pbar:
                for fut in as_completed(futures):
                    try:
                        s = fut.result()
                        total_saved += s
                    except Exception as e:
                        print("Worker error:", e)
                    pbar.update(1)
    # signal writer to finish
    q.put("__SENTINEL__")
    writer.join()

    print(f"Queued and wrote approx {total_saved} leaderboard rows (deduped by userId)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fetch Skycards airport leaderboards and user stats")
    parser.add_argument("--week", help="Specific ISO week to fetch (format YYYYWW)", type=str)
    parser.add_argument("--last", help="Number of latest ISO weeks to fetch (default 3)", type=int, default=3)
    parser.add_argument("--workers", help="Number of concurrent workers", type=int, default=12)
    args = parser.parse_args()

    if args.week:
        weeks = [args.week]
    else:
        weeks = get_last_n_weeks(args.last)

    try:
        process_all_airports(max_workers=args.workers, weeks=weeks)
    except KeyboardInterrupt:
        print("Interrupted by user", file=sys.stderr)
        sys.exit(1)
