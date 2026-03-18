from flask import Flask, render_template, request, g, jsonify
import sqlite3
import requests
from pathlib import Path
import logging
import subprocess
from datetime import datetime, timedelta
import json
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# Refresh scheduler state
REFRESH_STATE = {
    'last_refresh': None,
    'next_refresh': None,
    'total_users': 0,
    'refresh_interval_hours': 1,
    'status': 'idle',
    'last_error': None,
}

# API configuration for fetching user data
API_PROFILE_TEMPLATE = "https://api.skycards.oldapes.com/users/pub/{}"
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIwMTljMmVmMi0xMDEyLTcwNjEtYWUzNS0wNjlmMThhZTQyYjIiLCJqdGkiOiJIaVdiZlVFM0lnMjBONkhqYnVWT2pfeGFZa2VlNDRaSVZHcFpXX2htSGVNIiwiaWF0IjoxNzcwODI1MzM3LCJleHAiOjQ5MjY1ODUzMzd9.-fOLPEtuvKqOuLnk2EZT8f_Lf-ymMIp4_vjnVgqzNEo"
API_HEADERS = {
    "Authorization": f"Bearer {token}",
    "accept": "application/json",
    "accept-encoding": "gzip",
    "content-type": "application/json",
    "Host": "api.skycards.oldapes.com",
    "User-Agent": "okhttp/4.12.0",
    "x-client-version": "2.0.24",
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('refresh.log'),
        logging.StreamHandler()
    ]
)

# DB file location: use workspace-local BigDB/data/DB/highscore.db
DB_DIR = Path(__file__).resolve().parent / "data" / "DB"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "highscore.db"


def get_total_users():
    """Get the total count of users in the database."""
    try:
        db = get_db()
        cur = db.execute("SELECT COUNT(*) as count FROM airport_highscore")
        row = cur.fetchone()
        return row['count'] if row else 0
    except Exception as e:
        logging.error(f"Error counting users: {e}")
        return 0


def run_refresh_tasks():
    """Run the two refresh commands: api_test.py and Refresh.py"""
    try:
        REFRESH_STATE['status'] = 'running'
        REFRESH_STATE['last_error'] = None
        
        work_dir = Path(__file__).resolve().parent
        
        # Log the start of the refresh cycle
        logging.info("=" * 80)
        logging.info("🔄 REFRESH CYCLE STARTED")
        logging.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("=" * 80)
        
        # Run api_test.py
        logging.info("▶️  Starting api_test.py with --last 1 --workers 125...")
        try:
            result = subprocess.run(
                ['python3', 'api_test.py', '--last', '1', '--workers', '125'],
                cwd=work_dir,
                capture_output=True,
                timeout=600,
                text=True
            )
            if result.returncode == 0:
                logging.info(f"✅ api_test.py completed successfully")
                if result.stdout:
                    logging.info(f"Output: {result.stdout[:500]}")
            else:
                logging.warning(f"❌ api_test.py exited with code {result.returncode}")
                logging.warning(f"Error: {result.stderr[:300]}")
                REFRESH_STATE['last_error'] = f"api_test.py failed: {result.stderr[:200]}"
        except subprocess.TimeoutExpired:
            logging.error(f"❌ api_test.py timed out after 600 seconds")
            REFRESH_STATE['last_error'] = "api_test.py timed out"
        except Exception as e:
            logging.error(f"❌ Error running api_test.py: {e}")
            REFRESH_STATE['last_error'] = str(e)[:200]
        
        # Run Refresh.py
        logging.info("▶️  Starting Refresh.py with --threads 125...")
        try:
            result = subprocess.run(
                ['python3', 'Refresh.py', '--threads', '125'],
                cwd=work_dir,
                capture_output=True,
                timeout=600,
                text=True
            )
            if result.returncode == 0:
                logging.info(f"✅ Refresh.py completed successfully")
                if result.stdout:
                    logging.info(f"Output: {result.stdout[:500]}")
            else:
                logging.warning(f"❌ Refresh.py exited with code {result.returncode}")
                logging.warning(f"Error: {result.stderr[:300]}")
                if not REFRESH_STATE['last_error']:  # Preserve api_test error if present
                    REFRESH_STATE['last_error'] = f"Refresh.py failed: {result.stderr[:200]}"
        except subprocess.TimeoutExpired:
            logging.error(f"❌ Refresh.py timed out after 600 seconds")
            if not REFRESH_STATE['last_error']:
                REFRESH_STATE['last_error'] = "Refresh.py timed out"
        except Exception as e:
            logging.error(f"❌ Error running Refresh.py: {e}")
            if not REFRESH_STATE['last_error']:
                REFRESH_STATE['last_error'] = str(e)[:200]
        
        # Update refresh state
        REFRESH_STATE['last_refresh'] = datetime.now().isoformat()
        REFRESH_STATE['next_refresh'] = (datetime.now() + timedelta(hours=1)).isoformat()
        REFRESH_STATE['total_users'] = get_total_users()
        REFRESH_STATE['status'] = 'idle'
        
        logging.info("=" * 80)
        logging.info("✅ REFRESH CYCLE COMPLETED")
        logging.info(f"Total users in database: {REFRESH_STATE['total_users']}")
        logging.info(f"Next refresh scheduled: {REFRESH_STATE['next_refresh']}")
        logging.info("=" * 80)
        
    except Exception as e:
        logging.error(f"❌ Unexpected error in run_refresh_tasks: {e}")
        REFRESH_STATE['status'] = 'error'
        REFRESH_STATE['last_error'] = str(e)[:200]


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        # open connection with timeout and enable WAL to reduce locking
        db = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass
        try:
            db.execute("PRAGMA busy_timeout = 30000;")
        except Exception:
            pass
        g._database = db
    return db


# Initialize APScheduler
def init_scheduler():
    """Initialize the background scheduler for refresh tasks."""
    try:
        scheduler = BackgroundScheduler()
        
        # Schedule the refresh task to run every hour, starting 2 seconds after app starts
        # Uses daemon thread so it won't block Flask
        scheduler.add_job(
            run_refresh_tasks,
            'interval',
            hours=1,
            seconds=2,  # First run after 2 seconds, then every hour
            id='refresh_job',
            name='Refresh SkyCards data',
            replace_existing=True,
            max_instances=1
        )
        
        # Start the scheduler with daemon thread (won't block web server)
        if not scheduler.running:
            scheduler.start(paused=False)
            logging.info("🚀 Background scheduler initialized - first refresh in 2 seconds, then every hour")
        
        return scheduler
    except Exception as e:
        logging.error(f"Failed to initialize scheduler: {e}")
        return None


scheduler = None


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def refresh_user_data(user_id):
    """
    Fetch user data from the API and update the leaderboard database.
    
    Args:
        user_id (str): The UUID of the user to refresh
        
    Returns:
        dict: Contains updated user data or error information
    """
    url = API_PROFILE_TEMPLATE.format(user_id)
    logging.info(f"Fetching user data from API for UUID: {user_id}")
    
    try:
        resp = requests.get(url, headers=API_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        # Extract relevant stats from API response, try multiple possible field names
        user_xp = data.get("userXP") or data.get("xp")
        if user_xp is None:
            logging.warning(f"No XP data found in API response for {user_id}")
            return {
                "success": False,
                "userId": user_id,
                "error": "API response missing userXP/xp field"
            }
        
        user_name = data.get("userName", "Unknown")
        aircraft_count = data.get("aircraftCount") or data.get("aircraft_count") or data.get("numAircraftModels") or 0
        destinations = data.get("destinations") or data.get("destinationsCount") or data.get("numDestinations") or 0
        battle_wins = data.get("battleWins") or data.get("battlesWon") or data.get("wins") or data.get("numBattleWins") or 0
        
        # Update database
        db = get_db()
        
        # Check if user exists
        cur = db.execute("SELECT userId FROM airport_highscore WHERE userId = ?", (user_id,))
        exists = cur.fetchone()
        
        if exists:
            # Update existing user
            db.execute(
                """UPDATE airport_highscore 
                   SET userName = ?, userXP = ?, aircraftCount = ?, destinations = ?, battleWins = ? 
                   WHERE userId = ?""",
                (user_name, user_xp, aircraft_count, destinations, battle_wins, user_id)
            )
            logging.info(f"Updated user {user_name} (ID: {user_id})")
        else:
            # Insert new user
            db.execute(
                """INSERT INTO airport_highscore (userId, userName, userXP, aircraftCount, destinations, battleWins)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (user_id, user_name, user_xp, aircraft_count, destinations, battle_wins)
            )
            logging.info(f"Added new user {user_name} (ID: {user_id})")
        
        db.commit()
        
        return {
            "success": True,
            "userId": user_id,
            "userName": user_name,
            "userXP": user_xp,
            "aircraftCount": aircraft_count,
            "destinations": destinations,
            "battleWins": battle_wins,
            "message": f"Successfully refreshed {user_name}"
        }
        
    except requests.RequestException as e:
        logging.error(f"API request failed for user {user_id}: {e}")
        return {
            "success": False,
            "userId": user_id,
            "error": f"API request failed: {str(e)}"
        }
    except Exception as e:
        logging.error(f"Failed to refresh user {user_id}: {e}")
        return {
            "success": False,
            "userId": user_id,
            "error": f"Database update failed: {str(e)}"
        }


def top_by(column, limit=10):
    db = get_db()
    col = column
    sql = f"SELECT userId, userName, userXP, aircraftCount, destinations, battleWins FROM airport_highscore ORDER BY COALESCE({col},0) DESC LIMIT ?"
    cur = db.execute(sql, (limit,))
    return cur.fetchall()


# XP thresholds for levels (level 1 = 0)
LEVEL_THRESHOLDS = [
    0,
    10000,
    20000,
    30000,
    40000,
    60000,
    100000,
    130000,
    180000,
    230000,
    280000,
    360000,
    440000,
    520000,
    620000,
    720000,
    820000,
    920000,
    1020000,
    1220000,
    1420000,
    1620000,
    2620000,
    3620000,
    4620000,
    6620000,
    8620000,
    10620000,
    12620000,
    50000000,
    60000000,
    75000000,
    90000000,
    105000000,
    120000000,
    135000000,
    150000000,
    165000000,
    180000000,
    195000000,
    210000000,
    225000000,
    240000000,
    255000000,
    270000000,
    285000000,
    300000000,
    315000000,
    330000000,
    350000000,
]


# Emoji set to represent the 35 trophies (placeholder mapping)
ACH_EMOJIS = [
    "🥇", "🥈", "🥉", "🏆", "🏅", "🎖️", "🪙", "🛩️", "🚁", "✈️",
    "🗺️", "🌐", "🏔️", "🌅", "⭐", "🦅", "🐻", "🥥", "🐧", "🦄",
    "🗽", "🍁", "🦘", "🐉", "🪂", "🛫", "🪑", "🏃", "🥊", "🎮",
    "🔰", "⚔️", "🛡️", "🏁", "🏵️", "🎯",
]

# Optional explicit mapping from achievement key or normalized name -> emoji.
# Add known achievement ids here so their emoji matches their real name.
ACH_EMOJI_MAP = {
    'S1_COMMON100': '🛩️',
    'Visit_All_Continents': '🌐',
    'S1_GOLD1': '🥇',
}

# If a JSON mapping file exists next to the DB, load and merge it to allow
# customizing emoji associations without editing source.
try:
    import os
    import json as _json
    MAP_PATH = DB_PATH.parent / 'ach_emoji_map.json'
    if MAP_PATH.exists():
        try:
            with open(MAP_PATH, 'r', encoding='utf-8') as mf:
                user_map = _json.load(mf)
                # keys may be non-string; ensure strings
                for k, v in list(user_map.items()):
                    if isinstance(k, str) and isinstance(v, str):
                        ACH_EMOJI_MAP[k] = v
        except Exception:
            pass
except Exception:
    pass


# Simple description -> emoji heuristics (used when metadata/description is available)
DESC_KEYWORDS = [
    (('continent', 'continents', 'world', 'globe'), '🌐'),
    (('polar', 'arctic', 'antarctic', 'ice', 'polar bear', 'bear'), '🐻'),
    (('coconut', 'coco', 'tropic'), '🥥'),
    (('peng', 'penguin'), '🐧'),
    (('map', 'maps', 'countries'), '🗺️'),
    (('mount', 'volcano', 'alps', 'mountain'), '🏔️'),
    (('sun', 'sunrise'), '🌅'),
    (('europe', 'eu'), '🇪🇺'),
    (('canada', 'maple'), '🍁'),
    (('australia','kangaroo'), '🦘'),
    (('dragon',), '🐉'),
    (('statue','liberty'), '🗽'),
    (('plane','aircraft','airplane','pilot','airbus','boeing'), '🛩️'),
    (('helicopter','heli'), '🚁'),
    (('run','runner'), '🏃'),
    (('boxing','glove'), '🥊'),
    (('trophy','medal','podium','gold','silver','bronze'), '🏅'),
]


def choose_emoji_from_text(text: str):
    if not text:
        return None
    t = str(text).lower()
    for keys, emoji in DESC_KEYWORDS:
        for k in keys:
            if k in t:
                return emoji
    return None


def compute_level(xp):
    """Given XP (int), return (level, xp_current_level, xp_next_level, progress_float).
    Levels start at 1.
    If at or above highest threshold, next level is None and progress is 1.0
    """
    try:
        xp = int(xp or 0)
    except Exception:
        xp = 0
    # find highest threshold <= xp
    level = 1
    for i, thresh in enumerate(LEVEL_THRESHOLDS):
        if xp >= thresh:
            level = i + 1
        else:
            break
    # ensure bounds
    idx = level - 1
    curr = LEVEL_THRESHOLDS[idx] if idx < len(LEVEL_THRESHOLDS) else LEVEL_THRESHOLDS[-1]
    if idx + 1 < len(LEVEL_THRESHOLDS):
        nxt = LEVEL_THRESHOLDS[idx + 1]
        span = max(1, nxt - curr)
        progress = min(1.0, max(0.0, (xp - curr) / span))
    else:
        nxt = None
        progress = 1.0
    return {"level": level, "xp": xp, "curr": curr, "next": nxt, "progress": progress}


@app.route('/refresh-user/<user_id>', methods=['GET', 'POST'])
def refresh_user_route(user_id):
    """
    HTTP endpoint to refresh a specific user's data from the API.
    
    Usage: GET /refresh-user/{user_id}
    """
    result = refresh_user_data(user_id)
    return jsonify(result)


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get the current refresh status and stats."""
    status_copy = REFRESH_STATE.copy()
    status_copy['total_users'] = get_total_users()  # Always get current count
    return jsonify(status_copy)


@app.route('/')
def index():
    top_xp = top_by('userXP')
    top_aircraft = top_by('aircraftCount')
    top_airport = top_by('destinations')
    top_battles = top_by('battleWins')
    
    # Get current status
    status = REFRESH_STATE.copy()
    status['total_users'] = get_total_users()
    
    return render_template('index.html', top_xp=top_xp, top_aircraft=top_aircraft, top_airport=top_airport, top_battles=top_battles, status=status)


@app.route('/user', methods=['GET'])
def user_lookup():
    q = request.args.get('q','').strip()
    result = None
    rows = []
    if q:
        db = get_db()
        # try exact id match first
        # select all columns so trophy_* achievement columns are available on the result dict
        cur = db.execute('SELECT * FROM airport_highscore WHERE userId = ? COLLATE NOCASE', (q,))
        row = cur.fetchone()
        if row:
            # convert sqlite Row to plain dict for template iteration
            result = dict(row)
            lvl = compute_level(result.get('userXP'))
            result.update(lvl)
            # No canonical achievements tables — use boolean columns on airport_highscore
            result['trophies'] = []
            result['all_ach_count'] = 35
            # Prefer reading achievement boolean columns directly from airport_highscore (single-table approach)
            try:
                cur = db.execute("PRAGMA table_info(airport_highscore)")
                cols = [r[1] for r in cur.fetchall()]
                ach_cols = [c for c in cols if (c.startswith('trophy_') or c.startswith('ach_') or c.startswith('badge_') or c.startswith('troph') or c.isupper()) and c != 'trophy_nb']
                ach_list = []
                if ach_cols:
                    for idx, col in enumerate(sorted(ach_cols)):
                        name = col.replace('_', ' ').title()
                        meta = None
                        desc = None
                        owned_flag = bool(result.get(col)) if col in result else False
                        # try reading metadata stored as JSON in the column (if any)
                        try:
                            raw = result.get(col)
                            if isinstance(raw, (str,)):
                                # attempt to parse JSON metadata stored in column
                                try:
                                    parsed = json.loads(raw)
                                    if isinstance(parsed, dict):
                                        meta = parsed
                                        desc = parsed.get('description') or parsed.get('desc') or parsed.get('title')
                                except Exception:
                                    # not JSON, ignore
                                    pass
                        except Exception:
                            pass

                        # prefer explicit mapping by key or name, else try description heuristics,
                        # then fall back to positional emojis
                        emoji = ACH_EMOJI_MAP.get(col) or ACH_EMOJI_MAP.get(name)
                        if not emoji:
                            emoji = choose_emoji_from_text(desc or name)
                        if not emoji:
                            emoji = ACH_EMOJIS[idx] if idx < len(ACH_EMOJIS) else '🏵️'

                        ach_list.append({'key': col, 'name': name, 'meta': meta, 'desc': desc, 'owned': owned_flag, 'emoji': emoji})
                    result['ach_list'] = ach_list
                    result['all_ach_count'] = max(result.get('all_ach_count', 35), len(ach_cols))
                    result['trophies'] = [a for a in ach_list if a.get('owned')]
                else:
                    # fallback: build list from trophies array if present
                    result['ach_list'] = []
                    for i in range(result.get('all_ach_count', 35)):
                        emoji = ACH_EMOJIS[i] if i < len(ACH_EMOJIS) else '🏵️'
                        owned = i < len(result.get('trophies', []))
                        name = (result['trophies'][i]['name'] if owned else f'Trophy {i+1}') if result.get('trophies') else f'Trophy {i+1}'
                        result['ach_list'].append({'key': f'pl_{i+1}', 'name': name, 'meta': None, 'owned': owned, 'emoji': emoji})
                    result['trophies'] = [a for a in result['ach_list'] if a.get('owned')]
            except Exception:
                # final fallback: trophies array
                result['ach_list'] = []
                for i in range(result.get('all_ach_count', 35)):
                    emoji = ACH_EMOJIS[i] if i < len(ACH_EMOJIS) else '🏵️'
                    owned = i < len(result.get('trophies', []))
                    name = (result['trophies'][i]['name'] if owned else f'Trophy {i+1}') if result.get('trophies') else f'Trophy {i+1}'
                    result['ach_list'].append({'key': f'pl_{i+1}', 'name': name, 'meta': None, 'owned': owned, 'emoji': emoji})
                result['trophies'] = [a for a in result['ach_list'] if a.get('owned')]
        else:
            cur = db.execute('SELECT userId, userName, userXP, aircraftCount, destinations, battleWins FROM airport_highscore WHERE LOWER(userName) LIKE ? LIMIT 50', (f'%{q.lower()}%',))
            rows = []
            for r in cur.fetchall():
                d = dict(r)
                d.update(compute_level(d.get('userXP')))
                rows.append(d)
            # Note: we don't fetch trophies for each row in the matches list to keep lookup fast
    return render_template('user.html', query=q, result=result, rows=rows)


if __name__ == '__main__':
    # Initialize the background scheduler on startup
    logging.info("=" * 80)
    logging.info("🌐 Starting Flask server on http://0.0.0.0:5050")
    logging.info("=" * 80)
    scheduler = init_scheduler()
    
    app.run(host='0.0.0.0', port=5050, debug=True)
