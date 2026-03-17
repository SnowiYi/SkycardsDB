

import sqlite3
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import logging
import sys
import argparse
from multiprocessing import cpu_count
from tqdm import tqdm
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DB_PATH = "data/DB/highscore.db"

# Get all player IDs and usernames from airport_highscore
PLAYER_IDS_QUERY = "SELECT userId, userName FROM airport_highscore"
REFRESH_STATS_QUERY = "UPDATE airport_highscore SET userName = ?, userXP = ?, aircraftCount = ?, destinations = ?, battleWins = ? WHERE userId = ?"

API_PROFILE_TEMPLATE = "https://api.skycards.oldapes.com/users/pub/{}"
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Thread-local storage for database connections and HTTP sessions
_thread_local = threading.local()

# Global batch update queue
update_queue = []
queue_lock = threading.Lock()
BATCH_SIZE = 500  # Commit every 500 updates

def get_db_connection():
    """Get or create a database connection for the current thread."""
    if not hasattr(_thread_local, 'db_conn'):
        conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass
        try:
            conn.execute("PRAGMA busy_timeout = 30000;")
        except Exception:
            pass
        _thread_local.db_conn = conn
    return _thread_local.db_conn

def get_http_session():
    """Get or create an HTTP session for the current thread with connection pooling and retry strategy."""
    if not hasattr(_thread_local, 'http_session'):
        session = requests.Session()
        
        # Configure retry strategy with exponential backoff
        retry_strategy = Retry(
            total=3,  # total number of retries
            backoff_factor=0.5,  # exponential backoff: 0.5s, 1s, 2s
            status_forcelist=[429, 500, 502, 503, 504],  # retry on these status codes
            allowed_methods=["GET"]
        )
        
        # Create HTTPAdapter with connection pooling and retry strategy
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=100,
            pool_maxsize=100
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        _thread_local.http_session = session
    return _thread_local.http_session

def close_db_connection():
    """Close database connection and HTTP session for current thread."""
    if hasattr(_thread_local, 'db_conn'):
        _thread_local.db_conn.close()
        del _thread_local.db_conn
    if hasattr(_thread_local, 'http_session'):
        _thread_local.http_session.close()
        del _thread_local.http_session

def batch_update_stat(user_data, user_id):
    """Queue user data for batch update."""
    if user_data is None:
        return
    global update_queue
    with queue_lock:
        # user_data is a tuple: (userName, userXP, aircraftCount, destinations, battleWins)
        update_queue.append(user_data + (user_id,))
        if len(update_queue) >= BATCH_SIZE:
            flush_batch_updates()

def flush_batch_updates():
    """Flush all queued updates to database."""
    global update_queue
    if not update_queue:
        return
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.executemany(REFRESH_STATS_QUERY, update_queue)
        conn.commit()
        count = len(update_queue)
        update_queue = []
        return count
    except Exception as e:
        logging.error(f"Failed to batch update XP: {e}")
        update_queue = []
        return 0

def refresh_stats(person_id, username):
    """Fetch user stats from API with automatic retry on failure."""
    url = API_PROFILE_TEMPLATE.format(person_id)
    try:
        session = get_http_session()
        resp = session.get(url, headers=HEADERS, timeout=5)  # Reduced timeout to 5s
        resp.raise_for_status()
        data = resp.json()
        # Extract all user stats from API response
        # Try to get username from API, but use existing username only if API call fails completely
        user_name = data.get("name") or data.get("userName") or data.get("displayName") or ""
        user_xp = data.get("userXP") or data.get("xp") or 0
        aircraft_ct = (
            data.get("aircraftCount")
            or data.get("aircraft_count")
            or data.get("aircrafts")
            or data.get("aircraftsCount")
            or data.get("numAircraftModels")
            or 0
        )
        dest_ct = (
            data.get("destinations")
            or data.get("destinationsCount")
            or data.get("destinationCount")
            or data.get("numDestinations")
            or 0
        )
        battle_wins = data.get("battleWins") or data.get("battlesWon") or data.get("numBattleWins") or 0
        return (user_name, user_xp, aircraft_ct, dest_ct, battle_wins)
    except Exception as e:
        # If API call fails after retries, keep existing username
        logging.debug(f"Failed to fetch data for user {person_id}: {e}")
        return (username, 0, 0, 0, 0)


def main(num_threads):
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA busy_timeout = 30000;")
    except Exception:
        pass
    
    cursor = conn.cursor()
    cursor.execute(PLAYER_IDS_QUERY)
    player_ids = cursor.fetchall()
    total = len(player_ids)
    
    conn.close()
    
    logging.info(f"Starting refresh of {total} players with {num_threads} threads")

    def worker(row):
        (userId, userName) = row
        user_data = refresh_stats(userId, userName)
        # Queue update for batch processing
        batch_update_stat(user_data, userId)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        with tqdm(total=total, desc="Refreshing players", unit="player", ncols=80) as pbar:
            futures = {executor.submit(worker, row): row for row in player_ids}
            for fut in as_completed(futures):
                fut.result()  # propagate exceptions
                pbar.update(1)
    
    # Flush any remaining updates
    with queue_lock:
        remaining = flush_batch_updates()
    
    # Clean up thread-local connections
    close_db_connection()
    
    logging.info(f"Refresh complete! Processed {total} players")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh user stats with multithreading")
    # Use CPU count * 2 as default (good balance for I/O-bound operations)
    default_threads = max(16, cpu_count() * 2)
    parser.add_argument('--threads', type=int, default=default_threads, help=f'Number of threads to use (default: {default_threads})')
    args = parser.parse_args()
    main(args.threads)
