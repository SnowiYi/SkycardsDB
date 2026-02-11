#!/usr/bin/env python3
"""Generate a suggested achievement->emoji mapping by scanning DB columns.

Usage: python generate_emoji_map.py
This writes `BigDB/ach_emoji_map.json` with suggested mappings which you can
edit. It does not change the DB.
"""
import sqlite3
import json
from pathlib import Path

DB_DIR = Path(__file__).resolve().parent / 'data' / 'DB'
DB_DIR.mkdir(parents=True, exist_ok=True)
DB = DB_DIR / 'highscore.db'

KEYWORD_EMOJI = [
    (('continent', 'continents'), '🌐'),
    (('polar','bear'), '🐻'),
    (('coconut','coco'), '🥥'),
    (('peng',), '🐧'),
    (('map','maps'), '🗺️'),
    (('globe','world'), '🌐'),
    (('mount', 'volcano','alps'), '🏔️'),
    (('sun','sunrise'), '🌅'),
    (('europe','eu'), '🇪🇺'),
    (('eagle','hawk','bird','bald'), '🦅'),
    (('llama','alpaca'), '🦙'),
    (('statue','liberty'), '🗽'),
    (('canada','maple'), '🍁'),
    (('kangaroo','australia'), '🦘'),
    (('dragon',), '🐉'),
    (('plane','aircraft','airplane','pilot'), '🛩️'),
    (('helicopter','heli'), '🚁'),
    (('truck','lorry'), '🚚'),
    (('run','runner'), '🏃'),
    (('box','boxing','glove'), '🥊'),
    (('trophy','medal','podium'), '🏆'),
    (('star',), '⭐'),
    (('gold','silver','bronze','1','2','3'), '🏅'),
]


def suggest_for(name: str):
    n = name.lower()
    for keys, emoji in KEYWORD_EMOJI:
        for k in keys:
            if k in n:
                return emoji
    return None


def main():
    if not DB.exists():
        print('DB not found at', DB)
        return
    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(airport_highscore)")
    cols = [r[1] for r in cur.fetchall()]

    mapping = {}
    for c in cols:
        # ignore generic columns
        if c in ('userId','userName','userXP','aircraftCount','destinations','battleWins'):
            continue
        emoji = suggest_for(c)
        if emoji:
            mapping[c] = emoji

    out = DB.parent / 'ach_emoji_map.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    print('Wrote', out)


if __name__ == '__main__':
    main()
