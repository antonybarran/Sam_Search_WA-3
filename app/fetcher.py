# app/fetcher.py
"""
Fetches SAM.gov opportunities and upserts them into Postgres.

Key features:
- GET https://api.sam.gov/opportunities/v2/search
- Requires postedFrom and postedTo in MM/DD/YYYY format
- Polite retry/backoff (handles 429, 5xx)
- Tracks last postedFrom in `meta` table for incremental sync
- Optional ZIP filtering
- Uses upsert_many from app.db

Usage (Render cron example):
  python -m app.fetcher --days 2 --page-size 10 --max-records 300 --sleep-between 8 --delete-expired
"""

import argparse
import datetime as dt
import os
import random
import sys
import time
from typing import Dict, List, Optional

import requests

from .db import get_conn, ensure_schema, upsert_many, delete_expired

# ------------------------- Constants -------------------------

SAM_ENDPOINT = "https://api.sam.gov/opportunities/v2/search"
META_KEY = "last_posted_from"  # ISO YYYY-MM-DD
DATE_FMT = "%m/%d/%Y"

# ------------------------- Meta table helpers -------------------------

def ensure_meta_schema() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

def get_meta(key: str) -> Optional[str]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT value FROM meta WHERE key=%s", (key,))
        row = cur.fetchone()
        return row[0] if row else None

def set_meta(key: str, value: str) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
          INSERT INTO meta (key,value,updated_at)
          VALUES (%s,%s,NOW())
          ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW();
        """, (key,value))
        conn.commit()

# ------------------------- HTTP with backoff -------------------------

def sam_get(url: str, params: Dict, tries: int = 8, base_sleep: int = 2) -> requests.Response:
    for i in range(tries):
        r = requests.get(url, params=params, timeout=60)
        status = r.status_code
        print(f"[SAM] status={status} try={i+1}/{tries}")
        if status == 429:
            ra = r.headers.get("Retry-After")
            sleep = int(ra) if ra and ra.isdigit() else base_sleep * (2 ** i)
            sleep = int(sleep * (1.0 + random.random() * 0.3))
            print(f"[SAM] rate-limited, sleeping {sleep}s")
            time.sleep(sleep)
            continue
        if 500 <= status < 600:
            sleep = int(base_sleep * (2 ** i) * (1.0 + random.random() * 0.3))
            print(f"[SAM] server error {status}, sleeping {sleep}s")
            time.sleep(sleep)
            continue
        r.raise_for_status()
        return r
    raise RuntimeError(f"Failed after {tries} attempts; last={status} body={r.text[:500]}")

# ------------------------- Main fetch logic -------------------------

def fetch_and_store(args) -> None:
    ensure_schema()
    ensure_meta_schema()

    api_key = os.environ.get("SAM_API_KEY")
    if not api_key:
        print("[FATAL] SAM_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    # Determine date window
    today = dt.date.today()
    posted_to = today.strftime(DATE_FMT)

    last_seen = get_meta(META_KEY)
    if last_seen:
        posted_from_date = dt.date.fromisoformat(last_seen) + dt.timedelta(days=1)
    else:
        posted_from_date = today - dt.timedelta(days=args.days)

    posted_from = posted_from_date.strftime(DATE_FMT)

    print(f"[RUN] postedFrom={posted_from} postedTo={posted_to}")

    total = 0
    offset = 0
    while total < args.max_records:
        params = {
            "api_key": api_key,
            "postedFrom": posted_from,
            "postedTo": posted_to,
            "limit": str(args.page_size),
            "offset": str(offset),
        }
        r = sam_get(SAM_ENDPOINT, params)
        data = r.json()

        recs = data.get("opportunitiesData", [])
        if not recs:
            print("[DONE] No more records")
            break

        upsert_many(recs)
        total += len(recs)
        offset += args.page_size
        print(f"[UPsert] total={total}")

        if total >= args.max_records:
            break

        time.sleep(args.sleep_between)

    # Save high water mark
    set_meta(META_KEY, posted_to)
    print(f"[META] updated {META_KEY}={posted_to}")

    if args.delete_expired:
        delete_expired()
        print("[CLEANUP] expired records removed")

# ------------------------- CLI -------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=2)
    p.add_argument("--page-size", type=int, default=10)
    p.add_argument("--max-records", type=int, default=300)
    p.add_argument("--sleep-between", type=int, default=8)
    p.add_argument("--delete-expired", action="store_true")
    args = p.parse_args()
    fetch_and_store(args)

if __name__ == "__main__":
    main()
