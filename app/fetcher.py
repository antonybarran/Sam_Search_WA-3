# app/fetcher.py
"""
Fetches SAM.gov opportunities and upserts them into Postgres.

Key features:
- Backoff with Retry-After handling to avoid 429s
- Incremental sync (stores last successful postedFrom in meta table)
- Optional ZIP filtering (one ZIP per request = very polite)
- Batching + upserts via app.db helpers
- Clear logging for Render cron diagnostics

Required env:
- SAM_API_KEY
- DATABASE_URL (handled by app.db)

Example:
  python -m app.fetcher --days 2 --page-size 10 --max-records 300 --sleep-between 8
  python -m app.fetcher --zip-file app/wa3_zips.txt --days 1 --page-size 10 --max-records 200 --sleep-between 10
"""

import argparse
import datetime as dt
import json
import os
import random
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from .db import get_conn, ensure_schema, upsert_many, delete_expired


SAM_ENDPOINT = "https://api.sam.gov/prod/opportunities/v2/search"
META_KEY = "last_posted_from"


# ------------------------- Utility: meta table -------------------------

def ensure_meta_schema() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def get_meta(key: str) -> Optional[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM meta WHERE key = %s LIMIT 1;", (key,))
            row = cur.fetchone()
            return row[0] if row else None


def set_meta(key: str, value: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta (key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
                """,
                (key, value),
            )
        conn.commit()


# ------------------------- HTTP with backoff -------------------------

def sam_post(url: str, payload: Dict, tries: int = 8, base_sleep: int = 2) -> requests.Response:
    """POST to SAM with polite backoff. Honors Retry-After, adds jitter."""
    for i in range(tries):
        r = requests.post(url, json=payload, timeout=60)
        status = r.status_code
        print(f"[SAM] status={status} try={i+1}/{tries}")
        if status == 429:
            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    sleep = max(1, int(ra))
                except ValueError:
                    sleep = base_sleep * (2 ** i)
            else:
                sleep = base_sleep * (2 ** i)
            sleep = int(sleep * (1.0 + random.random() * 0.3))  # jitter 0–30%
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


# ------------------------- Data shaping -------------------------

def safe_get(d: Dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur if cur is not None else default


def shape_row(n: Dict) -> Dict:
    """
    Map a SAM notice JSON dict to our DB row fields.
    We try multiple common field names to be robust.
    """
    notice_id = n.get("noticeId") or n.get("id") or n.get("solicitationNumber") or n.get("uiLink") or ""
    title = n.get("title") or ""
    sol = n.get("solicitationNumber") or ""
    posted_date = (n.get("postedDate") or n.get("publishDate") or "")[:10] or None
    # a few possible due-date fields seen in responses:
    response_date = (
        n.get("responseDate")
        or n.get("dueDate")
        or n.get("archiveDate")
        or n.get("closeDate")
        or ""
    )[:10] or None

    set_aside = (
        n.get("typeOfSetAsideDescription")
        or n.get("typeOfSetAside")
        or n.get("setAside")
        or ""
    )

    naics = (
        n.get("naicsCode")
        or n.get("naics")
        or safe_get(n, "classification", "naics")
        or ""
    )

    org = (
        n.get("organizationName")
        or n.get("department")
        or n.get("office")
        or ""
    )

    city = (
        safe_get(n, "placeOfPerformance", "city")
        or safe_get(n, "placeOfPerformance", "location", "city")
        or n.get("city")
        or ""
    )

    state = (
        safe_get(n, "placeOfPerformance", "state")
        or safe_get(n, "placeOfPerformance", "location", "state")
        or n.get("state")
        or ""
    )

    zip_code = (
        safe_get(n, "placeOfPerformance", "zip")
        or safe_get(n, "placeOfPerformance", "location", "zip")
        or n.get("zip")
        or ""
    )

    url = n.get("uiLink") or n.get("url") or ""
    desc = n.get("description") or ""

    # ensure minimal ID
    if not notice_id:
        # last resort
        notice_id = f"{sol}-{posted_date or ''}-{title[:30]}"

    return {
        "id": notice_id,
        "title": title,
        "solicitation_number": sol,
        "posted_date": posted_date,
        "response_date": response_date,
        "set_aside": set_aside,
        "naics": naics,
        "org": org,
        "city": city,
        "state": state,
        "zip": zip_code,
        "url": url,
        "description": desc,
    }


# ------------------------- Fetch logic -------------------------

def load_zips(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        z = [line.strip() for line in f if line.strip()]
    print(f"[CFG] Loaded {len(z)} ZIPs from {path}")
    return z


def build_payload(posted_from: str,
                  page_size: int,
                  offset: int,
                  zip_code: Optional[str] = None,
                  naics: Optional[str] = None,
                  setaside: Optional[str] = None) -> Dict:
    filters: Dict = {"postedFrom": posted_from}
    # Keep fetch broad to reduce 429s; filter later in API.
    if zip_code:
        # place of performance by zip (per SAM docs)
        filters["placeOfPerformance"] = {"zip": [zip_code]}
    if naics:
        filters["naics"] = [naics] if "," not in naics else [x.strip() for x in naics.split(",") if x.strip()]
    if setaside:
        filters["setAside"] = [setaside] if "," not in setaside else [x.strip() for x in setaside.split(",") if x.strip()]

    payload = {
        "filters": filters,
        "page": {"size": page_size, "offset": offset}
    }
    return payload


def fetch_for_zip(sam_url: str,
                  api_key: str,
                  posted_from: str,
                  zip_code: Optional[str],
                  page_size: int,
                  max_records: int,
                  sleep_between: int,
                  naics: Optional[str],
                  setaside: Optional[str]) -> Tuple[int, int]:
    """
    Fetch pages for a single ZIP (or all if zip_code is None).
    Returns (total_upserted, total_seen)
    """
    total_upserted = 0
    total_seen = 0
    offset = 0
    consecutive_429_stops = 0

    while total_seen < max_records:
        payload = build_payload(posted_from, page_size, offset, zip_code, naics, setaside)
        r = sam_post(f"{sam_url}?api_key={api_key}", payload)
        data = {}
        try:
            data = r.json()
        except Exception:
            print("[ERR] JSON parse error:", r.text[:500])
            break

        result = data.get("result") or {}
        total_records = result.get("totalRecords") or 0
        items = result.get("searchResults") or []
        print(f"[PAGE] zip={zip_code or '*'} offset={offset} size={page_size} got={len(items)} totalRecords={total_records}")

        if not items:
            break

        rows = [shape_row(n) for n in items]
        upsert_many(rows)
        total_upserted += len(rows)
        total_seen += len(items)

        offset += page_size
        if total_seen >= max_records:
            break

        # polite pacing
        time.sleep(sleep_between)

    return total_upserted, total_seen


def run(args) -> None:
    ensure_schema()
    ensure_meta_schema()

    api_key = os.environ.get("SAM_API_KEY")
    if not api_key:
        print("[FATAL] SAM_API_KEY is not set")
        sys.exit(1)

    # Incremental posted_from: use meta if available, else today - days
    today = dt.date.today()
    default_from = (today - dt.timedelta(days=args.days)).isoformat()
    posted_from = get_meta(META_KEY) or default_from

    print(f"[RUN] posted_from={posted_from} days={args.days} page_size={args.page_size} "
          f"max_records={args.max_records} sleep_between={args.sleep_between}")

    sam_url = SAM_ENDPOINT

    total_upserted_all = 0
    total_seen_all = 0

    zips: List[str] = []
    if args.zip_file:
        zips = load_zips(args.zip_file)

    # If no ZIPs provided, do one broad run (polite page/limits)
    if not zips:
        up, seen = fetch_for_zip(
            sam_url=sam_url,
            api_key=api_key,
            posted_from=posted_from,
            zip_code=None,
            page_size=args.page_size,
            max_records=args.max_records,
            sleep_between=args.sleep_between,
            naics=args.naics,
            setaside=args.setaside,
        )
        total_upserted_all += up
        total_seen_all += seen
    else:
        # One ZIP per run is very polite and avoids 429s.
        for i, z in enumerate(zips, 1):
            print(f"[ZIP] {i}/{len(zips)} {z}")
            up, seen = fetch_for_zip(
                sam_url=sam_url,
                api_key=api_key,
                posted_from=posted_from,
                zip_code=z,
                page_size=args.page_size,
                max_records=args.max_records,
                sleep_between=args.sleep_between,
                naics=args.naics,
                setaside=args.setaside,
            )
            total_upserted_all += up
            total_seen_all += seen
            # brief pause between ZIPs
            time.sleep(max(1, args.sleep_between // 2))

    # Optional cleanup of expired
    if args.delete_expired:
        try:
            delete_expired()
            print("[CLEANUP] removed expired rows")
        except Exception as e:
            print("[WARN] cleanup failed:", e)

    # Update meta: we successfully ran, so next time start from today
    set_meta(META_KEY, today.isoformat())

    print(f"[DONE] upserted={total_upserted_all} seen={total_seen_all} posted_from_used={posted_from}")


# ------------------------- CLI -------------------------

def parse_args(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(description="Fetch SAM.gov opportunities and upsert into Postgres")
    p.add_argument("--zip-file", help="Path to a file of ZIPs (one per line). If omitted, fetches without ZIP filter.", default=None)
    p.add_argument("--days", type=int, default=2, help="How many days back to start (only used if meta has no last_posted_from).")
    p.add_argument("--page-size", type=int, default=10, help="SAM page size (polite small values recommended).")
    p.add_argument("--max-records", type=int, default=300, help="Max records per zip (or overall if no zips).")
    p.add_argument("--sleep-between", type=int, default=8, help="Seconds to sleep between page requests.")
    p.add_argument("--naics", type=str, default=None, help="Optional NAICS filter for fetching (comma-separated). Prefer filtering at the API layer instead.")
    p.add_argument("--setaside", type=str, default=None, help="Optional set-aside filter for fetching (comma-separated). Prefer filtering at the API layer instead.")
    p.add_argument("--delete-expired", action="store_true", help="Delete rows with response_date < today after fetch.")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    try:
        run(args)
        sys.exit(0)
    except Exception as e:
        print("[FATAL]", e)
        sys.exit(1)
