# app/fetcher.py
"""
Fetches SAM.gov opportunities and upserts them into Postgres.

Highlights
- Uses GET https://api.sam.gov/opportunities/v2/search (no /prod)
- Query params (not JSON body) + MM/DD/YYYY date format
- Polite backoff (honors Retry-After, exponential + jitter)
- Incremental sync via small `meta` table (key: last_posted_from)
- Optional ZIP/NAICS/set-aside filters (keep fetch broad to avoid 429s)
- Batching upserts using helpers from app.db

Run (Render Cron example):
  python -m app.fetcher --days 2 --page-size 10 --max-records 300 --sleep-between 8 --delete-expired
  python -m app.fetcher --zip-file app/wa3_zips.txt --days 2 --page-size 10 --max-records 200 --sleep-between 10 --delete-expired

Env:
  SAM_API_KEY   (required)
  DATABASE_URL  (used by app.db)
"""

import argparse
import datetime as dt
import json
import os
import random
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests

from .db import get_conn, ensure_schema, upsert_many, delete_expired

# ------------------------- Constants -------------------------

SAM_ENDPOINT = "https://api.sam.gov/opportunities/v2/search"
META_KEY = "last_posted_from"  # ISO (YYYY-MM-DD) stored in meta

# ------------------------- Meta table helpers -------------------------

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

# ------------------------- HTTP (GET) with backoff -------------------------

def sam_get(url: str, params: dict, tries: int = 8, base_sleep: int = 2) -> requests.Response:
    """
    GET to SAM with polite backoff.
    - Honors Retry-After (seconds) if present
    - Exponential backoff with 0–30% jitter
    """
    for i in range(tries):
        r = requests.get(url, params=params, timeout=60)
        status = r.status_code
        print(f"[SAM] status={status} try={i+1}/{tries} url={r.url}")
        if status == 429:
            ra = r.headers.get("Retry-After")
            try:
                sleep = max(1, int(ra)) if ra else base_sleep * (2 ** i)
            except ValueError:
                sleep = base_sleep * (2 ** i)
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
    Be tolerant of differing field names across variants.
    """
    notice_id = n.get("noticeId") or n.get("id") or n.get("solicitationNumber") or n.get("uiLink") or ""
    title = n.get("title") or ""
    sol = n.get("solicitationNumber") or ""
    posted_date = (n.get("postedDate") or n.get("publishDate") or "")[:10] or None
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
    url = n.get("uiLink") or n.get("link") or n.get("url") or ""
    desc = n.get("description") or ""

    if not notice_id:
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

# ------------------------- Params & paging -------------------------

def iso_to_mmddyyyy(d_iso: str) -> str:
    # "2025-08-13" -> "08/13/2025"
    y, m, d = d_iso.split("-")
    return f"{m}/{d}/{y}"

def build_params(
    api_key: str,
    posted_from_iso: str,
    limit: int,
    offset: int,
    zip_code: Optional[str] = None,
    naics: Optional[str] = None,
    setaside: Optional[str] = None,
) -> dict:
    """
    Start simple: postedFrom + paging. Add filters cautiously.
    Note: param names vary in docs; "zipcode" generally works.
    """
    params = {
        "api_key": api_key,
        "limit": str(limit),
        "offset": str(offset),
        "postedFrom": iso_to_mmddyyyy(posted_from_iso),
    }
    if zip_code:
        params["zipcode"] = zip_code  # if too few results, try placeOfPerformanceZip or popZip
    if naics:
        params["naics"] = naics  # comma-separated
    if setaside:
        params["setAside"] = setaside  # comma-separated or single value
    return params

def fetch_for_zip(
    api_key: str,
    posted_from_iso: str,
    zip_code: Optional[str],
    page_size: int,
    max_records: int,
    sleep_between: int,
    naics: Optional[str],
    setaside: Optional[str],
) -> Tuple[int, int]:
    """
    Fetch pages for a single ZIP (or all if zip_code is None).
    Returns (total_upserted, total_seen)
    """
    total_upserted = 0
    total_seen = 0
    offset = 0

    while total_seen < max_records:
        params = build_params(
            api_key=api_key,
            posted_from_iso=posted_from_iso,
            limit=page_size,
            offset=offset,
            zip_code=zip_code,
            naics=naics,
            setaside=setaside,
        )

        r = sam_get(SAM_ENDPOINT, params)
        try:
            data = r.json()
        except Exception:
            print("[ERR] JSON parse error:", r.text[:500])
            break

        # be flexible about envelope
        result = data.get("result") or data
        items = (
            result.get("searchResults")
            or result.get("opportunitiesData")
            or result.get("data")
            or []
        )
        total_records = result.get("totalRecords") or result.get("totalrecords") or len(items)

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

        time.sleep(sleep_between)

    return total_upserted, total_seen

# ------------------------- CLI runner -------------------------

def load_zips(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        z = [line.strip() for line in f if line.strip()]
    print(f"[CFG] Loaded {len(z)} ZIPs from {path}")
    return z

def run(args) -> None:
    ensure_schema()
    ensure_meta_schema()

    api_key = os.environ.get("SAM_API_KEY")
    if not api_key:
        print("[FATAL] SAM_API_KEY is not set")
        sys.exit(1)

    today = dt.date.today()
    default_from = (today - dt.timedelta(days=args.days)).isoformat()
    posted_from = get_meta(META_KEY) or default_from

    print(f"[RUN] posted_from={posted_from} days={args.days} page_size={args.page_size} "
          f"max_records={args.max_records} sleep_between={args.sleep_between}")

    total_upserted_all = 0
    total_seen_all = 0

    zips: List[str] = []
    if args.zip_file:
        zips = load_zips(args.zip_file)

    if not zips:
        up, seen = fetch_for_zip(
            api_key=api_key,
            posted_from_iso=posted_from,
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
        for i, z in enumerate(zips, 1):
            print(f"[ZIP] {i}/{len(zips)} {z}")
            up, seen = fetch_for_zip(
                api_key=api_key,
                posted_from_iso=posted_from,
                zip_code=z,
                page_size=args.page_size,
                max_records=args.max_records,
                sleep_between=args.sleep_between,
                naics=args.naics,
                setaside=args.setaside,
            )
            total_upserted_all += up
            total_seen_all += seen
            time.sleep(max(1, args.sleep_between // 2))  # tiny pause between ZIPs

    if args.delete_expired:
        try:
            delete_expired()
            print("[CLEANUP] removed expired rows")
        except Exception as e:
            print("[WARN] cleanup failed:", e)

    # move the cursor forward only after a successful run
    set_meta(META_KEY, today.isoformat())

    print(f"[DONE] upserted={total_upserted_all} seen={total_seen_all} posted_from_used={posted_from}")

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
