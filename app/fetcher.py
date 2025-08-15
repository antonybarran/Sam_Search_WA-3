# app/fetcher.py
"""
Fetches SAM.gov opportunities and upserts them into Postgres.

- GET https://api.sam.gov/opportunities/v2/search  (no /prod)
- Requires postedFrom & postedTo (MM/DD/YYYY)
- Polite backoff (handles 429/5xx)
- Tracks last postedFrom in meta for incremental sync
- Maps API records -> DB schema via shape_row()
"""

import argparse
import datetime as dt
import os
import random
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests
from .db import get_conn, ensure_schema, upsert_many, delete_expired

SAM_ENDPOINT = "https://api.sam.gov/opportunities/v2/search"
META_KEY = "last_posted_from"  # stored as ISO YYYY-MM-DD
DATE_FMT = "%m/%d/%Y"

# ------------------------- meta helpers -------------------------

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
        cur.execute("SELECT value FROM meta WHERE key=%s LIMIT 1;", (key,))
        row = cur.fetchone()
        return row[0] if row else None

def set_meta(key: str, value: str) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW();
            """,
            (key, value),
        )
        conn.commit()

# ------------------------- HTTP with backoff -------------------------

def sam_get(url: str, params: dict, tries: int = 8, base_sleep: int = 2) -> requests.Response:
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

# ------------------------- shaping -------------------------

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
    Map a SAM notice dict -> DB row dict expected by upsert_many().
    Handles small field-name variations defensively.
    """
    notice_id = (
        n.get("noticeId")
        or n.get("id")
        or n.get("solicitationNumber")
        or n.get("uiLink")
        or ""
    )
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
        # fallback identifier to avoid NULL PK
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

# ------------------------- params & paging -------------------------

def iso_to_mmddyyyy(d_iso: str) -> str:
    y, m, d = d_iso.split("-")
    return f"{m}/{d}/{y}"

def today_mmddyyyy() -> str:
    return dt.date.today().strftime(DATE_FMT)

def build_params(
    api_key: str,
    posted_from_iso: str,
    limit: int,
    offset: int,
    zip_code: Optional[str] = None,
    naics: Optional[str] = None,
    setaside: Optional[str] = None,
) -> dict:
    params = {
        "api_key": api_key,
        "postedFrom": iso_to_mmddyyyy(posted_from_iso),
        "postedTo": today_mmddyyyy(),  # SAM requires both
        "limit": str(limit),
        "offset": str(offset),
    }
    if zip_code:
        params["zipcode"] = zip_code  # if needed, try placeOfPerformanceZip or popZip
    if naics:
        params["naics"] = naics
    if setaside:
        params["setAside"] = setaside
    return params

def fetch_page(
    api_key: str,
    posted_from_iso: str,
    page_size: int,
    offset: int,
    zip_code: Optional[str],
    naics: Optional[str],
    setaside: Optional[str],
) -> Tuple[List[Dict], int]:
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
    data = r.json()

    # SAM payloads vary; try common envelopes
    result = data.get("result") or data
    items = (
        result.get("opportunitiesData")
        or result.get("searchResults")
        or result.get("data")
        or []
    )
    total_records = (
        result.get("totalRecords")
        or result.get("totalrecords")
        or len(items)
    )
    return items, int(total_records)

# ------------------------- runner -------------------------

def run(args) -> None:
    ensure_schema()
    ensure_meta_schema()

    api_key = os.environ.get("SAM_API_KEY")
    if not api_key:
        print("[FATAL] SAM_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    # figure out postedFrom/postedTo
    today = dt.date.today()
    posted_to_iso = today.isoformat()

    last = get_meta(META_KEY)
    if last:
        start_iso = (dt.date.fromisoformat(last) + dt.timedelta(days=1)).isoformat()
    else:
        start_iso = (today - dt.timedelta(days=args.days)).isoformat()

    print(f"[RUN] posted_from={start_iso} posted_to={posted_to_iso} page_size={args.page_size} max_records={args.max_records}")

    total_seen = 0
    total_upserted = 0
    offset = 0

    while total_seen < args.max_records:
        items, total_records = fetch_page(
            api_key=api_key,
            posted_from_iso=start_iso,
            page_size=args.page_size,
            offset=offset,
            zip_code=None,         # add zip filtering later if desired
            naics=None,            # can add comma-separated NAICS
            setaside=None,         # can add set-aside filter
        )
        print(f"[PAGE] offset={offset} got={len(items)} totalRecords={total_records}")
        if not items:
            break

        rows = [shape_row(n) for n in items]
        upsert_many(rows)

        total_seen += len(items)
        total_upserted += len(rows)
        offset += args.page_size

        if total_seen >= args.max_records:
            break

        time.sleep(args.sleep_between)

    # advance high-water mark only after successful run
    set_meta(META_KEY, posted_to_iso)
    print(f"[DONE] upserted={total_upserted} seen={total_seen} new_cursor={posted_to_iso}")

    if args.delete_expired:
        try:
            delete_expired()
            print("[CLEANUP] expired removed")
        except Exception as e:
            print("[WARN] cleanup failed:", e)

def parse_args():
    p = argparse.ArgumentParser(description="Fetch SAM.gov opportunities and store in Postgres.")
    p.add_argument("--days", type=int, default=2)
    p.add_argument("--page-size", type=int, default=10)
    p.add_argument("--max-records", type=int, default=300)
    p.add_argument("--sleep-between", type=int, default=8)
    p.add_argument("--delete-expired", action="store_true")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        run(args)
        sys.exit(0)
    except Exception as e:
        print("[FATAL]", e)
        sys.exit(1)
