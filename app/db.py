# app/db.py
import os
import json
import datetime as dt
from typing import List, Dict, Any

import psycopg


def get_conn():
    url = os.environ["DATABASE_URL"]
    return psycopg.connect(url, sslmode="require")


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS opportunities (
  id TEXT PRIMARY KEY,
  title TEXT,
  solicitation_number TEXT,
  posted_date DATE,
  response_date DATE,
  set_aside TEXT,
  naics TEXT,
  org TEXT,
  city TEXT,
  state TEXT,
  zip TEXT,
  url TEXT,
  description TEXT,
  inserted_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_opps_zip     ON opportunities(zip);
CREATE INDEX IF NOT EXISTS idx_opps_naics   ON opportunities(naics);
CREATE INDEX IF NOT EXISTS idx_opps_due     ON opportunities(response_date);
CREATE INDEX IF NOT EXISTS idx_opps_posted  ON opportunities(posted_date);
"""

def ensure_schema():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        conn.commit()


# --- Two UPSERT variants: named & positional ---
UPSERT_SQL_NAMED = """
INSERT INTO opportunities (
    id, title, solicitation_number, posted_date, response_date,
    set_aside, naics, org, city, state, zip, url, description, updated_at
) VALUES (
    %(id)s, %(title)s, %(solicitation_number)s, %(posted_date)s, %(response_date)s,
    %(set_aside)s, %(naics)s, %(org)s, %(city)s, %(state)s, %(zip)s, %(url)s, %(description)s, NOW()
)
ON CONFLICT (id) DO UPDATE SET
    title               = EXCLUDED.title,
    solicitation_number = EXCLUDED.solicitation_number,
    posted_date         = EXCLUDED.posted_date,
    response_date       = EXCLUDED.response_date,
    set_aside           = EXCLUDED.set_aside,
    naics               = EXCLUDED.naics,
    org                 = EXCLUDED.org,
    city                = EXCLUDED.city,
    state               = EXCLUDED.state,
    zip                 = EXCLUDED.zip,
    url                 = EXCLUDED.url,
    description         = EXCLUDED.description,
    updated_at          = NOW();
"""

UPSERT_SQL_POS = """
INSERT INTO opportunities (
    id, title, solicitation_number, posted_date, response_date,
    set_aside, naics, org, city, state, zip, url, description, updated_at
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, NOW()
)
ON CONFLICT (id) DO UPDATE SET
    title               = EXCLUDED.title,
    solicitation_number = EXCLUDED.solicitation_number,
    posted_date         = EXCLUDED.posted_date,
    response_date       = EXCLUDED.response_date,
    set_aside           = EXCLUDED.set_aside,
    naics               = EXCLUDED.naics,
    org                 = EXCLUDED.org,
    city                = EXCLUDED.city,
    state               = EXCLUDED.state,
    zip                 = EXCLUDED.zip,
    url                 = EXCLUDED.url,
    description         = EXCLUDED.description,
    updated_at          = NOW();
"""

_EXPECTED_ORDER = [
    "id", "title", "solicitation_number", "posted_date", "response_date",
    "set_aside", "naics", "org", "city", "state", "zip", "url", "description"
]


def _coerce_date(v: Any):
    if not v:
        return None
    if isinstance(v, dt.date):
        return v
    try:
        return dt.date.fromisoformat(str(v)[:10])
    except Exception:
        return None


def _scalarize(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool, dt.date, dt.datetime)):
        return v
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


def _normalize_rows(rows: Any) -> List[Dict[str, Any]]:
    if not rows:
        return []
    if isinstance(rows, dict):
        rows = [rows]
    out: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        rr["posted_date"] = _coerce_date(rr.get("posted_date"))
        rr["response_date"] = _coerce_date(rr.get("response_date"))
        for k in _EXPECTED_ORDER:
            if k not in ("posted_date", "response_date"):
                rr[k] = _scalarize(rr.get(k))
            else:
                rr.setdefault(k, None)
        for k in _EXPECTED_ORDER:
            rr.setdefault(k, None)
        out.append(rr)
    return out


def upsert_many(rows: Any):
    """
    Robust UPSERT:
      1) Try named parameters with dict rows.
      2) If the driver complains about '%s' / dict adaptation, fall back to positional SQL with tuples.
    """
    norm = _normalize_rows(rows)
    if not norm:
        return

    with get_conn() as conn, conn.cursor() as cur:
        try:
            # Attempt named style first
            cur.executemany(UPSERT_SQL_NAMED, norm)
            conn.commit()
            return
        except Exception as e:
            # Fall back to positional if named failed due to dict/%s mismatch
            # (or any other adaptation complaint)
            conn.rollback()
            tuples = [tuple(r[k] for k in _EXPECTED_ORDER) for r in norm]
            cur.executemany(UPSERT_SQL_POS, tuples)
            conn.commit()


def delete_expired():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            DELETE FROM opportunities
            WHERE response_date IS NOT NULL
              AND response_date < CURRENT_DATE;
        """)
        conn.commit()


if __name__ == "__main__":
    ensure_schema()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM opportunities;")
        print({"opportunities_count": cur.fetchone()[0]})
