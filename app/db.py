# app/db.py
import os
import datetime as dt
from typing import List, Dict, Any

import psycopg

# ----------------------------------
# Connection
# ----------------------------------
def get_conn():
    """
    Connect to Postgres using the DATABASE_URL env var.
    On Render this is set automatically when you link the database.
    """
    url = os.environ["DATABASE_URL"]
    return psycopg.connect(url, sslmode="require")


# ----------------------------------
# Schema
# ----------------------------------
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
    """Create tables/indexes if they don't exist (idempotent)."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        conn.commit()


# ----------------------------------
# UPSERT (named placeholders by default)
# ----------------------------------
UPSERT_SQL = """
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


# ----------------------------------
# Helpers
# ----------------------------------
_EXPECTED_ORDER = [
    "id", "title", "solicitation_number", "posted_date", "response_date",
    "set_aside", "naics", "org", "city", "state", "zip", "url", "description"
]

def _coerce_date(v: Any):
    """Return a python date or None from common string/None inputs."""
    if not v:
        return None
    if isinstance(v, dt.date):
        return v
    try:
        # handles strings like "2025-08-17T00:00:00" or "2025-08-17"
        return dt.date.fromisoformat(str(v)[:10])
    except Exception:
        return None


# ----------------------------------
# Upsert many (robust to dict/tuple styles)
# ----------------------------------
def upsert_many(rows: Any):
    """
    Accepts:
      - List[Dict[str, Any]]  (preferred)
      - Dict[str, Any]        (will be wrapped)
    Works with:
      - Named placeholders in UPSERT_SQL ( %(id)s )  <-- default here
      - Positional placeholders in UPSERT_SQL ( %s ) <-- auto-converts dicts to tuples

    Ensures all expected keys exist and dates are proper date objects.
    """
    if not rows:
        return

    # If caller passed a single dict, wrap it
    if isinstance(rows, dict):
        rows = [rows]

    # Normalize & coerce
    norm_rows: List[Dict[str, Any]] = []
    for r in rows:
        r = dict(r)  # copy to avoid mutating caller's object
        r["posted_date"] = _coerce_date(r.get("posted_date"))
        r["response_date"] = _coerce_date(r.get("response_date"))
        for k in _EXPECTED_ORDER:
            r.setdefault(k, None)
        norm_rows.append(r)

    uses_named = "%(" in UPSERT_SQL  # True for %(id)s style

    with get_conn() as conn, conn.cursor() as cur:
        if uses_named:
            # List[Dict] is correct for named-parameter SQL
            cur.executemany(UPSERT_SQL, norm_rows)
        else:
            # Convert dicts -> tuples in the correct column order for %s SQL
            seq_rows = [tuple(r[k] for k in _EXPECTED_ORDER) for r in norm_rows]
            cur.executemany(UPSERT_SQL, seq_rows)
        conn.commit()


def delete_expired():
    """Remove rows whose response_date is strictly before today."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            DELETE FROM opportunities
            WHERE response_date IS NOT NULL
              AND response_date < CURRENT_DATE;
        """)
        conn.commit()


# Optional quick CLI sanity check
if __name__ == "__main__":
    ensure_schema()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM opportunities;")
        print({"opportunities_count": cur.fetchone()[0]})
