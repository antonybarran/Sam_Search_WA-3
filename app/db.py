# app/db.py
import os
import psycopg2
import psycopg2.extras

def get_conn():
    """
    Connect to Postgres using the DATABASE_URL environment variable.
    On Render, set this via: Environment -> Add from Database.
    """
    url = os.environ["DATABASE_URL"]
    # Force TLS; Render Postgres requires SSL.
    return psycopg2.connect(url, sslmode="require")

# --------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------
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

-- Helpful indexes for filters used by the API
CREATE INDEX IF NOT EXISTS idx_opps_zip   ON opportunities(zip);
CREATE INDEX IF NOT EXISTS idx_opps_naics ON opportunities(naics);
CREATE INDEX IF NOT EXISTS idx_opps_due   ON opportunities(response_date);
CREATE INDEX IF NOT EXISTS idx_opps_posted ON opportunities(posted_date);
"""

UPSERT_SQL = """
INSERT INTO opportunities (
  id, title, solicitation_number, posted_date, response_date,
  set_aside, naics, org, city, state, zip, url, description, updated_at
)
VALUES (
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

DELETE_EXPIRED_SQL = """
DELETE FROM opportunities
WHERE response_date IS NOT NULL AND response_date < CURRENT_DATE;
"""

def ensure_schema():
    """Create tables/indexes if they don't exist (safe to call repeatedly)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()

def upsert_many(rows):
    """
    Efficiently UPSERT a list of dicts shaped like:
      {
        "id": str,
        "title": str,
        "solicitation_number": str,
        "posted_date": "YYYY-MM-DD" or None,
        "response_date": "YYYY-MM-DD" or None,
        "set_aside": str,
        "naics": str,
        "org": str,
        "city": str,
        "state": str,
        "zip": str,
        "url": str,
        "description": str,
      }
    """
    if not rows:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)
        conn.commit()

def delete_expired():
    """Remove rows whose response date has passed."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DELETE_EXPIRED_SQL)
        conn.commit()
