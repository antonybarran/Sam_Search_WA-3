# app/db.py
import os
import datetime as dt
import psycopg
from typing import List, Dict

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

def _coerce_date(v):
    if not v:
        return None
    if isinstance(v, dt.date):
        return v
    try:
        return dt.date.fromisoformat(str(v)[:10])
    except Exception:
        return None

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

def upsert_many(rows: List[Dict]):
    if not rows:
        return
    # normalize/ensure keys
    expected = ("id","title","solicitation_number","posted_date","response_date",
                "set_aside","naics","org","city","state","zip","url","description")
    for r in rows:
        r["posted_date"] = _coerce_date(r.get("posted_date"))
        r["response_date"] = _coerce_date(r.get("response_date"))
        for k in expected:
            r.setdefault(k, None)

    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
        conn.commit()

def delete_expired():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM opportunities WHERE response_date IS NOT NULL AND response_date < CURRENT_DATE;")
        conn.commit()
