from fastapi import FastAPI, Query, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
from datetime import date
import os

from .db import get_conn, ensure_schema, delete_expired

# -----------------------------------------------------------------------------
# FastAPI app setup
# -----------------------------------------------------------------------------
app = FastAPI(title="WA-03 SAM Opportunities API", version="1.0.0")

# Open CORS so Squarespace can fetch this API.
# If you want to lock it down, replace ["*"] with ["https://<your-squarespace-domain>"].
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Ensure schema exists on cold start (safe no-op if already created)
try:
    ensure_schema()
except Exception as e:
    # Don't crash app on startup if DB isn't ready yet; /health will reflect status.
    pass

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

@app.get("/health")
def health():
    """Simple liveness check + DB reachability."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
        db_ok = True
    except Exception:
        db_ok = False
    return {"ok": True, "db": db_ok}

@app.get("/opps")
def list_opps(
    active: bool = True,
    naics: Optional[str] = Query(None, description="Comma-separated NAICS filter, substring match."),
    keyword: Optional[str] = Query(None, description="Search in title/description, case-insensitive."),
    zip: Optional[str] = Query(None, description="Exact ZIP code, 5-digit."),
    setaside: Optional[str] = Query(None, description="Set-aside filter (e.g., 'Total Small Business', '8A')."),
    sort: str = Query("due_then_posted", description="due_then_posted | posted_desc"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Read-only list of WA-03 small-business opportunities we stored via the nightly fetcher.

    Example:
      /opps?active=true&naics=236220&keyword=bridge&zip=98661&limit=50
    """
    clauses = []
    params = {}

    if active:
        # show items with a due date today or later, or no due date
        clauses.append("(response_date IS NULL OR response_date >= CURRENT_DATE)")

    if naics:
        # simple substring match across the naics text column
        clauses.append("naics ILIKE %(naics)s")
        params["naics"] = f"%{naics}%"

    if keyword:
        clauses.append("(title ILIKE %(kw)s OR description ILIKE %(kw)s)")
        params["kw"] = f"%{keyword}%"

    if zip:
        clauses.append("zip = %(zip)s")
        params["zip"] = zip

    if setaside:
        clauses.append("set_aside ILIKE %(sa)s")
        params["sa"] = f"%{setaside}%"

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    if sort == "posted_desc":
        order_sql = "ORDER BY posted_date DESC NULLS LAST"
    else:
        # default: by due date ascending (soonest first), then newest posted
        order_sql = "ORDER BY response_date NULLS LAST, posted_date DESC NULLS LAST"

    sql = f"""
      SELECT
        id,
        title,
        solicitation_number,
        posted_date,
        response_date,
        set_aside,
        naics,
        org            AS "organizationName",
        city           AS "place_city",
        state          AS "place_state",
        zip            AS "place_zip",
        url            AS "website",
        description
      FROM opportunities
      {where_sql}
      {order_sql}
      LIMIT %(limit)s OFFSET %(offset)s;
    """

    params["limit"] = limit
    params["offset"] = offset

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d.name for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    return rows

# -----------------------------------------------------------------------------
# (Optional) Manual maintenance endpoints
# -----------------------------------------------------------------------------

@app.post("/maintenance/cleanup")
def cleanup_expired(
    admin_token: Optional[str] = Header(None, alias="X-Admin-Token"),
):
    """
    Deletes expired rows (response_date < today).
    This is safe to call; the cron fetcher also does this each run.

    Secure it by setting ADMIN_TOKEN in your Render env and passing
    X-Admin-Token: <the same token> when you call this endpoint.
    """
    required = os.environ.get("ADMIN_TOKEN")
    if required:
        if not admin_token or admin_token != required:
            raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        delete_expired()
        return {"ok": True, "deleted": "completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))