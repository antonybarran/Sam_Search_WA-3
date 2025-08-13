import os, time, random, json, argparse
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Set
import requests

from .db import ensure_schema, upsert_many, delete_expired

API_URL = "https://api.sam.gov/opportunities/v2/search"
API_KEY_ENV = "SAM_API_KEY"

SB_CODES = {"TOTAL SMALL BUSINESS","PARTIAL SMALL BUSINESS","8A","WOSB","EDWOSB","SDVOSB","HUBZONE"}

def normalize_zip(z: str) -> Optional[str]:
    if not z: return None
    digits = "".join(ch for ch in str(z) if ch.isdigit())
    return digits[:5] if len(digits)>=5 else None

def extract_zip(n: Dict[str,Any]) -> Optional[str]:
    pops = n.get("placeOfPerformance") or {}
    z = pops.get("zip") or pops.get("postalCode") or (pops.get("address") or {}).get("postalCode")
    return normalize_zip(z)

def get_set_aside(n: Dict[str,Any]) -> str:
    s = str(n.get("typeOfSetAside") or n.get("setAside") or "").upper()
    # quick normalize
    rep = [("SET-ASIDE",""),("SET ASIDE",""),("PROGRAM",""),("FOR",""),("FAR",""),("SUBPART","")]
    for a,b in rep: s=s.replace(a,b)
    s=" ".join(s.split())
    if "WOSB" in s or "WOMEN" in s: return "WOSB"
    if "EDWOSB" in s or "ECONOMICALLY" in s: return "EDWOSB"
    if "SDVOSB" in s or "SERVICE DISABLED" in s: return "SDVOSB"
    if "HUBZONE" in s: return "HUBZONE"
    if "8(A)" in s or "8A" in s: return "8A"
    if "TOTAL SMALL" in s or s=="SMALL BUSINESS": return "TOTAL SMALL BUSINESS"
    if "PARTIAL SMALL" in s: return "PARTIAL SMALL BUSINESS"
    return s

def mmddyyyy(dt: datetime) -> str:
    return dt.strftime("%m/%d/%Y")

def build_params(d0: datetime, d1: datetime, state: str, page_size: int, offset: int,
                 keywords: Optional[List[str]], naics_one: Optional[str]) -> Dict[str,Any]:
    p = {
        "postedFrom": mmddyyyy(d0),
        "postedTo": mmddyyyy(d1),
        "limit": page_size,
        "offset": offset,
        "state": state
    }
    if keywords: p["title"] = " OR ".join([k for k in keywords if k])
    if naics_one: p["ncode"] = naics_one
    return p

def fetch_window(api_key: str, d: datetime, states: List[str], zips: Set[str],
                 keywords: Optional[List[str]], naics: Optional[List[str]],
                 page_size: int, sleep_between: float, max_records: int) -> List[Dict[str,Any]]:
    session = requests.Session()
    session.headers.update({"User-Agent":"WA3-SB-Search/1.0"})
    session.trust_env = False

    collected: List[Dict[str,Any]] = []
    seen = set()
    naics_list = naics or [None]
    for st in states:
        for nc in naics_list:
            offset = 0
            while True:
                if len(collected) >= max_records: break
                params = build_params(d, d, st, page_size, offset, keywords, nc)
                params["api_key"] = api_key
                time.sleep(sleep_between + random.uniform(0,1.2))
                r = session.get(API_URL, params=params, timeout=60)
                if r.status_code == 429:
                    # honor Retry-After or backoff
                    wait = float(r.headers.get("Retry-After", 20))
                    time.sleep(min(wait, 180))
                    continue
                r.raise_for_status()
                data = r.json()
                batch = data.get("opportunitiesData") or data.get("data") or data.get("notices") or []
                if not batch: break
                added=0
                for n in batch:
                    nid = n.get("noticeId") or n.get("id") or f"{n.get('title','')}-{n.get('postedDate','')}"
                    if nid in seen: continue
                    seen.add(nid)
                    # SB set-aside + ZIP whitelist
                    if get_set_aside(n) not in SB_CODES: continue
                    z = extract_zip(n)
                    if not z or z not in zips: continue
                    collected.append(n); added+=1
                    if len(collected) >= max_records: break
                if added==0 or len(batch)<page_size: break
                offset += page_size
    return collected

def to_rows(notices: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    rows=[]
    for n in notices:
        pops = n.get("placeOfPerformance") or {}
        addr = pops.get("address") or {}
        rows.append({
            "id": n.get("noticeId") or n.get("id"),
            "title": n.get("title"),
            "solicitation_number": n.get("solicitationNumber"),
            "posted_date": (n.get("postedDate") or "")[:10],
            "response_date": (n.get("responseDate") or n.get("response_deadline") or "")[:10],
            "set_aside": get_set_aside(n),
            "naics": ",".join(n.get("naics") or []) if isinstance(n.get("naics"), list) else (n.get("naics") or ""),
            "org": n.get("organizationName"),
            "city": pops.get("city") or addr.get("city"),
            "state": pops.get("state") or pops.get("stateCode") or addr.get("state"),
            "zip": extract_zip(n),
            "url": n.get("website") or n.get("baseURL"),
            "description": (n.get("description") or "")[:2000],
        })
    return rows

def run(days:int, states:List[str], zip_path:str, page_size:int, max_records:int, sleep_between:float,
        keywords:Optional[List[str]], naics:Optional[List[str]]):
    api_key = os.environ[API_KEY_ENV]
    ensure_schema()
    delete_expired()
    with open(zip_path, "r", encoding="utf-8") as f:
        zips = {normalize_zip(line) for line in f if normalize_zip(line)}
    now = datetime.now(timezone.utc)
    all_items=[]
    for i in range(days):
        d = now - timedelta(days=i)
        items = fetch_window(api_key, d, states, zips, keywords, naics, page_size, sleep_between, max_records)
        all_items.extend(items)
    rows = to_rows(all_items)
    if rows:
        upsert_many(rows)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip-file", type=str, default="app/wa3_zips.txt")
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--states", type=str, default="WA")
    ap.add_argument("--page-size", type=int, default=10)
    ap.add_argument("--max-records", type=int, default=400)
    ap.add_argument("--sleep-between", type=float, default=8.0)
    ap.add_argument("--keywords", type=str, default="")
    ap.add_argument("--naics", type=str, default="")
    args = ap.parse_args()
    kw = [k.strip() for k in args.keywords.split(",") if k.strip()] or None
    nc = [n.strip() for n in args.naics.split(",") if n.strip()] or None
    run(args.days, [s.strip() for s in args.states.split(",") if s.strip()],
        args.zip_file, args.page_size, args.max_records, args.sleep_between, kw, nc)