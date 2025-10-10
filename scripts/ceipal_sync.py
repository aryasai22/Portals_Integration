# ceipal_sync.py
from __future__ import annotations

import os
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv


# ===================== logging =====================
def setup_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
log = logging.getLogger("ceipal")


# ===================== env / constants =====================
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"), override=True)

BASE = (os.getenv("CEIPAL_BASE_URL") or "https://api.ceipal.com").rstrip("/")
EMAIL = (os.getenv("CEIPAL_EMAIL") or "").strip()
PASSWORD = (os.getenv("CEIPAL_PASSWORD") or "").strip()
API_KEY = (os.getenv("CEIPAL_API_KEY") or "").strip()

SEED_ACCESS = (os.getenv("CEIPAL_AUTH_TOKEN") or "").strip()
SEED_REFRESH = (os.getenv("CEIPAL_REFRESH_TOKEN") or "").strip()

ACCESS_TTL_DEFAULT = int(os.getenv("CEIPAL_ACCESS_TTL_SECS", "3300"))
REFRESH_TTL_DEFAULT = int(os.getenv("CEIPAL_REFRESH_TTL_SECS", "604800"))

OUT_DIR = Path("output"); OUT_DIR.mkdir(exist_ok=True)
TOKEN_CACHE = OUT_DIR / "ceipal_token.json"

# endpoint discovery cache
EP_CACHE_FILE = OUT_DIR / "endpoint_cache.json"
try:
    _EP_CACHE = json.loads(EP_CACHE_FILE.read_text(encoding="utf-8"))
except Exception:
    _EP_CACHE = {}


# ===================== HTTP session =====================
def build_session(total_retries: int = 6, backoff_factor: float = 1.2) -> requests.Session:
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ===================== rate limiter =====================
class RateLimiter:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.01)
        self._lock = Lock()
        self._next = 0.0
    def wait(self):
        with self._lock:
            now = time.monotonic()
            if self._next > now:
                time.sleep(self._next - now)
            self._next = time.monotonic() + self.min_interval


# ===================== token manager =====================
class TokenManager:
    def __init__(self):
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.access_issued_at: Optional[float] = None
        self.refresh_issued_at: Optional[float] = None
        self.access_ttl = ACCESS_TTL_DEFAULT
        self.refresh_ttl = REFRESH_TTL_DEFAULT
        self._load_cache()

        now = time.time()
        if not self.access_token and SEED_ACCESS:
            self.access_token = SEED_ACCESS if SEED_ACCESS.lower().startswith("bearer ") else f"Bearer {SEED_ACCESS}"
            self.access_issued_at = now
        if not self.refresh_token and SEED_REFRESH:
            self.refresh_token = SEED_REFRESH
            self.refresh_issued_at = now

    def _load_cache(self):
        if TOKEN_CACHE.exists():
            try:
                data = json.loads(TOKEN_CACHE.read_text(encoding="utf-8"))
                self.access_token = data.get("access_token")
                self.refresh_token = data.get("refresh_token")
                self.access_issued_at = data.get("access_issued_at")
                self.refresh_issued_at = data.get("refresh_issued_at")
                self.access_ttl = int(data.get("access_ttl", self.access_ttl))
                self.refresh_ttl = int(data.get("refresh_ttl", self.refresh_ttl))
            except Exception:
                pass

    def _save_cache(self):
        TOKEN_CACHE.write_text(json.dumps({
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "access_issued_at": self.access_issued_at,
            "refresh_issued_at": self.refresh_issued_at,
            "access_ttl": self.access_ttl,
            "refresh_ttl": self.refresh_ttl,
        }, indent=2), encoding="utf-8")

    def _now(self) -> float: return time.time()

    def _access_expiring(self, skew: int = 300) -> bool:
        if not self.access_token or not self.access_issued_at or self.access_ttl <= 0: return True
        return (self.access_issued_at + self.access_ttl - skew) <= self._now()

    def _refresh_expiring(self, skew: int = 3600) -> bool:
        if not self.refresh_token or not self.refresh_issued_at or self.refresh_ttl <= 0: return True
        return (self.refresh_issued_at + self.refresh_ttl - skew) <= self._now()

    def auth_headers(self) -> Dict[str, str]:
        self.ensure_valid()
        return {"Authorization": self.access_token, "Content-Type": "application/json", "Accept": "application/json"}

    def ensure_valid(self):
        if not self.access_token:
            self._create(); return
        if self._access_expiring():
            if not self._refresh():
                self._create()
        elif self._refresh_expiring():
            if not self._refresh():
                self._create()

    def handle_expired_and_retry(self, call, *args, **kwargs):
        try:
            return call(*args, **kwargs)
        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            body = (resp.text if resp is not None else "") or ""
            code = getattr(resp, "status_code", None)
            if code in (401, 403) and "expired" in body.lower():
                log.info("[auth] expired; refreshing and retrying once")
                if self._refresh():
                    return call(*args, **kwargs)
                self._create()
                return call(*args, **kwargs)
            raise

    def _create(self):
        for k, v in {"CEIPAL_EMAIL": EMAIL, "CEIPAL_PASSWORD": PASSWORD, "CEIPAL_API_KEY": API_KEY}.items():
            if not v:
                raise RuntimeError(f"Missing {k} in .env")
        s = build_session()
        files = {
            "email": (None, EMAIL),
            "password": (None, PASSWORD),
            "api_key": (None, API_KEY),
            "json": (None, ""),  # required by CEIPAL to emit JSON
        }
        r = s.post(f"{BASE}/wf/v1/createAuthtoken", files=files, headers={"Accept": "application/json"}, timeout=(60, 120))
        if r.status_code >= 400:
            raise RuntimeError(f"[auth] create failed {r.status_code}: {r.text[:400]}")
        try:
            data = r.json()
        except Exception:
            raise RuntimeError(f"[auth] non-JSON: {r.text[:400]}")
        at = data.get("access_token") or data.get("token") or ""
        if not at: raise RuntimeError(f"[auth] missing access_token: {data}")
        self.access_token = at if at.lower().startswith("bearer ") else f"Bearer {at}"
        self.refresh_token = data.get("refresh_token") or None
        now = self._now()
        self.access_issued_at = now; self.refresh_issued_at = now
        if str(data.get("expires_in","")).isdigit(): self.access_ttl = int(data["expires_in"])
        if str(data.get("refresh_expires_in","")).isdigit(): self.refresh_ttl = int(data["refresh_expires_in"])
        self._save_cache()
        log.info("[auth] acquired new access token")

    def _refresh(self) -> bool:
        if not self.refresh_token: return False
        s = build_session()
        r = s.post(f"{BASE}/wf/v1/refreshToken",
                   json={"refresh_token": self.refresh_token, "json": ""},
                   headers={"Accept": "application/json"}, timeout=(60,120))
        if r.status_code >= 400:
            r = s.post(f"{BASE}/wf/v1/refreshToken",
                       files={"refresh_token": (None, self.refresh_token), "json": (None, "")},
                       headers={"Accept": "application/json"}, timeout=(60,120))
        if r.status_code >= 400:
            log.warning("[auth] refresh failed %s: %s", r.status_code, r.text[:400])
            return False
        try:
            data = r.json()
        except Exception:
            log.warning("[auth] refresh non-JSON: %s", r.text[:400]); return False
        at = data.get("access_token") or data.get("token") or ""
        if not at: log.warning("[auth] refresh missing access_token: %s", data); return False
        self.access_token = at if at.lower().startswith("bearer ") else f"Bearer {at}"
        if data.get("refresh_token"):
            self.refresh_token = data["refresh_token"]; self.refresh_issued_at = self._now()
        self.access_issued_at = self._now()
        if str(data.get("expires_in","")).isdigit(): self.access_ttl = int(data["expires_in"])
        if str(data.get("refresh_expires_in","")).isdigit(): self.refresh_ttl = int(data["refresh_expires_in"])
        self._save_cache()
        log.info("[auth] refreshed access token")
        return True

TOKEN = TokenManager()


# ===================== endpoint resolver =====================
def _save_ep_cache():
    try:
        EP_CACHE_FILE.write_text(json.dumps(_EP_CACHE, indent=2), encoding="utf-8")
    except Exception:
        pass

def resolve_endpoint(list_variants: List[str],
                     session: requests.Session,
                     params: Dict[str, Any],
                     connect_timeout: float,
                     read_timeout: float) -> str:
    cache_key = BASE + "::" + "|".join(list_variants)
    if cache_key in _EP_CACHE:
        return _EP_CACHE[cache_key]

    for rel in list_variants:
        url = f"{BASE}{rel}"
        def _do():
            return session.get(url, params=params, headers=TOKEN.auth_headers(),
                               timeout=(connect_timeout, read_timeout))
        r = TOKEN.handle_expired_and_retry(_do)
        if 200 <= r.status_code < 300 and "json" in (r.headers.get("Content-Type","").lower()):
            _EP_CACHE[cache_key] = url
            _save_ep_cache()
            log.info("[discover] using endpoint: %s", url)
            return url
    raise requests.HTTPError(f"None of the endpoint variants worked: {list_variants}")


# ===================== resource catalog =====================
# Set pagination to "auto" so we can adapt to:
# - next-link style (has "next")
# - page/num_pages style (accepts page=, returns num_pages/page_number)
# - offset/count style (limit/offset + count)
RESOURCE_CATALOG: Dict[str, Dict[str, Any]] = {
    "employees": {
        "list_url": f"{BASE}/wf/v1/getEmployees",
        "pagination": "auto",
        "list_params": lambda ns: {
            "limit": ns.limit,
            "sortorder": ns.sortorder,
            "sortby": ns.sortby,
        },
        "result_key": "results",
        "detail": {
            "url_tmpls": [
                f"{BASE}/wf/v1/getEmployeesDetail/{{id}}",
                f"{BASE}/wf/v1/getEmployeesDetail/{{id}}/{{id2}}",
            ],
            "id_keys": ["id", "employee_id", "EMPLOYEE_ID"],
            "id2_keys": ["id2", "encrypted_user_id", "user_encrypted_id", "alt_id"],
        },
    },

    "projects": {
        "list_variants": [
            "/wf/v1/getProjectsList",
            "/wf/v1/getProjectList",
            "/wf/v1/getProjects",
            "/wf/v1/getProjectLists",
        ],
        "pagination": "auto",
        "list_params": lambda ns: {
            "limit": ns.limit,
            "offset": ns.offset,
        },
        "result_key": "results",
        "detail": {
            "url_tmpls": [f"{BASE}/wf/v1/getProjectDetails?project_id={{id}}"],
            "id_keys": ["id", "project_id", "PROJECT_ID"],
        },
    },

    "placements": {
        "list_variants": [
            "/wf/v1/getPlacements",          # your tenant’s working endpoint
            "/wf/v1/getPlacementLists",
            "/wf/v1/getPlacementsList",
            "/wf/v1/getPlacementsLists",
            "/wf/v1/getPlacementList",
            "/wf/v1/getPlacement",
        ],
        "pagination": "auto",
        "list_params": lambda ns: {
            "limit": ns.limit,
            "offset": ns.offset,
            "sortorder": ns.sortorder,
            "sortby": ns.sortby,             # lets you pass --sortby job_code
        },
        "result_key": "results",
        "detail": {
            "url_tmpls": [f"{BASE}/wf/v1/getPlacementsDetails?placement_id={{id}}"],
            "id_keys": ["id", "placement_id", "PLACEMENT_ID"],
        },
    },

    "timesheets": {
        "list_variants": [
            "/wf/v1/getTimesheetsList",
            "/wf/v1/getTimesheetList",
            "/wf/v1/getTimesheets",
        ],
        "pagination": "auto",
        "list_params": lambda ns: {
            "limit": ns.limit,
            "offset": ns.offset,
            "from_date": ns.from_date,
            "to_date": ns.to_date,
        },
        "result_key": "results",
        "detail": None,
    },

    "expenses": {
        "list_variants": [
            "/wf/v1/getExpensesList",
            "/wf/v1/getExpenseList",
            "/wf/v1/getExpenses",
        ],
        "pagination": "auto",
        "list_params": lambda ns: {
            "limit": ns.limit,
            "offset": ns.offset,
            "from_date": ns.from_date,
            "to_date": ns.to_date,
        },
        "result_key": "results",
        "detail": {
            "url_tmpls": [f"{BASE}/wf/v1/getExpenseDetails?expense_id={{id}}"],
            "id_keys": ["id", "expense_id", "EXPENSE_ID"],
        },
    },

    "invoices": {
        "list_variants": [
            "/wf/v1/getInvoices",
            "/wf/v1/getInvoicesList",
            "/wf/v1/getInvoiceList",
        ],
        "pagination": "auto",
        "list_params": lambda ns: {
            "limit": ns.limit,
            "offset": ns.offset,
            "from_date": ns.from_date,
            "to_date": ns.to_date,
        },
        "result_key": "results",
        "detail": {
            "url_tmpls": [f"{BASE}/wf/v1/getInvoiceDetails?invoice_id={{id}}"],
            "id_keys": ["id", "invoice_id", "INVOICE_ID"],
        },
    },

    "clients": {
        "list_variants": [
            "/wf/v1/getClientsList",
            "/wf/v1/getClientList",
            "/wf/v1/getClients",
        ],
        "pagination": "auto",
        "list_params": lambda ns: {
            "limit": ns.limit,
            "offset": ns.offset,
            "sortorder": ns.sortorder,
        },
        "result_key": "results",
        "detail": {
            "url_tmpls": [f"{BASE}/wf/v1/getClientDetails?client_id={{id}}"],
            "id_keys": ["id", "client_id", "CLIENT_ID"],
        },
    },

    "vendors": {
        "list_variants": [
            "/wf/v1/getVendorsList",
            "/wf/v1/getVendorList",
            "/wf/v1/getVendors",
        ],
        "pagination": "auto",
        "list_params": lambda ns: {
            "limit": ns.limit,
            "offset": ns.offset,
            "sortorder": ns.sortorder,
        },
        "result_key": "results",
        "detail": {
            "url_tmpls": [f"{BASE}/wf/v1/getVendorDetails?vendor_id={{id}}"],
            "id_keys": ["id", "vendor_id", "VENDOR_ID"],
        },
    },

    "ess_tickets": {
        "list_variants": [
            "/wf/v1/getESSTickets",
            "/wf/v1/getESSTicketsList",
        ],
        "pagination": "auto",
        "list_params": lambda ns: {
            "limit": ns.limit,
            "offset": ns.offset,
            "status": ns.status,
        },
        "result_key": "results",
        "detail": {
            "url_tmpls": [f"{BASE}/wf/v1/getESSDetails?ticket_id={{id}}"],
            "id_keys": ["id", "ticket_id", "TICKET_ID"],
        },
    },

    "countries": {
        "list_variants": [
            "/wf/v1/getCountriesList",
            "/wf/v1/getCountryList",
        ],
        "pagination": "auto",
        "list_params": lambda ns: {},
        "result_key": "results",
        "detail": None,
    },

    "states": {
        "list_variants": [
            "/wf/v1/getStatesList",
            "/wf/v1/getStateList",
        ],
        "pagination": "auto",
        "list_params": lambda ns: {"country_id": ns.country_id},
        "result_key": "results",
        "detail": None,
    },
}



# ===================== helpers =====================
def _session_authed(retries: int) -> requests.Session:
    return build_session(total_retries=retries, backoff_factor=1.2)

def _get_json(call) -> Dict[str, Any]:
    r = TOKEN.handle_expired_and_retry(call)
    if r.status_code >= 400:
        log.error("HTTP %s %s :: %s", r.status_code, r.url, r.text[:300]); r.raise_for_status()
    try:
        return r.json()
    except Exception:
        raise RuntimeError(f"Non-JSON response from {r.url}: {r.text[:300]}")

def _extract_batch(data: Any, result_key: str) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        if result_key in data and isinstance(data[result_key], list):
            return data[result_key]
        # some endpoints return the list directly at top-level (rare)
        if isinstance(data.get("results"), list):
            return data["results"]
        return []
    if isinstance(data, list):
        return data
    return []


# ===================== AUTO pagination list fetch =====================
def fetch_list(resource: str, ns: argparse.Namespace) -> List[Dict[str, Any]]:
    cfg = RESOURCE_CATALOG[resource]
    params_fn = cfg["list_params"]
    result_key = cfg["result_key"]

    s = _session_authed(ns.retries)
    initial_params = params_fn(ns)

    # fixed URL or resolve variants
    if "list_variants" in cfg:
        list_url = resolve_endpoint(cfg["list_variants"], s, initial_params, ns.connect_timeout, ns.read_timeout)
    else:
        list_url = cfg["list_url"]

    rows: List[Dict[str, Any]] = []
    page = 0

    # FIRST CALL
    def _do_first():
        return s.get(list_url, params=initial_params, headers=TOKEN.auth_headers(),
                     timeout=(ns.connect_timeout, ns.read_timeout))
    data = _get_json(_do_first)
    batch = _extract_batch(data, result_key)
    rows.extend(batch)
    page += 1

    total = (data.get("count") if isinstance(data, dict) else None)
    num_pages = (data.get("num_pages") if isinstance(data, dict) else None)
    page_number = (data.get("page_number") if isinstance(data, dict) else None)
    next_url = (data.get("next") if isinstance(data, dict) else None)

    log.info("[list] page %d fetched %d | total %d%s",
             page, len(batch), len(rows),
             f" / reported {total}" if total is not None else "")

    # Strategy 1: follow 'next' if present
    if next_url:
        while next_url:
            def _do_next():
                return s.get(next_url, headers=TOKEN.auth_headers(),
                             timeout=(ns.connect_timeout, ns.read_timeout))
            data = _get_json(_do_next)
            batch = _extract_batch(data, result_key)
            rows.extend(batch)
            page += 1
            total = (data.get("count") if isinstance(data, dict) else total)
            log.info("[list] page %d fetched %d | total %d%s",
                     page, len(batch), len(rows),
                     f" / reported {total}" if total is not None else "")
            next_url = (data.get("next") if isinstance(data, dict) else None)
            if next_url:
                time.sleep(ns.sleep)
        return rows

    # Strategy 2: page/num_pages
    if isinstance(num_pages, int) and isinstance(page_number, int) and num_pages > page_number:
        current = page_number
        while current < num_pages:
            current += 1
            params = dict(initial_params)
            params["page"] = current
            def _do_page():
                return s.get(list_url, params=params, headers=TOKEN.auth_headers(),
                             timeout=(ns.connect_timeout, ns.read_timeout))
            data = _get_json(_do_page)
            batch = _extract_batch(data, result_key)
            rows.extend(batch)
            page += 1
            total = (data.get("count") if isinstance(data, dict) else total)
            log.info("[list] page %d fetched %d | total %d%s",
                     page, len(batch), len(rows),
                     f" / reported {total}" if total is not None else "")
            time.sleep(ns.sleep)
        return rows

    # Strategy 3: offset/count (if we appear short of count)
    if isinstance(total, int) and total > len(rows):
        offset = ns.offset + len(rows)
        while len(rows) < total:
            params = dict(initial_params)
            params["offset"] = offset
            def _do_off():
                return s.get(list_url, params=params, headers=TOKEN.auth_headers(),
                             timeout=(ns.connect_timeout, ns.read_timeout))
            data = _get_json(_do_off)
            batch = _extract_batch(data, result_key)
            if not batch:
                break
            rows.extend(batch)
            page += 1
            log.info("[list] page %d fetched %d | total %d / reported %d",
                     page, len(batch), len(rows), total)
            offset += ns.limit
            time.sleep(ns.sleep)
        return rows

    # If none of the above, assume we already got all rows from first call.
    return rows


# ===================== generic details fetcher =====================
def _pick_first(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, (int, float)) and str(v).strip():
            return str(v).strip()
    return None

def fetch_details_for(resource: str, rows: List[Dict[str, Any]], ns: argparse.Namespace) -> List[Dict[str, Any]]:
    cfg = RESOURCE_CATALOG[resource]
    det = cfg.get("detail")
    if not det:
        log.info("[detail] no details endpoint configured for '%s'", resource)
        return rows

    limiter = RateLimiter(ns.rps)
    s = _session_authed(ns.retries)

    def one(row: Dict[str, Any]) -> Dict[str, Any]:
        id1 = _pick_first(row, det["id_keys"])
        id2 = _pick_first(row, det.get("id2_keys", [])) if "id2_keys" in det else None
        if not id1:
            return {**row, "detail": None}

        for tmpl in det["url_tmpls"]:
            if ("{id2}" in tmpl) and not id2:
                continue
            limiter.wait()
            url = tmpl.format(id=id1, id2=id2 or "")
            def _do():
                return s.get(url, headers=TOKEN.auth_headers(),
                             timeout=(ns.connect_timeout, ns.read_timeout))
            try:
                data = _get_json(_do)
                return {**row, "detail": data}
            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                if status in (400, 404):
                    continue
                raise
            except Exception as e:
                resp = getattr(e, "response", None)
                status = getattr(resp, "status_code", None)
                if status in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After") if resp else None
                    wait = float(retry_after) if retry_after and retry_after.isdigit() else 1.0
                    log.warning("[detail] %s for id=%s; sleeping %.1fs", status, id1, wait)
                    time.sleep(wait)
                    try:
                        limiter.wait()
                        data = _get_json(_do)
                        return {**row, "detail": data}
                    except Exception:
                        return {**row, "detail": None}
                return {**row, "detail": None}
        return {**row, "detail": None}

    if ns.workers <= 1:
        out = []
        for i, r in enumerate(rows, 1):
            out.append(one(r))
            if i % 25 == 0:
                log.info("[detail] processed %d/%d", i, len(rows))
        return out

    out: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=ns.workers) as pool:
        futs = {pool.submit(one, r): r for r in rows}
        done = 0
        for fut in as_completed(futs):
            out.append(fut.result())
            done += 1
            if done % 25 == 0:
                log.info("[detail] processed %d/%d", done, len(rows))
    return out


# ===================== CLI / main =====================
def make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="CEIPAL Workforce fetcher (auto-pagination + optional details).")
    sub = p.add_subparsers(dest="resource", required=True)

    def add_common(sp: argparse.ArgumentParser, with_dates: bool = False):
        sp.add_argument("--limit", type=int, default=100)
        sp.add_argument("--offset", type=int, default=0)
        sp.add_argument("--sleep", type=float, default=0.2)
        sp.add_argument("--connect-timeout", type=float, default=60.0)
        sp.add_argument("--read-timeout", type=float, default=120.0)
        sp.add_argument("--retries", type=int, default=6)
        sp.add_argument("--with-details", action="store_true")
        sp.add_argument("--workers", type=int, default=1)
        sp.add_argument("--rps", type=float, default=0.8)
        sp.add_argument("--outdir", default="output")
        sp.add_argument("--debug", action="store_true")
        sp.add_argument("--sortorder", default="desc")
        sp.add_argument("--sortby", default="employee_number")
        if with_dates:
            sp.add_argument("--from-date", default=None, help="YYYY-MM-DD")
            sp.add_argument("--to-date", default=None, help="YYYY-MM-DD")

    add_common(sub.add_parser("employees"))
    add_common(sub.add_parser("projects"))
    add_common(sub.add_parser("placements"))
    add_common(sub.add_parser("timesheets"), with_dates=True)
    add_common(sub.add_parser("expenses"), with_dates=True)
    add_common(sub.add_parser("invoices"), with_dates=True)
    add_common(sub.add_parser("clients"))
    add_common(sub.add_parser("vendors"))
    sp_ess = sub.add_parser("ess_tickets"); add_common(sp_ess); sp_ess.add_argument("--status", default="Open")
    sp_states = sub.add_parser("states"); add_common(sp_states); sp_states.add_argument("--country-id", required=True)
    add_common(sub.add_parser("countries"))

    return p

def run_resource(resource: str, ns: argparse.Namespace) -> Tuple[Path, Optional[Path]]:
    setup_logging(ns.debug)

    if resource == "states" and not getattr(ns, "country_id", None):
        ns.country_id = ns.__dict__.get("country_id")

    rows = fetch_list(resource, ns)

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_dir = Path(ns.outdir); out_dir.mkdir(exist_ok=True)
    base_path = out_dir / f"ceipal_{resource}_list_{ts}.json"
    base_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("[write] %s -> %s rows=%d", resource, base_path, len(rows))

    det_path: Optional[Path] = None
    if ns.with_details:
        log.info("[detail] fetching %s details…", resource)
        enriched = fetch_details_for(resource, rows, ns)
        det_path = out_dir / f"ceipal_{resource}_with_details_{ts}.json"
        det_path.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")
        have = sum(1 for r in enriched if r.get("detail"))
        log.info("[write] %s details -> %s (detail for %d rows)", resource, det_path, have)

    return base_path, det_path

def main(argv: Optional[List[str]] = None) -> int:
    p = make_parser()
    ns = p.parse_args(argv)
    run_resource(ns.resource, ns)
    return 0

if __name__ == "__main__":
    sys.exit(main())
