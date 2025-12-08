"""CEIPAL API client with pagination and rate limiting"""
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from typing import List, Dict, Optional, Callable, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from src.common.config import settings
from src.extractors.ceipal.auth import TokenManager
from src.extractors.ceipal.config import get_resource_config, ResourceConfig

log = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter to avoid hitting API limits"""

    def __init__(self, requests_per_second: float):
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0

    def wait(self):
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()


class CeipalClient:
    """Main API client - handles auth, pagination, retries"""

    def __init__(self, token_manager=None):
        self.base_url = settings.ceipal_base_url
        self.token_manager = token_manager or TokenManager()
        self.detail_rate_limiter = RateLimiter(settings.ceipal_detail_rps)
        self.detail_workers = settings.ceipal_detail_workers
        self.session = self._build_session()

    def _build_session(self):
        # Retry on 429 and 5xx errors
        retry_strategy = Retry(
            total=settings.ceipal_retries,
            connect=settings.ceipal_retries,
            read=settings.ceipal_retries,
            status=settings.ceipal_retries,
            backoff_factor=settings.ceipal_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=32,
            pool_maxsize=32
        )
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make authenticated API request with error handling.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            params: Optional query parameters
            json_data: Optional JSON body data

        Returns:
            Parsed JSON response

        Raises:
            requests.exceptions.HTTPError: If request fails with 4xx/5xx status
        """
        headers = self.token_manager.get_headers()
        response = self.session.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json_data,
            timeout=(60, 120)  # (connect, read) timeouts
        )

        if response.status_code >= 400:
            log.error(f"Request failed: {response.status_code} - {response.text[:200]}")
            response.raise_for_status()

        return response.json()

    def fetch_list(
        self,
        resource_name: str,
        filters: Optional[Dict[str, Any]] = None,
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch list of records with pagination.

        Args:
            resource_name: Name of resource to fetch (e.g., 'employees')
            filters: Optional filters/parameters (e.g., {'limit': 50})
            progress_callback: Optional callback function called after each page

        Returns:
            List of record dictionaries

        Raises:
            ValueError: If resource name is unknown
            requests.exceptions.HTTPError: If API request fails
        """
        config = get_resource_config(resource_name)

        # Handle both Pydantic model and dict for backward compatibility
        if isinstance(config, ResourceConfig):
            endpoint = config.list_endpoint
            limit = config.pagination.page_size
            max_pages = config.pagination.max_pages
            required_params = config.required_params
        else:
            # Legacy dict format
            endpoint = config['list_endpoint']
            limit = config['pagination']['page_size']
            max_pages = config['pagination']['max_pages']
            required_params = config.get('required_params', {})

        url = f"{self.base_url}{endpoint}"
        all_records: List[Dict[str, Any]] = []
        page = 1

        # Allow filters to override page_size
        if filters and 'limit' in filters:
            limit = filters['limit']
            log.info(f"[{resource_name}] Starting list fetch (limit={limit} from filters)")
        else:
            log.info(f"[{resource_name}] Starting list fetch (limit={limit})")

        # Build params with required parameters from config
        params: Dict[str, Any] = {'limit': limit}
        params.update(required_params)

        # Override with user-provided filters
        if filters:
            params.update(filters)

        while url:
            try:
                data = self._make_request('GET', url, params=params)
                records = data.get('results', [])
                all_records.extend(records)

                if progress_callback:
                    progress_callback(page)

                total_count = data.get('count')
                log.debug(
                    f"[{resource_name}] Page {page}: {len(records)} records "
                    f"(total: {len(all_records)}/{total_count or '?'})"
                )

                next_url = data.get('next')
                if not next_url or len(records) == 0:
                    break

                if max_pages and page >= max_pages:
                    log.info(f"[{resource_name}] Reached max_pages limit ({max_pages})")
                    break

                url = next_url
                params = None  # Use URL's query params for subsequent pages
                page += 1

            except Exception as e:
                log.error(f"[{resource_name}] Failed to fetch page {page}: {e}")
                raise

        log.info(f"[{resource_name}] Fetched {len(all_records)} records from {page} pages")
        return all_records

    def fetch_detail(
        self,
        resource_name: str,
        record_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch single record detail with rate limiting.

        Args:
            resource_name: Name of resource (e.g., 'employees')
            record_id: ID of record to fetch

        Returns:
            Record dictionary or None if not found

        Raises:
            ValueError: If resource name is unknown
            requests.exceptions.HTTPError: If API request fails (except 404)
        """
        self.detail_rate_limiter.wait()  # Rate limit only detail calls

        config = get_resource_config(resource_name)

        # Handle both Pydantic model and dict
        if isinstance(config, ResourceConfig):
            endpoint = config.detail_endpoint.format(id=record_id)
        else:
            endpoint = config['detail_endpoint'].format(id=record_id)

        url = f"{self.base_url}{endpoint}"

        try:
            data = self._make_request('GET', url)
            return data.get('data')
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 404:
                log.warning(f"[{resource_name}] Record {record_id} not found")
                return None
            raise

    def fetch_details(
        self,
        resource_name: str,
        record_ids: List[str],
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch multiple record details in parallel.

        Args:
            resource_name: Name of resource (e.g., 'employees')
            record_ids: List of record IDs to fetch
            progress_callback: Optional callback(completed, total)

        Returns:
            List of record dictionaries (excluding not found records)

        Raises:
            ValueError: If resource name is unknown
        """
        if not record_ids:
            return []

        log.info(
            f"[{resource_name}] Fetching {len(record_ids)} details "
            f"(workers={self.detail_workers})"
        )

        results: List[Dict[str, Any]] = []
        completed = 0

        with ThreadPoolExecutor(max_workers=self.detail_workers) as executor:
            future_to_id = {
                executor.submit(self.fetch_detail, resource_name, rid): rid
                for rid in record_ids
            }

            for future in as_completed(future_to_id):
                record_id = future_to_id[future]
                completed += 1

                try:
                    detail = future.result()
                    if detail:
                        results.append(detail)

                    if progress_callback:
                        progress_callback(completed, len(record_ids))

                    if completed % 50 == 0:
                        log.debug(f"[{resource_name}] Details: {completed}/{len(record_ids)}")

                except Exception as e:
                    log.error(f"[{resource_name}] Failed to fetch detail {record_id}: {e}")

        log.info(f"[{resource_name}] Fetched {len(results)}/{len(record_ids)} details")
        return results

    def extract_resource(
        self,
        resource_name: str,
        filters: Optional[Dict[str, Any]] = None,
        fetch_details: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract resource with optional detail fetching and merging.

        Args:
            resource_name: Name of resource to extract
            filters: Optional filters/parameters
            fetch_details: Override config's fetch_details setting

        Returns:
            List of enriched records with metadata

        Raises:
            ValueError: If resource name is unknown
            requests.exceptions.HTTPError: If API requests fail
        """
        config = get_resource_config(resource_name)

        # Handle both Pydantic model and dict
        if isinstance(config, ResourceConfig):
            should_fetch_details = fetch_details if fetch_details is not None else config.fetch_details
            primary_key = config.primary_key
        else:
            should_fetch_details = fetch_details if fetch_details is not None else config['fetch_details']
            primary_key = config['primary_key']

        start_time = time.time()
        log.info(f"[{resource_name}] Starting extraction (fetch_details={should_fetch_details})")

        records = self.fetch_list(resource_name, filters=filters)

        # Apply sample limit if specified
        if filters and 'limit' in filters:
            sample_size = filters['limit']
            if len(records) > sample_size:
                log.info(f"[{resource_name}] Limiting from {len(records)} to {sample_size} records")
                records = records[:sample_size]

        if not records:
            log.warning(f"[{resource_name}] No records found")
            return []

        # Fetch details if needed
        if should_fetch_details:
            record_ids = [r.get(primary_key) for r in records if r.get(primary_key)]

            if record_ids:
                detailed_records = self.fetch_details(resource_name, record_ids)
                details_by_id = {r.get(primary_key): r for r in detailed_records}

                # Merge details into list records
                for record in records:
                    rid = record.get(primary_key)
                    if rid in details_by_id:
                        record.update(details_by_id[rid])

        # Add metadata
        extraction_time = datetime.utcnow().isoformat()
        for record in records:
            record['_extracted_at'] = extraction_time
            record['_resource'] = resource_name

        elapsed = time.time() - start_time
        log.info(
            f"[{resource_name}] Extraction complete: "
            f"{len(records)} records in {elapsed:.1f}s"
        )

        return records
