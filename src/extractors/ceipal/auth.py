"""Token management for CEIPAL API with caching"""
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict
from dataclasses import dataclass
import json

from src.common.config import settings

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TokenConfig:
    expiry_buffer_minutes: int = 5
    default_ttl_seconds: int = 3600
    cache_enabled: bool = settings.checkpoint_enabled


@dataclass
class CachedToken:
    token: str
    expiry: datetime

    def is_valid(self, buffer_minutes=5):
        return self.expiry > datetime.utcnow() + timedelta(minutes=buffer_minutes)

    def to_dict(self):
        return {
            'token': self.token,
            'expiry': self.expiry.isoformat()
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            token=data['token'],
            expiry=datetime.fromisoformat(data['expiry'])
        )


class TokenManager:
    """Manages auth tokens with automatic refresh and disk caching"""

    def __init__(self, config=None):
        self.base_url = settings.ceipal_base_url
        self.email = settings.ceipal_email
        self.password = settings.ceipal_password
        self.api_key = settings.ceipal_api_key
        self.config = config or TokenConfig()

        self._cached_token: Optional[CachedToken] = None
        self.cache_file = settings.checkpoint_dir / "ceipal_token.json"

        if self.config.cache_enabled:
            self._load_cached_token()

    @property
    def is_token_valid(self) -> bool:
        """Check if current token is still valid"""
        if self._cached_token is None:
            return False
        return self._cached_token.is_valid(self.config.expiry_buffer_minutes)

    def _load_cached_token(self) -> None:
        """Load token from cache file if it exists and is valid"""
        if not self.cache_file.exists():
            return

        try:
            with open(self.cache_file, 'r') as f:
                data = json.load(f)

            cached = CachedToken.from_dict(data)

            if cached.is_valid(self.config.expiry_buffer_minutes):
                self._cached_token = cached
                mins_left = (cached.expiry - datetime.utcnow()).total_seconds() / 60
                log.info(f"Loaded cached token (expires in {mins_left:.0f} min)")
            else:
                log.info("Cached token expired, will refresh")

        except Exception as e:
            log.warning(f"Failed to load cached token: {e}")

    def _save_cached_token(self) -> None:
        """Save current token to cache file"""
        if not self.config.cache_enabled or self._cached_token is None:
            return

        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.cache_file, 'w') as f:
                json.dump(self._cached_token.to_dict(), f)

            log.debug("Token cached to disk")

        except Exception as e:
            log.warning(f"Failed to cache token: {e}")

    def _authenticate(self) -> str:
        """
        Authenticate with CEIPAL API and get new token.

        Returns:
            Access token string

        Raises:
            requests.exceptions.RequestException: If authentication fails
            ValueError: If response doesn't contain access token
        """
        log.info("Authenticating with CEIPAL API...")
        auth_url = f"{self.base_url}/wf/v1/createAuthtoken"

        # CEIPAL uses multipart form-data, needs "json" field to return JSON
        files = {
            "email": (None, self.email),
            "password": (None, self.password),
            "api_key": (None, self.api_key),
            "json": (None, "")
        }

        try:
            response = requests.post(
                auth_url,
                files=files,
                headers={"Accept": "application/json"},
                timeout=60
            )
            log.debug(f"Auth response status: {response.status_code}")
            response.raise_for_status()

            data = response.json()
            access_token = data.get('access_token') or data.get('token')

            if not access_token:
                raise ValueError(f"No access_token in auth response: {data}")

            # Store as cached token
            expires_in = data.get('expires_in', self.config.default_ttl_seconds)
            expiry = datetime.utcnow() + timedelta(seconds=expires_in)

            self._cached_token = CachedToken(
                token=access_token,
                expiry=expiry
            )

            log.info(f"Authentication successful (expires in {expires_in}s)")
            self._save_cached_token()

            return access_token

        except requests.exceptions.RequestException as e:
            log.error(f"Authentication failed: {e}")
            raise

    def get_token(self) -> str:
        """
        Get valid access token, refreshing if necessary.

        Returns:
            Valid access token

        Raises:
            requests.exceptions.RequestException: If authentication fails
        """
        if self.is_token_valid:
            return self._cached_token.token

        return self._authenticate()

    def get_headers(self) -> Dict[str, str]:
        """
        Get HTTP headers with valid authentication token.

        Returns:
            Dictionary of headers including Authorization and Content-Type

        Raises:
            requests.exceptions.RequestException: If authentication fails
        """
        token = self.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def invalidate_token(self) -> None:
        """
        Invalidate current token and clear cache.

        Useful when token is known to be invalid (e.g., after 401 error).
        """
        self._cached_token = None
        if self.cache_file.exists():
            try:
                self.cache_file.unlink()
                log.debug("Token cache cleared")
            except Exception as e:
                log.warning(f"Failed to clear token cache: {e}")
