"""
Low-level OKX API client with rate limiting and retry logic.
Handles both the public REST API and the historical data export system.
"""

import time
import io
import zipfile
import requests

from . import config
from .utils import rate_limiter, retry_with_backoff, setup_logger

logger = setup_logger("okx_client")


class OKXClient:
    """HTTP client for OKX public and private-API endpoints."""

    def __init__(self, session_cookies: dict | None = None):
        """
        Args:
            session_cookies: Optional dict of browser cookies for authenticated
                             requests (needed for the historical data export API).
                             Not required for public REST endpoints.
        """
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
        })
        if session_cookies:
            self.session.cookies.update(session_cookies)

    # ── Public REST API ──────────────────────────────────────────────────

    @retry_with_backoff(max_retries=5, base_delay=1.0)
    def get_instruments(self, inst_type: str) -> list[dict]:
        """
        GET /api/v5/public/instruments
        Returns list of instrument dicts for the given type (SPOT, SWAP, etc.).
        """
        rate_limiter.wait("instruments", config.RATE_LIMIT_INSTRUMENTS)
        url = config.OKX_BASE_URL + config.EP_INSTRUMENTS
        resp = self.session.get(url, params={"instType": inst_type}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "0":
            raise RuntimeError(f"OKX API error: {data}")
        return data["data"]

    @retry_with_backoff(max_retries=5, base_delay=1.0)
    def get_funding_rate_history(
        self, inst_id: str, after: str | None = None, limit: int = 100
    ) -> list[dict]:
        """
        GET /api/v5/public/funding-rate-history
        Returns up to `limit` records. Use `after` for backward pagination
        (records older than this timestamp).
        """
        rate_limiter.wait("funding_history", config.RATE_LIMIT_FUNDING_HISTORY)
        url = config.OKX_BASE_URL + config.EP_FUNDING_RATE_HISTORY
        params = {"instId": inst_id, "limit": str(limit)}
        if after:
            params["after"] = after
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "0":
            raise RuntimeError(f"OKX API error: {data}")
        return data["data"]

    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def get_orderbook(self, inst_id: str, depth: int = 400) -> dict:
        """
        GET /api/v5/market/books
        Returns CURRENT order book snapshot (not historical).
        """
        rate_limiter.wait("orderbook", config.RATE_LIMIT_ORDERBOOK)
        url = config.OKX_BASE_URL + config.EP_ORDERBOOK
        resp = self.session.get(
            url, params={"instId": inst_id, "sz": str(depth)}, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "0":
            raise RuntimeError(f"OKX API error: {data}")
        return data["data"][0] if data["data"] else {}

    # ── CDN Bulk Download (swaprate / trades) ────────────────────────────

    @retry_with_backoff(max_retries=3, base_delay=2.0)
    def cdn_list_files(self, path: str, size: int = 100, marker: str = "") -> dict:
        """
        List files on the OKX static CDN.
        Returns dict with 'recordFileList', 'isTruncate', 'nextMarker'.
        """
        rate_limiter.wait("cdn", config.RATE_LIMIT_CDN)
        url = config.OKX_BASE_URL + config.EP_CDN_LIST
        params = {"path": path, "size": str(size)}
        if marker:
            params["marker"] = marker
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if str(data.get("code")) != "0":
            raise RuntimeError(f"CDN list error: {data}")
        return data["data"]

    def cdn_list_all_files(self, path: str) -> list[dict]:
        """List ALL files at a CDN path (handles pagination)."""
        all_files = []
        marker = ""
        while True:
            result = self.cdn_list_files(path, size=100, marker=marker)
            files = result.get("recordFileList", [])
            all_files.extend(files)
            if not result.get("isTruncate", False):
                break
            marker = result.get("nextMarker", "")
            if not marker:
                break
        return all_files

    @retry_with_backoff(max_retries=3, base_delay=2.0)
    def cdn_download_zip(self, cdn_path: str, filename: str) -> bytes:
        """Download a ZIP file from the CDN and return raw bytes."""
        rate_limiter.wait("cdn_download", config.RATE_LIMIT_CDN)
        url = f"{config.CDN_STATIC_BASE}/{cdn_path}/{filename}"
        resp = self.session.get(url, timeout=120)
        resp.raise_for_status()
        return resp.content

    def cdn_download_and_extract_csv(self, cdn_path: str, filename: str) -> str:
        """Download a ZIP from CDN, extract the CSV inside, return as string."""
        raw = self.cdn_download_zip(cdn_path, filename)
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                raise RuntimeError(f"No CSV found in {filename}")
            return zf.read(csv_names[0]).decode("utf-8", errors="replace")

    # ── Historical Data Export API ───────────────────────────────────────

    @retry_with_backoff(max_retries=3, base_delay=3.0)
    def export_get_instruments(self, inst_type: str) -> list[str]:
        """
        GET /priapi/v5/broker/public/trade-data/instruments
        Returns list of instrument family strings (e.g. ['BTC-USDT', 'ETH-USDT']).
        """
        rate_limiter.wait("export", config.RATE_LIMIT_EXPORT)
        url = config.OKX_BASE_URL + config.EP_TRADE_DATA_INSTRUMENTS
        resp = self.session.get(url, params={"instType": inst_type}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if str(data.get("code")) != "0":
            raise RuntimeError(f"Export instruments error: {data}")
        return data["data"].get("instList", [])

    @retry_with_backoff(max_retries=3, base_delay=3.0)
    def export_request_download(
        self,
        module: str,
        inst_type: str,
        inst_families: list[str],
        begin_ms: str,
        end_ms: str,
        date_aggr: str = "daily",
    ) -> dict:
        """
        POST /priapi/v5/broker/public/trade-data/download-link

        Request a data export. Returns dict with 'details' containing
        download URLs. May return empty details if authentication is
        missing or data is unavailable.

        Args:
            module: Module code (e.g. MODULE_ORDER_BOOK_400 = "4")
            inst_type: "SPOT" or "SWAP"
            inst_families: List of instrument families, e.g. ["BTC-USDT"]
            begin_ms: Start timestamp in milliseconds (string)
            end_ms: End timestamp in milliseconds (string)
            date_aggr: "daily" or "monthly"
        """
        rate_limiter.wait("export", config.RATE_LIMIT_EXPORT)
        url = config.OKX_BASE_URL + config.EP_TRADE_DATA_DOWNLOAD

        # Build instQueryParam based on inst_type
        if inst_type == "SPOT":
            inst_query = {"instIdList": inst_families}
        else:
            inst_query = {"instFamilyList": inst_families}

        body = {
            "module": module,
            "instType": inst_type,
            "instQueryParam": inst_query,
            "dateQuery": {
                "dateAggrType": date_aggr,
                "begin": begin_ms,
                "end": end_ms,
            },
        }

        resp = self.session.post(
            url,
            json=body,
            headers={
                "Referer": "https://www.okx.com/en-us/historical-data",
                "Origin": "https://www.okx.com",
            },
            timeout=300,  # export can take up to 4 minutes
        )
        resp.raise_for_status()
        data = resp.json()
        if str(data.get("code")) != "0":
            raise RuntimeError(f"Export download error: {data}")
        return data["data"]

    def download_file(self, url: str, output_path: str | None = None) -> bytes:
        """Download a file from any URL. Optionally save to disk."""
        resp = self.session.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        content = resp.content
        if output_path:
            from pathlib import Path
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "wb") as f:
                f.write(content)
        return content
