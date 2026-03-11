"""
State management for resumable downloads.
Tracks which date chunks have been downloaded for each instrument/data-type combo.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

from . import config
from .utils import setup_logger

logger = setup_logger("chunking")


class StateManager:
    """Manages download state in a JSON manifest for resume capability."""

    def __init__(self, manifest_path: Path | None = None):
        self.manifest_path = manifest_path or config.MANIFEST_FILE
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load()

    def _load(self) -> dict:
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load manifest: {e}. Starting fresh.")
        return {}

    def save(self):
        with open(self.manifest_path, "w") as f:
            json.dump(self.state, f, indent=2, default=str)

    def _key(self, data_type: str, inst_id: str) -> str:
        """Create a state key like 'funding:BTC-USDT-SWAP'."""
        return f"{data_type}:{inst_id}"

    def is_complete(self, data_type: str, inst_id: str) -> bool:
        """Check if all data for this instrument/type has been fully downloaded."""
        key = self._key(data_type, inst_id)
        entry = self.state.get(key, {})
        return entry.get("complete", False)

    def mark_complete(self, data_type: str, inst_id: str):
        """Mark an instrument/type as fully downloaded."""
        key = self._key(data_type, inst_id)
        if key not in self.state:
            self.state[key] = {}
        self.state[key]["complete"] = True
        self.state[key]["completed_at"] = datetime.utcnow().isoformat()
        self.save()

    def get_completed_chunks(self, data_type: str, inst_id: str) -> list[str]:
        """Get list of completed chunk identifiers."""
        key = self._key(data_type, inst_id)
        return self.state.get(key, {}).get("chunks", [])

    def mark_chunk_done(self, data_type: str, inst_id: str, chunk_id: str):
        """Mark a specific chunk as downloaded."""
        key = self._key(data_type, inst_id)
        if key not in self.state:
            self.state[key] = {"chunks": [], "complete": False}
        if "chunks" not in self.state[key]:
            self.state[key]["chunks"] = []
        if chunk_id not in self.state[key]["chunks"]:
            self.state[key]["chunks"].append(chunk_id)
        self.state[key]["last_updated"] = datetime.utcnow().isoformat()
        self.save()

    def get_last_timestamp(self, data_type: str, inst_id: str) -> str | None:
        """Get the last downloaded timestamp (for pagination resume)."""
        key = self._key(data_type, inst_id)
        return self.state.get(key, {}).get("last_timestamp")

    def set_last_timestamp(self, data_type: str, inst_id: str, ts: str):
        """Store the last downloaded timestamp for resume."""
        key = self._key(data_type, inst_id)
        if key not in self.state:
            self.state[key] = {"chunks": [], "complete": False}
        self.state[key]["last_timestamp"] = ts
        self.save()

    def get_record_count(self, data_type: str, inst_id: str) -> int:
        """Get the number of records downloaded so far."""
        key = self._key(data_type, inst_id)
        return self.state.get(key, {}).get("record_count", 0)

    def set_record_count(self, data_type: str, inst_id: str, count: int):
        key = self._key(data_type, inst_id)
        if key not in self.state:
            self.state[key] = {"chunks": [], "complete": False}
        self.state[key]["record_count"] = count
        self.save()


def generate_date_chunks(
    start_date: str | datetime,
    end_date: str | datetime,
    chunk_days: int = config.DEFAULT_CHUNK_DAYS,
) -> list[tuple[datetime, datetime]]:
    """
    Split a date range into chunks of `chunk_days` days.

    Args:
        start_date: Start date (string 'YYYY-MM-DD' or datetime)
        end_date: End date (string 'YYYY-MM-DD' or datetime)
        chunk_days: Number of days per chunk

    Returns:
        List of (chunk_start, chunk_end) datetime tuples.
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    chunks = []
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end

    return chunks


def generate_monthly_periods(
    start_date: str | datetime, end_date: str | datetime
) -> list[str]:
    """
    Generate list of YYYYMM strings covering start_date to end_date.
    Used for CDN bulk download paths.
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    months = []
    current = start_date.replace(day=1)
    while current <= end_date:
        months.append(current.strftime("%Y%m"))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months
