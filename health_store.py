"""Health data store with JSONL persistence for the Fleet Lighthouse.

Provides persistent storage for health check history, supporting
JSONL format with configurable retention, query, and export capabilities.
"""

from __future__ import annotations

import csv
import json
import os
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from io import StringIO
from typing import Any, Optional


class HealthStatus(str, Enum):
    """Health status of a monitored component."""

    OK = "OK"
    DEGRADED = "DEGRADED"
    DOWN = "DOWN"
    UNKNOWN = "UNKNOWN"


@dataclass
class HealthRecord:
    """A single health check data point for a component."""

    component: str
    status: str
    response_time_ms: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    error: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HealthRecord:
        """Create a HealthRecord from a dictionary."""
        return cls(
            component=data["component"],
            status=data["status"],
            response_time_ms=data.get("response_time_ms", 0.0),
            timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            error=data.get("error", ""),
            metadata=data.get("metadata", {}),
        )


class HealthStore:
    """Persistent health data storage using JSONL format.

    Stores health check history per component with configurable retention.
    Supports querying latest status, historical data, statistics, and
    export to JSON/CSV formats.

    Attributes:
        data_dir: Directory where JSONL files are stored.
        max_records: Maximum records to keep per component (default 1000).
        _lock: Thread lock for concurrent access safety.
    """

    def __init__(self, data_dir: str = "health_data", max_records: int = 1000) -> None:
        """Initialize the health store.

        Args:
            data_dir: Path to directory for JSONL storage files.
            max_records: Maximum number of records to retain per component.
        """
        self.data_dir: str = data_dir
        self.max_records: int = max_records
        self._lock: threading.Lock = threading.Lock()
        self._cache: dict[str, list[HealthRecord]] = {}
        os.makedirs(self.data_dir, exist_ok=True)

    def _file_path(self, component: str) -> str:
        """Get the JSONL file path for a component.

        Args:
            component: Name of the monitored component.

        Returns:
            Absolute file path for the component's JSONL file.
        """
        safe_name = component.lower().replace(" ", "_").replace("/", "_")
        return os.path.join(self.data_dir, f"{safe_name}.jsonl")

    def record(self, entry: HealthRecord) -> None:
        """Persist a health record to the JSONL store.

        Appends the record to the component's JSONL file and updates
        the in-memory cache. Automatically prunes old records when
        the cache exceeds max_records.

        Args:
            entry: The HealthRecord to store.
        """
        with self._lock:
            line = json.dumps(entry.to_dict(), separators=(",", ":"))
            fpath = self._file_path(entry.component)
            with open(fpath, "a", encoding="utf-8") as f:
                f.write(line + "\n")

            if entry.component not in self._cache:
                self._cache[entry.component] = []
            self._cache[entry.component].append(entry)

            # Prune if exceeding max_records
            if len(self._cache[entry.component]) > self.max_records:
                self._prune_cache(entry.component)

    def _prune_cache(self, component: str) -> None:
        """Prune cache entries for a component down to max_records.

        Also rewrites the JSONL file to match the pruned cache.

        Args:
            component: Name of the component to prune.
        """
        records = self._cache[component]
        if len(records) <= self.max_records:
            return
        self._cache[component] = records[-self.max_records :]
        # Rewrite JSONL file
        fpath = self._file_path(component)
        with open(fpath, "w", encoding="utf-8") as f:
            for rec in self._cache[component]:
                f.write(json.dumps(rec.to_dict(), separators=(",", ":")) + "\n")

    def get_latest(self, component: str) -> Optional[HealthRecord]:
        """Get the most recent health record for a component.

        Args:
            component: Name of the component to query.

        Returns:
            The latest HealthRecord, or None if no records exist.
        """
        with self._lock:
            records = self._load_component(component)
            return records[-1] if records else None

    def get_history(
        self, component: str, limit: int = 100, offset: int = 0
    ) -> list[HealthRecord]:
        """Get health history for a component.

        Args:
            component: Name of the component.
            limit: Maximum number of records to return.
            offset: Number of records to skip from the most recent.

        Returns:
            List of HealthRecord entries, newest first.
        """
        with self._lock:
            records = self._load_component(component)
            # records is oldest-first; return newest-first with pagination
            # offset=0, limit=5 → last 5 records (newest)
            # offset=5, limit=5 → previous 5 records
            end_idx = len(records) - offset
            start_idx = max(0, end_idx - limit)
            subset = records[start_idx:end_idx]
            return list(reversed(subset))

    def get_all_latest(self) -> dict[str, HealthRecord]:
        """Get the latest health record for all known components.

        Returns:
            Dictionary mapping component names to their latest HealthRecord.
        """
        result: dict[str, HealthRecord] = {}
        with self._lock:
            self._refresh_cache_index()
            for component in self._cache:
                if self._cache[component]:
                    result[component] = self._cache[component][-1]
        return result

    def get_statistics(self, component: str) -> dict[str, Any]:
        """Compute aggregate statistics for a component.

        Calculates average response time, uptime percentage,
        total checks, and status distribution over retained history.

        Args:
            component: Name of the component.

        Returns:
            Dictionary with computed statistics.
        """
        with self._lock:
            records = self._load_component(component)
        if not records:
            return {
                "component": component,
                "total_checks": 0,
                "avg_response_ms": 0.0,
                "uptime_pct": 0.0,
                "ok_count": 0,
                "degraded_count": 0,
                "down_count": 0,
            }
        ok_count = sum(1 for r in records if r.status == HealthStatus.OK.value)
        degraded_count = sum(1 for r in records if r.status == HealthStatus.DEGRADED.value)
        down_count = sum(1 for r in records if r.status == HealthStatus.DOWN.value)
        total = len(records)
        avg_ms = sum(r.response_time_ms for r in records) / total
        uptime_pct = ((ok_count + degraded_count * 0.5) / total) * 100 if total > 0 else 0.0
        return {
            "component": component,
            "total_checks": total,
            "avg_response_ms": round(avg_ms, 2),
            "uptime_pct": round(uptime_pct, 2),
            "ok_count": ok_count,
            "degraded_count": degraded_count,
            "down_count": down_count,
        }

    def export_json(self, component: Optional[str] = None) -> str:
        """Export health data to JSON format.

        Args:
            component: Specific component to export, or None for all.

        Returns:
            JSON string of exported health records.
        """
        with self._lock:
            if component:
                records = self._load_component(component)
                data = [r.to_dict() for r in records]
            else:
                data = {}
                self._refresh_cache_index()
                for comp in sorted(self._cache):
                    data[comp] = [r.to_dict() for r in self._cache[comp]]
        return json.dumps(data, indent=2)

    def export_csv(self, component: str) -> str:
        """Export health history for a component to CSV format.

        Args:
            component: Name of the component to export.

        Returns:
            CSV string with health records.
        """
        with self._lock:
            records = self._load_component(component)
        if not records:
            return ""
        output = StringIO()
        fieldnames = ["component", "status", "response_time_ms", "timestamp", "error"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow({
                "component": r.component,
                "status": r.status,
                "response_time_ms": r.response_time_ms,
                "timestamp": r.timestamp,
                "error": r.error,
            })
        return output.getvalue()

    def prune_old_data(self, max_age_hours: int = 168) -> int:
        """Remove records older than the specified age.

        Args:
            max_age_hours: Maximum age of records to keep (default 168 = 7 days).

        Returns:
            Number of records pruned.
        """
        pruned = 0
        cutoff = datetime.now(timezone.utc).timestamp() - (max_age_hours * 3600)
        with self._lock:
            self._refresh_cache_index()
            for component in list(self._cache.keys()):
                filtered = []
                for rec in self._cache[component]:
                    try:
                        ts = datetime.fromisoformat(rec.timestamp).timestamp()
                        if ts >= cutoff:
                            filtered.append(rec)
                        else:
                            pruned += 1
                    except (ValueError, TypeError):
                        filtered.append(rec)  # keep records with bad timestamps
                self._cache[component] = filtered
                # Rewrite file
                fpath = self._file_path(component)
                with open(fpath, "w", encoding="utf-8") as f:
                    for rec in self._cache[component]:
                        f.write(json.dumps(rec.to_dict(), separators=(",", ":")) + "\n")
        return pruned

    def _load_component(self, component: str) -> list[HealthRecord]:
        """Load all records for a component from disk into cache.

        Args:
            component: Name of the component.

        Returns:
            List of HealthRecord entries loaded from the JSONL file.
        """
        if component in self._cache:
            return self._cache[component]
        records: list[HealthRecord] = []
        fpath = self._file_path(component)
        if not os.path.exists(fpath):
            self._cache[component] = records
            return records
        with open(fpath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    records.append(HealthRecord.from_dict(data))
                except (json.JSONDecodeError, KeyError):
                    continue
        self._cache[component] = records
        return records

    def _refresh_cache_index(self) -> None:
        """Scan the data directory and load any new component files."""
        if not os.path.isdir(self.data_dir):
            return
        existing_files = {self._file_path(k) for k in self._cache}
        for fname in os.listdir(self.data_dir):
            if fname.endswith(".jsonl"):
                fpath = os.path.join(self.data_dir, fname)
                if fpath not in existing_files:
                    component = fname[:-6].replace("_", " ")
                    self._load_component(component)

    @property
    def known_components(self) -> list[str]:
        """List all known component names.

        Returns:
            Sorted list of component names found in the data directory.
        """
        with self._lock:
            self._refresh_cache_index()
            return sorted(self._cache.keys())
