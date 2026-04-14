"""Alert management system for the Fleet Lighthouse.

Provides configurable alerting with multiple channels, deduplication,
rule evaluation, and acknowledgment tracking. Supports log, file, and
webhook notification channels.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.request
import urllib.error
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


class AlertSeverity(str, Enum):
    """Severity levels for alerts."""

    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


class AlertType(str, Enum):
    """Types of alerts that can be fired."""

    AGENT_DOWN = "AGENT_DOWN"
    AGENT_DEGRADED = "AGENT_DEGRADED"
    HIGH_ERROR_RATE = "HIGH_ERROR_RATE"
    SLOW_RESPONSE = "SLOW_RESPONSE"
    DISK_FULL = "DISK_FULL"
    RECOVERY = "RECOVERY"


@dataclass
class Alert:
    """Represents a single alert instance.

    Attributes:
        alert_type: The type/category of the alert.
        component: Name of the affected component.
        severity: Severity level of the alert.
        message: Human-readable alert description.
        timestamp: When the alert was created (ISO format).
        alert_id: Unique identifier for this alert.
        acknowledged: Whether the alert has been acknowledged.
        resolved: Whether the alert has been resolved.
        resolved_at: When the alert was resolved (ISO format).
        metadata: Additional context attached to the alert.
    """

    alert_type: str
    component: str
    severity: str
    message: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    alert_id: str = ""
    acknowledged: bool = False
    resolved: bool = False
    resolved_at: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Generate alert_id if not set."""
        if not self.alert_id:
            short_time = datetime.now(timezone.utc).strftime("%H%M%S")
            self.alert_id = f"{self.alert_type}-{self.component}-{short_time}"

    def to_dict(self) -> dict[str, Any]:
        """Convert alert to dictionary."""
        return asdict(self)


@dataclass
class AlertRule:
    """Rule definition for when to fire an alert.

    Attributes:
        alert_type: Type of alert this rule generates.
        condition: Callable that returns True when the rule triggers.
        severity: Severity level for alerts from this rule.
        cooldown_seconds: Minimum seconds between repeated alerts of this type.
        description: Human-readable rule description.
    """

    alert_type: str
    condition: Callable[[dict[str, Any]], bool]
    severity: str = AlertSeverity.WARNING.value
    cooldown_seconds: int = 300
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert rule to a serializable dictionary (without callable)."""
        return {
            "alert_type": self.alert_type,
            "severity": self.severity,
            "cooldown_seconds": self.cooldown_seconds,
            "description": self.description,
        }


class AlertChannel:
    """Base class for alert notification channels."""

    def send(self, alert: Alert) -> bool:
        """Send an alert through this channel.

        Args:
            alert: The Alert instance to send.

        Returns:
            True if delivery succeeded, False otherwise.
        """
        raise NotImplementedError


class LogChannel(AlertChannel):
    """Send alerts to the Python logger."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """Initialize the log channel.

        Args:
            logger: Logger instance to use. Creates a default if None.
        """
        self.logger = logger or logging.getLogger("fleet.lighthouse.alerts")

    def send(self, alert: Alert) -> bool:
        """Log the alert at the appropriate level.

        Args:
            alert: The Alert instance to log.

        Returns:
            Always True (logging cannot fail in a meaningful way).
        """
        level = {
            AlertSeverity.CRITICAL.value: logging.CRITICAL,
            AlertSeverity.WARNING.value: logging.WARNING,
            AlertSeverity.INFO.value: logging.INFO,
        }.get(alert.severity, logging.WARNING)
        self.logger.log(level, "[%s] %s — %s", alert.alert_type, alert.component, alert.message)
        return True


class FileChannel(AlertChannel):
    """Append alerts to a JSONL file."""

    def __init__(self, filepath: str = "alerts.jsonl") -> None:
        """Initialize the file channel.

        Args:
            filepath: Path to the alerts output file.
        """
        self.filepath: str = filepath

    def send(self, alert: Alert) -> bool:
        """Append alert to the JSONL file.

        Args:
            alert: The Alert instance to write.

        Returns:
            True if write succeeded, False on I/O error.
        """
        try:
            os.makedirs(os.path.dirname(self.filepath) if os.path.dirname(self.filepath) else ".", exist_ok=True)
            with open(self.filepath, "a", encoding="utf-8") as f:
                f.write(json.dumps(alert.to_dict(), separators=(",", ":")) + "\n")
            return True
        except OSError:
            return False


class WebhookChannel(AlertChannel):
    """Send alerts to a webhook URL via HTTP POST."""

    def __init__(self, url: str, timeout: int = 5) -> None:
        """Initialize the webhook channel.

        Args:
            url: Target webhook URL.
            timeout: Request timeout in seconds.
        """
        self.url: str = url
        self.timeout: int = timeout

    def send(self, alert: Alert) -> bool:
        """POST alert payload to the webhook URL.

        Args:
            alert: The Alert instance to send.

        Returns:
            True if POST succeeded (2xx), False otherwise.
        """
        try:
            payload = json.dumps(alert.to_dict()).encode("utf-8")
            req = urllib.request.Request(
                self.url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return 200 <= resp.status < 300
        except (urllib.error.URLError, OSError, TimeoutError):
            return False


class AlertManager:
    """Fleet alerting system with rules, channels, and deduplication.

    Manages alert lifecycle from detection through resolution.
    Supports multiple notification channels, configurable rules,
    alert deduplication, and acknowledgment tracking.

    Attributes:
        rules: List of configured AlertRule instances.
        channels: List of AlertChannel instances for notifications.
        active_alerts: Currently active (unresolved) alerts.
        alert_history: Full history of all alerts fired.
        dedup_window: Seconds to suppress duplicate alerts.
    """

    def __init__(self, dedup_window: int = 300) -> None:
        """Initialize the alert manager.

        Args:
            dedup_window: Seconds to suppress duplicate alerts for the same
                          component+type combination.
        """
        self.rules: list[AlertRule] = []
        self.channels: list[AlertChannel] = []
        self.active_alerts: list[Alert] = []
        self.alert_history: list[Alert] = []
        self.dedup_window: int = dedup_window
        self._lock: threading.Lock = threading.Lock()
        self._dedup_tracker: dict[str, float] = {}  # key -> last fire timestamp
        self._total_events: int = 0
        self._setup_default_rules()

    def _setup_default_rules(self) -> None:
        """Configure built-in default alert rules."""
        self.rules = [
            AlertRule(
                alert_type=AlertType.AGENT_DOWN.value,
                condition=lambda ctx: ctx.get("status") == "DOWN",
                severity=AlertSeverity.CRITICAL.value,
                cooldown_seconds=60,
                description="Fires when a component is DOWN",
            ),
            AlertRule(
                alert_type=AlertType.AGENT_DEGRADED.value,
                condition=lambda ctx: ctx.get("status") == "DEGRADED",
                severity=AlertSeverity.WARNING.value,
                cooldown_seconds=120,
                description="Fires when a component is DEGRADED",
            ),
            AlertRule(
                alert_type=AlertType.HIGH_ERROR_RATE.value,
                condition=lambda ctx: ctx.get("error_count", 0) >= 5,
                severity=AlertSeverity.WARNING.value,
                cooldown_seconds=300,
                description="Fires when error count reaches threshold",
            ),
            AlertRule(
                alert_type=AlertType.SLOW_RESPONSE.value,
                condition=lambda ctx: ctx.get("response_time_ms", 0) > 1000,
                severity=AlertSeverity.WARNING.value,
                cooldown_seconds=300,
                description="Fires when response time exceeds 1000ms",
            ),
            AlertRule(
                alert_type=AlertType.DISK_FULL.value,
                condition=lambda ctx: ctx.get("disk_pct", 0) > 90,
                severity=AlertSeverity.CRITICAL.value,
                cooldown_seconds=600,
                description="Fires when disk usage exceeds 90%",
            ),
            AlertRule(
                alert_type=AlertType.RECOVERY.value,
                condition=lambda ctx: ctx.get("prev_status") in ("DOWN", "DEGRADED") and ctx.get("status") == "OK",
                severity=AlertSeverity.INFO.value,
                cooldown_seconds=60,
                description="Fires when a component recovers from failure",
            ),
        ]

    def add_channel(self, channel: AlertChannel) -> None:
        """Register a notification channel.

        Args:
            channel: An AlertChannel instance to add.
        """
        self.channels.append(channel)

    def evaluate(self, context: dict[str, Any]) -> list[Alert]:
        """Evaluate all rules against the given context and fire alerts.

        Args:
            context: Dictionary with component health data. Expected keys:
                     component, status, response_time_ms, error_count, etc.

        Returns:
            List of Alert instances that were fired.
        """
        fired: list[Alert] = []
        component = context.get("component", "unknown")
        for rule in self.rules:
            try:
                if not rule.condition(context):
                    continue
            except Exception:
                continue

            # Check deduplication
            dedup_key = f"{rule.alert_type}:{component}"
            now = time.time()
            last_fire = self._dedup_tracker.get(dedup_key, 0)
            if now - last_fire < self.dedup_window:
                continue

            alert = Alert(
                alert_type=rule.alert_type,
                component=component,
                severity=rule.severity,
                message=rule.description or f"{rule.alert_type} on {component}",
                metadata={"response_time_ms": context.get("response_time_ms", 0)},
            )
            self._dedup_tracker[dedup_key] = now
            self._fire_alert(alert)
            fired.append(alert)

        return fired

    def _fire_alert(self, alert: Alert) -> None:
        """Dispatch an alert to all registered channels.

        Args:
            alert: The Alert to dispatch.
        """
        with self._lock:
            self.active_alerts.append(alert)
            self.alert_history.append(alert)
            self._total_events += 1
        for channel in self.channels:
            try:
                channel.send(alert)
            except Exception:
                pass

    def acknowledge(self, alert_id: str) -> bool:
        """Mark an active alert as acknowledged.

        Args:
            alert_id: The unique ID of the alert to acknowledge.

        Returns:
            True if the alert was found and acknowledged, False otherwise.
        """
        with self._lock:
            for alert in self.active_alerts:
                if alert.alert_id == alert_id and not alert.acknowledged:
                    alert.acknowledged = True
                    return True
        return False

    def resolve(self, component: str) -> int:
        """Resolve all active alerts for a component.

        Args:
            component: Name of the component to resolve alerts for.

        Returns:
            Number of alerts that were resolved.
        """
        resolved_count = 0
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            for alert in self.active_alerts:
                if alert.component == component and not alert.resolved:
                    alert.resolved = True
                    alert.resolved_at = now
                    resolved_count += 1
            # Move resolved alerts out of active
            self.active_alerts = [
                a for a in self.active_alerts if not a.resolved
            ]
        return resolved_count

    def get_active_alerts(self) -> list[Alert]:
        """Get all currently active (unresolved) alerts.

        Returns:
            List of active Alert instances.
        """
        with self._lock:
            return [a for a in self.active_alerts if not a.resolved]

    def get_alert_history(self, limit: int = 50) -> list[Alert]:
        """Get recent alert history.

        Args:
            limit: Maximum number of alerts to return.

        Returns:
            List of recent Alert instances, newest first.
        """
        with self._lock:
            return list(reversed(self.alert_history[-limit:]))

    @property
    def total_events(self) -> int:
        """Total number of alert events ever fired."""
        return self._total_events

    @property
    def active_count(self) -> int:
        """Number of currently active alerts."""
        with self._lock:
            return len([a for a in self.active_alerts if not a.resolved])
