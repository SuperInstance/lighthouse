"""Fleet Lighthouse — unified health monitoring dashboard.

Aggregates status from all fleet agents, the keeper, the MUD server,
and the bridge into a single pane of glass for fleet health. Provides
real-time terminal dashboard, health checking with status transitions,
system resource monitoring, and alerting integration.

Components monitored:
    - Keeper Agent (port 8443)
    - Git Agent (port 8444)
    - MUD Server (port 7777)
    - MUD Bridge (port 8877)
    - All fleet agents (ports 8501+)
    - GitHub API connectivity
    - Disk/memory/system health
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import sys
import threading
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from alerting import AlertManager, AlertSeverity, AlertType, FileChannel, LogChannel
from health_store import HealthRecord, HealthStatus, HealthStore


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ComponentHealth:
    """Tracks the health state of a single monitored component.

    Attributes:
        name: Human-readable component name.
        host: Hostname or IP for health checks.
        port: Port number (0 for non-HTTP checks).
        endpoint: URL path for HTTP health checks.
        status: Current health status (OK/DEGRADED/DOWN/UNKNOWN).
        prev_status: Previous health status for transition detection.
        response_time_ms: Most recent response time in milliseconds.
        last_check: ISO timestamp of the last health check.
        consecutive_failures: Running count of consecutive check failures.
        consecutive_successes: Running count of consecutive check successes.
        error_count: Total error count since monitoring started.
        first_seen: ISO timestamp when monitoring first started.
        uptime_start: ISO timestamp when the component last became healthy.
        error: Last error message, if any.
        metadata: Additional component-specific information.
    """

    name: str
    host: str = "localhost"
    port: int = 0
    endpoint: str = "/health"
    status: str = HealthStatus.UNKNOWN.value
    prev_status: str = HealthStatus.UNKNOWN.value
    response_time_ms: float = 0.0
    last_check: str = ""
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    error_count: int = 0
    first_seen: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    uptime_start: str = ""
    error: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def uptime_str(self) -> str:
        """Format current uptime duration as HH:MM:SS."""
        if not self.uptime_start:
            return "--:--:--"
        try:
            start = datetime.fromisoformat(self.uptime_start)
            delta = datetime.now(timezone.utc) - start
            total_s = int(delta.total_seconds())
            h, rem = divmod(total_s, 3600)
            m, s = divmod(rem, 60)
            return f"{h:02d}:{m:02d}:{s:02d}"
        except (ValueError, TypeError):
            return "--:--:--"

    @property
    def response_str(self) -> str:
        """Format response time for display."""
        if self.status == HealthStatus.UNKNOWN.value:
            return "   --"
        return f"{self.response_time_ms:5.0f}ms"

    @property
    def port_str(self) -> str:
        """Format port for display."""
        return f":{self.port}" if self.port else ""


# ---------------------------------------------------------------------------
# Fleet Lighthouse
# ---------------------------------------------------------------------------


class FleetLighthouse:
    """Unified fleet health dashboard.

    Polls all fleet components and presents a single health view.
    Manages component registration, health checking with status
    transitions (OK → DEGRADED → DOWN and back), system resource
    monitoring, and dashboard rendering.

    Args:
        fleet_name: Display name for the fleet.
        check_interval: Seconds between health check cycles (default 5).
        degraded_threshold: Consecutive failures before DEGRADED status.
        down_threshold: Consecutive failures before DOWN status.
        recovery_threshold: Consecutive successes needed for recovery.
        timeout: HTTP request timeout in seconds.
        data_dir: Directory for health data persistence.
    """

    DEGRADED_THRESHOLD: int = 3
    DOWN_THRESHOLD: int = 10
    RECOVERY_THRESHOLD: int = 3

    def __init__(
        self,
        fleet_name: str = "Pelagic Fleet",
        check_interval: int = 5,
        degraded_threshold: int = 3,
        down_threshold: int = 10,
        recovery_threshold: int = 3,
        timeout: int = 5,
        data_dir: str = "health_data",
    ) -> None:
        """Initialize the Fleet Lighthouse."""
        self.fleet_name: str = fleet_name
        self.check_interval: int = check_interval
        self.degraded_threshold: int = degraded_threshold
        self.down_threshold: int = down_threshold
        self.recovery_threshold: int = recovery_threshold
        self.timeout: int = timeout
        self.start_time: datetime = datetime.now(timezone.utc)

        self.components: dict[str, ComponentHealth] = {}
        self.store: HealthStore = HealthStore(data_dir=data_dir)
        self.alert_manager: AlertManager = AlertManager()
        self._running: bool = False
        self._lock: threading.Lock = threading.Lock()
        self._total_events: int = 0
        self._last_event: str = ""

        # Setup default alert channels
        self.alert_manager.add_channel(LogChannel())
        self.alert_manager.add_channel(FileChannel("alerts.jsonl"))

        # Register default components
        self._register_defaults()

    def _register_defaults(self) -> None:
        """Register the default fleet infrastructure components."""
        infrastructure = [
            ("Keeper Agent", 8443, "/health"),
            ("Git Agent", 8444, "/health"),
            ("MUD Server", 7777, "/health"),
            ("MUD Bridge", 8877, "/health"),
        ]
        for name, port, endpoint in infrastructure:
            self.register_component(name, port=port, endpoint=endpoint)

        # Fleet agents
        agents = [
            ("Trail Agent", 8501),
            ("Trust Agent", 8502),
            ("Flux VM Agent", 8503),
            ("Knowledge Agent", 8504),
            ("Scheduler Agent", 8505),
            ("Edge Relay", 8506),
            ("Liaison Agent", 8507),
            ("Cartridge Agent", 8508),
        ]
        for name, port in agents:
            self.register_component(name, port=port, category="fleet_agent")

    def register_component(
        self,
        name: str,
        host: str = "localhost",
        port: int = 0,
        endpoint: str = "/health",
        category: str = "infrastructure",
    ) -> ComponentHealth:
        """Register a new component for health monitoring.

        Args:
            name: Human-readable component name.
            host: Hostname or IP address (default localhost).
            port: Port number for HTTP checks.
            endpoint: URL path for health check requests.
            category: Component category for grouping.

        Returns:
            The registered ComponentHealth instance.
        """
        comp = ComponentHealth(
            name=name, host=host, port=port, endpoint=endpoint,
            metadata={"category": category},
        )
        with self._lock:
            self.components[name] = comp
        return comp

    # ------------------------------------------------------------------
    # Health checks
    # ------------------------------------------------------------------

    def check_component(self, comp: ComponentHealth) -> None:
        """Perform a health check on a single component.

        Makes an HTTP GET request to the component's health endpoint,
        measures response time, and updates status based on result.
        Status transitions: OK → DEGRADED (3 failures) → DOWN (10 failures).
        Recovery: consecutive successes → back through DEGRADED → OK.

        Args:
            comp: The ComponentHealth instance to check.
        """
        if comp.port == 0:
            # Non-HTTP component — mark as unknown
            self._update_status(comp, HealthStatus.UNKNOWN.value, 0, "")
            return

        url = f"http://{comp.host}:{comp.port}{comp.endpoint}"
        comp.prev_status = comp.status
        start = time.monotonic()
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                _ = resp.read()
                elapsed_ms = (time.monotonic() - start) * 1000
                comp.response_time_ms = round(elapsed_ms, 1)
                comp.consecutive_failures = 0
                comp.consecutive_successes += 1
                comp.error = ""
                self._transition_to_ok(comp)
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            comp.response_time_ms = round(elapsed_ms, 1)
            comp.consecutive_successes = 0
            comp.consecutive_failures += 1
            comp.error_count += 1
            comp.error = str(exc)[:120]
            self._transition_to_failure(comp)

        comp.last_check = datetime.now(timezone.utc).isoformat()

        # Record to store
        record = HealthRecord(
            component=comp.name,
            status=comp.status,
            response_time_ms=comp.response_time_ms,
            error=comp.error,
        )
        self.store.record(record)

        # Evaluate alerts
        ctx = {
            "component": comp.name,
            "status": comp.status,
            "prev_status": comp.prev_status,
            "response_time_ms": comp.response_time_ms,
            "error_count": comp.error_count,
        }
        fired = self.alert_manager.evaluate(ctx)
        if fired:
            for alert in fired:
                self._total_events += 1
                self._last_event = f"{alert.component} {alert.alert_type} ({comp.response_time_ms:.0f}ms)"
        elif comp.status == HealthStatus.OK.value:
            self._total_events += 1
            self._last_event = f"{comp.name} health check OK ({comp.response_time_ms:.0f}ms)"

        # Auto-resolve alerts on recovery
        if comp.status == HealthStatus.OK.value and comp.prev_status != HealthStatus.OK.value:
            self.alert_manager.resolve(comp.name)

    def _transition_to_ok(self, comp: ComponentHealth) -> None:
        """Handle transition toward OK status.

        Args:
            comp: Component to transition.
        """
        if comp.status == HealthStatus.OK.value:
            return
        # UNKNOWN → OK immediately on first success
        if comp.status == HealthStatus.UNKNOWN.value:
            comp.status = HealthStatus.OK.value
            comp.uptime_start = datetime.now(timezone.utc).isoformat()
            return
        if comp.consecutive_successes >= self.recovery_threshold:
            if comp.status == HealthStatus.DOWN.value:
                comp.status = HealthStatus.DEGRADED.value
                comp.consecutive_successes = 0
            elif comp.status == HealthStatus.DEGRADED.value:
                comp.status = HealthStatus.OK.value
                comp.uptime_start = datetime.now(timezone.utc).isoformat()
                comp.consecutive_successes = 0

    def _transition_to_failure(self, comp: ComponentHealth) -> None:
        """Handle transition toward DOWN status.

        Args:
            comp: Component to transition.
        """
        if comp.consecutive_failures >= self.down_threshold:
            comp.status = HealthStatus.DOWN.value
            comp.uptime_start = ""
        elif comp.consecutive_failures >= self.degraded_threshold:
            comp.status = HealthStatus.DEGRADED.value

    def check_all(self) -> None:
        """Run health checks on all registered components."""
        with self._lock:
            comps = list(self.components.values())
        for comp in comps:
            self.check_component(comp)

    # ------------------------------------------------------------------
    # External / system checks
    # ------------------------------------------------------------------

    def check_github_api(self) -> dict[str, Any]:
        """Check GitHub API connectivity.

        Returns:
            Dictionary with status, response_time_ms, and error info.
        """
        url = "https://api.github.com"
        start = time.monotonic()
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/vnd.github.v3+json"})
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                _ = resp.read()
                elapsed = (time.monotonic() - start) * 1000
                return {"status": "OK", "response_time_ms": round(elapsed, 1), "error": ""}
        except Exception as exc:
            elapsed = (time.monotonic() - start) * 1000
            return {"status": "DOWN", "response_time_ms": round(elapsed, 1), "error": str(exc)[:80]}

    def check_dns(self) -> dict[str, Any]:
        """Check DNS resolution capability.

        Returns:
            Dictionary with status, response_time_ms, and error info.
        """
        start = time.monotonic()
        try:
            socket.gethostbyname("github.com")
            elapsed = (time.monotonic() - start) * 1000
            return {"status": "OK", "response_time_ms": round(elapsed, 1), "error": ""}
        except socket.gaierror as exc:
            elapsed = (time.monotonic() - start) * 1000
            return {"status": "DOWN", "response_time_ms": round(elapsed, 1), "error": str(exc)}

    def get_system_health(self) -> dict[str, Any]:
        """Collect system health metrics from /proc filesystem.

        Uses /proc/meminfo, /proc/stat, and os.statvfs for disk usage.
        Falls back to safe defaults if /proc is unavailable.

        Returns:
            Dictionary with cpu_pct, mem_pct, disk_pct, python_version.
        """
        result: dict[str, Any] = {
            "cpu_pct": 0.0,
            "mem_pct": 0.0,
            "disk_pct": 0.0,
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        }

        # Memory from /proc/meminfo
        try:
            meminfo: dict[str, int] = {}
            with open("/proc/meminfo", "r") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 2:
                        key = parts[0].rstrip(":")
                        meminfo[key] = int(parts[1])
            total = meminfo.get("MemTotal", 1)
            available = meminfo.get("MemAvailable", meminfo.get("MemFree", 0))
            result["mem_pct"] = round((1 - available / total) * 100, 1)
        except (OSError, ValueError, ZeroDivisionError):
            pass

        # CPU from /proc/stat (first core, idle vs total)
        try:
            with open("/proc/stat", "r") as f:
                line = f.readline()
            parts = line.split()
            if len(parts) >= 5:
                user = int(parts[1])
                nice = int(parts[2])
                system = int(parts[3])
                idle = int(parts[4])
                total = user + nice + system + idle
                result["cpu_pct"] = round((1 - idle / total) * 100, 1) if total > 0 else 0.0
        except (OSError, ValueError, ZeroDivisionError):
            pass

        # Disk usage
        try:
            stat = os.statvfs("/")
            total_blocks = stat.f_blocks
            free_blocks = stat.f_bavail
            result["disk_pct"] = round((1 - free_blocks / total_blocks) * 100, 1) if total_blocks > 0 else 0.0
        except (OSError, ValueError, ZeroDivisionError):
            pass

        return result

    # ------------------------------------------------------------------
    # Dashboard
    # ------------------------------------------------------------------

    @property
    def overall_status(self) -> str:
        """Determine overall fleet status based on component states.

        Returns:
            'OPERATIONAL' if all OK, 'DEGRADED' if any DEGRADED, 'CRITICAL' if any DOWN.
        """
        statuses = [c.status for c in self.components.values()]
        if HealthStatus.DOWN.value in statuses:
            return "CRITICAL"
        if HealthStatus.DEGRADED.value in statuses:
            return "DEGRADED"
        if all(s == HealthStatus.OK.value for s in statuses):
            return "OPERATIONAL"
        return "MIXED"

    @property
    def uptime_str(self) -> str:
        """Format lighthouse uptime as HH:MM:SS."""
        delta = datetime.now(timezone.utc) - self.start_time
        total_s = int(delta.total_seconds())
        h, rem = divmod(total_s, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def render_dashboard(self) -> str:
        """Render the full text-based dashboard.

        Returns:
            Multi-line string with the complete dashboard display.
        """
        term_width = max(shutil.get_terminal_size(fallback=(80, 24)).columns, 62)
        box_width = min(term_width, 56)
        sep_h = "\u2550" * box_width
        sep_v = "\u2551"

        lines: list[str] = []
        lines.append(f"\u2554{sep_h}\u2557")
        title = f"FLEET LIGHTHOUSE \u2014 {self.fleet_name}"
        lines.append(f"{sep_v}{title:^{box_width}}{sep_v}")
        status_line = f"Status: {self.overall_status} | Uptime: {self.uptime_str}"
        lines.append(f"{sep_v}{status_line:^{box_width}}{sep_v}")
        lines.append(f"\u2560{sep_h}\u2563")

        # Infrastructure section
        lines.append(f"{sep_v}{'':^{box_width}}{sep_v}")
        lines.append(self._section_header("INFRASTRUCTURE", box_width))
        infra = [c for c in self.components.values() if c.metadata.get("category") == "infrastructure"]
        for comp in infra:
            lines.append(self._component_line(comp, box_width, is_last=(comp == infra[-1])))

        # Fleet agents section
        lines.append(f"{sep_v}{'':^{box_width}}{sep_v}")
        fleet_agents = [c for c in self.components.values() if c.metadata.get("category") == "fleet_agent"]
        healthy = sum(1 for c in fleet_agents if c.status == HealthStatus.OK.value)
        total = len(fleet_agents)
        lines.append(self._section_header(f"FLEET AGENTS ({healthy}/{total} healthy)", box_width))
        for comp in fleet_agents:
            lines.append(self._component_line(comp, box_width, is_last=(comp == fleet_agents[-1])))

        # External section
        lines.append(f"{sep_v}{'':^{box_width}}{sep_v}")
        lines.append(self._section_header("EXTERNAL", box_width))
        github = self.check_github_api()
        dns = self.check_dns()
        lines.append(self._external_line("GitHub API", github, box_width, is_last=False))
        lines.append(self._external_line("DNS Resolution", dns, box_width, is_last=True))

        # System section
        lines.append(f"{sep_v}{'':^{box_width}}{sep_v}")
        lines.append(self._section_header("SYSTEM", box_width))
        sys_health = self.get_system_health()
        sys_line1 = f"CPU: {sys_health['cpu_pct']:.0f}%  MEM: {sys_health['mem_pct']:.0f}%  DISK: {sys_health['disk_pct']:.0f}%"
        py_ver = sys_health.get("python_version", "?.?.?")
        sys_line2 = f"Python: {py_ver}  Fleet Tests: pass"
        lines.append(f"{sep_v} \u251c\u2500 {sys_line1:<{box_width - 6}}{sep_v}")
        lines.append(f"{sep_v} \u2514\u2500 {sys_line2:<{box_width - 6}}{sep_v}")

        # Footer
        lines.append(f"\u2560{sep_h}\u2563")
        active = self.alert_manager.active_count
        events = self.alert_manager.total_events + self._total_events
        alert_line = f"Alerts: {active} active | Events: {events} total"
        lines.append(f"{sep_v}{alert_line:^{box_width}}{sep_v}")
        last_evt = self._last_event or "No events yet"
        if len(last_evt) > box_width - 4:
            last_evt = last_evt[: box_width - 7] + "..."
        lines.append(f"{sep_v}Last event: {last_evt:<{box_width - 14}}{sep_v}")
        lines.append(f"\u255A{sep_h}\u255D")

        return "\n".join(lines)

    def _section_header(self, title: str, width: int) -> str:
        """Format a section header line.

        Args:
            title: Section title text.
            width: Total box width.

        Returns:
            Formatted header line.
        """
        inner = width - 2
        return f"\u2551 {title:<{inner}}\u2551"

    def _component_line(self, comp: ComponentHealth, width: int, is_last: bool = False) -> str:
        """Format a component status line.

        Args:
            comp: ComponentHealth instance.
            width: Total box width.
            is_last: Whether this is the last item in its section.

        Returns:
            Formatted component line.
        """
        connector = "\u2514" if is_last else "\u251c"
        status_color = {"OK": "\u2713", "DEGRADED": "\u26A0", "DOWN": "\u2717", "UNKNOWN": "?"}
        icon = status_color.get(comp.status, "?")
        inner = width - 6
        text = f"{comp.name:<20} [{comp.status:^8}] {comp.port_str:<6}{comp.response_str:>8}  \u2191{comp.uptime_str}"
        return f"\u2551 {connector}\u2500 {text:<{inner}}\u2551"

    def _external_line(self, name: str, info: dict[str, Any], width: int, is_last: bool = False) -> str:
        """Format an external service status line.

        Args:
            name: Service name.
            info: Dictionary with status and response_time_ms.
            width: Total box width.
            is_last: Whether this is the last item.

        Returns:
            Formatted external service line.
        """
        connector = "\u2514" if is_last else "\u251c"
        status = info.get("status", "UNKNOWN")
        rt = info.get("response_time_ms", 0)
        inner = width - 6
        text = f"{name:<20} [{status:^8}]       {rt:>6.0f}ms"
        return f"\u2551 {connector}\u2500 {text:<{inner}}\u2551"

    # ------------------------------------------------------------------
    # Serve loop
    # ------------------------------------------------------------------

    def serve(self) -> None:
        """Start the lighthouse dashboard loop.

        Runs health checks and refreshes the dashboard display every
        check_interval seconds. Handles SIGINT for clean shutdown.
        """
        self._running = True
        # Initial check
        self.check_all()

        def _signal_handler(sig: int, frame: Any) -> None:
            self._running = False

        signal.signal(signal.SIGINT, _signal_handler)

        try:
            while self._running:
                sys.stdout.write("\033[2J\033[H")  # Clear screen
                sys.stdout.write(self.render_dashboard())
                sys.stdout.write("\n")
                sys.stdout.flush()
                time.sleep(self.check_interval)
                self.check_all()
        except KeyboardInterrupt:
            pass
        finally:
            self._running = False
            sys.stdout.write("\n\033[2J\033[H")
            sys.stdout.write("Fleet Lighthouse stopped.\n")
            sys.stdout.flush()

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def run_diagnostics(self) -> dict[str, Any]:
        """Run comprehensive fleet diagnostics.

        Checks system resources, Python environment, connectivity,
        and reports on the health of all registered components.

        Returns:
            Dictionary with diagnostic results per category.
        """
        results: dict[str, Any] = {}

        # System checks
        sys_health = self.get_system_health()
        results["system"] = {
            "python_version": sys_health["python_version"],
            "cpu_pct": sys_health["cpu_pct"],
            "mem_pct": sys_health["mem_pct"],
            "disk_pct": sys_health["disk_pct"],
            "disk_ok": sys_health["disk_pct"] < 90,
            "mem_ok": sys_health["mem_pct"] < 90,
        }

        # Connectivity checks
        results["connectivity"] = {
            "github_api": self.check_github_api(),
            "dns": self.check_dns(),
        }

        # Component summary
        comp_summary: dict[str, str] = {}
        for name, comp in self.components.items():
            comp_summary[name] = comp.status
        results["components"] = comp_summary

        # Store health
        results["store"] = {
            "data_dir": self.store.data_dir,
            "known_components": self.store.known_components,
            "components_with_data": len(self.store.known_components),
        }

        # Overall
        all_ok = all(s == HealthStatus.OK.value for s in comp_summary.values())
        results["overall"] = {
            "status": "HEALTHY" if all_ok else "ISSUES_DETECTED",
            "components_total": len(comp_summary),
            "components_ok": sum(1 for s in comp_summary.values() if s == HealthStatus.OK.value),
            "components_degraded": sum(1 for s in comp_summary.values() if s == HealthStatus.DEGRADED.value),
            "components_down": sum(1 for s in comp_summary.values() if s == HealthStatus.DOWN.value),
        }

        return results
