"""Tests for the Fleet Lighthouse system.

Covers:
    - Health checks with mock HTTP server
    - HealthStore JSONL persistence, query, export, pruning
    - AlertManager rule evaluation, deduplication, channels
    - Dashboard rendering (capture stdout)
    - System health checks
    - CLI subcommands
    - FleetLighthouse status transitions

Uses only stdlib: unittest, http.server, threading, json, io, tempfile.
"""

from __future__ import annotations

import http.server
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timezone

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alerting import (
    Alert,
    AlertManager,
    AlertSeverity,
    AlertType,
    FileChannel,
    LogChannel,
)
from health_store import HealthRecord, HealthStatus, HealthStore
from lighthouse import ComponentHealth, FleetLighthouse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_free_port() -> int:
    """Find a free port on localhost.

    Returns:
        An available port number.
    """
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


class _HealthHandler(http.server.BaseHTTPRequestHandler):
    """Mock HTTP handler that responds 200 to /health."""

    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        """Suppress log output during tests."""
        pass


class _SlowHandler(http.server.BaseHTTPRequestHandler):
    """Mock HTTP handler that responds slowly (2s delay)."""

    def do_GET(self) -> None:
        time.sleep(2)
        self.send_response(200)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


def _start_server(handler_class: type, port: int) -> http.server.HTTPServer:
    """Start a test HTTP server in a background thread.

    Args:
        handler_class: Request handler class to use.
        port: Port to bind to.

    Returns:
        Running HTTPServer instance.
    """
    server = http.server.HTTPServer(("localhost", port), handler_class)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    # Brief pause to ensure server is ready
    time.sleep(0.2)
    return server


# ---------------------------------------------------------------------------
# HealthStore Tests
# ---------------------------------------------------------------------------


class TestHealthStore(unittest.TestCase):
    """Tests for the HealthStore JSONL persistence layer."""

    def setUp(self) -> None:
        """Create a temporary directory for test data."""
        self.tmpdir = tempfile.mkdtemp()
        self.store = HealthStore(data_dir=self.tmpdir, max_records=50)

    def tearDown(self) -> None:
        """Clean up temporary test data."""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_record_and_retrieve(self) -> None:
        """Store a record and retrieve it."""
        rec = HealthRecord(
            component="Test Agent",
            status="OK",
            response_time_ms=12.5,
        )
        self.store.record(rec)
        latest = self.store.get_latest("Test Agent")
        self.assertIsNotNone(latest)
        self.assertEqual(latest.component, "Test Agent")
        self.assertEqual(latest.status, "OK")
        self.assertAlmostEqual(latest.response_time_ms, 12.5, places=1)

    def test_multiple_records_history(self) -> None:
        """Store multiple records and retrieve history."""
        for i in range(10):
            self.store.record(HealthRecord(
                component="Agent X",
                status="OK" if i % 2 == 0 else "DEGRADED",
                response_time_ms=float(i * 3),
            ))
        history = self.store.get_history("Agent X", limit=5)
        self.assertEqual(len(history), 5)
        # Newest first — last record should be DEGRADED
        self.assertEqual(history[0].status, "DEGRADED")

    def test_get_all_latest(self) -> None:
        """Get latest records for all components."""
        self.store.record(HealthRecord(component="A", status="OK", response_time_ms=1))
        self.store.record(HealthRecord(component="B", status="DOWN", response_time_ms=0))
        all_latest = self.store.get_all_latest()
        self.assertEqual(len(all_latest), 2)
        self.assertEqual(all_latest["A"].status, "OK")
        self.assertEqual(all_latest["B"].status, "DOWN")

    def test_statistics(self) -> None:
        """Verify computed statistics are correct."""
        for i in range(20):
            status = "OK" if i < 15 else ("DEGRADED" if i < 18 else "DOWN")
            self.store.record(HealthRecord(
                component="Stat Agent",
                status=status,
                response_time_ms=float(10 + i),
            ))
        stats = self.store.get_statistics("Stat Agent")
        self.assertEqual(stats["total_checks"], 20)
        self.assertEqual(stats["ok_count"], 15)
        self.assertEqual(stats["degraded_count"], 3)
        self.assertEqual(stats["down_count"], 2)
        self.assertGreater(stats["avg_response_ms"], 0)
        self.assertGreater(stats["uptime_pct"], 0)

    def test_export_json(self) -> None:
        """Export health data as JSON."""
        self.store.record(HealthRecord(component="Exp Agent", status="OK", response_time_ms=5))
        data_str = self.store.export_json("Exp Agent")
        parsed = json.loads(data_str)
        self.assertIsInstance(parsed, list)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["status"], "OK")

    def test_export_csv(self) -> None:
        """Export health data as CSV."""
        self.store.record(HealthRecord(component="CSV Agent", status="OK", response_time_ms=7))
        csv_str = self.store.export_csv("CSV Agent")
        lines = csv_str.strip().split("\n")
        self.assertEqual(len(lines), 2)  # header + data
        self.assertIn("component", lines[0])
        self.assertIn("CSV Agent", lines[1])

    def test_prune_old_data(self) -> None:
        """Verify old records are pruned correctly."""
        # Insert records with a very old timestamp
        old_time = "2020-01-01T00:00:00+00:00"
        now_time = "2099-01-01T00:00:00+00:00"
        self.store.record(HealthRecord(
            component="Prune Agent", status="OK", timestamp=old_time,
        ))
        self.store.record(HealthRecord(
            component="Prune Agent", status="OK", timestamp=now_time,
        ))
        pruned = self.store.prune_old_data(max_age_hours=1)
        # The old record should be pruned
        self.assertGreaterEqual(pruned, 1)
        remaining = self.store.get_history("Prune Agent", limit=100)
        self.assertEqual(len(remaining), 1)

    def test_max_records_truncation(self) -> None:
        """Verify cache truncation when exceeding max_records."""
        for i in range(60):
            self.store.record(HealthRecord(
                component="Max Agent", status="OK", response_time_ms=float(i),
            ))
        history = self.store.get_history("Max Agent", limit=200)
        self.assertLessEqual(len(history), 50)

    def test_known_components(self) -> None:
        """Verify known_components lists all stored components."""
        self.store.record(HealthRecord(component="Alpha", status="OK"))
        self.store.record(HealthRecord(component="Beta", status="DOWN"))
        known = self.store.known_components
        self.assertIn("Alpha", known)
        self.assertIn("Beta", known)

    def test_nonexistent_component(self) -> None:
        """Querying a nonexistent component returns None/empty."""
        self.assertIsNone(self.store.get_latest("Ghost"))
        self.assertEqual(self.store.get_history("Ghost"), [])
        stats = self.store.get_statistics("Ghost")
        self.assertEqual(stats["total_checks"], 0)


# ---------------------------------------------------------------------------
# AlertManager Tests
# ---------------------------------------------------------------------------


class TestAlertManager(unittest.TestCase):
    """Tests for the AlertManager rule evaluation and dedup system."""

    def setUp(self) -> None:
        """Create a fresh AlertManager for each test."""
        self.manager = AlertManager(dedup_window=1)

    def test_agent_down_fires(self) -> None:
        """DOWN status triggers AGENT_DOWN alert."""
        alerts = self.manager.evaluate({
            "component": "Test Agent",
            "status": "DOWN",
        })
        types = [a.alert_type for a in alerts]
        self.assertIn(AlertType.AGENT_DOWN.value, types)

    def test_agent_degraded_fires(self) -> None:
        """DEGRADED status triggers AGENT_DEGRADED alert."""
        alerts = self.manager.evaluate({
            "component": "Test Agent",
            "status": "DEGRADED",
        })
        types = [a.alert_type for a in alerts]
        self.assertIn(AlertType.AGENT_DEGRADED.value, types)

    def test_recovery_fires(self) -> None:
        """Recovery from DOWN triggers RECOVERY alert."""
        alerts = self.manager.evaluate({
            "component": "Test Agent",
            "status": "OK",
            "prev_status": "DOWN",
        })
        types = [a.alert_type for a in alerts]
        self.assertIn(AlertType.RECOVERY.value, types)

    def test_high_error_rate_fires(self) -> None:
        """Error count exceeding threshold triggers alert."""
        alerts = self.manager.evaluate({
            "component": "Test Agent",
            "status": "OK",
            "error_count": 10,
        })
        types = [a.alert_type for a in alerts]
        self.assertIn(AlertType.HIGH_ERROR_RATE.value, types)

    def test_slow_response_fires(self) -> None:
        """Slow response time triggers SLOW_RESPONSE alert."""
        alerts = self.manager.evaluate({
            "component": "Test Agent",
            "status": "OK",
            "response_time_ms": 2000,
        })
        types = [a.alert_type for a in alerts]
        self.assertIn(AlertType.SLOW_RESPONSE.value, types)

    def test_disk_full_fires(self) -> None:
        """High disk usage triggers DISK_FULL alert."""
        alerts = self.manager.evaluate({
            "component": "System",
            "status": "OK",
            "disk_pct": 95,
        })
        types = [a.alert_type for a in alerts]
        self.assertIn(AlertType.DISK_FULL.value, types)

    def test_deduplication(self) -> None:
        """Duplicate alerts are suppressed within the dedup window."""
        ctx = {"component": "Dedup Agent", "status": "DOWN"}
        first = self.manager.evaluate(ctx)
        second = self.manager.evaluate(ctx)
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 0)

    def test_acknowledge_alert(self) -> None:
        """Alerts can be acknowledged."""
        self.manager.evaluate({"component": "Ack Agent", "status": "DOWN"})
        active = self.manager.get_active_alerts()
        self.assertTrue(len(active) > 0)
        alert_id = active[0].alert_id
        result = self.manager.acknowledge(alert_id)
        self.assertTrue(result)
        active2 = self.manager.get_active_alerts()
        self.assertTrue(active2[0].acknowledged)

    def test_resolve_alerts(self) -> None:
        """Alerts for a component can be resolved."""
        self.manager.evaluate({"component": "Resolve Agent", "status": "DOWN"})
        self.manager.evaluate({"component": "Resolve Agent", "status": "DEGRADED"})
        resolved = self.manager.resolve("Resolve Agent")
        self.assertEqual(resolved, 2)
        self.assertEqual(len(self.manager.get_active_alerts()), 0)

    def test_alert_history(self) -> None:
        """Alert history is tracked correctly."""
        self.manager.evaluate({"component": "Hist Agent", "status": "DOWN"})
        history = self.manager.get_alert_history(limit=10)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].component, "Hist Agent")

    def test_total_events(self) -> None:
        """Total events counter increments correctly."""
        self.assertEqual(self.manager.total_events, 0)
        self.manager.evaluate({"component": "Ev Agent", "status": "DOWN"})
        self.assertGreaterEqual(self.manager.total_events, 1)

    def test_file_channel(self) -> None:
        """File channel writes alerts to a JSONL file."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", mode="w", delete=False) as f:
            fpath = f.name
        try:
            channel = FileChannel(fpath)
            alert = Alert(
                alert_type="TEST",
                component="FileTest",
                severity="INFO",
                message="Test alert",
            )
            result = channel.send(alert)
            self.assertTrue(result)
            with open(fpath, "r") as f:
                lines = f.readlines()
            self.assertEqual(len(lines), 1)
            data = json.loads(lines[0])
            self.assertEqual(data["alert_type"], "TEST")
        finally:
            os.unlink(fpath)

    def test_log_channel(self) -> None:
        """Log channel sends alert without error."""
        channel = LogChannel()
        alert = Alert(
            alert_type="LOG_TEST",
            component="LogTest",
            severity="INFO",
            message="Test log alert",
        )
        result = channel.send(alert)
        self.assertTrue(result)


# ---------------------------------------------------------------------------
# FleetLighthouse Tests
# ---------------------------------------------------------------------------


class TestFleetLighthouse(unittest.TestCase):
    """Tests for the FleetLighthouse dashboard and health checking."""

    def setUp(self) -> None:
        """Create a temporary directory and lighthouse instance."""
        self.tmpdir = tempfile.mkdtemp()
        self.lh = FleetLighthouse(
            fleet_name="Test Fleet",
            data_dir=self.tmpdir,
            check_interval=1,
        )

    def tearDown(self) -> None:
        """Clean up temporary test data."""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_register_component(self) -> None:
        """Components can be registered and retrieved."""
        comp = self.lh.register_component("Custom Agent", port=9999)
        self.assertEqual(comp.name, "Custom Agent")
        self.assertEqual(comp.port, 9999)
        self.assertIn("Custom Agent", self.lh.components)

    def test_default_components_registered(self) -> None:
        """Default infrastructure and fleet agents are registered."""
        self.assertIn("Keeper Agent", self.lh.components)
        self.assertIn("Trail Agent", self.lh.components)
        self.assertGreater(len(self.lh.components), 10)

    def test_health_check_ok(self) -> None:
        """Health check against a running server yields OK status."""
        port = _find_free_port()
        server = _start_server(_HealthHandler, port)
        try:
            comp = self.lh.register_component("Mock OK", port=port)
            self.lh.check_component(comp)
            self.assertEqual(comp.status, HealthStatus.OK.value)
            self.assertGreater(comp.response_time_ms, 0)
        finally:
            server.shutdown()

    def test_health_check_down(self) -> None:
        """Health check against a non-running server yields failure."""
        port = _find_free_port()
        comp = ComponentHealth(name="Mock Down", port=port, endpoint="/health")
        # Run multiple checks to trigger DOWN
        for _ in range(12):
            self.lh.check_component(comp)
        self.assertEqual(comp.status, HealthStatus.DOWN.value)
        self.assertGreater(comp.consecutive_failures, 0)

    def test_health_check_timeout(self) -> None:
        """Slow server triggers timeout handling."""
        port = _find_free_port()
        server = _start_server(_SlowHandler, port)
        try:
            comp = ComponentHealth(name="Slow Agent", port=port, endpoint="/health")
            # Set very short timeout to force failure
            original_timeout = self.lh.timeout
            self.lh.timeout = 0.5
            self.lh.check_component(comp)
            self.assertNotEqual(comp.status, HealthStatus.OK.value)
            self.lh.timeout = original_timeout
        finally:
            server.shutdown()

    def test_degraded_transition(self) -> None:
        """Status transitions through DEGRADED on consecutive failures."""
        port = _find_free_port()
        comp = ComponentHealth(name="Degrade Agent", port=port, endpoint="/health")
        # Exactly 3 failures → DEGRADED
        for _ in range(3):
            self.lh.check_component(comp)
        self.assertEqual(comp.status, HealthStatus.DEGRADED.value)

    def test_recovery_transition(self) -> None:
        """Component recovers from DEGRADED back to OK."""
        port = _find_free_port()
        server = _start_server(_HealthHandler, port)
        try:
            comp = ComponentHealth(name="Recover Agent", port=port, endpoint="/health")
            # Push to DOWN
            self.lh.down_threshold = 5
            self.lh.degraded_threshold = 2
            self.lh.recovery_threshold = 2

            # No server running yet — simulate failures
            orig_port = comp.port
            comp.port = _find_free_port()  # wrong port
            for _ in range(6):
                self.lh.check_component(comp)
            self.assertEqual(comp.status, HealthStatus.DOWN.value)

            # Now point to the real server
            comp.port = port
            # Need enough successes: DOWN→DEGRADED→OK
            for _ in range(self.lh.recovery_threshold * 2 + 2):
                self.lh.check_component(comp)
            self.assertEqual(comp.status, HealthStatus.OK.value)
        finally:
            server.shutdown()

    def test_check_all(self) -> None:
        """check_all() runs health checks on every registered component."""
        self.lh.check_all()
        for comp in self.lh.components.values():
            self.assertNotEqual(comp.last_check, "")

    def test_overall_status_operational(self) -> None:
        """Overall status is OPERATIONAL when all components are OK."""
        for comp in self.lh.components.values():
            comp.status = HealthStatus.OK.value
        self.assertEqual(self.lh.overall_status, "OPERATIONAL")

    def test_overall_status_critical(self) -> None:
        """Overall status is CRITICAL when any component is DOWN."""
        self.lh.components["Keeper Agent"].status = HealthStatus.DOWN.value
        self.assertEqual(self.lh.overall_status, "CRITICAL")

    def test_overall_status_degraded(self) -> None:
        """Overall status is DEGRADED when any component is DEGRADED."""
        self.lh.components["Keeper Agent"].status = HealthStatus.DEGRADED.value
        self.assertEqual(self.lh.overall_status, "DEGRADED")

    def test_uptime_str(self) -> None:
        """Uptime string is formatted correctly."""
        comp = ComponentHealth(name="Test")
        comp.uptime_start = datetime.now(timezone.utc).isoformat()
        self.assertRegex(comp.uptime_str, r"\d{2}:\d{2}:\d{2}")


# ---------------------------------------------------------------------------
# System Health Tests
# ---------------------------------------------------------------------------


class TestSystemHealth(unittest.TestCase):
    """Tests for system health monitoring functions."""

    def setUp(self) -> None:
        """Create a temporary directory for test data."""
        self.tmpdir = tempfile.mkdtemp()
        self.lh = FleetLighthouse(data_dir=self.tmpdir)

    def tearDown(self) -> None:
        """Clean up temporary test data."""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_system_health_returns_dict(self) -> None:
        """get_system_health returns expected keys."""
        result = self.lh.get_system_health()
        self.assertIn("cpu_pct", result)
        self.assertIn("mem_pct", result)
        self.assertIn("disk_pct", result)
        self.assertIn("python_version", result)

    def test_system_health_values_in_range(self) -> None:
        """System health percentages are between 0 and 100."""
        result = self.lh.get_system_health()
        for key in ("cpu_pct", "mem_pct", "disk_pct"):
            self.assertGreaterEqual(result[key], 0)
            self.assertLessEqual(result[key], 100)

    def test_python_version(self) -> None:
        """Python version is a valid version string."""
        result = self.lh.get_system_health()
        version = result["python_version"]
        parts = version.split(".")
        self.assertEqual(len(parts), 3)
        for p in parts:
            self.assertTrue(p.isdigit())

    def test_disk_usage(self) -> None:
        """Disk usage is nonzero (the filesystem has files on it)."""
        result = self.lh.get_system_health()
        self.assertGreater(result["disk_pct"], 0)


# ---------------------------------------------------------------------------
# Dashboard Tests
# ---------------------------------------------------------------------------


class TestDashboard(unittest.TestCase):
    """Tests for dashboard rendering and output."""

    def setUp(self) -> None:
        """Create a temporary directory and lighthouse instance."""
        self.tmpdir = tempfile.mkdtemp()
        self.lh = FleetLighthouse(fleet_name="Test Fleet", data_dir=self.tmpdir)

    def tearDown(self) -> None:
        """Clean up temporary test data."""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_render_dashboard_contains_title(self) -> None:
        """Dashboard output contains the fleet name."""
        output = self.lh.render_dashboard()
        self.assertIn("Test Fleet", output)

    def test_render_dashboard_contains_sections(self) -> None:
        """Dashboard output contains all section headers."""
        output = self.lh.render_dashboard()
        self.assertIn("INFRASTRUCTURE", output)
        self.assertIn("FLEET AGENTS", output)
        self.assertIn("EXTERNAL", output)
        self.assertIn("SYSTEM", output)

    def test_render_dashboard_contains_components(self) -> None:
        """Dashboard output lists registered components."""
        output = self.lh.render_dashboard()
        self.assertIn("Keeper Agent", output)
        self.assertIn("Trail Agent", output)

    def test_render_dashboard_contains_footer(self) -> None:
        """Dashboard output contains alert/event footer."""
        output = self.lh.render_dashboard()
        self.assertIn("Alerts:", output)
        self.assertIn("Events:", output)

    def test_render_with_real_check(self) -> None:
        """Dashboard renders correctly after health checks."""
        self.lh.check_all()
        output = self.lh.render_dashboard()
        # Should contain uptime info from the checks
        self.assertIn("Status:", output)


# ---------------------------------------------------------------------------
# Diagnostics Tests
# ---------------------------------------------------------------------------


class TestDiagnostics(unittest.TestCase):
    """Tests for the fleet diagnostics system."""

    def setUp(self) -> None:
        """Create a temporary directory and lighthouse instance."""
        self.tmpdir = tempfile.mkdtemp()
        self.lh = FleetLighthouse(data_dir=self.tmpdir)

    def tearDown(self) -> None:
        """Clean up temporary test data."""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_diagnostics_returns_all_sections(self) -> None:
        """Diagnostics output contains all expected sections."""
        results = self.lh.run_diagnostics()
        self.assertIn("system", results)
        self.assertIn("connectivity", results)
        self.assertIn("components", results)
        self.assertIn("store", results)
        self.assertIn("overall", results)

    def test_diagnostics_system_info(self) -> None:
        """System diagnostics include resource usage."""
        results = self.lh.run_diagnostics()
        sys_info = results["system"]
        self.assertIn("python_version", sys_info)
        self.assertIn("cpu_pct", sys_info)
        self.assertIn("mem_pct", sys_info)
        self.assertIn("disk_pct", sys_info)

    def test_diagnostics_connectivity(self) -> None:
        """Connectivity checks return status information."""
        results = self.lh.run_diagnostics()
        conn = results["connectivity"]
        self.assertIn("github_api", conn)
        self.assertIn("dns", conn)
        self.assertIn("status", conn["github_api"])
        self.assertIn("status", conn["dns"])

    def test_diagnostics_overall_summary(self) -> None:
        """Overall summary contains component counts."""
        results = self.lh.run_diagnostics()
        overall = results["overall"]
        self.assertIn("status", overall)
        self.assertIn("components_total", overall)
        self.assertIn("components_ok", overall)
        self.assertGreater(overall["components_total"], 0)


if __name__ == "__main__":
    unittest.main()
