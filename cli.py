"""Fleet Lighthouse CLI — command-line interface for the health dashboard.

Subcommands:
    serve      Start the live lighthouse dashboard.
    status     One-shot fleet status check and display.
    history    Show health history for a component.
    alerts     Display active and recent alerts.
    export     Export health data to JSON or CSV format.
    doctor     Run comprehensive fleet diagnostics.

Usage:
    python cli.py serve [--interval N] [--fleet-name NAME]
    python cli.py status
    python cli.py history <component> [--limit N]
    python cli.py alerts [--all]
    python cli.py export [--component NAME] [--format json|csv] [--output FILE]
    python cli.py doctor
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Optional

# Ensure the lighthouse package directory is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from alerting import AlertSeverity
from health_store import HealthStatus
from lighthouse import FleetLighthouse


def create_lighthouse(args: argparse.Namespace) -> FleetLighthouse:
    """Create a FleetLighthouse instance from parsed CLI arguments.

    Args:
        args: Parsed argument namespace with optional fleet_name and data_dir.

    Returns:
        Configured FleetLighthouse instance.
    """
    return FleetLighthouse(
        fleet_name=getattr(args, "fleet_name", "Pelagic Fleet"),
        data_dir=getattr(args, "data_dir", "health_data"),
    )


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


def cmd_serve(args: argparse.Namespace) -> int:
    """Start the live lighthouse dashboard.

    Continuously refreshes the health display at the specified interval.
    Press Ctrl+C to stop.

    Args:
        args: Namespace with interval and fleet_name options.

    Returns:
        Exit code (0 for clean shutdown).
    """
    lh = FleetLighthouse(
        fleet_name=args.fleet_name,
        check_interval=args.interval,
        data_dir=args.data_dir,
    )
    lh.serve()
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Run a one-shot fleet status check.

    Checks all registered components once and displays the dashboard.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Exit code (0 if all OK, 1 if any issues detected).
    """
    lh = create_lighthouse(args)
    lh.check_all()
    print(lh.render_dashboard())
    return 0 if lh.overall_status == "OPERATIONAL" else 1


def cmd_history(args: argparse.Namespace) -> int:
    """Show health history for a specific component.

    Args:
        args: Namespace with component name and optional limit.

    Returns:
        Exit code (0 on success, 1 if component not found).
    """
    lh = create_lighthouse(args)
    records = lh.store.get_history(args.component, limit=args.limit)

    if not records:
        print(f"No health history found for component: {args.component}")
        return 1

    print(f"\n  Health History: {args.component} (last {len(records)} records)\n")
    print(f"  {'Timestamp':<26} {'Status':<10} {'Response':>10}  Error")
    print(f"  {'─' * 26} {'─' * 10} {'─' * 10}  {'─' * 20}")

    for rec in records:
        ts_short = rec.timestamp[:26]
        err = rec.error[:20] if rec.error else ""
        print(f"  {ts_short:<26} {rec.status:<10} {rec.response_time_ms:>8.1f}ms  {err}")

    # Also show statistics
    stats = lh.store.get_statistics(args.component)
    print(f"\n  Statistics:")
    print(f"    Total checks:    {stats['total_checks']}")
    print(f"    Avg response:    {stats['avg_response_ms']:.1f}ms")
    print(f"    Uptime:          {stats['uptime_pct']:.1f}%")
    print(f"    OK/DEGRADED/DOWN: {stats['ok_count']}/{stats['degraded_count']}/{stats['down_count']}")
    return 0


def cmd_alerts(args: argparse.Namespace) -> int:
    """Display active and recent alerts.

    Args:
        args: Namespace with optional --all flag to show full history.

    Returns:
        Exit code (always 0).
    """
    lh = create_lighthouse(args)
    active = lh.alert_manager.get_active_alerts()
    total = lh.alert_manager.total_events

    print(f"\n  Fleet Alerts (active: {len(active)}, total events: {total})\n")

    if active:
        print("  ACTIVE ALERTS:")
        print(f"  {'ID':<30} {'Type':<20} {'Component':<18} {'Severity':<10}")
        print(f"  {'─' * 30} {'─' * 20} {'─' * 18} {'─' * 10}")
        for alert in active:
            ack = " [ACK]" if alert.acknowledged else ""
            print(
                f"  {alert.alert_id:<30} {alert.alert_type:<20} "
                f"{alert.component:<18} {alert.severity:<10}{ack}"
            )
    else:
        print("  No active alerts. All clear.")

    if args.all:
        history = lh.alert_manager.get_alert_history(limit=50)
        if history:
            print(f"\n  RECENT ALERT HISTORY (last {len(history)}):")
            print(f"  {'Time':<22} {'Type':<20} {'Component':<18} {'Severity':<10}")
            print(f"  {'─' * 22} {'─' * 20} {'─' * 18} {'─' * 10}")
            for alert in history:
                ts = alert.timestamp[:19]
                resolved = " [RESOLVED]" if alert.resolved else ""
                print(
                    f"  {ts:<22} {alert.alert_type:<20} "
                    f"{alert.component:<18} {alert.severity:<10}{resolved}"
                )

    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Export health data to JSON or CSV format.

    Args:
        args: Namespace with component, format, and output options.

    Returns:
        Exit code (0 on success, 1 on error).
    """
    lh = create_lighthouse(args)
    fmt = args.format.lower()

    if fmt == "csv":
        if not args.component:
            print("Error: --component is required for CSV export.")
            return 1
        data = lh.store.export_csv(args.component)
    else:
        data = lh.store.export_json(args.component)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(data)
        size = os.path.getsize(args.output)
        print(f"  Exported to {args.output} ({size} bytes, {fmt.upper()})")
    else:
        print(data)

    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    """Run comprehensive fleet diagnostics.

    Checks system health, connectivity, component status, and
    data store integrity.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Exit code (0 if all healthy, 1 if issues detected).
    """
    lh = create_lighthouse(args)
    print("\n  Running Fleet Diagnostics...\n")
    results = lh.run_diagnostics()

    issues_found = False

    # System
    sys_info = results["system"]
    disk_icon = "\u2713" if sys_info["disk_ok"] else "\u2717"
    mem_icon = "\u2713" if sys_info["mem_ok"] else "\u2717"
    print(f"  System:")
    print(f"    Python version:  {sys_info['python_version']}")
    print(f"    {disk_icon}  Disk usage:      {sys_info['disk_pct']}%")
    print(f"    {mem_icon}  Memory usage:    {sys_info['mem_pct']}%")
    if not sys_info["disk_ok"] or not sys_info["mem_ok"]:
        issues_found = True

    # Connectivity
    print(f"\n  Connectivity:")
    for name, info in results["connectivity"].items():
        icon = "\u2713" if info["status"] == "OK" else "\u2717"
        print(f"    {icon}  {name}: {info['status']} ({info['response_time_ms']:.0f}ms)")
        if info["status"] != "OK":
            issues_found = True

    # Components
    print(f"\n  Components ({results['overall']['components_total']} registered):")
    for name, status in results["components"].items():
        icon = {"OK": "\u2713", "DEGRADED": "\u26A0", "DOWN": "\u2717"}.get(status, "?")
        print(f"    {icon}  {name}: {status}")
        if status in (HealthStatus.DEGRADED.value, HealthStatus.DOWN.value):
            issues_found = True

    # Store
    print(f"\n  Data Store:")
    print(f"    Directory:  {results['store']['data_dir']}")
    print(f"    Components: {results['store']['components_with_data']} with data")

    # Summary
    print(f"\n  Overall: {results['overall']['status']}")
    summary = results["overall"]
    print(
        f"    OK: {summary['components_ok']}  "
        f"Degraded: {summary['components_degraded']}  "
        f"Down: {summary['components_down']}"
    )

    if issues_found:
        print("\n  \u26A0 Issues detected. Review output above.")
        return 1
    else:
        print("\n  \u2713 All checks passed. Fleet is healthy.")
        return 0


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser with all subcommands.

    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        prog="lighthouse",
        description="Fleet Lighthouse — unified health monitoring dashboard",
    )
    parser.add_argument(
        "--fleet-name",
        default="Pelagic Fleet",
        help="Name of the fleet (default: Pelagic Fleet)",
    )
    parser.add_argument(
        "--data-dir",
        default="health_data",
        help="Directory for health data storage (default: health_data)",
    )

    sub = parser.add_subparsers(dest="command", help="Available commands")

    # serve
    p_serve = sub.add_parser("serve", help="Start the live dashboard")
    p_serve.add_argument("--interval", type=int, default=5, help="Check interval in seconds")
    p_serve.set_defaults(func=cmd_serve)

    # status
    p_status = sub.add_parser("status", help="One-shot fleet status check")
    p_status.set_defaults(func=cmd_status)

    # history
    p_history = sub.add_parser("history", help="Show health history for a component")
    p_history.add_argument("component", help="Component name to query")
    p_history.add_argument("--limit", type=int, default=20, help="Max records to show")
    p_history.set_defaults(func=cmd_history)

    # alerts
    p_alerts = sub.add_parser("alerts", help="Display active alerts")
    p_alerts.add_argument("--all", action="store_true", help="Show full alert history")
    p_alerts.set_defaults(func=cmd_alerts)

    # export
    p_export = sub.add_parser("export", help="Export health data")
    p_export.add_argument("--component", help="Specific component (required for CSV)")
    p_export.add_argument("--format", choices=["json", "csv"], default="json", help="Output format")
    p_export.add_argument("--output", help="Output file path (default: stdout)")
    p_export.set_defaults(func=cmd_export)

    # doctor
    p_doctor = sub.add_parser("doctor", help="Run fleet diagnostics")
    p_doctor.set_defaults(func=cmd_doctor)

    return parser


def main() -> int:
    """Entry point for the Fleet Lighthouse CLI.

    Parses arguments and dispatches to the appropriate subcommand handler.

    Returns:
        Exit code from the subcommand handler.
    """
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
