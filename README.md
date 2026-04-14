# Fleet Lighthouse

Unified health monitoring dashboard for the Pelagic Fleet.

## Files

- `lighthouse.py` — FleetLighthouse class with health checking, status transitions, dashboard rendering, and diagnostics
- `health_store.py` — Persistent JSONL health data store with query, statistics, export, and pruning
- `alerting.py` — Alert management system with configurable rules, deduplication, and multiple channels
- `cli.py` — Command-line interface with serve, status, history, alerts, export, and doctor subcommands
- `tests/test_lighthouse.py` — Comprehensive test suite (48 tests)

## Usage

```bash
# Start live dashboard
python cli.py serve

# One-shot status check
python cli.py status

# View health history
python cli.py history "Keeper Agent"

# View alerts
python cli.py alerts --all

# Export data
python cli.py export --format json
python cli.py export --component "Trail Agent" --format csv --output trail.csv

# Run diagnostics
python cli.py doctor
```

## Components Monitored

| Component | Port | Category |
|-----------|------|----------|
| Keeper Agent | 8443 | Infrastructure |
| Git Agent | 8444 | Infrastructure |
| MUD Server | 7777 | Infrastructure |
| MUD Bridge | 8877 | Infrastructure |
| Trail Agent | 8501 | Fleet Agent |
| Trust Agent | 8502 | Fleet Agent |
| Flux VM Agent | 8503 | Fleet Agent |
| Knowledge Agent | 8504 | Fleet Agent |
| Scheduler Agent | 8505 | Fleet Agent |
| Edge Relay | 8506 | Fleet Agent |
| Liaison Agent | 8507 | Fleet Agent |
| Cartridge Agent | 8508 | Fleet Agent |

## Health Status Transitions

- **OK** → 3 consecutive failures → **DEGRADED**
- **DEGRADED** → 10 total consecutive failures → **DOWN**
- **DOWN** → 3 consecutive successes → **DEGRADED**
- **DEGRADED** → 3 consecutive successes → **OK**
- **UNKNOWN** → first success → **OK**
