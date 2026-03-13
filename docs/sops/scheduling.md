# Scheduling Daily Pipeline Runs

This guide covers setting up automated daily pipeline execution.

## Overview

The daily pipeline:
1. Fetches Cloudflare logs for configured domains
2. Runs the ETL transformation
3. Creates session aggregations
4. Generates reports

All configuration is read from `config.enc.yaml` - no hardcoded settings.

## Quick Start

### Manual Run

```bash
# Activate environment
source venv/bin/activate

# Run for yesterday
./scripts/daily_pipeline.sh

# Run for last 7 days
./scripts/daily_pipeline.sh --days 7

# Preview without writing
./scripts/daily_pipeline.sh --dry-run
```

### Cron Setup

```bash
# Edit crontab
crontab -e

# Add daily run at 6 AM
0 6 * * * /path/to/project/scripts/daily_pipeline.sh >> /path/to/project/logs/cron.log 2>&1
```

## Configuration

### Prerequisites

1. **SOPS Key**: Must be accessible at `~/.sops/age/keys.txt`
2. **Config File**: `config.enc.yaml` with domains configured
3. **Virtual Environment**: `venv/` with dependencies installed

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SOPS_AGE_KEY_FILE` | `~/.sops/age/keys.txt` | Path to SOPS decryption key |

### Config File (`config.enc.yaml`)

The pipeline reads all settings from the encrypted config:

```yaml
# Cloudflare API token (shared for all domains)
cloudflare:
  api_token: "your-token"

# Domains to process
domains:
  - domain: example.com
    zone_id: "abc123..."
    db_name: example.db
  - domain: another.com
    zone_id: "def456..."
    db_name: another.db
```

## Cron Examples

### Daily at 6 AM

```cron
0 6 * * * /path/to/project/scripts/daily_pipeline.sh
```

### Twice Daily (6 AM and 6 PM)

```cron
0 6,18 * * * /path/to/project/scripts/daily_pipeline.sh
```

### Weekly Full Refresh (Sunday at 2 AM)

```cron
0 2 * * 0 /path/to/project/scripts/daily_pipeline.sh --days 7
```

### With Logging

```cron
0 6 * * * /path/to/project/scripts/daily_pipeline.sh >> /path/to/project/logs/cron.log 2>&1
```

## Systemd Timer (Alternative to Cron)

For systems using systemd, timers provide better logging and management.

### Create Service File

```bash
sudo nano /etc/systemd/system/llm-pipeline.service
```

```ini
[Unit]
Description=LLM Bot Pipeline Daily Run
After=network.target

[Service]
Type=oneshot
User=your-username
WorkingDirectory=/path/to/project
ExecStart=/path/to/project/scripts/daily_pipeline.sh
Environment=SOPS_AGE_KEY_FILE=/home/your-username/.sops/age/keys.txt

[Install]
WantedBy=multi-user.target
```

### Create Timer File

```bash
sudo nano /etc/systemd/system/llm-pipeline.timer
```

```ini
[Unit]
Description=Run LLM Bot Pipeline Daily

[Timer]
OnCalendar=*-*-* 06:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

### Enable Timer

```bash
sudo systemctl daemon-reload
sudo systemctl enable llm-pipeline.timer
sudo systemctl start llm-pipeline.timer

# Check status
systemctl list-timers | grep llm-pipeline
```

## Logs

### Log Location

Logs are stored in `logs/` directory:

```
logs/
├── pipeline_20260202.log    # Daily log files
├── pipeline_20260201.log
└── cron.log                 # Cron output (if configured)
```

### Log Rotation

The script automatically removes logs older than 30 days.

### Viewing Logs

```bash
# Latest log
tail -f logs/pipeline_$(date +%Y%m%d).log

# Recent errors
grep -i error logs/pipeline_*.log | tail -20
```

## Monitoring

### Check Last Run

> **Important**: The daily pipeline uses multi-domain databases named by the
> `db_name` field in `config.enc.yaml` (e.g. `data/example.db`).
> Do not confuse with the single-domain default `data/llm-bot-logs.db`.
> See [Architecture: Database Paths](../architecture.md#database-paths).

```bash
# Check log for success/failure
tail -20 logs/pipeline_$(date +%Y%m%d).log

# Check database for recent data (use your actual domain db_name)
sqlite3 data/example.db "SELECT MAX(request_date) FROM bot_requests_daily;"
```

### Health Check Script

```bash
#!/bin/bash
# Check if pipeline ran successfully today

LOG_FILE="logs/pipeline_$(date +%Y%m%d).log"

if grep -q "completed successfully" "$LOG_FILE" 2>/dev/null; then
    echo "✅ Pipeline ran successfully today"
    exit 0
else
    echo "❌ Pipeline did not complete successfully"
    exit 1
fi
```

## Troubleshooting

### Common Issues

**SOPS key not found**
```
ERROR: SOPS key not found at ~/.sops/age/keys.txt
```
Solution: Ensure key file exists and has correct permissions (600).

**Config not found**
```
ERROR: config.enc.yaml not found
```
Solution: Run from project root or check working directory.

**Virtual environment not found**
```
ERROR: Virtual environment not found at venv/
```
Solution: Create venv with `python -m venv venv && pip install -r requirements.txt`

**Cloudflare API errors**
```
HTTP 400: Retention is not turned on
```
Solution: Enable Log Retention in Cloudflare dashboard for each zone.

### Debug Mode

Run with verbose logging:

```bash
./scripts/daily_pipeline.sh -v
```

### Manual Python Run

If the shell script fails, try running Python directly:

```bash
source venv/bin/activate
export SOPS_AGE_KEY_FILE=~/.sops/age/keys.txt
python scripts/run_multi_domain.py --fetch --report -v
```
