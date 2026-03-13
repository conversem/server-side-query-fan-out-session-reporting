#!/bin/bash
#
# Daily Pipeline Runner
#
# Fetches Cloudflare logs and runs the reporting pipeline.
# Designed to be run via cron or manually.
#
# All configuration is read from config.enc.yaml (SOPS encrypted).
# No client-specific settings in this script.
#
# Usage:
#   ./scripts/daily_pipeline.sh              # Run for yesterday
#   ./scripts/daily_pipeline.sh --days 3     # Run for last 3 days
#   ./scripts/daily_pipeline.sh --dry-run    # Preview only
#
# Cron example (run daily at 6 AM):
#   0 6 * * * /path/to/project/scripts/daily_pipeline.sh >> /path/to/project/logs/daily.log 2>&1

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

# Get script directory (handles symlinks)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Log directory
LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p "$LOG_DIR"

# Default settings (can be overridden via args)
DAYS_BACK=1
DRY_RUN=""
VERBOSE=""

# =============================================================================
# Argument Parsing
# =============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --days)
            DAYS_BACK="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --days N     Fetch last N days (default: 1 = yesterday)"
            echo "  --dry-run    Preview without writing data"
            echo "  -v           Verbose logging"
            echo "  -h           Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# =============================================================================
# Setup
# =============================================================================

# Timestamp for logging
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
LOG_FILE="${LOG_DIR}/pipeline_$(date +%Y%m%d).log"

log() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $*" | tee -a "$LOG_FILE"
}

log "=========================================="
log "Daily Pipeline Starting"
log "=========================================="
log "Project root: $PROJECT_ROOT"
log "Days back: $DAYS_BACK"
log "Dry run: ${DRY_RUN:-no}"

# Calculate date range
END_DATE=$(date -d "yesterday" +%Y-%m-%d)
START_DATE=$(date -d "$DAYS_BACK days ago" +%Y-%m-%d)

log "Date range: $START_DATE to $END_DATE"

# =============================================================================
# Environment Setup
# =============================================================================

cd "$PROJECT_ROOT"

# Activate virtual environment
if [[ -f "venv/bin/activate" ]]; then
    source venv/bin/activate
    log "Virtual environment activated"
else
    log "ERROR: Virtual environment not found at venv/"
    exit 1
fi

# Check SOPS key
if [[ -z "${SOPS_AGE_KEY_FILE:-}" ]]; then
    export SOPS_AGE_KEY_FILE="$HOME/.sops/age/keys.txt"
fi

if [[ ! -f "$SOPS_AGE_KEY_FILE" ]]; then
    log "ERROR: SOPS key not found at $SOPS_AGE_KEY_FILE"
    exit 1
fi

# Check config exists
if [[ ! -f "config.enc.yaml" ]]; then
    log "ERROR: config.enc.yaml not found"
    exit 1
fi

log "Configuration validated"

# =============================================================================
# Run Pipeline
# =============================================================================

log "Starting multi-domain fetch and report..."

# Run the pipeline
set +e  # Don't exit on error, capture it
python scripts/run_multi_domain.py \
    --fetch \
    --report \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    $DRY_RUN \
    $VERBOSE \
    2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
set -e

# =============================================================================
# Summary
# =============================================================================

if [[ $EXIT_CODE -eq 0 ]]; then
    log "=========================================="
    log "Pipeline completed successfully"
    log "=========================================="
else
    log "=========================================="
    log "Pipeline failed with exit code: $EXIT_CODE"
    log "=========================================="
fi

# Rotate old logs (keep last 30 days)
find "$LOG_DIR" -name "pipeline_*.log" -mtime +30 -delete 2>/dev/null || true

exit $EXIT_CODE
