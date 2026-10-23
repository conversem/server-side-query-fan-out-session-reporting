#!/bin/bash
# Cron wrapper for daily ETL pipeline
# Ensures proper environment setup and logging
#
# Usage (crontab):
#   0 4 * * * /path/to/project/scripts/cron_daily_etl.sh
#
# Manual run:
#   ./scripts/cron_daily_etl.sh

set -euo pipefail

# Configuration
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV_DIR="${PROJECT_DIR}/venv"
LOG_DIR="${PROJECT_DIR}/logs"
SOPS_KEY="${HOME}/.sops/age/keys.txt"

# Timestamp for this run
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/cron_etl_${TIMESTAMP}.log"

# Ensure log directory exists
mkdir -p "${LOG_DIR}"

# Export environment
export SOPS_AGE_KEY_FILE="${SOPS_KEY}"
export PATH="${VENV_DIR}/bin:${PATH}"
export PYTHONUNBUFFERED=1

# Change to project directory
cd "${PROJECT_DIR}"

# Check virtual environment exists
if [ ! -f "${VENV_DIR}/bin/activate" ]; then
    echo "ERROR: Virtual environment not found at ${VENV_DIR}" | tee -a "${LOG_FILE}"
    exit 1
fi

# Check SOPS key exists
if [ ! -f "${SOPS_KEY}" ]; then
    echo "ERROR: SOPS key not found at ${SOPS_KEY}" | tee -a "${LOG_FILE}"
    exit 1
fi

# Activate virtual environment
source "${VENV_DIR}/bin/activate"

# Run pipeline with logging
echo "======================================================" >> "${LOG_FILE}"
echo "=== ETL Pipeline Started: $(date)" >> "${LOG_FILE}"
echo "=== User: $(whoami)" >> "${LOG_FILE}"
echo "=== Working Directory: $(pwd)" >> "${LOG_FILE}"
echo "======================================================" >> "${LOG_FILE}"

python scripts/run_daily_etl.py --verbose --processing-mode gcp_bq 2>&1 | tee -a "${LOG_FILE}"
EXIT_CODE=${PIPESTATUS[0]}

echo "" >> "${LOG_FILE}"
echo "======================================================" >> "${LOG_FILE}"
echo "=== ETL Pipeline Finished: $(date)" >> "${LOG_FILE}"
echo "=== Exit Code: ${EXIT_CODE}" >> "${LOG_FILE}"
echo "======================================================" >> "${LOG_FILE}"

# Cleanup old logs (keep 30 days)
find "${LOG_DIR}" -name "cron_etl_*.log" -mtime +30 -delete 2>/dev/null || true
find "${LOG_DIR}" -name "daily_etl_*.log" -mtime +30 -delete 2>/dev/null || true

exit ${EXIT_CODE}
