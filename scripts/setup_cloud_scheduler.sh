#!/usr/bin/env bash
# Set up Cloud Scheduler to trigger the pipeline daily
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
REGION="${GCP_REGION:-europe-west1}"
JOB_NAME="${CLOUD_RUN_JOB_NAME:-llm-bot-pipeline}"
SCHEDULE="${PIPELINE_SCHEDULE:-0 4 * * *}"
SCHEDULER_NAME="${SCHEDULER_JOB_NAME:-daily-llm-bot-pipeline}"
SERVICE_ACCOUNT="${SCHEDULER_SA:-pipeline-scheduler@${PROJECT_ID}.iam.gserviceaccount.com}"

echo "Creating Cloud Scheduler job: ${SCHEDULER_NAME}"
echo "  Schedule: ${SCHEDULE} (UTC)"
echo "  Target: Cloud Run Job ${JOB_NAME}"

SCHEDULER_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

SCHEDULER_ARGS=(
    --project="${PROJECT_ID}"
    --location="${REGION}"
    --schedule="${SCHEDULE}"
    --time-zone="UTC"
    --uri="${SCHEDULER_URI}"
    --http-method=POST
    --oauth-service-account-email="${SERVICE_ACCOUNT}"
    --description="Daily LLM bot pipeline execution"
)

if gcloud scheduler jobs describe "${SCHEDULER_NAME}" \
    --project="${PROJECT_ID}" --location="${REGION}" >/dev/null 2>&1; then
    echo "Updating existing scheduler job..."
    gcloud scheduler jobs update http "${SCHEDULER_NAME}" "${SCHEDULER_ARGS[@]}"
else
    echo "Creating new scheduler job..."
    gcloud scheduler jobs create http "${SCHEDULER_NAME}" "${SCHEDULER_ARGS[@]}"
fi

echo "Cloud Scheduler job ready."
echo "Test with: gcloud scheduler jobs run ${SCHEDULER_NAME} --project=${PROJECT_ID} --location=${REGION}"
