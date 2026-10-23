#!/usr/bin/env bash
# Deploy the LLM Bot Pipeline to GCP Cloud Run as a Job.
#
# Prerequisites:
#   - gcloud CLI authenticated with sufficient permissions
#   - Docker (or Cloud Build enabled)
#   - The GCP project must have Cloud Run, Artifact Registry, and
#     Cloud Scheduler APIs enabled.
#
# Usage:
#   ./infrastructure/cloud_run/deploy.sh <PROJECT_ID> <REGION> <SA_EMAIL>
#
# See docs/gcp-deployment.md for full guide.
set -euo pipefail

PROJECT_ID="${1:?Usage: deploy.sh <PROJECT_ID> <REGION> <SA_EMAIL>}"
REGION="${2:?Usage: deploy.sh <PROJECT_ID> <REGION> <SA_EMAIL>}"
SA_EMAIL="${3:?Usage: deploy.sh <PROJECT_ID> <REGION> <SA_EMAIL>}"

IMAGE="gcr.io/${PROJECT_ID}/llm-bot-pipeline"
JOB_NAME="llm-bot-daily-etl"
SCHEDULER_NAME="daily-etl-trigger"

echo "=== LLM Bot Pipeline — Cloud Run Deployment ==="
echo "  Project:  ${PROJECT_ID}"
echo "  Region:   ${REGION}"
echo "  SA:       ${SA_EMAIL}"
echo ""

# --- 1. Build and push container image ---
echo "[1/4] Building container image..."
gcloud builds submit \
  --project="${PROJECT_ID}" \
  --tag="${IMAGE}:latest" \
  .

# --- 2. Create / update Cloud Run Job ---
echo "[2/4] Deploying Cloud Run Job..."
gcloud run jobs replace infrastructure/cloud_run/service.yaml \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID}" \
  --update-env-vars="USE_SECRET_MANAGER=true,PROCESSING_MODE=gcp_bq"

# --- 3. Grant IAM roles to the service account ---
echo "[3/4] Granting IAM roles..."
for role in \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/secretmanager.secretAccessor \
  roles/run.invoker; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${role}" \
    --condition=None \
    --quiet 2>/dev/null || true
done

# --- 4. Create / update Cloud Scheduler trigger ---
echo "[4/4] Configuring Cloud Scheduler..."
if gcloud scheduler jobs describe "${SCHEDULER_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" >/dev/null 2>&1; then
  gcloud scheduler jobs update http "${SCHEDULER_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --schedule="0 4 * * *" \
    --time-zone="Europe/Brussels" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}"
else
  gcloud scheduler jobs create http "${SCHEDULER_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --schedule="0 4 * * *" \
    --time-zone="Europe/Brussels" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}"
fi

echo ""
echo "=== Deployment complete ==="
echo "  Job:       gcloud run jobs describe ${JOB_NAME} --region=${REGION}"
echo "  Scheduler: gcloud scheduler jobs describe ${SCHEDULER_NAME} --location=${REGION}"
echo "  Manual run: gcloud run jobs execute ${JOB_NAME} --region=${REGION}"
