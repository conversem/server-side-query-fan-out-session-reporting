#!/usr/bin/env bash
# Deploy Cloud Run Job for LLM Bot Pipeline
set -euo pipefail

# Configuration (override via environment)
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
REGION="${GCP_REGION:-europe-west1}"
REPO="${ARTIFACT_REPO:-llm-bot-pipeline}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/llm-bot-pipeline"
JOB_NAME="${CLOUD_RUN_JOB_NAME:-llm-bot-pipeline}"
SA_ID="${GCP_SA_ID:-llm-bot-pipeline}"

# Validate PROJECT_ID format
if ! [[ "${PROJECT_ID}" =~ ^[a-z][a-z0-9-]{4,28}[a-z0-9]$ ]]; then
    echo "ERROR: Invalid GCP project ID: ${PROJECT_ID}" >&2
    exit 1
fi

echo "Ensuring Artifact Registry repository exists..."
gcloud artifacts repositories create "${REPO}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --description="LLM Bot Pipeline images" 2>/dev/null || true

echo "Building container image..."
docker build -t "${IMAGE}" .

echo "Pushing to Artifact Registry..."
docker push "${IMAGE}"

echo "Deploying Cloud Run Job..."
TEMP_YAML=$(mktemp)
trap 'rm -f "${TEMP_YAML}"' EXIT
sed -e "s/PROJECT_ID_PLACEHOLDER/${PROJECT_ID}/g" \
    -e "s/SA_ID_PLACEHOLDER/${SA_ID}/g" \
    deploy/cloud-run-job.yaml > "${TEMP_YAML}"

gcloud run jobs replace "${TEMP_YAML}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}"

echo "Deployment complete: ${JOB_NAME}"
echo "Execute manually: gcloud run jobs execute ${JOB_NAME} --project=${PROJECT_ID} --region=${REGION}"
