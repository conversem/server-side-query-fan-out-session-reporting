# Cloud Deployment Files

This directory contains configuration files for deploying the LLM Bot Traffic Pipeline to Google Cloud.

> **Authoritative guide:** See [`docs/cloud-deployment.md`](../docs/cloud-deployment.md) for complete deployment instructions, secret management, scheduling, and troubleshooting.

## Deployment: Cloud Run Jobs (Recommended)

Cloud Run Jobs are ideal for scheduled batch workloads like ETL pipelines.

### Prerequisites

1. Enable required APIs:
   ```bash
   gcloud services enable run.googleapis.com cloudscheduler.googleapis.com \
       secretmanager.googleapis.com artifactregistry.googleapis.com
   ```

2. Create service account with required permissions:
   ```bash
   gcloud iam service-accounts create pipeline-sa \
       --display-name "Pipeline Service Account"

   gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
       --member="serviceAccount:pipeline-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
       --role="roles/bigquery.dataEditor"

   gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
       --member="serviceAccount:pipeline-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
       --role="roles/bigquery.jobUser"
   ```

### Build and Deploy

1. Build container image (Artifact Registry):
   ```bash
   export GCP_REGION=europe-west1
   gcloud builds submit \
       --tag ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/llm-bot-pipeline/llm-bot-pipeline
   ```

2. Create Cloud Run Job:
   ```bash
   gcloud run jobs create llm-bot-pipeline \
       --image ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/llm-bot-pipeline/llm-bot-pipeline:latest \
       --region $GCP_REGION \
       --set-env-vars "GCP_PROJECT_ID=$GCP_PROJECT_ID,USE_SECRET_MANAGER=true,CONFIG_SECRET=pipeline-config,STORAGE_BACKEND=bigquery" \
       --service-account pipeline-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com \
       --memory 1Gi \
       --cpu 1 \
       --task-timeout 43200
   ```

3. Create Cloud Scheduler trigger for daily runs:
   ```bash
   gcloud iam service-accounts create scheduler-sa \
       --display-name "Scheduler Service Account"

   gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
       --member="serviceAccount:scheduler-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
       --role="roles/run.invoker"

   gcloud scheduler jobs create http llm-bot-pipeline-daily \
       --schedule "0 6 * * *" \
       --uri "https://${GCP_REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$GCP_PROJECT_ID/jobs/llm-bot-pipeline:run" \
       --http-method POST \
       --oauth-service-account-email scheduler-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com \
       --location $GCP_REGION
   ```

4. Manually trigger a run:
   ```bash
   gcloud run jobs execute llm-bot-pipeline --region $GCP_REGION
   ```

## BigQuery Datasets

The pipeline uses three datasets:

| Dataset | Default Name | Purpose |
|---------|-------------|---------|
| **Raw** | `bot_logs_raw` | Ingested log data |
| **Staging** | `bot_logs_staging` | Intermediate processing tables |
| **Report** | `bot_logs` | Analysis-ready data for dashboards |

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GCP_PROJECT_ID` | Google Cloud project ID | Yes |
| `GCP_LOCATION` | BigQuery location (default: EU) | No |
| `STORAGE_BACKEND` | Storage backend (`bigquery`) | Yes |
| `USE_SECRET_MANAGER` | Enable GCP Secret Manager (`true`) | Yes (Cloud Run) |
| `CONFIG_SECRET` | Secret Manager secret name | Yes (Cloud Run) |
| `BQ_DATASET_RAW` | Raw data dataset (default: `bot_logs_raw`) | No |
| `BQ_DATASET_REPORT` | Report data dataset (default: `bot_logs`) | No |

## Monitoring and Alerting

### View Logs

```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=llm-bot-pipeline" \
    --project=$GCP_PROJECT_ID --limit 100
```

### Set Up Alerting

1. Create notification channel (email):
   ```bash
   gcloud alpha monitoring channels create \
       --display-name="Pipeline Alerts" \
       --type=email \
       --channel-labels=email_address=your-email@example.com
   ```

2. Create alert policy for job failures:
   ```bash
   gcloud alpha monitoring policies create \
       --display-name="Pipeline Job Failures" \
       --condition-display-name="Job Failed" \
       --condition-filter='resource.type="cloud_run_job" AND metric.type="run.googleapis.com/job/completed_execution_count" AND metric.labels.result="failed"' \
       --condition-threshold-value=1 \
       --condition-threshold-comparison=COMPARISON_GE \
       --notification-channels=CHANNEL_ID
   ```

## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure service account has BigQuery roles
2. **Timeout**: Increase job timeout for large data volumes
3. **Memory Error**: Increase memory allocation
4. **Scheduler Not Triggering**: Check scheduler service account permissions

### Debug Commands

```bash
# Check job status
gcloud run jobs describe llm-bot-pipeline --region $GCP_REGION

# Check execution history
gcloud run jobs executions list --job llm-bot-pipeline --region $GCP_REGION

# Check scheduler status
gcloud scheduler jobs describe llm-bot-pipeline-daily --location $GCP_REGION
```

