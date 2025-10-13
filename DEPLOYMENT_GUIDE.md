# GCP ETL Pipeline Deployment Guide

## Overview

This guide walks you through deploying an automated ETL pipeline on GCP that runs daily at 2 AM with the following workflow:

```
Data Extractor → Data Loader → Data Validator
```

Each step only runs if the previous one succeeds.

## Architecture

- **Cloud Scheduler**: Triggers the workflow daily at 2 AM
- **Cloud Workflows**: Orchestrates the pipeline execution
- **Cloud Run Jobs**: Runs each ETL component in containers
- **BigQuery**: Data warehouse destination
- **PostgreSQL**: Staging database

## Prerequisites

1. GCP Project: `fluent-grin-474614-d8`
2. `gcloud` CLI installed and authenticated
3. Docker installed (for local testing)
4. Required GCP APIs enabled (script will enable them)

## Files Created

```
.
├── data_extractor.py          # Extracts data from CDC API to PostgreSQL
├── data_loader.py             # Loads data from PostgreSQL to BigQuery
├── data_validator.py          # Validates BigQuery data load
├── workflow.yaml              # GCP Workflows orchestration
├── deploy_workflow.sh         # Automated deployment script
├── Dockerfile.extractor       # Container for extractor
├── Dockerfile.loader          # Container for loader
├── Dockerfile.validator       # Container for validator
└── .env                       # Environment variables (DO NOT COMMIT)
```

## Deployment Steps

### Option 1: Automated Deployment (Recommended)

```bash
# Make the script executable
chmod +x deploy_workflow.sh

# Run the deployment
./deploy_workflow.sh
```

This will:
1. Enable required GCP APIs
2. Create service account with necessary permissions
3. Build and deploy all Cloud Run jobs
4. Deploy the workflow
5. Set up Cloud Scheduler for daily 2 AM runs

### Option 2: Manual Deployment

#### Step 1: Enable APIs

```bash
gcloud services enable \
    workflows.googleapis.com \
    cloudscheduler.googleapis.com \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    --project=fluent-grin-474614-d8
```

#### Step 2: Create Service Account

```bash
# Create service account
gcloud iam service-accounts create etl-pipeline-sa \
    --display-name="ETL Pipeline Service Account" \
    --project=fluent-grin-474614-d8

# Grant permissions
gcloud projects add-iam-policy-binding fluent-grin-474614-d8 \
    --member="serviceAccount:etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding fluent-grin-474614-d8 \
    --member="serviceAccount:etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com" \
    --role="roles/run.invoker"

gcloud projects add-iam-policy-binding fluent-grin-474614-d8 \
    --member="serviceAccount:etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com" \
    --role="roles/workflows.invoker"
```

#### Step 3: Build and Deploy Cloud Run Jobs

```bash
# Build and deploy extractor
gcloud builds submit --tag gcr.io/fluent-grin-474614-d8/data-extractor \
    -f Dockerfile.extractor

gcloud run jobs create data-extractor-job \
    --image gcr.io/fluent-grin-474614-d8/data-extractor \
    --region=us-central1 \
    --service-account=etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com \
    --max-retries=1 \
    --task-timeout=30m

# Build and deploy loader
gcloud builds submit --tag gcr.io/fluent-grin-474614-d8/data-loader \
    -f Dockerfile.loader

gcloud run jobs create data-loader-job \
    --image gcr.io/fluent-grin-474614-d8/data-loader \
    --region=us-central1 \
    --service-account=etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com \
    --max-retries=1 \
    --task-timeout=30m

# Build and deploy validator
gcloud builds submit --tag gcr.io/fluent-grin-474614-d8/data-validator \
    -f Dockerfile.validator

gcloud run jobs create data-validator-job \
    --image gcr.io/fluent-grin-474614-d8/data-validator \
    --region=us-central1 \
    --service-account=etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com \
    --max-retries=1 \
    --task-timeout=10m
```

#### Step 4: Deploy Workflow

```bash
gcloud workflows deploy cdc-etl-pipeline \
    --source=workflow.yaml \
    --location=us-central1 \
    --service-account=etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com
```

#### Step 5: Schedule Daily Execution

```bash
# Create scheduler job for 2 AM daily
gcloud scheduler jobs create http cdc-etl-daily-job \
    --location=us-central1 \
    --schedule="0 2 * * *" \
    --time-zone="America/New_York" \
    --uri="https://workflowexecutions.googleapis.com/v1/projects/fluent-grin-474614-d8/locations/us-central1/workflows/cdc-etl-pipeline/executions" \
    --message-body="{}" \
    --oauth-service-account-email=etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com
```

## Testing

### Test Individual Components Locally

```bash
# Test extractor
python data_extractor.py

# Test loader
python data_loader.py

# Test validator
python data_validator.py
```

### Test Cloud Run Jobs

```bash
# Test extractor job
gcloud run jobs execute data-extractor-job --region=us-central1

# Test loader job
gcloud run jobs execute data-loader-job --region=us-central1

# Test validator job
gcloud run jobs execute data-validator-job --region=us-central1
```

### Test Workflow Manually

```bash
# Trigger workflow manually
gcloud workflows run cdc-etl-pipeline \
    --location=us-central1

# View execution status
gcloud workflows executions list cdc-etl-pipeline \
    --location=us-central1

# View execution details
gcloud workflows executions describe <EXECUTION_ID> \
    --workflow=cdc-etl-pipeline \
    --location=us-central1
```

### Test Scheduler

```bash
# Run scheduler job immediately (don't wait for 2 AM)
gcloud scheduler jobs run cdc-etl-daily-job --location=us-central1

# View scheduler job details
gcloud scheduler jobs describe cdc-etl-daily-job --location=us-central1

# View scheduler logs
gcloud scheduler jobs list --location=us-central1
```

## Monitoring

### View Workflow Logs

```bash
# List recent executions
gcloud workflows executions list cdc-etl-pipeline --location=us-central1 --limit=10

# View execution logs
gcloud workflows executions describe-last cdc-etl-pipeline --location=us-central1
```

### View Cloud Run Logs

```bash
# View extractor logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=data-extractor-job" --limit=50

# View loader logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=data-loader-job" --limit=50

# View validator logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=data-validator-job" --limit=50
```

### Set Up Alerts

Create alerts in GCP Console for:
- Workflow execution failures
- Job execution timeouts
- Data validation failures

## Environment Variables

Ensure your `.env` file contains:

```env
# Database Configuration
DB_HOST=34.41.29.118
DB_PORT=5432
DB_NAME=staging-db
DB_USER=postgres
DB_PASSWORD=<your-password>

# BigQuery Configuration
BQ_PROJECT=fluent-grin-474614-d8
BQ_DATASET=chronic_health_analytics
BQ_TABLE=cleaned_cdc_chronic_disease

# Validation Configuration
MIN_EXPECTED_ROWS=100000
MAX_DATA_AGE_HOURS=24

# API Configuration
CDC_API_URL="https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD"
STAGING_CDC_TABLE=staging.staging_cdc_chronic_disease
```

## Updating the Pipeline

To update any component:

```bash
# Update extractor
gcloud builds submit --tag gcr.io/fluent-grin-474614-d8/data-extractor -f Dockerfile.extractor
gcloud run jobs update data-extractor-job --image gcr.io/fluent-grin-474614-d8/data-extractor --region=us-central1

# Update loader
gcloud builds submit --tag gcr.io/fluent-grin-474614-d8/data-loader -f Dockerfile.loader
gcloud run jobs update data-loader-job --image gcr.io/fluent-grin-474614-d8/data-loader --region=us-central1

# Update validator
gcloud builds submit --tag gcr.io/fluent-grin-474614-d8/data-validator -f Dockerfile.validator
gcloud run jobs update data-validator-job --image gcr.io/fluent-grin-474614-d8/data-validator --region=us-central1

# Update workflow
gcloud workflows deploy cdc-etl-pipeline --source=workflow.yaml --location=us-central1
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Ensure service account has correct permissions
   - Check that Cloud Run jobs have proper service account attached

2. **Workflow Execution Fails**
   - Check individual job logs for errors
   - Verify environment variables are set correctly
   - Ensure PostgreSQL and BigQuery are accessible

3. **Scheduler Not Triggering**
   - Verify scheduler job exists: `gcloud scheduler jobs list --location=us-central1`
   - Check scheduler logs for errors
   - Ensure correct timezone is set

4. **Database Connection Issues**
   - Verify PostgreSQL IP is whitelisted for Cloud Run
   - Check VPC connectivity if using private IP
   - Verify credentials in `.env` file

## Cost Optimization

- Cloud Run Jobs: Pay only for execution time
- Cloud Workflows: First 5,000 steps free per month
- Cloud Scheduler: $0.10 per job per month
- BigQuery: Storage + query costs

Estimated monthly cost: $5-20 depending on data volume

## Security Best Practices

1. **Never commit `.env` file to version control**
2. Use Secret Manager for sensitive credentials
3. Implement least-privilege IAM policies
4. Enable VPC Service Controls for data protection
5. Regularly rotate service account keys
6. Enable audit logging

## Next Steps

1. Set up monitoring dashboards in GCP Console
2. Create alerting policies for pipeline failures
3. Implement data quality checks in validator
4. Add retry logic with exponential backoff
5. Set up email notifications for failures
