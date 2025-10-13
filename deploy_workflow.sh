#!/bin/bash

# GCP ETL Pipeline Deployment Script
# This script deploys all components to GCP

set -e

# Configuration
PROJECT_ID="fluent-grin-474614-d8"
REGION="us-central1"
WORKFLOW_NAME="cdc-etl-pipeline"
SERVICE_ACCOUNT="etl-pipeline-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=================================================="
echo "GCP ETL Pipeline Deployment"
echo "=================================================="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo ""

# Step 1: Enable required APIs
echo "Step 1: Enabling required GCP APIs..."
gcloud services enable \
    workflows.googleapis.com \
    cloudscheduler.googleapis.com \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    --project=$PROJECT_ID

# Step 2: Create service account if it doesn't exist
echo ""
echo "Step 2: Setting up service account..."
if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT --project=$PROJECT_ID &>/dev/null; then
    gcloud iam service-accounts create etl-pipeline-sa \
        --display-name="ETL Pipeline Service Account" \
        --project=$PROJECT_ID
    echo "Service account created"
else
    echo "Service account already exists"
fi

# Grant necessary permissions
echo "Granting permissions to service account..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/run.invoker"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/workflows.invoker"

# Step 3: Build and deploy Cloud Run jobs
echo ""
echo "Step 3: Building and deploying Cloud Run jobs..."

# Data Extractor
echo "Deploying data_extractor..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/data-extractor \
    --project=$PROJECT_ID

gcloud run jobs create data-extractor-job \
    --image gcr.io/$PROJECT_ID/data-extractor \
    --region=$REGION \
    --service-account=$SERVICE_ACCOUNT \
    --max-retries=1 \
    --task-timeout=30m \
    --project=$PROJECT_ID \
    || gcloud run jobs update data-extractor-job \
        --image gcr.io/$PROJECT_ID/data-extractor \
        --region=$REGION \
        --project=$PROJECT_ID

# Data Loader
echo "Deploying data_loader..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/data-loader \
    --project=$PROJECT_ID

gcloud run jobs create data-loader-job \
    --image gcr.io/$PROJECT_ID/data-loader \
    --region=$REGION \
    --service-account=$SERVICE_ACCOUNT \
    --max-retries=1 \
    --task-timeout=30m \
    --project=$PROJECT_ID \
    || gcloud run jobs update data-loader-job \
        --image gcr.io/$PROJECT_ID/data-loader \
        --region=$REGION \
        --project=$PROJECT_ID

# Data Validator
echo "Deploying data_validator..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/data-validator \
    --project=$PROJECT_ID

gcloud run jobs create data-validator-job \
    --image gcr.io/$PROJECT_ID/data-validator \
    --region=$REGION \
    --service-account=$SERVICE_ACCOUNT \
    --max-retries=1 \
    --task-timeout=10m \
    --project=$PROJECT_ID \
    || gcloud run jobs update data-validator-job \
        --image gcr.io/$PROJECT_ID/data-validator \
        --region=$REGION \
        --project=$PROJECT_ID

# Step 4: Deploy workflow
echo ""
echo "Step 4: Deploying workflow..."
gcloud workflows deploy $WORKFLOW_NAME \
    --source=workflow.yaml \
    --location=$REGION \
    --service-account=$SERVICE_ACCOUNT \
    --project=$PROJECT_ID

# Step 5: Create Cloud Scheduler job
echo ""
echo "Step 5: Setting up Cloud Scheduler..."

# Delete existing scheduler job if it exists
gcloud scheduler jobs delete cdc-etl-daily-job \
    --location=$REGION \
    --project=$PROJECT_ID \
    --quiet || true

# Create new scheduler job for 2 AM daily
gcloud scheduler jobs create http cdc-etl-daily-job \
    --location=$REGION \
    --schedule="0 2 * * *" \
    --time-zone="America/New_York" \
    --uri="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/workflows/$WORKFLOW_NAME/executions" \
    --message-body="{}" \
    --oauth-service-account-email=$SERVICE_ACCOUNT \
    --project=$PROJECT_ID

echo ""
echo "=================================================="
echo "Deployment Complete!"
echo "=================================================="
echo ""
echo "Workflow deployed: $WORKFLOW_NAME"
echo "Scheduled to run daily at 2:00 AM"
echo ""
echo "To manually trigger the workflow:"
echo "  gcloud workflows run $WORKFLOW_NAME --location=$REGION --project=$PROJECT_ID"
echo ""
echo "To view workflow executions:"
echo "  gcloud workflows executions list $WORKFLOW_NAME --location=$REGION --project=$PROJECT_ID"
echo ""
