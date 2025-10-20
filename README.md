# GCP ETL Pipeline - CDC Chronic Disease Data

An automated ETL pipeline that extracts chronic disease data from the CDC API, processes it through PostgreSQL, loads it into BigQuery, and validates data quality - all orchestrated with Cloud Composer (Airflow).

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Components](#components)
- [Prerequisites](#prerequisites)
- [Setup & Deployment](#setup--deployment)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [CI/CD Pipeline](#cicd-pipeline)
- [Testing](#testing)
- [Cost Estimation](#cost-estimation)

---

## Overview

This project implements a production-ready ETL pipeline on Google Cloud Platform that:
- Extracts data from the CDC Chronic Disease Indicators API
- Stages data in PostgreSQL (Cloud SQL)
- Loads clean data into BigQuery for analytics
- Validates data quality and freshness
- Sends email alerts on success/failure
- Runs daily at 5:00 AM UTC via Cloud Composer

**Data Source**: [CDC Chronic Disease Indicators](https://data.cdc.gov/Chronic-Disease-Indicators/U-S-Chronic-Disease-Indicators-CDI-/g4ie-h725)

---

## Architecture

For detailed architecture diagrams with complete data flow, see **[DIAGRAMS.md](DIAGRAMS.md)**

### Quick Overview

**Runtime Flow (Daily @ 5:00 AM UTC)**
```
Cloud Composer DAG
    ↓
1. Data Extractor (Cloud Run Job)
   → CDC API → 50K chunks → GCS → PostgreSQL
    ↓
2. Data Loader (Cloud Run Job)
   → PostgreSQL → Clean/Validate → BigQuery (10K chunks)
    ↓
3. Data Validator (Cloud Run Job)
   → BigQuery → Quality Checks → Email Alert (Success/Failure)
```

**CI/CD Flow (On Push to main)**
```
Git Push
    ↓
Cloud Build → Run Tests
    ↓ (parallel)
Build 3 Docker Images → Push to Artifact Registry
    ↓
Cloud Run Jobs auto-update with latest images
```

---

## Components

### 1. Data Extractor (`data_extractor.py`)
**Purpose**: Extracts data from CDC API → GCS → PostgreSQL staging table

**Process**:
1. Fetches data from CDC API with retry logic
2. Chunks data into 50,000-row segments
3. Saves chunks to GCS transient bucket
4. Loads chunks one-by-one into PostgreSQL staging table
5. Cleans up transient files after successful load (unless `KEEP_TRANSIENT_FILES=true`)

**Cloud Run Job**: `data-extractor`
- **Image**: `us-central1-docker.pkg.dev/fluent-grin-474614-d8/etl-pipeline/data-extractor:latest`
- **Memory**: 2Gi
- **Timeout**: 30 minutes

### 2. Data Loader (`data_loader.py`)
**Purpose**: Cleans PostgreSQL data and loads to BigQuery

**Process**:
1. Reads from PostgreSQL staging table in chunks
2. Cleans and normalizes data (remove duplicates, trim whitespace, handle nulls)
3. Validates data quality (logical consistency, range checks)
4. Loads to BigQuery in 10,000-row chunks
5. Auto-creates/updates BigQuery table schema

**Cloud Run Job**: `data-loader`
- **Image**: `us-central1-docker.pkg.dev/fluent-grin-474614-d8/etl-pipeline/data-loader:latest`
- **Memory**: 2Gi
- **Timeout**: 20 minutes

### 3. Data Validator (`data_validator.py`)
**Purpose**: Validates BigQuery data quality and completeness

**Validations**:
- Table existence check
- Row count validation (minimum 100,000 rows)
- Schema validation (required columns: yearstart, yearend, locationabbr, topic, loaded_at, load_date)
- Data quality checks (distinct years ≥ 5, distinct locations ≥ 10, distinct topics)
- Null value detection in critical fields

**Cloud Run Job**: `data-validator`
- **Image**: `us-central1-docker.pkg.dev/fluent-grin-474614-d8/etl-pipeline/data-validator:latest`
- **Memory**: 512Mi
- **Timeout**: 5 minutes

### 4. Airflow DAG (`dags/etl_dag_updt.py`)
**Purpose**: Orchestrates the entire ETL pipeline

**Features**:
- Sequential task execution: Extractor → Loader → Validator
- Email alerts on failure (any task fails) - uses `TriggerRule.ONE_FAILED`
- Email alerts on success (all tasks succeed) - uses `TriggerRule.ALL_SUCCESS`
- Configurable via Airflow Variables (PROJECT_ID, REGION, ALERT_EMAILS)
- Daily schedule at 5:00 AM UTC (`schedule_interval="0 5 * * *"`)

---

## Prerequisites

### GCP Resources Required:
- **Project**: GCP project with billing enabled
- **APIs Enabled**:
  - Cloud Run API
  - Cloud Composer API
  - Cloud Build API
  - Artifact Registry API
  - Cloud SQL API
  - BigQuery API
  - Secret Manager API
  - Cloud Storage API

### Infrastructure:
- **Cloud SQL PostgreSQL** instance in VPC
- **GCS Bucket** for transient storage
- **BigQuery Dataset** for final data
- **Artifact Registry** repository for Docker images
- **Cloud Composer** environment (Composer 2)
- **VPC Network** (for PostgreSQL connectivity)

---

## Setup & Deployment

### 1. Clone the Repository
```bash
git clone https://github.com/iamadityavarma/ETL-GCP.git
cd ETL-GCP
```

### 2. Configure Environment Variables in Secret Manager

Create the following secrets in GCP Secret Manager:

```bash
# Database Configuration
DB_HOST=10.50.0.3
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_user
DB_PASSWORD=your_password

# API Configuration
CDC_API_URL=https://data.cdc.gov/resource/g4ie-h725.csv

# GCS Configuration
GCS_TRANSIENT_BUCKET=etl-transient-bucket
GCS_TRANSIENT_PREFIX=transient/

# BigQuery Configuration
BQ_PROJECT=fluent-grin-474614-d8
BQ_DATASET=your_dataset
BQ_TABLE=cleaned_cdc_chronic_disease

# Staging Table
STAGING_CDC_TABLE=staging.staging_cdc_chronic_disease

# Data Settings
API_CHUNK_SIZE=50000
MIN_EXPECTED_ROWS=100000
KEEP_TRANSIENT_FILES=false
```

### 3. Deploy Cloud Run Jobs

The CI/CD pipeline automatically builds and deploys Docker images on every push to `main` branch.

**Manual deployment** (if needed):
```bash
# Build and push images
gcloud builds submit --config=cloudbuild.yaml .

# Create Cloud Run Jobs (one-time setup)
gcloud run jobs create data-extractor \
  --image us-central1-docker.pkg.dev/fluent-grin-474614-d8/etl-pipeline/data-extractor:latest \
  --region us-central1 \
  --service-account etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com \
  --memory 2Gi \
  --timeout 30m

gcloud run jobs create data-loader \
  --image us-central1-docker.pkg.dev/fluent-grin-474614-d8/etl-pipeline/data-loader:latest \
  --region us-central1 \
  --service-account etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com \
  --memory 2Gi \
  --timeout 20m

gcloud run jobs create data-validator \
  --image us-central1-docker.pkg.dev/fluent-grin-474614-d8/etl-pipeline/data-validator:latest \
  --region us-central1 \
  --service-account etl-pipeline-sa@fluent-grin-474614-d8.iam.gserviceaccount.com \
  --memory 512Mi \
  --timeout 5m
```

### 4. Configure Cloud Composer

**Set Airflow Variables**:
```bash
gcloud composer environments run etl-composer-env \
  --location us-central1 \
  variables set -- PROJECT_ID fluent-grin-474614-d8

gcloud composer environments run etl-composer-env \
  --location us-central1 \
  variables set -- REGION us-central1

gcloud composer environments run etl-composer-env \
  --location us-central1 \
  variables set -- ALERT_EMAILS "your-email@gmail.com"
```

**Upload DAG**:
```bash
gcloud storage cp dags/etl_dag_updt.py gs://[COMPOSER-BUCKET]/dags/
```

### 5. Configure Email Alerts (Optional)

In Airflow UI → Admin → Connections, create `smtp_default`:
- **Connection Type**: Email
- **Host**: smtp.gmail.com
- **Login**: your-email@gmail.com
- **Password**: [App Password]
- **Port**: 587
- **Extra**: `{"use_tls": true}`

---

## Usage

### Manual Trigger
In Airflow UI:
1. Navigate to DAGs
2. Find `etl_pipeline_daily`
3. Click the "Play" button to trigger manually

### Command Line Trigger
```bash
gcloud composer environments run etl-composer-env \
  --location us-central1 \
  dags trigger -- etl_pipeline_daily
```

### Scheduled Execution
The DAG runs automatically every day at **5:00 AM UTC**.

To change the schedule, edit `dags/etl_dag_updt.py`:
```python
schedule_interval="0 5 * * *",  # Cron format
```

---

## Monitoring

### View Logs

**Cloud Run Job Logs**:
```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=data-extractor" \
  --limit 50 \
  --format json
```

**Airflow Task Logs**:
- Airflow UI → DAGs → etl_pipeline_daily → Graph View → Click task → Logs

**BigQuery Data Validation**:
```sql
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT yearstart) as years,
  COUNT(DISTINCT locationabbr) as locations,
  MAX(loaded_at) as last_load
FROM `fluent-grin-474614-d8.your_dataset.cleaned_cdc_chronic_disease`;
```

### Email Alerts

You'll receive emails at configured address for:
- **Failure**: Any task in the pipeline fails
- **Success**: All tasks complete successfully

---

## CI/CD Pipeline

### GitHub Actions + Cloud Build

**Trigger**: Every push to `main` branch

**Process**:
1. Run unit tests (excludes integration tests)
2. Build 3 Docker images in parallel
3. Push images to Artifact Registry with:
   - Commit SHA tag (e.g., `abc1234`)
   - `latest` tag

**Cloud Build Configuration**: `cloudbuild.yaml`

**View Build Status**:
```bash
gcloud builds list --limit=5
```

### Local Development

**Run tests locally**:
```bash
pip install -r requirements.txt
pip install -r requirements-test.txt
pytest tests/ -v -m "not integration"
```

**Build Docker image locally**:
```bash
docker build -f Dockerfile.extractor -t data-extractor .
docker run --env-file .env data-extractor
```

---

## Testing

### Unit Tests (`tests/test_*.py`)
- **Mocked tests** - no infrastructure required
- Run in CI/CD automatically
- Test business logic, error handling, SQL injection protection

**Run unit tests**:
```bash
pytest tests/ -v -m "not integration" --cov=. --cov-report=term-missing
```

### Integration Tests (`tests/test_integration.py`)
- **Real infrastructure tests** - require VPN/VPC access
- Test actual API, PostgreSQL, GCS, BigQuery connectivity
- Run manually before production deployment

**Run integration tests**:
```bash
pytest tests/test_integration.py -v -m integration
```

---

## Cost Estimation

**Monthly Costs** (approximate):

| Service | Usage | Cost |
|---------|-------|------|
| Cloud Composer 2 (Small) | 24/7 | $150-200 |
| Cloud Run Jobs | 3 jobs/day, ~5 min each | $5-10 |
| Cloud SQL PostgreSQL | db-n1-standard-1 | $50-100 |
| BigQuery Storage | ~500MB dataset | $0.01 |
| BigQuery Queries | Ad-hoc analytics | $5-20 |
| Cloud Storage | Transient files | $1-5 |
| Artifact Registry | Docker images | $2-5 |
| **Total** | | **~$213-341/month** |

**Cost Optimization Tips**:
- Delete transient GCS files after load (`KEEP_TRANSIENT_FILES=false`)
- Use Cloud SQL backups instead of continuous HA
- Stop Composer environment during non-business hours (dev/staging)
- Use BigQuery flat-rate pricing if query volume increases

---

## Project Structure

```
GCP-ETL/
├── data_extractor.py          # API → GCS → PostgreSQL
├── data_loader.py             # PostgreSQL → BigQuery
├── data_validator.py          # BigQuery data quality checks
├── requirements.txt           # Python dependencies
├── requirements-test.txt      # Test dependencies
├── Dockerfile.extractor       # Extractor container
├── Dockerfile.loader          # Loader container
├── Dockerfile.validator       # Validator container
├── cloudbuild.yaml           # CI/CD configuration
├── .gitignore                # Git ignore rules
├── dags/
│   ├── etl_dag_updt.py       # Main Airflow DAG
│   └── test_email.py         # Email testing DAG
├── tests/
│   ├── test_extractor.py     # Unit tests for extractor
│   ├── test_loader.py        # Unit tests for loader
│   ├── test_validator.py     # Unit tests for validator
│   └── test_integration.py   # Integration tests
└── README.md                 # This file
```

---

## Troubleshooting

### Common Issues

**1. PostgreSQL connection timeout**
- Check VPC configuration
- Verify Cloud Run Job has VPC connector
- Confirm PostgreSQL allows connections from Cloud Run IP range

**2. BigQuery permission denied**
- Verify service account has `roles/bigquery.dataEditor` and `roles/bigquery.jobUser`
- Check dataset exists in correct project

**3. Email alerts not working**
- Configure SMTP connection in Airflow UI
- Use Gmail App Password, not regular password
- Check spam folder

**4. DAG not appearing in Airflow**
- Wait 1-2 minutes for DAG to sync
- Check DAG file syntax: `python dags/etl_dag_updt.py`
- View Composer logs for import errors

**5. Cloud Run Job fails with OOM**
- Increase memory allocation (--memory 4Gi)
- Reduce chunk size (API_CHUNK_SIZE=25000)

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## License

This project is licensed under the MIT License.

---

## Contact

**Maintainer**: Aditya Varma
**Email**: aditya.adityavarma@gmail.com
**GitHub**: https://github.com/iamadityavarma/ETL-GCP

---

## Acknowledgments

- **Data Source**: CDC Chronic Disease Indicators
- **Platform**: Google Cloud Platform
- **Orchestration**: Apache Airflow (Cloud Composer)
