# GCP ETL Pipeline - Architecture Diagrams

## 1. Runtime ETL Pipeline (Daily Execution)

This diagram shows the daily ETL workflow orchestrated by Cloud Composer (Airflow).

```mermaid
graph TB
    subgraph "Cloud Composer - Airflow DAG"
        START[Schedule: Daily 5:00 AM UTC]
        EXTRACTOR[run_data_extractor<br/>Cloud Run Job]
        LOADER[run_data_loader<br/>Cloud Run Job]
        VALIDATOR[run_data_validator<br/>Cloud Run Job]
        SUCCESS[Send Success Email]
        FAILURE[Send Failure Email]
    end

    subgraph "Data Extractor Process"
        API[CDC API<br/>CSV Endpoint]
        CHUNK[Chunk Data<br/>50K rows each]
        GCS[GCS Transient Bucket<br/>etl-transient-bucket]
        PG_LOAD[Load to PostgreSQL<br/>chunk by chunk]
        CLEANUP[Cleanup GCS Files]
    end

    subgraph "Data Loader Process"
        PG_READ[Read from PostgreSQL<br/>staging table]
        CLEAN[Clean & Normalize Data]
        VALIDATE_DATA[Validate Data Quality]
        BQ_WRITE[Write to BigQuery<br/>10K row chunks]
    end

    subgraph "Data Validator Process"
        BQ_CHECK1[Check Table Exists]
        BQ_CHECK2[Validate Row Count]
        BQ_CHECK3[Validate Schema]
        BQ_CHECK4[Validate Data Quality]
    end

    subgraph "Infrastructure"
        POSTGRES[(PostgreSQL<br/>Cloud SQL<br/>10.50.0.3)]
        BIGQUERY[(BigQuery<br/>cleaned_cdc_chronic_disease)]
    end

    START --> EXTRACTOR

    EXTRACTOR --> API
    API --> CHUNK
    CHUNK --> GCS
    GCS --> PG_LOAD
    PG_LOAD --> POSTGRES
    POSTGRES --> CLEANUP

    EXTRACTOR -->|Success| LOADER
    EXTRACTOR -->|Failure| FAILURE

    LOADER --> PG_READ
    PG_READ --> POSTGRES
    POSTGRES --> CLEAN
    CLEAN --> VALIDATE_DATA
    VALIDATE_DATA --> BQ_WRITE
    BQ_WRITE --> BIGQUERY

    LOADER -->|Success| VALIDATOR
    LOADER -->|Failure| FAILURE

    VALIDATOR --> BQ_CHECK1
    BQ_CHECK1 --> BIGQUERY
    BIGQUERY --> BQ_CHECK2
    BQ_CHECK2 --> BQ_CHECK3
    BQ_CHECK3 --> BQ_CHECK4

    VALIDATOR -->|Success| SUCCESS
    VALIDATOR -->|Failure| FAILURE

    classDef taskNode fill:#4285F4,stroke:#1a73e8,stroke-width:2px,color:#fff
    classDef dataNode fill:#34A853,stroke:#188038,stroke-width:2px,color:#fff
    classDef storageNode fill:#FBBC04,stroke:#F29900,stroke-width:2px,color:#000
    classDef alertNode fill:#EA4335,stroke:#C5221F,stroke-width:2px,color:#fff
    classDef successNode fill:#34A853,stroke:#188038,stroke-width:2px,color:#fff

    class EXTRACTOR,LOADER,VALIDATOR taskNode
    class API,CHUNK,CLEAN,VALIDATE_DATA dataNode
    class GCS,POSTGRES,BIGQUERY storageNode
    class FAILURE alertNode
    class SUCCESS successNode
```

---

## 2. CI/CD Build Pipeline

This diagram shows the automated build and deployment process triggered on code push.

```mermaid
graph TB
    subgraph "Trigger"
        PUSH[Git Push to main branch]
    end

    subgraph "Cloud Build - Test Phase"
        TEST[Install Dependencies<br/>pip install requirements]
        PYTEST[Run Unit Tests<br/>pytest -m 'not integration']
    end

    subgraph "Cloud Build - Build Phase (Parallel)"
        BUILD_EXT[Build Extractor Image<br/>Dockerfile.extractor]
        BUILD_LOAD[Build Loader Image<br/>Dockerfile.loader]
        BUILD_VAL[Build Validator Image<br/>Dockerfile.validator]
    end

    subgraph "Cloud Build - Push Phase (Parallel)"
        PUSH_EXT[Push Extractor<br/>Tags: latest, COMMIT_SHA]
        PUSH_LOAD[Push Loader<br/>Tags: latest, COMMIT_SHA]
        PUSH_VAL[Push Validator<br/>Tags: latest, COMMIT_SHA]
    end

    subgraph "Artifact Registry"
        REGISTRY[us-central1-docker.pkg.dev<br/>etl-pipeline/]
        IMG_EXT[data-extractor:latest<br/>data-extractor:abc1234]
        IMG_LOAD[data-loader:latest<br/>data-loader:abc1234]
        IMG_VAL[data-validator:latest<br/>data-validator:abc1234]
    end

    subgraph "Cloud Run Jobs (Auto-update)"
        JOB_EXT[data-extractor job<br/>uses latest tag]
        JOB_LOAD[data-loader job<br/>uses latest tag]
        JOB_VAL[data-validator job<br/>uses latest tag]
    end

    PUSH --> TEST
    TEST --> PYTEST

    PYTEST -->|Tests Pass| BUILD_EXT
    PYTEST -->|Tests Pass| BUILD_LOAD
    PYTEST -->|Tests Pass| BUILD_VAL

    BUILD_EXT --> PUSH_EXT
    BUILD_LOAD --> PUSH_LOAD
    BUILD_VAL --> PUSH_VAL

    PUSH_EXT --> REGISTRY
    PUSH_LOAD --> REGISTRY
    PUSH_VAL --> REGISTRY

    REGISTRY --> IMG_EXT
    REGISTRY --> IMG_LOAD
    REGISTRY --> IMG_VAL

    IMG_EXT -.->|Next DAG run uses<br/>updated image| JOB_EXT
    IMG_LOAD -.->|Next DAG run uses<br/>updated image| JOB_LOAD
    IMG_VAL -.->|Next DAG run uses<br/>updated image| JOB_VAL

    classDef triggerNode fill:#EA4335,stroke:#C5221F,stroke-width:2px,color:#fff
    classDef testNode fill:#FBBC04,stroke:#F29900,stroke-width:2px,color:#000
    classDef buildNode fill:#4285F4,stroke:#1a73e8,stroke-width:2px,color:#fff
    classDef pushNode fill:#34A853,stroke:#188038,stroke-width:2px,color:#fff
    classDef registryNode fill:#9AA0A6,stroke:#5F6368,stroke-width:2px,color:#fff
    classDef jobNode fill:#A142F4,stroke:#7627BB,stroke-width:2px,color:#fff

    class PUSH triggerNode
    class TEST,PYTEST testNode
    class BUILD_EXT,BUILD_LOAD,BUILD_VAL buildNode
    class PUSH_EXT,PUSH_LOAD,PUSH_VAL pushNode
    class REGISTRY,IMG_EXT,IMG_LOAD,IMG_VAL registryNode
    class JOB_EXT,JOB_LOAD,JOB_VAL jobNode
```

---

## Pipeline Flow Summary

### Runtime Flow (ETL Execution)
1. **Data Extractor** (Cloud Run Job)
   - Extracts data from CDC API
   - Chunks data into 50K row segments
   - Saves chunks to GCS transient bucket
   - Loads chunks into PostgreSQL staging table
   - Cleans up GCS files

2. **Data Loader** (Cloud Run Job)
   - Reads data from PostgreSQL staging table
   - Cleans and normalizes data
   - Validates data quality
   - Loads data to BigQuery in 10K row chunks

3. **Data Validator** (Cloud Run Job)
   - Validates BigQuery table exists
   - Validates row count (min 100K rows)
   - Validates schema structure
   - Validates data quality metrics

4. **Email Alerts**
   - Success email: Sent when all tasks complete
   - Failure email: Sent if any task fails

### Build Flow (CI/CD)
1. **Test Phase**
   - Install Python dependencies
   - Run unit tests (excluding integration tests)
   - Generate coverage report

2. **Build Phase** (Parallel)
   - Build 3 Docker images simultaneously
   - Tag with commit SHA and 'latest'

3. **Push Phase** (Parallel)
   - Push all images to Artifact Registry
   - Multiple tags per image

4. **Auto-Update**
   - Cloud Run Jobs automatically use latest images
   - Next DAG run executes with updated code

---

## Key Components

| Component | Type | Purpose |
|-----------|------|---------|
| **Cloud Composer** | Orchestration | Schedules and manages DAG execution |
| **Cloud Run Jobs** | Compute | Executes containerized ETL tasks |
| **Cloud SQL (PostgreSQL)** | Database | Staging layer for data processing |
| **BigQuery** | Data Warehouse | Final analytics-ready data storage |
| **GCS** | Object Storage | Temporary storage for data chunks |
| **Artifact Registry** | Container Registry | Stores Docker images |
| **Cloud Build** | CI/CD | Automated testing and deployment |

---

## Data Flow

```
CDC API (CSV)
    ↓ (50K chunks)
GCS Transient Bucket
    ↓ (load chunk by chunk)
PostgreSQL (Cloud SQL)
    ↓ (clean, validate, 10K chunks)
BigQuery (Final Table)
    ↓ (quality validation)
Success/Failure Email Alert
```
