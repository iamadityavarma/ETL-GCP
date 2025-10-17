# GCP Deployment Guide - Transient File Storage

Guide for deploying the ETL pipeline to GCP with optimal transient file storage.

---

## **Storage Options Comparison**

| Option | Storage Location | Persistence | Size Limit | Cost | Speed | Use Case |
|--------|-----------------|-------------|------------|------|-------|----------|
| **Ephemeral Disk** | `/tmp` on instance | Lost on restart | 1-10GB | Free | Fastest | Dev, small datasets |
| **GCS Bucket** | Cloud Storage | Persistent | Unlimited | ~$0.02/GB/month | Fast | Production, fault tolerance |
| **Persistent Disk** | Attached disk | Persistent | Configurable | ~$0.04/GB/month | Fast | Not recommended |

---

## **Recommended Approach**

### **For Development/Testing: Ephemeral Disk**
```bash
TRANSIENT_DIR=/tmp/transient_data
KEEP_TRANSIENT_FILES=false
```

### **For Production: GCS Bucket**
```bash
GCS_TRANSIENT_BUCKET=your-etl-transient-bucket
GCS_TRANSIENT_PREFIX=etl_runs/
KEEP_TRANSIENT_FILES=false
```

---

## **Option 1: Ephemeral Disk (Simple Setup)**

### **Setup for Cloud Run**

```yaml
# cloudbuild.yaml
steps:
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', 'gcr.io/$PROJECT_ID/etl-pipeline', '.']
  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', 'gcr.io/$PROJECT_ID/etl-pipeline']
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: gcloud
    args:
      - 'run'
      - 'deploy'
      - 'etl-pipeline'
      - '--image=gcr.io/$PROJECT_ID/etl-pipeline'
      - '--region=us-central1'
      - '--memory=4Gi'
      - '--timeout=3600'
      - '--set-env-vars=TRANSIENT_DIR=/tmp/transient_data,KEEP_TRANSIENT_FILES=false'
```

### **Limitations**
- Cloud Run `/tmp` is limited to **4GB** (can be increased with memory allocation)
- Files lost if container restarts
- No resume capability

---

## **Option 2: GCS Bucket (Recommended for Production)**

### **Step 1: Create GCS Bucket**

```bash
# Create bucket for transient files
gcloud storage buckets create gs://etl-transient-bucket \
    --location=us-central1 \
    --storage-class=STANDARD \
    --uniform-bucket-level-access

# Set lifecycle policy to auto-delete old files
cat > lifecycle.json <<EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 7,
          "matchesPrefix": ["etl_transient/"]
        }
      }
    ]
  }
}
EOF

gcloud storage buckets update gs://your-etl-transient-bucket \
    --lifecycle-file=lifecycle.json
```

### **Step 2: Update Environment Variables**

```bash
# .env for GCP
GCS_TRANSIENT_BUCKET=your-etl-transient-bucket
GCS_TRANSIENT_PREFIX=etl_transient/
KEEP_TRANSIENT_FILES=false

# If KEEP_TRANSIENT_FILES=false, files deleted after success
# Lifecycle policy will cleanup files older than 7 days (safety net)
```

### **Step 3: Grant Permissions**

```bash
# For Cloud Run
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

# For Compute Engine (uses default service account)
gcloud compute instances add-iam-policy-binding INSTANCE_NAME \
    --zone=us-central1-a \
    --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"
```

### **Step 4: Modify `data_extractor.py`**

Add GCS support by using the factory function:

```python
# At the top of data_extractor.py
import os

# Add conditional import
def create_transient_manager():
    """Auto-detect storage backend"""
    gcs_bucket = os.getenv('GCS_TRANSIENT_BUCKET')

    if gcs_bucket:
        from transient_storage_gcs import GCSTransientFileManager
        logger.info(f"Using GCS transient storage: gs://{gcs_bucket}")
        return GCSTransientFileManager(bucket_name=gcs_bucket)
    else:
        logger.info("Using local transient storage")
        return TransientFileManager()


# In APIExtractor.__init__()
class APIExtractor:
    def __init__(self):
        self.session = self._create_session()
        self.transient_manager = create_transient_manager()  # Auto-detects
```

### **Step 5: Update `requirements.txt`**

```txt
# Add GCS client library
google-cloud-storage==2.14.0
```

### **Step 6: Deploy**

```bash
# Cloud Run
gcloud run deploy etl-pipeline \
    --image=gcr.io/YOUR_PROJECT/etl-pipeline \
    --set-env-vars=GCS_TRANSIENT_BUCKET=your-etl-transient-bucket \
    --service-account=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT.iam.gserviceaccount.com

# Compute Engine (via startup script)
gcloud compute instances create etl-vm \
    --metadata=GCS_TRANSIENT_BUCKET=your-etl-transient-bucket \
    --scopes=https://www.googleapis.com/auth/cloud-platform
```

---

## **Cost Analysis**

### **Ephemeral Disk (Free)**
```
Cost: $0.00 (included with compute)
```

### **GCS Bucket (Typical Run)**
```
Dataset size: 500MB (uncompressed CSV)
Chunks: 11 files × 50MB each = 550MB

Storage costs:
- Upload: Free
- Storage: $0.02/GB/month × 0.55GB × (1 hour / 730 hours) = $0.000015
- Download (to Cloud Run): Free (same region)
- Delete: Free

Total per run: < $0.001 (~$0.03/month for daily runs)
```

**Recommendation:** GCS is essentially free for this use case.

---

## **Deployment Architectures**

### **Architecture 1: Cloud Run (Serverless)**

```
Trigger (Cloud Scheduler)
   ↓
Cloud Run Job
   ├─ API Extraction → GCS Transient
   │                    ↓
   └─ Load to PostgreSQL
         ↓
   Cloud Run Job
   └─ PostgreSQL → BigQuery
```

**Environment:**
```bash
GCS_TRANSIENT_BUCKET=your-etl-transient
TRANSIENT_DIR=/tmp  # Fallback if GCS fails
```

---

### **Architecture 2: Compute Engine VM**

```
Cron Job on VM
   ↓
Python Script
   ├─ API Extraction → Local Disk (/tmp)
   └─ Load to PostgreSQL → BigQuery
```

**Environment:**
```bash
TRANSIENT_DIR=/tmp/transient_data
# OR
GCS_TRANSIENT_BUCKET=your-etl-transient  # For fault tolerance
```

---

### **Architecture 3: Cloud Composer (Airflow)**

```
Airflow DAG
   ├─ Task 1: Extract API → GCS Transient
   ├─ Task 2: Load GCS → PostgreSQL
   └─ Task 3: PostgreSQL → BigQuery
```

**Environment (in Airflow Variables):**
```python
# airflow_variables.json
{
  "GCS_TRANSIENT_BUCKET": "your-etl-transient",
  "GCS_TRANSIENT_PREFIX": "composer_runs/"
}
```

---

## **Best Practices**

### **1. Bucket Organization**

```
gs://your-etl-transient-bucket/
├── etl_runs/
│   ├── 2025-01-16_10-30-00/
│   │   ├── cdc_chunk_0000.csv
│   │   ├── cdc_chunk_0001.csv
│   │   └── metadata.json
│   └── 2025-01-15_10-30-00/
│       └── ...
└── archived/
    └── ...
```

**Implementation:**
```python
# Add timestamp to prefix
import datetime
timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
GCS_TRANSIENT_PREFIX = f'etl_runs/{timestamp}/'
```

### **2. Lifecycle Policies**

```json
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 7},
        "description": "Delete transient files older than 7 days"
      },
      {
        "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
        "condition": {"age": 1},
        "description": "Move to cheaper storage after 1 day"
      }
    ]
  }
}
```

### **3. Error Handling**

```python
# In data_extractor.py
try:
    transient_manager = create_transient_manager()
    transient_manager.save_chunk(df, idx)
except Exception as e:
    logger.error(f"Failed to save to GCS, falling back to local: {e}")
    # Fallback to local storage
    local_manager = TransientFileManager()
    local_manager.save_chunk(df, idx)
```

### **4. Monitoring**

```python
# Log GCS operations for Cloud Logging
logger.info(
    f"GCS_OPERATION",
    extra={
        "bucket": bucket_name,
        "operation": "save_chunk",
        "chunk_index": idx,
        "rows": len(df),
        "size_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
    }
)
```

---

## **Migration from Local to GCS**

### **Step 1: Add GCS support**
```bash
pip install google-cloud-storage
```

### **Step 2: Test locally with GCS**
```bash
# Set environment variable
export GCS_TRANSIENT_BUCKET=your-test-bucket

# Run pipeline
python data_extractor.py
```

### **Step 3: Verify in GCS Console**
```bash
gsutil ls gs://your-test-bucket/etl_transient/
```

### **Step 4: Deploy to GCP**
```bash
gcloud run deploy ...
```

---

## **Troubleshooting**

### **Issue: "Permission denied" when writing to GCS**

```bash
# Check service account permissions
gcloud projects get-iam-policy YOUR_PROJECT_ID \
    --flatten="bindings[].members" \
    --filter="bindings.members:YOUR_SERVICE_ACCOUNT"

# Grant storage admin
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:YOUR_SERVICE_ACCOUNT" \
    --role="roles/storage.objectAdmin"
```

### **Issue: "Bucket not found"**

```bash
# Verify bucket exists
gsutil ls gs://your-etl-transient-bucket/

# Create if missing
gcloud storage buckets create gs://your-etl-transient-bucket
```

### **Issue: Slow GCS uploads**

```bash
# Use same region for bucket and compute
# Bucket: us-central1
# Cloud Run: us-central1
# Cloud SQL: us-central1
```

---

## **Recommended Setup for Production**

```bash
# 1. Create GCS bucket
gcloud storage buckets create gs://prod-etl-transient \
    --location=us-central1 \
    --uniform-bucket-level-access

# 2. Set lifecycle (auto-delete after 7 days)
cat > lifecycle.json <<EOF
{"lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7}}]}}
EOF
gcloud storage buckets update gs://prod-etl-transient --lifecycle-file=lifecycle.json

# 3. Deploy with GCS
gcloud run deploy etl-pipeline \
    --set-env-vars=GCS_TRANSIENT_BUCKET=prod-etl-transient,KEEP_TRANSIENT_FILES=false

# 4. Monitor
gcloud logging read "resource.type=cloud_run_revision AND GCS_OPERATION" --limit 50
```

---

## **Summary**

| Environment | Recommendation | Configuration |
|-------------|---------------|---------------|
| **Local Dev** | Local files | `TRANSIENT_DIR=./transient_data` |
| **GCP Dev/Test** | Ephemeral disk | `TRANSIENT_DIR=/tmp/transient_data` |
| **GCP Production** | GCS Bucket | `GCS_TRANSIENT_BUCKET=prod-etl-transient` |

**Bottom line:** Use **GCS bucket** for production. It's cheap, reliable, and provides fault tolerance.
