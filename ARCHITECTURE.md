# GCP ETL Pipeline Architecture

## Overview

This document describes the enhanced ETL pipeline architecture with **API chunking** and **transient file storage** layers.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     CDC API (Data Source)                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ HTTP GET (CSV Format)
                         │ Retry Logic: 3 attempts
                         │ Timeout: 180s
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   API Extractor (data_extractor.py)             │
│  - Session with retry strategy                                  │
│  - Stream processing                                            │
│  - Response validation                                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ Full DataFrame
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Chunking Layer (NEW!)                              │
│  - Split into 50,000 row chunks (configurable)                  │
│  - Memory efficient                                             │
│  - Progress tracking per chunk                                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ Chunk DataFrame(s)
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         Transient File Storage (NEW!)                           │
│  Location: ./transient_data/                                    │
│  Format:                                                        │
│    - cdc_chunk_0000.csv                                         │
│    - cdc_chunk_0001.csv                                         │
│    - cdc_chunk_0002.csv                                         │
│    ...                                                          │
│                                                                 │
│  Benefits:                                                      │
│    ✅ Fault tolerance (resume from failed chunk)                │
│    ✅ Memory efficiency (process one chunk at a time)           │
│    ✅ Audit trail (keep files for debugging)                    │
│    ✅ Decouples API extraction from DB loading                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ Sequential chunk loading
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         PostgreSQL Loader (data_extractor.py)                   │
│  - Read chunk files one at a time                               │
│  - Create/truncate staging table on first chunk                 │
│  - Bulk insert with execute_values (10k batch size)             │
│  - Connection retry logic (3 attempts)                          │
│  - Small delay between chunks to avoid overwhelming DB          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ INSERT INTO staging.staging_cdc_chronic_disease
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         PostgreSQL Staging Table                                │
│  - Schema auto-detected from DataFrame                          │
│  - Metadata columns: loaded_at, load_date                       │
│  - Optimized for low-resource environments                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ (data_loader.py handles next steps)
                         │
                         ▼
          [Data Cleaning & Transformation]
                         │
                         ▼
              [BigQuery Load]
```

---

## Key Components

### 1. **APIExtractor** (`data_extractor.py:142-240`)

Extracts data from CDC API with two modes:

#### **Legacy Mode** (for backward compatibility)
```python
extractor = APIExtractor()
df = extractor.extract_cdc_data()
# Returns full DataFrame in memory
```

#### **Chunked Mode** (NEW - recommended)
```python
extractor = APIExtractor()
transient_manager = extractor.extract_and_chunk_to_transient()
# Returns TransientFileManager with saved chunk files
```

**Features:**
- HTTP session with retry strategy (3 attempts, exponential backoff)
- Handles 429, 500, 502, 503, 504 status codes
- 180-second timeout
- Streams response for memory efficiency
- Validates response (empty check)

---

### 2. **TransientFileManager** (`data_extractor.py:73-135`)

Manages intermediate file storage between API and database.

**Methods:**
- `save_chunk(df, chunk_index)` - Save DataFrame to CSV
- `load_chunk(filepath)` - Load DataFrame from CSV
- `list_chunks()` - List all chunk files sorted by index
- `cleanup()` - Remove transient directory (respects KEEP_TRANSIENT_FILES)
- `get_total_rows()` - Count total rows across all chunks

**File Naming:**
```
cdc_chunk_0000.csv
cdc_chunk_0001.csv
cdc_chunk_0002.csv
...
```

**Configuration:**
```bash
TRANSIENT_DIR=./transient_data         # Where to store chunks
KEEP_TRANSIENT_FILES=false             # Auto-delete after success
API_CHUNK_SIZE=50000                   # Rows per chunk
```

---

### 3. **PostgreSQLLoader** (`data_extractor.py:247-508`)

Loads data from transient files or DataFrames to PostgreSQL.

#### **New Method: `load_from_transient_files()`**

```python
db_loader = PostgreSQLLoader()
db_loader.connect()
db_loader.load_from_transient_files('staging.table_name', transient_manager)
```

**How it works:**
1. Lists all chunk files in order
2. Loads first chunk to create table schema
3. Iterates through chunks sequentially
4. Loads each chunk using `load_data()` method
5. Tracks progress (rows loaded per chunk)

**Benefits:**
- Memory efficient (one chunk at a time)
- Fault tolerant (can resume from failed chunk)
- Progress visibility
- Connection retry built-in

---

## Pipeline Flow

### **Enhanced Pipeline (with transient files)**

```python
def run_pipeline():
    # Step 1: Extract & Chunk to Transient Files
    extractor = APIExtractor()
    transient_manager = extractor.extract_and_chunk_to_transient()

    # Step 2: Connect to Database
    db_loader = PostgreSQLLoader()
    db_loader.connect()

    # Step 3: Load from Transient Files
    db_loader.load_from_transient_files('staging.table', transient_manager)

    # Step 4: Cleanup
    transient_manager.cleanup()
```

---

## Configuration

### Environment Variables

```bash
# Database Configuration
DB_HOST=10.123.45.67
DB_PORT=5432
DB_NAME=etl_database
DB_USER=etl_user
DB_PASSWORD=secret_password

# API Configuration
CDC_API_URL=https://data.cdc.gov/resource/g4ie-h725.csv?$limit=1000000

# Staging Table
STAGING_CDC_TABLE=staging.staging_cdc_chronic_disease

# Chunking Settings (NEW)
API_CHUNK_SIZE=50000              # Rows per chunk (default: 50k)
TRANSIENT_DIR=./transient_data    # Transient file directory
KEEP_TRANSIENT_FILES=false        # Keep files after success (default: false)
```

---

## Benefits of Transient File Layer

### 1. **Fault Tolerance**
- If database connection fails mid-load, transient files are preserved
- Can resume from last successful chunk
- No need to re-fetch from API

### 2. **Memory Efficiency**
- Process one chunk at a time instead of loading 500k+ rows into memory
- Suitable for resource-constrained environments
- Prevents OOM errors on large datasets

### 3. **Audit Trail**
- Set `KEEP_TRANSIENT_FILES=true` to retain chunks
- Useful for debugging data quality issues
- Can replay chunks if needed

### 4. **Decoupling**
- API extraction separate from database loading
- Can extract data once, load to multiple targets
- Easier to test individual components

### 5. **Progress Visibility**
- Clear logging per chunk
- Easy to identify which chunk failed
- Better observability

---

## Error Handling

### API Extraction Failures
```
[ERROR] CDC API request timeout
→ Automatic retry (up to 3 attempts)
→ Exponential backoff (1s, 2s, 4s)
→ If all retries fail: raise exception, keep transient files for debugging
```

### Database Connection Failures
```
[ERROR] Database connection failed
→ Automatic retry (up to 3 attempts)
→ 2-second delay between retries
→ Reconnection attempt
→ Transient files preserved for manual retry
```

### Chunk Load Failures
```
[ERROR] Failed to load chunk cdc_chunk_0005.csv
→ Pipeline stops immediately
→ All transient files kept for debugging
→ Can manually resume from chunk_0005 after fixing issue
```

---

## Performance Tuning

### Adjust Chunk Size
```bash
# Larger chunks = fewer files, more memory
API_CHUNK_SIZE=100000

# Smaller chunks = more files, less memory
API_CHUNK_SIZE=25000
```

### Database Batch Size
In `load_data()` method:
```python
chunk_size = 10000  # DB insert batch size
page_size = 500     # execute_values page size
```

### Delay Between Chunks
```python
time.sleep(0.1)  # 100ms delay to prevent overwhelming DB
```

---

## Logging Output Example

```
================================================================================
API TO STAGING PIPELINE STARTED - 2025-01-16 10:30:00
================================================================================

[STEP 1] Extracting data from CDC API with chunking
[SUCCESS] Successfully extracted 542,321 CDC records
[CHUNKING] Splitting 542,321 rows into 11 chunk(s) of 50,000 rows
[TRANSIENT] Saved chunk 0 (50,000 rows) to cdc_chunk_0000.csv
[CHUNKING] Chunk 1/11 saved (50,000 rows)
[TRANSIENT] Saved chunk 1 (50,000 rows) to cdc_chunk_0001.csv
[CHUNKING] Chunk 2/11 saved (50,000 rows)
...
[SUCCESS] All chunks saved to transient storage

[STEP 2] Connecting to PostgreSQL
[SUCCESS] Connected to PostgreSQL database

[STEP 3] Loading data from transient files to staging table
[TRANSIENT] Found 11 chunk file(s)
[TRANSIENT→DB] Processing chunk 1/11: cdc_chunk_0000.csv
[TRANSIENT] Loaded 50,000 rows from cdc_chunk_0000.csv
[PROGRESS] Loaded 50,000/50,000 rows (100.0%)
[SUCCESS] Loaded 50,000 rows into staging.staging_cdc_chronic_disease
[TRANSIENT→DB] Chunk 1/11 loaded | Total: 50,000 rows
...
[SUCCESS] All 11 transient file(s) loaded into staging.staging_cdc_chronic_disease
[SUCCESS] Total rows loaded: 542,321

[STEP 4] Cleanup transient files
[TRANSIENT] Cleaned up transient directory: ./transient_data

================================================================================
PIPELINE COMPLETED SUCCESSFULLY
Duration: 245.67 seconds
Total chunks processed: N/A (cleaned up)
End time: 2025-01-16 10:34:06
================================================================================
```

---

## Migration Guide

### Old Code (Direct Load)
```python
# Old approach
extractor = APIExtractor()
df = extractor.extract_cdc_data()  # All data in memory

db_loader = PostgreSQLLoader()
db_loader.connect()
db_loader.create_staging_table('staging.table', df)
db_loader.load_data('staging.table', df)
```

### New Code (Transient Files)
```python
# New approach
extractor = APIExtractor()
transient_manager = extractor.extract_and_chunk_to_transient()  # Chunked to files

db_loader = PostgreSQLLoader()
db_loader.connect()
db_loader.load_from_transient_files('staging.table', transient_manager)  # Load from files
transient_manager.cleanup()  # Optional cleanup
```

---

## Testing

Unit tests remain unchanged and use mocks. New tests can be added for:
- TransientFileManager chunk save/load
- Chunking logic in extract_and_chunk_to_transient()
- load_from_transient_files() method

---

## Future Enhancements

1. **Parallel Chunk Processing**: Load multiple chunks concurrently
2. **Compression**: Compress transient files (gzip) to save disk space
3. **Cloud Storage**: Store chunks in GCS instead of local filesystem
4. **Incremental Loads**: Only load new/changed chunks
5. **Checkpointing**: Save pipeline state for exact resume capability
