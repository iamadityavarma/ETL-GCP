# ETL Pipeline Flow Diagrams

This document contains detailed flow diagrams for the three main components of the CDC Chronic Disease ETL pipeline.

---

## 1. Data Extractor Flow (data_extractor.py)

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA EXTRACTOR PIPELINE                   │
└─────────────────────────────────────────────────────────────┘

START: run_pipeline()
   │
   ├─► [STEP 1] Validate Configuration
   │              └─► Check env variables (DB_HOST, DB_NAME, etc.)
   │                   └─► Raise error if missing
   │
   ├─► [STEP 2] Extract Data from CDC API
   │              │
   │              ├─► Create HTTP Session with retry strategy
   │              │    └─► Retry: 3 attempts, backoff on 429(Too Many Requests)/500(Internal Server Error)/502(Bad Gateway)/503(Service Unavailable)/504(Gateway Timeout)
   │              │
   │              ├─► Make GET request to CDC_API_URL
   │              │    └─► Timeout: 180 seconds
   │              │    └─► Stream response
   │              │
   │              ├─► Parse CSV response → pandas DataFrame
   │              │
   │              └─► Validate: Check if DataFrame is empty
   │                   └─► Log success: "Extracted X records"
   │
   ├─► [STEP 3] Connect to PostgreSQL
   │              └─► psycopg2.connect()
   │                   └─► SSL mode: prefer
   │
   ├─► [STEP 4] Prepare Staging Table
   │              │
   │              ├─► Check if table exists
   │              │    └─► YES → Check schema match
   │              │         ├─► MATCH → TRUNCATE table (fast)
   │              │         └─► NO MATCH → DROP & CREATE new table
   │              │
   │              └─► NO → CREATE new table
   │                   └─► Map pandas dtypes → PostgreSQL types
   │                   └─► Add metadata columns (loaded_at, load_date)
   │
   ├─► [STEP 5] Load Data into Staging
   │              │
   │              ├─► Clean column names (lowercase, replace spaces)
   │              ├─► Replace NaN with None
   │              │
   │              ├─► Chunked bulk insert (10,000 rows/chunk)
   │              │    ├─► Page size: 500 rows
   │              │    ├─► Retry logic: 3 attempts
   │              │    │    └─► Reconnect if connection lost
   │              │    ├─► Log progress: X/Y rows (%)
   │              │    └─► Small delay (0.1s) between chunks
   │              │
   │              └─► Commit transaction
   │
   ├─► [SUCCESS] Log completion statistics
   │              └─► Total records loaded
   │              └─► Duration
   │
   └─► Close database connection

END
```

**Key Features:**
- **API Extraction**: Robust retry mechanism with exponential backoff
- **Smart Table Management**: TRUNCATE if schema matches, otherwise DROP/CREATE
- **Chunked Loading**: Optimized for low-resource environments
- **Connection Recovery**: Automatic reconnection on database failures
- **Progress Logging**: Real-time progress updates

---

## 2. Data Loader Flow (data_loader.py)

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA LOADER PIPELINE                     │
└─────────────────────────────────────────────────────────────┘

START: main()
   │
   ├─► Connect to PostgreSQL
   │    └─► PostgreSQLConnector.connect()
   │
   ├─► [PHASE 1] SQL-Based Cleaning
   │              │
   │              ├─► Remove duplicates
   │              │    └─► DELETE using ctid (keep MIN ctid per unique row)
   │              │
   │              └─► Trim whitespace from all TEXT columns
   │                   └─► Dynamic loop over all text columns
   │
   ├─► Get total row count from staging table
   │
   ├─► Initialize BigQuery Writer
   │    └─► Project: BQ_PROJECT
   │    └─► Dataset: BQ_DATASET
   │    └─► Table: BQ_TABLE
   │
   ├─► [PHASE 2] Chunked Processing Loop
   │              │
   │              │  [Process 10,000 rows at a time]
   │              │
   │              ├─► Read chunk from PostgreSQL (OFFSET/LIMIT)
   │              │
   │              ├─► Pandas Cleaning
   │              │    ├─► Convert date columns → datetime
   │              │    ├─► Fill numeric NaN → 0
   │              │    └─► Fill string NaN → "Unknown" + lowercase + trim
   │              │
   │              ├─► Data Validation (inline)
   │              │    ├─► Check yearstart ≤ yearend
   │              │    ├─► Check datavalue in [0, 100]
   │              │    ├─► Check missing critical fields
   │              │    ├─► Check duplicates
   │              │    └─► Log issues + save anomalies to CSV
   │              │
   │              ├─► Add BigQuery timestamps
   │              │    ├─► loaded_at: current timestamp
   │              │    └─► load_date: current date
   │              │
   │              ├─► Write to BigQuery
   │              │    ├─► First chunk → REPLACE table
   │              │    └─► Subsequent chunks → APPEND
   │              │
   │              └─► Repeat until all rows processed
   │
   ├─► [SUCCESS] Log completion
   │
   └─► Close PostgreSQL connection

END
```

**Key Features:**
- **Two-Phase Cleaning**: SQL-based deduplication + pandas-based transformation
- **Schema Validation**: Automatic schema inference and validation
- **Data Quality**: Inline validation with anomaly logging
- **BigQuery Integration**: Chunked writes with REPLACE/APPEND strategy
- **Memory Efficient**: Processes data in manageable chunks

---

## 3. Data Validator Flow (data_validator.py)

```
┌─────────────────────────────────────────────────────────────┐
│                   DATA VALIDATOR PIPELINE                    │
└─────────────────────────────────────────────────────────────┘

START: main()
   │
   ├─► Initialize BigQuery Client
   │    └─► Project: BQ_PROJECT
   │
   ├─► [VALIDATION 1] Check Table Exists
   │                   └─► client.get_table(table_id)
   │                        └─► PASS: Table found
   │                        └─► FAIL: Table not found
   │
   ├─► [VALIDATION 2] Validate Row Count
   │                   └─► SELECT COUNT(*) FROM table
   │                        └─► PASS: row_count >= MIN_EXPECTED_ROWS (100,000)
   │                        └─► FAIL: row_count < threshold
   │
   ├─► [VALIDATION 3] Validate Data Freshness
   │                   └─► SELECT MAX(loaded_at), hours_since_load
   │                        └─► PASS: hours_since_load ≤ MAX_DATA_AGE_HOURS (24)
   │                        └─► FAIL: Data too old
   │
   ├─► [VALIDATION 4] Validate Schema
   │                   └─► Check required fields present:
   │                        ├─► yearstart
   │                        ├─► yearend
   │                        ├─► locationabbr
   │                        ├─► topic
   │                        ├─► loaded_at
   │                        └─► load_date
   │                        └─► PASS: All present
   │                        └─► FAIL: Missing fields
   │
   ├─► [VALIDATION 5] Validate Data Quality
   │                   │
   │                   ├─► Count distinct values:
   │                   │    ├─► Distinct years
   │                   │    ├─► Distinct locations
   │                   │    └─► Distinct topics
   │                   │
   │                   ├─► Check for null critical fields:
   │                   │    ├─► yearstart IS NULL
   │                   │    ├─► locationabbr IS NULL
   │                   │    └─► topic IS NULL
   │                   │
   │                   └─► Sanity checks:
   │                        ├─► PASS: ≥5 distinct years
   │                        ├─► PASS: ≥10 distinct locations
   │                        └─► FAIL: Below thresholds
   │
   ├─► Generate Validation Summary Report
   │    └─► List all validations with PASS/FAIL status
   │
   ├─► Determine exit code
   │    ├─► All passed → Exit code 0 (success)
   │    └─► Any failed → Exit code 1 (failure)
   │
   └─► EXIT

END
```

**Key Features:**
- **Comprehensive Validation**: 5 independent validation checks
- **Quality Metrics**: Distinct value counts and null checks
- **Freshness Monitoring**: Ensures data is up-to-date
- **Exit Code Reporting**: Machine-readable success/failure status
- **Detailed Logging**: Complete validation summary with specific failures

---

## Complete ETL Pipeline Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│                     COMPLETE ETL PIPELINE                             │
└──────────────────────────────────────────────────────────────────────┘

[1] DATA EXTRACTOR (data_extractor.py)
    │
    ├─► Extract from CDC API
    ├─► Load into PostgreSQL staging table
    └─► Output: staging.staging_cdc_chronic_disease
         │
         │
         ▼
[2] DATA LOADER (data_loader.py)
    │
    ├─► Read from PostgreSQL staging
    ├─► Clean & validate data
    ├─► Transform data
    └─► Load into BigQuery
         │
         │
         ▼
[3] DATA VALIDATOR (data_validator.py)
    │
    ├─► Validate BigQuery table
    ├─► Check data quality
    ├─► Verify freshness
    └─► Return success/failure status
```

---

## Component Reference

| Component | Input | Output | Key Function |
|-----------|-------|--------|--------------|
| **Data Extractor** | CDC API (CSV) | PostgreSQL staging table | API extraction with retry logic |
| **Data Loader** | PostgreSQL staging | BigQuery table | Data cleaning & transformation |
| **Data Validator** | BigQuery table | Exit code (0/1) | Quality validation & monitoring |

---

## Configuration Requirements

### Data Extractor Environment Variables
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `CDC_API_URL`
- `STAGING_CDC_TABLE`

### Data Loader Environment Variables
- PostgreSQL: Same as extractor
- BigQuery: `BQ_PROJECT`, `BQ_DATASET`, `BQ_TABLE`

### Data Validator Environment Variables
- `BQ_PROJECT`, `BQ_DATASET`, `BQ_TABLE`
- `MIN_EXPECTED_ROWS` (default: 100,000)
- `MAX_DATA_AGE_HOURS` (default: 24)

---

## Error Handling Strategy

1. **Data Extractor**:
   - API failures → Retry with exponential backoff
   - Connection loss → Automatic reconnection
   - Schema changes → Automatic table recreation

2. **Data Loader**:
   - Schema mismatches → Logged warnings
   - Data quality issues → Logged to CSV
   - Processing continues despite validation warnings

3. **Data Validator**:
   - Any validation failure → Exit code 1
   - All validations pass → Exit code 0
   - Suitable for CI/CD pipeline integration

---

Generated: 2025-10-10
