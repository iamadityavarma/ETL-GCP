# 🔄 BigQuery ETL Workflow

## Complete Data Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          CDC CHRONIC DISEASE DATA                       │
│                    https://data.cdc.gov (CSV API)                       │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 │ HTTP GET Request
                                 │ data_extractor.py
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          POSTGRESQL STAGING                             │
│                   staging.staging_cdc_chronic_disease                   │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ Raw Data (As-is from API)                                      │   │
│  │ - Duplicates may exist                                         │   │
│  │ - Whitespace not cleaned                                       │   │
│  │ - No validation                                                │   │
│  │ - Metadata: loaded_at, load_date                               │   │
│  └────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 │ SELECT * FROM staging
                                 │ data_loader.py
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          SQL CLEANING LAYER                             │
│                         (PostgreSQL Functions)                          │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ 1. Remove Duplicates (GROUP BY, HAVING COUNT(*) > 1)          │   │
│  │ 2. Trim Whitespace (TRIM on all TEXT columns)                 │   │
│  └────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 │ pd.read_sql (10K rows/chunk)
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         PANDAS CLEANING LAYER                           │
│                         (Python + pandas)                               │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ 1. Date Formatting                                             │   │
│  │    - Parse 'date' columns → datetime64[ns]                     │   │
│  │    - Handle invalid dates → NaT                                │   │
│  │                                                                 │   │
│  │ 2. Null Handling                                               │   │
│  │    - Numbers → fillna(0)                                       │   │
│  │    - Text → fillna('Unknown')                                  │   │
│  │                                                                 │   │
│  │ 3. Text Standardization                                        │   │
│  │    - Convert to lowercase                                      │   │
│  │    - Strip extra whitespace                                    │   │
│  └────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 │ validate_data(df)
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA VALIDATION LAYER                           │
│                       (Quality Checks + Logging)                        │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ ✓ Logical Checks                                               │   │
│  │   - yearstart <= yearend                                       │   │
│  │   - datavalue in range [0, 100]                                │   │
│  │                                                                 │   │
│  │ ✓ Null Checks                                                  │   │
│  │   - Critical fields: yearstart, yearend, locationabbr, topic   │   │
│  │                                                                 │   │
│  │ ✓ Duplicate Detection                                          │   │
│  │   - Log count of duplicate rows                                │   │
│  │                                                                 │   │
│  │ ✓ Anomaly Logging                                              │   │
│  │   - Save issues → validation_issues_log.csv                    │   │
│  └────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 │ pandas_gbq.to_gbq()
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            BIGQUERY TABLE                               │
│               {project}.{dataset}.cleaned_cdc_chronic_disease           │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐   │
│  │ Clean, Validated Data                                          │   │
│  │                                                                 │   │
│  │ ✅ No duplicates                                                │   │
│  │ ✅ Standardized text (lowercase, trimmed)                       │   │
│  │ ✅ Nulls handled (0 or 'unknown')                               │   │
│  │ ✅ Dates parsed correctly                                       │   │
│  │ ✅ Validated for logical consistency                            │   │
│  │ ✅ Schema auto-inferred from DataFrame                          │   │
│  │                                                                 │   │
│  │ Columns:                                                        │   │
│  │  - yearstart (INTEGER)                                         │   │
│  │  - yearend (INTEGER)                                           │   │
│  │  - locationabbr (STRING)                                       │   │
│  │  - locationdesc (STRING)                                       │   │
│  │  - topic (STRING)                                              │   │
│  │  - question (STRING)                                           │   │
│  │  - datavalue (FLOAT)                                           │   │
│  │  - stratification1 (STRING)                                    │   │
│  │  - ... (all CDC columns)                                       │   │
│  │  - loaded_at (TIMESTAMP)                                       │   │
│  │  - load_date (DATE)                                            │   │
│  └────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 │ Query & Analysis
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA CONSUMERS                                  │
│                                                                          │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐     │
│  │   BI Tools       │  │  Data Science    │  │   Analysts       │     │
│  │                  │  │                  │  │                  │     │
│  │  • Looker Studio │  │  • Notebooks     │  │  • SQL Queries   │     │
│  │  • Tableau       │  │  • Python/R      │  │  • Reports       │     │
│  │  • Power BI      │  │  • ML Models     │  │  • Dashboards    │     │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🔧 Technical Implementation

### Files & Responsibilities

```
data_extractor.py (Lines 1-423)
├── APIExtractor class
│   ├── extract_cdc_data()          → Fetch CSV from CDC API
│   └── _create_session()           → HTTP retry logic
│
└── PostgreSQLLoader class
    ├── create_staging_table()      → CREATE/TRUNCATE table
    ├── load_data()                 → Bulk INSERT with chunking
    └── schema_matches()            → Check if schema changed

───────────────────────────────────────────────────────────

data_loader.py (Lines 1-397)
├── PostgreSQLConnector class
│   ├── connect()                   → Connect to PostgreSQL
│   └── fetch_data()                → SELECT * FROM staging
│
├── SchemaValidator class
│   ├── infer_schema()              → Map pandas → PostgreSQL types
│   └── validate_existing_table()   → Check schema compatibility
│
├── PostgreSQLLoader class
│   └── create_staging_table()      → Smart table creation/truncation
│
├── BigQueryWriter class ⭐
│   └── write()                     → pandas_gbq.to_gbq()
│       ├── if_exists='replace'     → First chunk (create table)
│       └── if_exists='append'      → Subsequent chunks (add data)
│
├── clean_and_normalize_sql()       → SQL-level cleaning
├── pandas_cleaning()               → DataFrame-level cleaning
└── validate_data()                 → Quality checks + anomaly logging
```

---

## 📊 Data Transformation Examples

### Example 1: Text Standardization

**Before (Raw API Data):**
```
locationdesc: "  California  "
topic: "CANCER  "
```

**After (BigQuery):**
```
locationdesc: "california"
topic: "cancer"
```

### Example 2: Null Handling

**Before (Raw API Data):**
```
datavalue: NaN
stratification1: NaN
```

**After (BigQuery):**
```
datavalue: 0.0
stratification1: "unknown"
```

### Example 3: Date Parsing

**Before (Raw API Data):**
```
yearstart: "2020"  (TEXT)
yearend: "2021"    (TEXT)
```

**After (BigQuery):**
```
yearstart: 2020    (INTEGER)
yearend: 2021      (INTEGER)
```

---

## ⚡ Performance Characteristics

### Chunked Processing

```
Total Rows: 124,875
Chunk Size: 10,000

┌─────────────────────────────────────────┐
│ Chunk 1:  Rows 1-10,000    (REPLACE)   │  ← Creates table
├─────────────────────────────────────────┤
│ Chunk 2:  Rows 10,001-20,000 (APPEND)  │
├─────────────────────────────────────────┤
│ Chunk 3:  Rows 20,001-30,000 (APPEND)  │
├─────────────────────────────────────────┤
│ ...                                     │
├─────────────────────────────────────────┤
│ Chunk 13: Rows 120,001-124,875 (APPEND)│
└─────────────────────────────────────────┘

Time Estimate:
- 10K rows → ~10-15 seconds per chunk
- 125K rows → ~2-3 minutes total
```

### Resource Usage

| Stage | Memory | CPU | Network |
|-------|--------|-----|---------|
| Extract (API) | Low (streaming) | Low | High (download) |
| SQL Clean | Database RAM | Database CPU | Low |
| Pandas Clean | ~chunk_size × row_size | Medium | Low |
| BQ Upload | Low (pandas-gbq handles) | Low | High (upload) |

---

## 🎯 Configuration Options

### Adjust Chunk Size

```python
# data_loader.py, line 365
chunk_size = 10000  # Default

# Options:
chunk_size = 5000   # Slower, less memory
chunk_size = 20000  # Faster, more memory
chunk_size = 1000   # For testing
```

### Change Load Behavior

```python
# data_loader.py, lines 379-383

# Option 1: Always replace table (fresh start)
bq_writer.write(df_valid, if_exists='replace')

# Option 2: Always append (incremental load)
bq_writer.write(df_valid, if_exists='append')

# Option 3: Current behavior (replace first, append rest)
if first_chunk:
    bq_writer.write(df_valid, if_exists='replace')
else:
    bq_writer.write(df_valid, if_exists='append')
```

### Modify Validation Rules

```python
# data_loader.py, lines 292-341

# Example: Add custom validation
if 'age' in df.columns:
    invalid_age = df[(df['age'] < 0) | (df['age'] > 120)]
    if not invalid_age.empty:
        issues.append(f"{len(invalid_age)} rows have invalid age")
```

---

## 📈 Monitoring Points

### Key Metrics to Track

```python
# In data_loader.py main() function:

1. Total rows processed       → logger.info(f"Total rows: {total_rows}")
2. Chunk processing time       → Track time per chunk
3. Validation issues           → Count of issues logged
4. BigQuery write success      → Check for exceptions
5. Final row count in BQ       → Query after load
```

### Example Query to Verify Load

```sql
-- Check if data loaded correctly
SELECT 
    COUNT(*) as total_rows,
    MIN(yearstart) as min_year,
    MAX(yearend) as max_year,
    COUNT(DISTINCT locationabbr) as unique_states,
    COUNT(DISTINCT topic) as unique_topics,
    MAX(loaded_at) as last_load_time
FROM `your-project.your_dataset.cleaned_cdc_chronic_disease`
```

---

## 🔄 Scheduling Options

### Option 1: Cron (Linux/Mac)
```bash
# Daily at 2 AM
0 2 * * * cd /path/to/GCP-ETL && python data_loader.py >> logs/loader.log 2>&1
```

### Option 2: Task Scheduler (Windows)
```
1. Open Task Scheduler
2. Create Basic Task
3. Trigger: Daily at 2:00 AM
4. Action: Start Program
   - Program: C:\path\to\python.exe
   - Arguments: data_loader.py
   - Start in: C:\path\to\GCP-ETL
```

### Option 3: Cloud Scheduler (GCP)
```bash
gcloud scheduler jobs create http etl-daily \
    --schedule="0 2 * * *" \
    --uri="https://your-cloud-run-url/run" \
    --http-method=POST
```

---

## 🎓 Next Steps

### Immediate
1. ✅ Run `test_bigquery_connection.py`
2. ✅ Run full pipeline: `data_extractor.py` → `data_loader.py`
3. ✅ Verify data in BigQuery Console

### Short Term
4. ✅ Create visualizations (Looker Studio, Tableau)
5. ✅ Set up scheduled runs (cron/scheduler)
6. ✅ Add monitoring/alerting

### Long Term
7. ✅ Optimize with partitioning/clustering
8. ✅ Add incremental load logic
9. ✅ Create data quality dashboard

---

**Pipeline Ready! 🚀** See `SETUP_SUMMARY.md` for quick start guide.

