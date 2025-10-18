# Code Flow Documentation

This document explains how the ETL pipeline works step-by-step, covering every function and how they interact with each other.

## Table of Contents
- [Pipeline Execution Flow](#pipeline-execution-flow)
- [1. Data Extractor Flow](#1-data-extractor-flow)
- [2. Data Loader Flow](#2-data-loader-flow)
- [3. Data Validator Flow](#3-data-validator-flow)
- [4. Airflow Orchestration Flow](#4-airflow-orchestration-flow)
- [Function Interaction Diagram](#function-interaction-diagram)

---

## Pipeline Execution Flow

```
Airflow DAG Trigger
    ↓
[1] Data Extractor (data_extractor.py)
    ↓
[2] Data Loader (data_loader.py)
    ↓
[3] Data Validator (data_validator.py)
    ↓
Email Alerts (success/failure)
```

---

## 1. Data Extractor Flow

**File**: `data_extractor.py`

**Entry Point**: `run_pipeline()` (line 461)

### Execution Steps:

```
run_pipeline()
    │
    ├─> validate_config()                    # Validate environment variables
    │
    ├─> APIExtractor.__init__()
    │   └─> _create_session()                # Create HTTP session with retry logic
    │   └─> GCSTransientStorage.__init__()   # Initialize GCS client
    │
    ├─> extract_and_chunk_to_gcs()
    │   ├─> session.get(CDC_API_URL)         # Fetch data from API
    │   ├─> pd.read_csv()                    # Parse CSV response
    │   └─> Loop for each chunk:
    │       └─> save_chunk()                 # Save DataFrame to GCS
    │           └─> bucket.blob().upload_from_string()
    │
    ├─> PostgreSQLLoader.__init__()
    │   └─> connect()                        # Connect to PostgreSQL
    │
    ├─> load_from_gcs()
    │   ├─> list_chunks()                    # Get all chunk files from GCS
    │   ├─> load_chunk(first_chunk)          # Load first chunk to infer schema
    │   ├─> create_staging_table()
    │   │   ├─> table_exists()               # Check if table exists
    │   │   ├─> schema_matches()             # Check if schema matches
    │   │   │   └─> get_table_columns()
    │   │   └─> TRUNCATE or DROP/CREATE table
    │   │
    │   └─> Loop for each chunk:
    │       ├─> load_chunk()                 # Download chunk from GCS
    │       └─> load_data()                  # Insert into PostgreSQL
    │           └─> execute_values()         # Batch INSERT
    │
    ├─> cleanup()                            # Delete GCS transient files
    │   └─> bucket.list_blobs().delete()
    │
    └─> close()                              # Close database connection
```

### Detailed Function Breakdown:

#### 1.1 `validate_config()` (line 450)
**Purpose**: Validate that required environment variables are set

```python
def validate_config():
    required = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing = [var for var in required if not os.getenv(var)]

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
```

**What it does**:
- Checks if critical environment variables exist
- Raises error if any are missing
- Prevents pipeline from running with incomplete configuration

---

#### 1.2 `GCSTransientStorage.__init__()` (line 78)
**Purpose**: Initialize Google Cloud Storage client

```python
def __init__(self):
    self.bucket_name = Config.GCS_BUCKET
    self.prefix = Config.GCS_PREFIX
    self.storage_client = storage.Client()
    self.bucket = self.storage_client.bucket(self.bucket_name)
```

**What it does**:
- Creates GCS client using Application Default Credentials
- Gets reference to the transient bucket
- Prepares for file upload/download operations

---

#### 1.3 `save_chunk()` (line 95)
**Purpose**: Save a DataFrame chunk to GCS as CSV

```python
def save_chunk(self, df, chunk_index):
    filename = f"{Config.TRANSIENT_FILE_PREFIX}_{chunk_index:04d}.csv"
    blob_path = f"{self.prefix}{filename}"

    csv_string = df.to_csv(index=False)
    blob = self.bucket.blob(blob_path)
    blob.upload_from_string(csv_string, content_type='text/csv')
```

**What it does**:
- Converts DataFrame to CSV string
- Generates filename like `cdc_chunk_0000.csv`
- Uploads to GCS path: `gs://bucket/transient/cdc_chunk_0000.csv`
- Logs progress

**Why GCS?**:
- Decouples API extraction from database loading
- Allows recovery if PostgreSQL load fails
- Enables parallel processing (future enhancement)

---

#### 1.4 `APIExtractor._create_session()` (line 163)
**Purpose**: Create HTTP session with retry logic

```python
def _create_session(self):
    session = requests.Session()
    retry_strategy = Retry(
        total=Config.API_RETRY_ATTEMPTS,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session
```

**What it does**:
- Creates a requests session with automatic retry
- Retries on server errors (500, 502, 503, 504) and rate limiting (429)
- Uses exponential backoff (wait 1s, 2s, 4s between retries)
- Only retries GET requests (safe to retry)

**Why retries?**:
- CDC API may be temporarily unavailable
- Network issues may cause transient failures
- Prevents pipeline failure due to temporary issues

---

#### 1.5 `extract_and_chunk_to_gcs()` (line 181)
**Purpose**: Main extraction logic - fetch API data and chunk to GCS

```python
def extract_and_chunk_to_gcs(self):
    # Get data from API
    response = self.session.get(
        Config.CDC_API_URL,
        timeout=Config.API_TIMEOUT,
        stream=True
    )
    response.raise_for_status()

    # Parse CSV
    df_full = pd.read_csv(StringIO(response.text))

    # Chunk and save
    chunk_size = Config.API_CHUNK_SIZE
    num_chunks = (total_rows + chunk_size - 1) // chunk_size

    for idx in range(num_chunks):
        start = idx * chunk_size
        end = min(start + chunk_size, total_rows)
        df_chunk = df_full.iloc[start:end]

        self.gcs_storage.save_chunk(df_chunk, idx)
```

**What it does**:
1. Fetches full dataset from CDC API (timeout: 180 seconds)
2. Parses CSV response into pandas DataFrame
3. Splits into 50,000-row chunks
4. Saves each chunk to GCS

**Why chunking?**:
- API returns ~500,000+ rows (too large for single transaction)
- Reduces memory usage
- Enables incremental loading to PostgreSQL
- Allows progress tracking

---

#### 1.6 `PostgreSQLLoader.connect()` (line 243)
**Purpose**: Establish database connection

```python
def connect(self):
    self.conn = psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        sslmode='prefer'
    )
```

**What it does**:
- Connects to PostgreSQL using credentials from environment variables
- Uses SSL if available (sslmode='prefer')
- Stores connection in `self.conn` for reuse

---

#### 1.7 `create_staging_table()` (line 314)
**Purpose**: Create or prepare the PostgreSQL staging table

```python
def create_staging_table(self, table_name, df):
    if self.table_exists(table_name):
        if self.schema_matches(table_name, df):
            # Schema unchanged - just TRUNCATE (fast)
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            return
        else:
            # Schema changed - DROP and CREATE (flexible)
            pass

    # Map pandas dtypes to PostgreSQL types
    type_mapping = {
        'object': 'TEXT',
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }

    # Build column definitions
    columns = []
    for col in df.columns:
        clean_col = col.lower().replace(' ', '_')
        pg_type = type_mapping.get(str(df[col].dtype), 'TEXT')
        columns.append(f'"{clean_col}" {pg_type}')

    # Add metadata columns
    columns.append('loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
    columns.append('load_date DATE DEFAULT CURRENT_DATE')

    # Create table
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    cursor.execute(f"CREATE TABLE {table_name} ({', '.join(columns)});")
```

**What it does**:
1. **Checks if table exists**: Uses `information_schema.tables`
2. **Schema comparison**:
   - If schema matches → `TRUNCATE` (fast, keeps structure)
   - If schema changed → `DROP` + `CREATE` (flexible)
3. **Infers schema** from DataFrame dtypes
4. **Cleans column names**: Converts to lowercase, replaces spaces with underscores
5. **Adds metadata columns**: `loaded_at`, `load_date`

**Why dynamic table creation?**:
- CDC API schema may change (new columns added)
- Automatic schema evolution
- No manual DDL updates needed

---

#### 1.8 `load_data()` (line 379)
**Purpose**: Insert DataFrame into PostgreSQL table

```python
def load_data(self, table_name, df):
    # Clean column names
    df.columns = [
        col.lower().replace(' ', '_').replace('-', '_')
        for col in df.columns
    ]

    # Replace NaN with None for proper NULL handling
    df = df.where(pd.notna(df), None)

    # Build INSERT query
    columns = ', '.join([f'"{col}"' for col in df.columns])
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES %s"

    # Convert DataFrame to tuples
    data_tuples = [tuple(row) for row in df.values]

    # Batch insert using execute_values
    with self.conn.cursor() as cursor:
        execute_values(cursor, insert_sql, data_tuples, page_size=1000)
        self.conn.commit()
```

**What it does**:
1. **Normalizes column names** to match table schema
2. **Handles NaN values**: Converts to None (NULL in SQL)
3. **Builds parameterized INSERT**: Prevents SQL injection
4. **Batch insert**: Uses `execute_values()` with page_size=1000
   - Inserts 1000 rows per batch
   - Much faster than row-by-row inserts

**Why execute_values?**:
- Optimized for bulk inserts
- Reduces network round-trips
- 10-100x faster than executemany()

---

#### 1.9 `cleanup()` (line 137)
**Purpose**: Delete transient files from GCS

```python
def cleanup(self):
    if Config.KEEP_TRANSIENT_FILES:
        logger.info(f"[GCS] Keeping files (KEEP_TRANSIENT_FILES=true)")
        return

    blobs = list(self.bucket.list_blobs(prefix=self.prefix))
    for blob in blobs:
        blob.delete()
```

**What it does**:
- Lists all files in `gs://bucket/transient/`
- Deletes each blob
- Skips cleanup if `KEEP_TRANSIENT_FILES=true` (for debugging)

**Why cleanup?**:
- Saves GCS storage costs
- Prevents accumulation of old files
- Keeps bucket clean

---

## 2. Data Loader Flow

**File**: `data_loader.py`

**Entry Point**: `run_pipeline()` (line 461)

### Execution Steps:

```
run_pipeline()
    │
    ├─> validate_config()                    # Validate environment variables
    │
    ├─> PostgreSQLLoader.__init__()
    │   └─> connect()                        # Connect to PostgreSQL
    │
    ├─> load_staging_to_bigquery()
    │   ├─> read_staging_data()
    │   │   └─> Loop in chunks:
    │   │       ├─> cursor.execute(SELECT)    # Read 50,000 rows
    │   │       └─> pd.DataFrame()            # Convert to DataFrame
    │   │
    │   └─> push_to_bigquery()
    │       ├─> to_gbq()                     # Pandas → BigQuery
    │       │   ├─> Create table if not exists
    │       │   ├─> Infer schema from DataFrame
    │       │   └─> Upload data
    │       │
    │       └─> Add metadata columns (loaded_at, load_date)
    │
    └─> close()                              # Close database connection
```

### Detailed Function Breakdown:

#### 2.1 `PostgreSQLLoader.connect()` (line 43)
**Purpose**: Connect to PostgreSQL (same as extractor)

```python
def connect(self):
    self.conn = psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        sslmode='prefer'
    )
```

**What it does**:
- Connects to Cloud SQL PostgreSQL in VPC
- Uses same credentials as extractor
- Reuses connection for multiple queries

---

#### 2.2 `read_staging_data()` (line 63)
**Purpose**: Read data from PostgreSQL staging table in chunks

```python
def read_staging_data(self, table_name, chunk_size=50000):
    offset = 0

    while True:
        query = f"""
        SELECT * FROM {table_name}
        ORDER BY loaded_at
        LIMIT {chunk_size} OFFSET {offset}
        """

        df = pd.read_sql(query, self.conn)

        if df.empty:
            break

        yield df  # Generator - returns one chunk at a time
        offset += chunk_size
```

**What it does**:
1. **Reads in chunks**: 50,000 rows at a time
2. **Uses LIMIT/OFFSET**: Pagination to avoid loading all data in memory
3. **Generator pattern**: Yields one chunk at a time
4. **Orders by loaded_at**: Ensures consistent ordering

**Why chunking?**:
- PostgreSQL staging table may have 500,000+ rows
- Loading all rows at once would consume too much memory
- Allows incremental BigQuery uploads

---

#### 2.3 `push_to_bigquery()` (line 95)
**Purpose**: Upload DataFrame to BigQuery

```python
def push_to_bigquery(self, df, table_id, if_exists='replace'):
    # Clean column names
    df.columns = [
        col.lower().replace(' ', '_').replace('-', '_')
        for col in df.columns
    ]

    # Remove metadata columns (will be re-added by BigQuery)
    df = df.drop(columns=['loaded_at', 'load_date'], errors='ignore')

    # Upload to BigQuery
    df.to_gbq(
        destination_table=table_id,
        project_id=Config.BQ_PROJECT,
        if_exists=if_exists,
        progress_bar=False,
        chunksize=10000
    )
```

**What it does**:
1. **Normalizes column names** (same as loader)
2. **Drops metadata columns** from PostgreSQL (BigQuery adds its own)
3. **Uploads using pandas-gbq**:
   - `if_exists='replace'`: First chunk replaces table
   - `if_exists='append'`: Subsequent chunks append
   - `chunksize=10000`: Uploads 10,000 rows per API call

**Why pandas-gbq?**:
- Automatic schema inference
- Handles data type conversion
- Simpler than BigQuery Python client
- Optimized for DataFrame uploads

---

#### 2.4 `load_staging_to_bigquery()` (line 125)
**Purpose**: Main loader logic - PostgreSQL → BigQuery

```python
def load_staging_to_bigquery(self):
    table_id = f"{Config.BQ_DATASET}.{Config.BQ_TABLE}"

    first_chunk = True
    total_rows = 0

    for chunk_df in self.read_staging_data(Config.STAGING_CDC_TABLE):
        if first_chunk:
            self.push_to_bigquery(chunk_df, table_id, if_exists='replace')
            first_chunk = False
        else:
            self.push_to_bigquery(chunk_df, table_id, if_exists='append')

        total_rows += len(chunk_df)
        logger.info(f"Loaded {total_rows:,} rows to BigQuery")
```

**What it does**:
1. **First chunk**: Replaces BigQuery table (clears old data)
2. **Subsequent chunks**: Appends to existing table
3. **Tracks progress**: Logs cumulative row count

**Why replace on first chunk?**:
- Ensures clean state (no duplicate data)
- Refreshes entire dataset daily
- Simpler than incremental updates

---

## 3. Data Validator Flow

**File**: `data_validator.py`

**Entry Point**: `main()` (line 167)

### Execution Steps:

```
main()
    │
    ├─> bigquery.Client()                    # Initialize BigQuery client
    │
    ├─> check_table_exists()
    │   └─> client.get_table(table_id)       # Check if table exists
    │
    ├─> validate_row_count()
    │   ├─> SELECT COUNT(*) FROM table
    │   └─> Compare with MIN_EXPECTED_ROWS
    │
    ├─> validate_schema()
    │   ├─> client.get_table(table_id)
    │   └─> Check required columns exist
    │
    ├─> validate_data_quality()
    │   ├─> Query data quality metrics
    │   │   ├─> COUNT(DISTINCT yearstart)
    │   │   ├─> COUNT(DISTINCT locationabbr)
    │   │   └─> COUNTIF(field IS NULL)
    │   └─> Check thresholds
    │
    └─> sys.exit(0 or 1)                     # Exit code for Airflow
```

### Detailed Function Breakdown:

#### 3.1 `check_table_exists()` (line 50)
**Purpose**: Verify BigQuery table exists

```python
def check_table_exists(client, table_id):
    try:
        client.get_table(table_id)
        logger.info(f"[PASS] Table exists: {table_id}")
        return True
    except Exception as e:
        logger.error(f"[FAIL] Table not found: {table_id} - {e}")
        return False
```

**What it does**:
- Calls BigQuery API to get table metadata
- Returns True if table exists
- Returns False and logs error if not found

**Why this check?**:
- Prevents validation on non-existent table
- Early failure detection
- Critical prerequisite for all other validations

---

#### 3.2 `validate_row_count()` (line 61)
**Purpose**: Ensure table has minimum expected rows

```python
def validate_row_count(client, table_id, min_rows):
    query = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
    result = client.query(query).result()
    rows = list(result)

    if not rows:
        logger.error(f"[FAIL] Row count check failed: No data returned")
        return False, 0

    row_count = rows[0]['row_count']

    if row_count >= min_rows:
        logger.info(f"[PASS] Row count: {row_count:,} rows (min: {min_rows:,})")
        return True, row_count
    else:
        logger.error(f"[FAIL] Row count: {row_count:,} rows (min: {min_rows:,})")
        return False, row_count
```

**What it does**:
1. Runs `SELECT COUNT(*)` query
2. Checks if result is empty (defensive programming)
3. Compares row count to `MIN_EXPECTED_ROWS` (default: 100,000)
4. Returns (pass/fail, row_count)

**Why row count validation?**:
- Detects incomplete data loads
- CDC dataset has ~500,000 rows historically
- If count < 100,000, likely a failure occurred

---

#### 3.3 `validate_schema()` (line 142)
**Purpose**: Verify table has required columns

```python
def validate_schema(client, table_id):
    table = client.get_table(table_id)
    schema_fields = [field.name for field in table.schema]

    required_fields = ['yearstart', 'yearend', 'locationabbr', 'topic', 'loaded_at', 'load_date']
    missing_fields = [field for field in required_fields if field not in schema_fields]

    if missing_fields:
        logger.error(f"[FAIL] Schema validation failed: Missing fields {missing_fields}")
        return False

    logger.info(f"[PASS] Schema validation passed: All required fields present")
    return True
```

**What it does**:
1. Gets table schema from BigQuery metadata
2. Extracts column names
3. Checks if required columns exist
4. Returns False if any are missing

**Why schema validation?**:
- Ensures downstream analytics queries won't fail
- Detects schema drift (API changes)
- Validates critical columns for dashboards

---

#### 3.4 `validate_data_quality()` (line 85)
**Purpose**: Check data quality metrics and null values

```python
def validate_data_quality(client, table_id):
    query = f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT yearstart) as distinct_years,
        COUNT(DISTINCT locationabbr) as distinct_locations,
        COUNT(DISTINCT topic) as distinct_topics,
        COUNTIF(yearstart IS NULL) as null_yearstart,
        COUNTIF(locationabbr IS NULL) as null_location,
        COUNTIF(topic IS NULL) as null_topic
    FROM `{table_id}`
    """

    result = client.query(query).result()
    rows = list(result)

    if not rows:
        logger.error(f"[FAIL] Data quality check failed: No data returned")
        return False

    stats = rows[0]

    # Log metrics
    logger.info(f"  Total rows: {stats['total_rows']:,}")
    logger.info(f"  Distinct years: {stats['distinct_years']}")
    logger.info(f"  Distinct locations: {stats['distinct_locations']}")

    # Check critical nulls
    critical_nulls = stats['null_yearstart'] + stats['null_location'] + stats['null_topic']
    if critical_nulls > 0:
        logger.warning(f"[WARN] Found {critical_nulls} rows with null critical fields")

    # Sanity checks
    if stats['distinct_years'] < 5:
        logger.error(f"[FAIL] Too few distinct years ({stats['distinct_years']})")
        return False

    if stats['distinct_locations'] < 10:
        logger.error(f"[FAIL] Too few distinct locations ({stats['distinct_locations']})")
        return False

    logger.info("[PASS] Data quality checks passed")
    return True
```

**What it does**:
1. **Aggregates metrics**:
   - Distinct years (expect 10-20 years)
   - Distinct locations (expect 50+ states/territories)
   - Distinct topics (expect 100+ health topics)
2. **Counts NULL values** in critical columns
3. **Sanity checks**:
   - At least 5 distinct years
   - At least 10 distinct locations
4. **Logs warnings** for NULL values (non-blocking)

**Why data quality checks?**:
- Detects data corruption
- Identifies API changes (fewer topics than expected)
- Validates data completeness

---

#### 3.5 `main()` - Orchestration (line 167)
**Purpose**: Run all validations and set exit code

```python
def main():
    client = bigquery.Client(project=Config.BQ_PROJECT)
    table_id = f"{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}"

    validations = []

    # 1. Check table exists (CRITICAL - stop if this fails)
    table_exists = check_table_exists(client, table_id)
    validations.append(("Table Exists", table_exists))

    if not table_exists:
        logger.error("[FAIL] Table does not exist. Skipping remaining validations.")
    else:
        # 2. Validate row count
        row_count_passed, row_count = validate_row_count(client, table_id, Config.MIN_EXPECTED_ROWS)
        validations.append(("Row Count", row_count_passed))

        # 3. Validate schema
        validations.append(("Schema", validate_schema(client, table_id)))

        # 4. Validate data quality
        validations.append(("Data Quality", validate_data_quality(client, table_id)))

    # Summary
    all_passed = True
    for validation_name, passed in validations:
        status = "[PASS]" if passed else "[FAIL]"
        logger.info(f"{validation_name:.<30} {status}")
        if not passed:
            all_passed = False

    if all_passed:
        sys.exit(0)  # Success - Airflow marks task as succeeded
    else:
        sys.exit(1)  # Failure - Airflow marks task as failed
```

**What it does**:
1. **Sequential validation**: Stops early if table doesn't exist
2. **Collects results**: Stores all validation outcomes
3. **Prints summary**: Shows pass/fail for each check
4. **Sets exit code**:
   - `0` = Success (all validations passed)
   - `1` = Failure (one or more validations failed)

**Why exit codes?**:
- Airflow uses exit codes to determine task success/failure
- Exit code 0 → Task succeeds → Continue to next task
- Exit code 1 → Task fails → Trigger failure email alert

---

## 4. Airflow Orchestration Flow

**File**: `dags/etl_dag_updt.py`

### Execution Steps:

```
Airflow Scheduler
    │
    ├─> Check schedule: "0 5 * * *" (5:00 AM UTC daily)
    │
    ├─> Create DAG instance
    │   ├─> Load Airflow Variables
    │   │   ├─> PROJECT_ID
    │   │   ├─> REGION
    │   │   └─> ALERT_EMAILS
    │   │
    │   └─> Define tasks
    │       ├─> run_extractor (CloudRunExecuteJobOperator)
    │       ├─> run_loader (CloudRunExecuteJobOperator)
    │       ├─> run_validator (CloudRunExecuteJobOperator)
    │       ├─> failure_alert (EmailOperator)
    │       └─> success_alert (EmailOperator)
    │
    ├─> Execute task: run_extractor
    │   ├─> CloudRunExecuteJobOperator.execute()
    │   ├─> Calls Cloud Run Jobs API
    │   ├─> Triggers: data-extractor job
    │   ├─> Waits for completion (polls every 10s)
    │   └─> Returns exit code
    │
    ├─> If extractor succeeds → Execute task: run_loader
    │   ├─> Triggers: data-loader job
    │   └─> Waits for completion
    │
    ├─> If loader succeeds → Execute task: run_validator
    │   ├─> Triggers: data-validator job
    │   └─> Waits for completion
    │
    ├─> Execute alert tasks (parallel)
    │   ├─> failure_alert (trigger_rule=ONE_FAILED)
    │   │   └─> Sends email if ANY task failed
    │   │
    │   └─> success_alert (trigger_rule=ALL_SUCCESS)
    │       └─> Sends email if ALL tasks succeeded
    │
    └─> Mark DAG run as SUCCESS or FAILED
```

### Detailed Function Breakdown:

#### 4.1 DAG Initialization (line 16)
**Purpose**: Define DAG configuration

```python
with DAG(
    dag_id="etl_pipeline_daily",
    default_args=default_args,
    description="ETL pipeline: extractor → loader → validator",
    schedule_interval="0 5 * * *",  # Cron: Daily at 5:00 AM UTC
    start_date=days_ago(1),
    catchup=False,  # Don't backfill historical runs
) as dag:
```

**What it does**:
- **dag_id**: Unique identifier for this DAG
- **schedule_interval**: Cron expression (5 AM UTC = 12 AM EST)
- **start_date**: When DAG becomes active
- **catchup=False**: Prevents running missed schedules

---

#### 4.2 Variable Loading (line 25)
**Purpose**: Load configuration from Airflow Variables

```python
PROJECT_ID = Variable.get("PROJECT_ID")
REGION = Variable.get("REGION")
ALERT_EMAILS = [e.strip() for e in Variable.get("ALERT_EMAILS").split(",")]
```

**What it does**:
- Reads Airflow Variables (set via Airflow UI or gcloud)
- Parses comma-separated emails into list
- Makes DAG configurable without code changes

---

#### 4.3 Task Definitions (line 29)
**Purpose**: Define Cloud Run Job execution tasks

```python
run_extractor = CloudRunExecuteJobOperator(
    task_id="run_data_extractor",
    project_id=PROJECT_ID,
    region=REGION,
    job_name="data-extractor",
)
```

**What it does**:
- Creates Airflow task
- **CloudRunExecuteJobOperator**: Executes Cloud Run Job and waits
- **job_name**: Must match Cloud Run Job name exactly
- **task_id**: Unique identifier within DAG

**How it works**:
1. Calls Cloud Run Jobs API: `jobs.run()`
2. Starts job execution
3. Polls every 10 seconds for completion
4. Returns success/failure based on job exit code

---

#### 4.4 Email Alert Tasks (line 52)
**Purpose**: Send email notifications

```python
failure_alert = EmailOperator(
    task_id="send_failure_email",
    to=ALERT_EMAILS,
    subject="ETL Pipeline Failed",
    html_content="""
        <h3> ETL Pipeline Execution Failed</h3>
        <p>Check task logs in Airflow for details.</p>
    """,
    trigger_rule=TriggerRule.ONE_FAILED,
)

success_alert = EmailOperator(
    task_id="send_success_email",
    to=ALERT_EMAILS,
    subject="ETL Pipeline Succeeded",
    html_content="<h3>All ETL steps completed successfully.</h3>",
    trigger_rule=TriggerRule.ALL_SUCCESS,
)
```

**What it does**:
- **trigger_rule=ONE_FAILED**: Runs if ANY upstream task fails
- **trigger_rule=ALL_SUCCESS**: Runs only if ALL upstream tasks succeed
- Sends HTML email via configured SMTP connection

---

#### 4.5 Task Dependencies (line 72)
**Purpose**: Define execution order

```python
# Sequential execution
run_extractor >> run_loader >> run_validator

# Connect all tasks to failure alert
[run_extractor, run_loader, run_validator] >> failure_alert

# Only validator connects to success alert
run_validator >> success_alert
```

**What it does**:
- **Sequential**: Extractor → Loader → Validator (one after another)
- **Failure alert**: Monitors all 3 tasks
  - If extractor fails → failure email sent immediately
  - If loader fails → failure email sent
  - If validator fails → failure email sent
- **Success alert**: Only runs after validator succeeds
  - Implies extractor and loader also succeeded

**Execution scenarios**:
1. **All succeed**: Extractor ✓ → Loader ✓ → Validator ✓ → Success email ✓
2. **Extractor fails**: Extractor ✗ → Loader (skipped) → Validator (skipped) → Failure email ✓
3. **Loader fails**: Extractor ✓ → Loader ✗ → Validator (skipped) → Failure email ✓
4. **Validator fails**: Extractor ✓ → Loader ✓ → Validator ✗ → Failure email ✓

---

## Function Interaction Diagram

### Complete End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AIRFLOW DAG SCHEDULER                            │
│                    Cron: 0 5 * * * (5:00 AM UTC)                        │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     TASK 1: run_data_extractor                           │
│              CloudRunExecuteJobOperator (job: data-extractor)            │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
         ┌───────────────────────────────────────────────┐
         │     data_extractor.py: run_pipeline()         │
         ├───────────────────────────────────────────────┤
         │  1. validate_config()                         │
         │     └─> Check env vars exist                  │
         │                                                │
         │  2. APIExtractor.__init__()                   │
         │     ├─> _create_session()                     │
         │     │   └─> Create HTTP session with retries  │
         │     └─> GCSTransientStorage.__init__()        │
         │         └─> Initialize GCS client              │
         │                                                │
         │  3. extract_and_chunk_to_gcs()                │
         │     ├─> session.get(CDC_API_URL)              │
         │     │   └─> Fetch ~500K rows from API         │
         │     ├─> pd.read_csv(response.text)            │
         │     │   └─> Parse CSV into DataFrame          │
         │     └─> For each 50K-row chunk:               │
         │         └─> save_chunk(df, idx)               │
         │             └─> GCS upload: cdc_chunk_0000.csv│
         │                                                │
         │  4. PostgreSQLLoader.__init__()               │
         │     └─> connect()                             │
         │         └─> psycopg2.connect(Cloud SQL)       │
         │                                                │
         │  5. load_from_gcs()                           │
         │     ├─> list_chunks()                         │
         │     │   └─> Get all chunk files from GCS      │
         │     ├─> load_chunk(first)                     │
         │     │   └─> Download CSV, parse to DataFrame  │
         │     ├─> create_staging_table(df)              │
         │     │   ├─> table_exists()                    │
         │     │   ├─> schema_matches()                  │
         │     │   │   └─> get_table_columns()           │
         │     │   └─> TRUNCATE or DROP/CREATE           │
         │     └─> For each chunk:                       │
         │         ├─> load_chunk(blob)                  │
         │         └─> load_data(df)                     │
         │             └─> execute_values(INSERT)        │
         │                 └─> Batch insert 1000 rows    │
         │                                                │
         │  6. cleanup()                                 │
         │     └─> Delete all GCS transient files        │
         │                                                │
         │  7. close()                                   │
         │     └─> conn.close()                          │
         └───────────────────────────────────────────────┘
                                 │
                                 │ Exit Code: 0 (Success)
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      TASK 2: run_data_loader                             │
│               CloudRunExecuteJobOperator (job: data-loader)              │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
         ┌───────────────────────────────────────────────┐
         │      data_loader.py: run_pipeline()           │
         ├───────────────────────────────────────────────┤
         │  1. validate_config()                         │
         │                                                │
         │  2. PostgreSQLLoader.__init__()               │
         │     └─> connect()                             │
         │                                                │
         │  3. load_staging_to_bigquery()                │
         │     ├─> read_staging_data() [GENERATOR]       │
         │     │   └─> For each 50K rows:                │
         │     │       ├─> SELECT * LIMIT 50000 OFFSET X  │
         │     │       └─> pd.read_sql() → yield df       │
         │     │                                          │
         │     └─> For each chunk:                       │
         │         └─> push_to_bigquery(df)              │
         │             ├─> Clean column names            │
         │             ├─> Drop metadata columns         │
         │             └─> df.to_gbq()                   │
         │                 ├─> First: if_exists='replace'│
         │                 └─> Rest: if_exists='append'  │
         │                                                │
         │  4. close()                                   │
         │     └─> conn.close()                          │
         └───────────────────────────────────────────────┘
                                 │
                                 │ Exit Code: 0 (Success)
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     TASK 3: run_data_validator                           │
│             CloudRunExecuteJobOperator (job: data-validator)             │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
         ┌───────────────────────────────────────────────┐
         │       data_validator.py: main()               │
         ├───────────────────────────────────────────────┤
         │  1. bigquery.Client()                         │
         │     └─> Initialize BigQuery client            │
         │                                                │
         │  2. check_table_exists()                      │
         │     └─> client.get_table(table_id)            │
         │         └─> Returns: True/False                │
         │                                                │
         │  3. If table exists:                          │
         │     ├─> validate_row_count()                  │
         │     │   ├─> SELECT COUNT(*)                   │
         │     │   └─> Check: count >= 100,000           │
         │     │                                          │
         │     ├─> validate_schema()                     │
         │     │   ├─> Get table.schema                  │
         │     │   └─> Check required columns exist      │
         │     │                                          │
         │     └─> validate_data_quality()               │
         │         ├─> SELECT aggregates:                │
         │         │   ├─> COUNT(DISTINCT yearstart)     │
         │         │   ├─> COUNT(DISTINCT locationabbr)  │
         │         │   └─> COUNTIF(field IS NULL)        │
         │         └─> Check thresholds:                 │
         │             ├─> Years >= 5                    │
         │             └─> Locations >= 10               │
         │                                                │
         │  4. Print validation summary                  │
         │     ├─> Table Exists.........[PASS]           │
         │     ├─> Row Count............[PASS]           │
         │     ├─> Schema...............[PASS]           │
         │     └─> Data Quality.........[PASS]           │
         │                                                │
         │  5. sys.exit(0 or 1)                          │
         │     ├─> 0 = All passed                        │
         │     └─> 1 = One or more failed                │
         └───────────────────────────────────────────────┘
                                 │
                                 │ Exit Code: 0 (Success)
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  EMAIL ALERT TASKS (PARALLEL)                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  failure_alert (trigger_rule=ONE_FAILED)                                │
│  ├─> Checks: Did ANY task fail?                                        │
│  ├─> If YES → Send failure email                                       │
│  └─> If NO → Skip (no email sent)                                      │
│                                                                          │
│  success_alert (trigger_rule=ALL_SUCCESS)                               │
│  ├─> Checks: Did ALL tasks succeed?                                    │
│  ├─> If YES → Send success email                                       │
│  └─> If NO → Skip (no email sent)                                      │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                         ┌───────────────┐
                         │  DAG COMPLETE │
                         │  Status: ✓    │
                         └───────────────┘
```

---

## Key Takeaways

### 1. Data Flow
```
CDC API → GCS (transient) → PostgreSQL (staging) → BigQuery (analytics)
```

### 2. Error Handling
- **API retries**: Automatic retry on 429, 500, 502, 503, 504
- **Chunking**: Prevents memory overflow and enables recovery
- **Exit codes**: Communicate success/failure to Airflow
- **Email alerts**: Notify on any task failure

### 3. Performance Optimizations
- **Batch inserts**: `execute_values()` with page_size=1000
- **Chunking**: 50,000 rows per chunk (memory-efficient)
- **Generator pattern**: Yields data incrementally (no full load)
- **Parallel processing**: Airflow can run multiple DAGs concurrently

### 4. Data Quality
- **Validation at every step**:
  - Extractor: API response validation
  - Loader: Schema inference and matching
  - Validator: Row count, schema, data quality checks
- **Defensive programming**: Empty result checks, NULL handling

### 5. Observability
- **Structured logging**: Every function logs progress
- **Cloud Logging**: All logs sent to GCP Cloud Logging
- **Exit codes**: Clear success/failure indication
- **Email alerts**: Immediate notification on issues

---

## Common Questions

**Q: Why use GCS as transient storage instead of loading directly to PostgreSQL?**
A:
- Decouples API extraction from database loading
- Allows recovery if database load fails (chunks already saved)
- Enables parallel processing (future enhancement)
- Reduces API calls (don't need to re-fetch on failure)

**Q: Why chunk data into 50,000 rows?**
A:
- Memory efficiency (don't load 500K+ rows at once)
- Progress tracking (can see incremental progress)
- Faster recovery (resume from last successful chunk)
- Prevents transaction timeout in PostgreSQL

**Q: Why TRUNCATE vs DROP/CREATE in create_staging_table()?**
A:
- TRUNCATE is 10-100x faster (doesn't recreate table structure)
- Use TRUNCATE when schema unchanged (common case)
- Use DROP/CREATE when API adds new columns (rare case)
- Automatic detection via schema_matches()

**Q: Why use exit codes instead of raising exceptions?**
A:
- Airflow uses exit codes to determine task success/failure
- Exit code 0 = success → continue to next task
- Exit code 1 = failure → trigger failure alerts
- Standard practice for containerized jobs

**Q: Why validate data quality instead of just loading?**
A:
- Detects silent failures (e.g., API returns partial data)
- Prevents bad data from reaching analytics dashboards
- Early detection of API schema changes
- Builds trust in data pipeline

---

This document provides a complete understanding of how the ETL pipeline works at the code level. Use it to explain the pipeline in interviews, documentation, or presentations.
