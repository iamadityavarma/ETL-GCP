
"""
Data Preprocessor Script

This script performs the following steps:
1. Connects to a PostgreSQL database using credentials from environment variables.
2. Cleans and normalizes data in a staging table using SQL (removes duplicates, trims whitespace).
3. Loads the cleaned data into a pandas DataFrame for further processing.
4. Uses pandas to handle date formatting, null values, and standardizes categorical fields.
5. Writes the fully cleaned data to a new table in the database.
"""

# ================================================================
# CDC Chronic Disease ETL Pipeline with Schema Validation
# ================================================================

# Standard library imports
import os
import sys
import logging
from datetime import datetime

# Third-party imports
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas_gbq

# ================================================================
# Environment & Logging Configuration
# ================================================================

# Load .env file if it exists (for local development only)
# In production (GCP), environment variables come from Secret Manager or Cloud Run/GKE config
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ================================================================
# Configuration
# ================================================================

class Config:
    DB_HOST = os.getenv('DB_HOST')
    DB_HOST_PUBLIC = os.getenv('DB_HOST_PUBLIC')  # Fallback public IP
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    STAGING_CDC_TABLE = os.getenv('STAGING_CDC_TABLE', 'staging.staging_cdc_chronic_disease')

    BQ_PROJECT = os.getenv('BQ_PROJECT')
    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_TABLE = os.getenv('BQ_TABLE', 'cleaned_cdc_chronic_disease')


# ================================================================
# PostgreSQL Connector
# ================================================================

class PostgreSQLConnector:
    def __init__(self):
        self.conn = None

    def connect(self):
        """Try connecting to private IP first, fallback to public IP if it fails"""
        hosts_to_try = [Config.DB_HOST]
        if Config.DB_HOST_PUBLIC:
            hosts_to_try.append(Config.DB_HOST_PUBLIC)

        last_error = None
        for host in hosts_to_try:
            try:
                logger.info(f"Attempting to connect to PostgreSQL at {host}:{Config.DB_PORT}")
                self.conn = psycopg2.connect(
                    host=host,
                    port=Config.DB_PORT,
                    database=Config.DB_NAME,
                    user=Config.DB_USER,
                    password=Config.DB_PASSWORD,
                    connect_timeout=10
                )
                logger.info(f"Successfully connected to PostgreSQL database at {host}")
                return
            except psycopg2.OperationalError as e:
                last_error = e
                logger.warning(f"Failed to connect to {host}: {e}")
                continue

        # If we get here, all connection attempts failed
        logger.error("Failed to connect to database using all available hosts")
        raise last_error

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def fetch_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, self.conn)


# ================================================================
# Schema Validator
# ================================================================

class SchemaValidator:
    """
    Automatically infers PostgreSQL-compatible schema from a pandas DataFrame
    and validates compatibility with an existing database table.
    """

    PANDAS_TO_PG = {
        "object": "TEXT",
        "string": "TEXT",
        "int64": "BIGINT",
        "Int64": "BIGINT",
        "float64": "DOUBLE PRECISION",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "category": "TEXT",
    }

    def infer_schema(self, df: pd.DataFrame) -> dict:
        inferred = {}
        for col, dtype in df.dtypes.items():
            dtype_str = str(dtype)
            pg_type = self.PANDAS_TO_PG.get(dtype_str, "TEXT")
            inferred[col] = pg_type
        return inferred

    def validate_existing_table(self, conn, table_name: str, inferred_schema: dict):
        """Compare inferred schema to existing table and log mismatches"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                    """,
                    (table_name.split('.')[-1],),
                )
                existing = dict(cursor.fetchall())

            if not existing:
                logger.info(f"No existing table schema found for {table_name}.")
                return False

            mismatches = []
            for col, dtype in inferred_schema.items():
                if col not in existing:
                    mismatches.append(f"Missing column: {col}")
                elif existing[col].upper() != dtype.upper():
                    mismatches.append(f"Type mismatch for {col}: DB={existing[col]}, DF={dtype}")

            if mismatches:
                logger.warning(f"Schema mismatches detected for {table_name}:")
                for m in mismatches:
                    logger.warning(f" - {m}")
                return False

            logger.info(f"Existing schema matches inferred DataFrame schema.")
            return True

        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return False


# ================================================================
# PostgreSQL Loader (Staging Table Manager)
# ================================================================

class PostgreSQLLoader:
    def __init__(self, conn):
        self.conn = conn

    def table_exists(self, table_name):
        schema, tbl = table_name.split('.') if '.' in table_name else ('public', table_name)
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                );
            """, (schema, tbl))
            return cursor.fetchone()[0]

    def create_staging_table(self, table_name, df):
        """
        Smart table creation:
        - If table exists and schema matches inferred schema: TRUNCATE (fast)
        - If table doesn't exist or schema changed: DROP and CREATE (flexible)
        """
        validator = SchemaValidator()
        inferred_schema = validator.infer_schema(df)

        if self.table_exists(table_name):
            if validator.validate_existing_table(self.conn, table_name, inferred_schema):
                logger.info(f"Table {table_name} exists with matching schema — using TRUNCATE")
                try:
                    with self.conn.cursor() as cursor:
                        cursor.execute(f"TRUNCATE TABLE {table_name};")
                        self.conn.commit()
                    logger.info(f"Truncated table: {table_name}")
                    return
                except psycopg2.Error as e:
                    logger.error(f"Failed to truncate table {table_name}: {e}")
                    self.conn.rollback()
                    raise
            else:
                logger.info(f"Table {table_name} schema changed — recreating table")
        else:
            logger.info(f"Creating new table: {table_name}")

        # Build CREATE TABLE SQL
        columns = [f'"{col}" {pg_type}' for col, pg_type in inferred_schema.items()]
        columns.append('loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        columns.append('load_date DATE DEFAULT CURRENT_DATE')

        create_sql = f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
            {', '.join(columns)}
        );
        """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_sql)
                self.conn.commit()
            logger.info(f"Created table: {table_name}")
        except psycopg2.Error as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            self.conn.rollback()
            raise

# ================================================================
# BigQuery Writer
# ================================================================

class BigQueryWriter:
    def __init__(self, project, dataset, table):
        self.project = project
        self.dataset = dataset
        self.table = table
        self.full_table_id = f"{project}.{dataset}.{table}"

        # Initialize BigQuery client using default credentials (works in production)
        # This automatically uses:
        # - Workload Identity (GKE)
        # - Service Account attached to Cloud Run/Compute Engine
        # - GOOGLE_APPLICATION_CREDENTIALS env var if set
        try:
            self.client = bigquery.Client(project=project)
            logger.info("BigQuery client initialized with default credentials")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise

    def write(self, df, if_exists='replace'):
        logger.info(f"Writing {len(df)} rows to BigQuery table: {self.full_table_id}")

        # Use BigQuery client directly (no browser auth needed)
        job_config = bigquery.LoadJobConfig(
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if if_exists == 'replace'
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
            autodetect=True  # Auto-detect schema from DataFrame
        )

        try:
            job = self.client.load_table_from_dataframe(
                df,
                f"{self.dataset}.{self.table}",
                job_config=job_config
            )
            job.result()  # Wait for the job to complete
            logger.info(f"Successfully wrote {len(df)} rows to BigQuery table: {self.full_table_id}")
        except Exception as e:
            logger.error(f"Failed to write to BigQuery: {e}")
            raise


# ================================================================
# Cleaning Functions
# ================================================================

def clean_and_normalize_sql(conn, table_name):
    with conn.cursor() as cursor:
        # Remove duplicates using ctid
        cursor.execute(f"""
            DELETE FROM {table_name}
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM {table_name}
                GROUP BY ({table_name}.*)
            );
        """)
        cursor.execute(f"""
            DO $$
            DECLARE r RECORD;
            BEGIN
                FOR r IN SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name.split('.')[-1]}' AND data_type = 'text' LOOP
                    EXECUTE 'UPDATE {table_name} SET ' || r.column_name || ' = TRIM(' || r.column_name || ')';
                END LOOP;
            END$$;
        """)
        conn.commit()
    logger.info("SQL cleaning done.")


def pandas_cleaning(df):
    for col in df.columns:
        if 'date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col] = df[col].fillna(0)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('Unknown').str.lower().str.strip()
    return df

# ================================================================
# Data Validation Layer
# ================================================================

def validate_data(df):
    """
    Perform automated data validation checks and log anomalies:
    - Logical consistency (e.g., yearstart <= yearend)
    - Range validation (e.g., datavalue between 0–100)
    - Null checks for critical columns
    - Reports validation summary at the end
    """
    issues = []

    # 1. Logical Consistency
    if {'yearstart', 'yearend'}.issubset(df.columns):
        invalid_years = df[df['yearstart'] > df['yearend']]
        if not invalid_years.empty:
            issues.append(f"{len(invalid_years)} rows have yearstart > yearend")

    # 2. Numeric Range Checks
    if 'datavalue' in df.columns:
        invalid_values = df[(df['datavalue'] < 0) | (df['datavalue'] > 100)]
        if not invalid_values.empty:
            issues.append(f"{len(invalid_values)} rows have datavalue outside [0,100]")

    # 3. Missing Critical Fields
    critical_fields = ['yearstart', 'yearend', 'locationabbr', 'topic']
    missing_critical = {col: df[col].isna().sum() for col in critical_fields if col in df.columns and df[col].isna().sum() > 0}
    if missing_critical:
        for col, count in missing_critical.items():
            issues.append(f"{count} missing values in {col}")

    # 4. Duplicates (optional extra check)
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        issues.append(f"{duplicate_count} duplicate rows detected")

    # 5. Validation Summary
    if issues:
        logger.warning("Data validation issues found:")
        for issue in issues:
            logger.warning(f" - {issue}")

        # Save anomalies to CSV for audit
        anomaly_log = "validation_issues_log.csv"
        df_invalid = pd.concat([invalid_years, invalid_values]).drop_duplicates() if ('invalid_years' in locals() or 'invalid_values' in locals()) else pd.DataFrame()
        if not df_invalid.empty:
            df_invalid.to_csv(anomaly_log, index=False)
            logger.info(f"Validation anomalies saved to {anomaly_log}")
    else:
        logger.info("Data validation passed — no issues found.")

    return df



# ================================================================
# Main Pipeline
# ================================================================

def main():
    logger.info("Starting CDC ETL pipeline ...")

    db = PostgreSQLConnector()
    db.connect()
    loader = PostgreSQLLoader(db.conn)

    try:
        clean_and_normalize_sql(db.conn, Config.STAGING_CDC_TABLE)

        with db.conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {Config.STAGING_CDC_TABLE}")
            total_rows = cursor.fetchone()[0]
        logger.info(f"Total rows in staging table: {total_rows}")

        bq_writer = BigQueryWriter(Config.BQ_PROJECT, Config.BQ_DATASET, Config.BQ_TABLE)
        chunk_size = 10000
        offset = 0
        first_chunk = True

        while offset < total_rows:
            logger.info(f"Processing rows {offset+1} to {min(offset+chunk_size, total_rows)}...")
            df_chunk = pd.read_sql(
                f"SELECT * FROM {Config.STAGING_CDC_TABLE} OFFSET {offset} LIMIT {chunk_size}",
                db.conn
            )
            df_clean = pandas_cleaning(df_chunk)
            df_valid = validate_data(df_clean)

            # Add fresh BigQuery timestamps
            df_valid['loaded_at'] = pd.Timestamp.now()
            df_valid['load_date'] = pd.Timestamp.now().date()

            if first_chunk:
                bq_writer.write(df_valid, if_exists='replace')
                first_chunk = False
            else:
                bq_writer.write(df_valid, if_exists='append')

            offset += chunk_size

        logger.info("Data preprocessing and BigQuery load completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
