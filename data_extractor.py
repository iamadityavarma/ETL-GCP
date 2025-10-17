"""
API to GCS ETL
Extracts data from CDC API and loads chunks into GCS bucket
Designed to run daily via cron job on GCP
"""

import os
import sys
import logging
from datetime import datetime
from io import StringIO

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from google.cloud import storage

# Load environment variables from .env file
load_dotenv()

# Configure logging - stdout only for GCP Cloud Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration from environment variables"""

    # Database
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')

    # API endpoints
    CDC_API_URL = os.getenv('CDC_API_URL')

    # Table names
    STAGING_CDC_TABLE = os.getenv('STAGING_CDC_TABLE', 'staging.staging_cdc_chronic_disease')

    # API settings
    API_TIMEOUT = 180
    API_RETRY_ATTEMPTS = 3
    API_CHUNK_SIZE = int(os.getenv('API_CHUNK_SIZE', '50000'))  # Rows per chunk

    # GCS Transient Storage
    GCS_BUCKET = os.getenv('GCS_TRANSIENT_BUCKET')  # e.g., 'my-etl-bucket'
    GCS_PREFIX = os.getenv('GCS_TRANSIENT_PREFIX', 'transient/')  # e.g., 'transient/'
    TRANSIENT_FILE_PREFIX = 'cdc_chunk'
    KEEP_TRANSIENT_FILES = os.getenv('KEEP_TRANSIENT_FILES', 'false').lower() == 'true'


# ============================================================================
# GCS TRANSIENT STORAGE
# ============================================================================

class GCSTransientStorage:
    """
    Simple GCS storage for transient chunk files.
    Stores chunks in GCS, loads them one at a time to PostgreSQL.
    """

    def __init__(self):
        if not Config.GCS_BUCKET:
            raise ValueError("GCS_BUCKET environment variable required")

        self.bucket_name = Config.GCS_BUCKET
        self.prefix = Config.GCS_PREFIX
        if self.prefix and not self.prefix.endswith('/'):
            self.prefix += '/'

        try:
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(self.bucket_name)
            logger.info(f"[GCS] Using bucket: gs://{self.bucket_name}/{self.prefix}")
        except Exception as e:
            logger.error(f"[GCS] Failed to initialize: {e}")
            raise

    def save_chunk(self, df, chunk_index):
        """Save DataFrame chunk to GCS"""
        filename = f"{Config.TRANSIENT_FILE_PREFIX}_{chunk_index:04d}.csv"
        blob_path = f"{self.prefix}{filename}"

        try:
            csv_string = df.to_csv(index=False)
            blob = self.bucket.blob(blob_path)
            blob.upload_from_string(csv_string, content_type='text/csv')
            logger.info(f"[GCS] Saved chunk {chunk_index} ({len(df):,} rows) → gs://{self.bucket_name}/{blob_path}")
            return blob_path
        except Exception as e:
            logger.error(f"[GCS] Failed to save chunk {chunk_index}: {e}")
            raise

    def load_chunk(self, blob_path):
        """Load DataFrame from GCS"""
        try:
            blob = self.bucket.blob(blob_path)
            csv_string = blob.download_as_text()
            df = pd.read_csv(StringIO(csv_string))
            logger.info(f"[GCS] Loaded {len(df):,} rows from gs://{self.bucket_name}/{blob_path}")
            return df
        except Exception as e:
            logger.error(f"[GCS] Failed to load chunk: {e}")
            raise

    def list_chunks(self):
        """List all chunk blobs sorted by index"""
        try:
            blobs = list(self.bucket.list_blobs(prefix=self.prefix))
            chunk_blobs = [
                blob.name for blob in blobs
                if Config.TRANSIENT_FILE_PREFIX in blob.name and blob.name.endswith('.csv')
            ]
            chunk_blobs.sort()
            logger.info(f"[GCS] Found {len(chunk_blobs)} chunk(s)")
            return chunk_blobs
        except Exception as e:
            logger.error(f"[GCS] Failed to list chunks: {e}")
            raise

    def cleanup(self):
        """Delete all transient files from GCS"""
        if Config.KEEP_TRANSIENT_FILES:
            logger.info(f"[GCS] Keeping files (KEEP_TRANSIENT_FILES=true)")
            return

        try:
            blobs = list(self.bucket.list_blobs(prefix=self.prefix))
            for blob in blobs:
                blob.delete()
            logger.info(f"[GCS] Deleted {len(blobs)} file(s)")
        except Exception as e:
            logger.warning(f"[GCS] Cleanup failed: {e}")


# ============================================================================
# API EXTRACTOR
# ============================================================================

class APIExtractor:
    """Extract data from CDC API, chunk it, and save to GCS"""

    def __init__(self):
        self.session = self._create_session()
        self.gcs_storage = GCSTransientStorage()

    def _create_session(self):
        """Create requests session with retry strategy"""
        try:
            session = requests.Session()
            retry_strategy = Retry(
                total=Config.API_RETRY_ATTEMPTS,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            return session
        except Exception as e:
            logger.error(f"[ERROR] Failed to create session: {e}")
            raise

    def extract_and_chunk_to_gcs(self):
        """
        Extract API data, chunk it, save to GCS.
        Simple and straightforward.
        """
        logger.info("[STEP 1] Extracting data from CDC API...")

        try:
            # Get data from API
            response = self.session.get(
                Config.CDC_API_URL,
                timeout=Config.API_TIMEOUT,
                stream=True
            )
            response.raise_for_status()

            df_full = pd.read_csv(StringIO(response.text))

            if df_full.empty:
                raise ValueError("API returned empty dataset")

            total_rows = len(df_full)
            logger.info(f"[API] Extracted {total_rows:,} rows")

            # Chunk and save to GCS
            chunk_size = Config.API_CHUNK_SIZE
            num_chunks = (total_rows + chunk_size - 1) // chunk_size

            logger.info(f"[CHUNKING] Splitting into {num_chunks} chunk(s) of {chunk_size:,} rows")

            for idx in range(num_chunks):
                start = idx * chunk_size
                end = min(start + chunk_size, total_rows)
                df_chunk = df_full.iloc[start:end]

                self.gcs_storage.save_chunk(df_chunk, idx)
                logger.info(f"[PROGRESS] Chunk {idx + 1}/{num_chunks} saved")

            logger.info(f"[SUCCESS] All {num_chunks} chunk(s) saved to GCS")
            return self.gcs_storage

        except requests.exceptions.Timeout:
            logger.error("[ERROR] API timeout")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"[ERROR] API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"[ERROR] Extraction failed: {e}")
            raise


# ============================================================================
# POSTGRESQL LOADER
# ============================================================================

class PostgreSQLLoader:
    """Load data into PostgreSQL staging tables from DataFrames or transient files"""

    def __init__(self):
        self.conn = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                database=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                sslmode='prefer'
            )
            logger.info("[SUCCESS] Connected to PostgreSQL database")
            
        except psycopg2.Error as e:
            logger.error(f"[ERROR] Database connection failed: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def table_exists(self, table_name):
        """Check if table exists in database"""
        check_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(check_sql, (table_name,))
                return cursor.fetchone()[0]
        except psycopg2.Error:
            return False
    
    def get_table_columns(self, table_name):
        """Get existing table column names and types"""
        columns_sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(columns_sql, (table_name,))
                return {row[0]: row[1] for row in cursor.fetchall()}
        except psycopg2.Error:
            return {}
    
    def schema_matches(self, table_name, df):
        """Check if existing table schema matches incoming DataFrame"""
        existing_cols = self.get_table_columns(table_name)
        
        if not existing_cols:
            return False
        
        # Clean DataFrame column names
        df_cols = {col.lower().replace(' ', '_').replace('-', '_').replace('.', '_') 
                   for col in df.columns}
        
        # Ignore metadata columns
        existing_cols_set = {col for col in existing_cols.keys() 
                            if col not in ('loaded_at', 'load_date')}
        
        # Schema matches if columns are the same
        return df_cols == existing_cols_set
    
    def create_staging_table(self, table_name, df):
        """
        Smart table creation:
        - If table exists and schema matches: TRUNCATE (fast)
        - If table doesn't exist or schema changed: DROP and CREATE (flexible)
        """
        
        if self.table_exists(table_name):
            if self.schema_matches(table_name, df):
                logger.info(f"Table {table_name} exists with matching schema - using TRUNCATE")
                try:
                    with self.conn.cursor() as cursor:
                        cursor.execute(f"TRUNCATE TABLE {table_name};")
                        self.conn.commit()
                    logger.info(f"[SUCCESS] Truncated table: {table_name}")
                    return
                except psycopg2.Error as e:
                    logger.error(f"[ERROR] Failed to truncate table {table_name}: {e}")
                    self.conn.rollback()
                    raise
            else:
                logger.info(f"Table {table_name} schema changed - recreating table")
        else:
            logger.info(f"Creating new table: {table_name}")
        
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
            # Clean column name
            clean_col = col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            pg_type = type_mapping.get(str(df[col].dtype), 'TEXT')
            columns.append(f'"{clean_col}" {pg_type}')
        
        # Add metadata columns
        columns.append('loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        columns.append('load_date DATE DEFAULT CURRENT_DATE')
        
        # Create table SQL
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
            logger.info(f"[SUCCESS] Created table: {table_name}")
            
        except psycopg2.Error as e:
            logger.error(f"[ERROR] Failed to create table {table_name}: {e}")
            self.conn.rollback()
            raise
    
    def load_data(self, table_name, df):
        """
        Load DataFrame into PostgreSQL table
        No additional chunking - loads entire chunk from GCS at once
        """
        logger.info(f"Loading {len(df):,} rows into {table_name}...")

        # Clean column names to match table
        df.columns = [
            col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            for col in df.columns
        ]

        # Replace NaN with None for proper NULL handling
        df = df.where(pd.notna(df), None)

        # Build INSERT query
        columns = ', '.join([f'"{col}"' for col in df.columns])
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES %s"

        # Convert DataFrame to tuples
        data_tuples = [tuple(row) for row in df.values]

        try:
            with self.conn.cursor() as cursor:
                execute_values(cursor, insert_sql, data_tuples, page_size=1000)
                self.conn.commit()

            logger.info(f"[SUCCESS] Loaded {len(df):,} rows into {table_name}")

        except psycopg2.Error as e:
            logger.error(f"[ERROR] Failed to load data into {table_name}: {e}")
            self.conn.rollback()
            raise

    def load_from_gcs(self, table_name, gcs_storage):
        """
        Load chunks from GCS directly to PostgreSQL.
        Simple: Read chunk → Load to PG → Next chunk
        """
        logger.info(f"[STEP 2] Loading chunks from GCS to {table_name}")

        chunk_blobs = gcs_storage.list_chunks()
        total_chunks = len(chunk_blobs)

        if total_chunks == 0:
            logger.warning("[WARNING] No chunks found in GCS")
            return

        # Create table from first chunk
        first_chunk = gcs_storage.load_chunk(chunk_blobs[0])
        self.create_staging_table(table_name, first_chunk)

        # Load each chunk
        total_rows = 0
        for idx, blob_path in enumerate(chunk_blobs):
            logger.info(f"[LOADING] Chunk {idx + 1}/{total_chunks}")

            df = gcs_storage.load_chunk(blob_path)
            self.load_data(table_name, df)

            total_rows += len(df)
            logger.info(f"[PROGRESS] {total_rows:,} rows loaded")

        logger.info(f"[SUCCESS] Loaded {total_rows:,} total rows from {total_chunks} chunk(s)")


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def validate_config():
    """Validate required environment variables"""
    required = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing = [var for var in required if not os.getenv(var)]
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    logger.info("[SUCCESS] Configuration validated")


def run_pipeline():
    """
    Simple ETL Pipeline:
    1. Extract API data → Chunk → Save to GCS
    2. Load GCS chunks → PostgreSQL (one chunk at a time)
    3. Cleanup GCS
    """

    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info(f"ETL PIPELINE STARTED - {start_time}")
    logger.info("=" * 80)

    db_loader = None
    gcs_storage = None

    try:
        validate_config()

        # Extract API → Chunk → GCS
        extractor = APIExtractor()
        gcs_storage = extractor.extract_and_chunk_to_gcs()

        # Load GCS chunks → PostgreSQL
        db_loader = PostgreSQLLoader()
        db_loader.connect()
        db_loader.load_from_gcs(Config.STAGING_CDC_TABLE, gcs_storage)

        # Cleanup GCS
        gcs_storage.cleanup()

        # Success
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETED")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"PIPELINE FAILED: {str(e)}")
        logger.error("=" * 80)

        if gcs_storage:
            logger.info("[INFO] GCS files kept for debugging")

        return 1

    finally:
        if db_loader:
            db_loader.close()


if __name__ == "__main__":
    exit_code = run_pipeline()
    sys.exit(exit_code)