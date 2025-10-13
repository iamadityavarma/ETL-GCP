"""
API to PostgreSQL Staging ETL
Extracts data from CDC API and loads into PostgreSQL staging table
Designed to run daily via cron job on GCP
"""

import os
import sys
import logging
import time
from datetime import datetime
from io import StringIO

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

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


# ============================================================================
# API EXTRACTOR
# ============================================================================

class APIExtractor:
    """Extract data from CDC API with retry logic"""
    
    def __init__(self):
        self.session = self._create_session()
    
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
            logger.error(f"Error creating session: {e}")
            raise

    
    def extract_cdc_data(self):
        """
        Extract CDC Chronic Disease Indicators data
        Returns: pandas DataFrame
        """
        logger.info("Starting CDC data extraction...")
        
        try:
            response = self.session.get(
                Config.CDC_API_URL,
                timeout=Config.API_TIMEOUT,
                stream=True
            )
            response.raise_for_status()
            
            # Parse CSV
            df = pd.read_csv(StringIO(response.text))
            
            if df.empty:
                raise ValueError("CDC API returned empty dataset")
            
            logger.info(f"[SUCCESS] Successfully extracted {len(df):,} CDC records")
            return df
            
        except requests.exceptions.Timeout:
            logger.error("[ERROR] CDC API request timeout")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"[ERROR] CDC API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"[ERROR] Error processing CDC data: {e}")
            raise


# ============================================================================
# POSTGRESQL LOADER
# ============================================================================

class PostgreSQLLoader:
    """Load data into PostgreSQL staging tables"""
    
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
        Load DataFrame into PostgreSQL table using chunked bulk insert
        Optimized for low-resource database environments
        """
        logger.info(f"Loading data into {table_name}...")

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

        # Configuration for low-resource database
        chunk_size = 10000  # Process 5000 rows at a time
        page_size = 500    # Small page size for low memory
        total_rows = len(df)
        loaded_rows = 0

        try:
            # Process data in chunks
            for i in range(0, total_rows, chunk_size):
                chunk_df = df.iloc[i:i + chunk_size]
                data_tuples = [tuple(row) for row in chunk_df.values]

                # Connection recovery logic
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        # Check if connection is still alive
                        if self.conn.closed:
                            logger.info("Connection lost, reconnecting...")
                            self.connect()

                        with self.conn.cursor() as cursor:
                            execute_values(
                                cursor,
                                insert_sql,
                                data_tuples,
                                page_size=page_size
                            )
                            self.conn.commit()
                        break  # Success, exit retry loop

                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                        logger.warning(f"Connection error on attempt {attempt + 1}/{max_retries}: {e}")
                        if attempt < max_retries - 1:
                            logger.info("Retrying in 2 seconds...")
                            time.sleep(2)
                            try:
                                self.connect()  # Reconnect
                            except Exception as reconnect_error:
                                logger.error(f"Reconnection failed: {reconnect_error}")
                        else:
                            raise  # Max retries exceeded

                loaded_rows += len(chunk_df)
                logger.info(f"[PROGRESS] Loaded {loaded_rows:,}/{total_rows:,} rows ({loaded_rows/total_rows*100:.1f}%)")

                # Small delay to prevent overwhelming the database
                time.sleep(0.1)

            logger.info(f"[SUCCESS] Loaded {total_rows:,} rows into {table_name}")

        except psycopg2.Error as e:
            logger.error(f"[ERROR] Failed to load data into {table_name}: {e}")
            self.conn.rollback()
            raise


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
    """Execute complete API to staging pipeline"""
    
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info(f"API TO STAGING PIPELINE STARTED - {start_time}")
    logger.info("=" * 80)
    
    db_loader = None
    
    try:
        # Validate configuration
        validate_config()
        
        # Step 1: Extract data from CDC API
        logger.info("\n[STEP 1] Extracting data from CDC API")
        extractor = APIExtractor()
        cdc_data = extractor.extract_cdc_data()
        
        # Step 2: Connect to PostgreSQL
        logger.info("\n[STEP 2] Connecting to PostgreSQL")
        db_loader = PostgreSQLLoader()
        db_loader.connect()
        
        # Step 3: Create/Truncate staging table
        logger.info("\n[STEP 3] Preparing staging table")
        db_loader.create_staging_table(Config.STAGING_CDC_TABLE, cdc_data)
        
        # Step 4: Load data into staging
        logger.info("\n[STEP 4] Loading data into staging table")
        db_loader.load_data(Config.STAGING_CDC_TABLE, cdc_data)
        
        # Success
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"CDC records loaded: {len(cdc_data):,}")
        logger.info(f"End time: {end_time}")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"PIPELINE FAILED: {str(e)}")
        logger.error("=" * 80)
        return 1
        
    finally:
        if db_loader:
            db_loader.close()


if __name__ == "__main__":
    exit_code = run_pipeline()
    sys.exit(exit_code)