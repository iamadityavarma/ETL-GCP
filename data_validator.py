"""
Data Validation Script

Validates that data has been successfully loaded into BigQuery:
1. Checks table exists
2. Validates row count
3. Validates schema
4. Validates data quality metrics
5. Returns exit code 0 for success, 1 for failure
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd

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
    BQ_PROJECT = os.getenv('BQ_PROJECT')
    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_TABLE = os.getenv('BQ_TABLE', 'cleaned_cdc_chronic_disease')
    MIN_EXPECTED_ROWS = int(os.getenv('MIN_EXPECTED_ROWS', '100000'))


# ================================================================
# Validation Functions
# ================================================================

def check_table_exists(client, table_id):
    """Check if the BigQuery table exists"""
    try:
        client.get_table(table_id)
        logger.info(f"[PASS] Table exists: {table_id}")
        return True
    except Exception as e:
        logger.error(f"[FAIL] Table not found: {table_id} - {e}")
        return False


def validate_row_count(client, table_id, min_rows):
    """Validate that the table has minimum expected rows"""
    try:
        query = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
        result = client.query(query).result()
        rows = list(result)

        if not rows:
            logger.error(f"[FAIL] Row count check failed: No data returned")
            return False, 0

        row_count = rows[0]['row_count']

        if row_count >= min_rows:
            logger.info(f"[PASS] Row count validation passed: {row_count:,} rows (min: {min_rows:,})")
            return True, row_count
        else:
            logger.error(f"[FAIL] Row count validation failed: {row_count:,} rows (min: {min_rows:,})")
            return False, row_count
    except Exception as e:
        logger.error(f"[FAIL] Row count check failed: {e}")
        return False, 0


def validate_data_quality(client, table_id):
    """Perform data quality checks"""
    try:
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

        logger.info("Data Quality Metrics:")
        logger.info(f"  Total rows: {stats['total_rows']:,}")
        logger.info(f"  Distinct years: {stats['distinct_years']}")
        logger.info(f"  Distinct locations: {stats['distinct_locations']}")
        logger.info(f"  Distinct topics: {stats['distinct_topics']}")

        # Check for critical nulls
        critical_nulls = stats['null_yearstart'] + stats['null_location'] + stats['null_topic']

        if critical_nulls > 0:
            logger.warning(f"[WARN] Found {critical_nulls} rows with null critical fields")
            logger.warning(f"  - Null yearstart: {stats['null_yearstart']}")
            logger.warning(f"  - Null locationabbr: {stats['null_location']}")
            logger.warning(f"  - Null topic: {stats['null_topic']}")
        else:
            logger.info("[PASS] No null values in critical fields")

        # Basic sanity checks
        if stats['distinct_years'] < 5:
            logger.error(f"[FAIL] Data quality issue: Too few distinct years ({stats['distinct_years']})")
            return False

        if stats['distinct_locations'] < 10:
            logger.error(f"[FAIL] Data quality issue: Too few distinct locations ({stats['distinct_locations']})")
            return False

        logger.info("[PASS] Data quality checks passed")
        return True

    except Exception as e:
        logger.error(f"[FAIL] Data quality check failed: {e}")
        return False


def validate_schema(client, table_id):
    """Validate table schema has expected columns"""
    try:
        table = client.get_table(table_id)
        schema_fields = [field.name for field in table.schema]

        required_fields = ['yearstart', 'yearend', 'locationabbr', 'topic', 'loaded_at', 'load_date']
        missing_fields = [field for field in required_fields if field not in schema_fields]

        if missing_fields:
            logger.error(f"[FAIL] Schema validation failed: Missing fields {missing_fields}")
            return False

        logger.info(f"[PASS] Schema validation passed: All required fields present")
        return True

    except Exception as e:
        logger.error(f"[FAIL] Schema validation failed: {e}")
        return False


# ================================================================
# Main Validation Pipeline
# ================================================================

def main():
    logger.info("Starting BigQuery data validation...")

    # Initialize BigQuery client
    try:
        client = bigquery.Client(project=Config.BQ_PROJECT)
        logger.info(f"Connected to BigQuery project: {Config.BQ_PROJECT}")
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        sys.exit(1)

    table_id = f"{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}"

    # Run all validations
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
    logger.info("\n" + "="*60)
    logger.info("VALIDATION SUMMARY")
    logger.info("="*60)

    all_passed = True
    for validation_name, passed in validations:
        status = "[PASS]" if passed else "[FAIL]"
        logger.info(f"{validation_name:.<30} {status}")
        if not passed:
            all_passed = False

    logger.info("="*60)

    if all_passed:
        logger.info("All validations passed!")
        sys.exit(0)
    else:
        logger.error("Some validations failed. Please investigate.")
        sys.exit(1)


if __name__ == "__main__":
    main()
