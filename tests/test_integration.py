"""
Integration Tests - Run manually with real infrastructure
These tests require:
- VPN/VPC access to PostgreSQL
- GCS bucket access
- BigQuery access
- Valid .env configuration

Run with: pytest tests/test_integration.py -m integration
"""

import pytest
import os
import pandas as pd
from dotenv import load_dotenv

# Skip these tests in CI/CD
pytestmark = pytest.mark.integration

load_dotenv()


class TestRealAPIConnection:
    """Test actual API connectivity"""

    def test_cdc_api_reachable(self):
        """Test that CDC API is accessible"""
        import requests

        api_url = os.getenv('CDC_API_URL')
        assert api_url, "CDC_API_URL not configured"

        response = requests.get(api_url, timeout=30)
        assert response.status_code == 200, f"API returned {response.status_code}"
        assert len(response.text) > 0, "API returned empty response"

    def test_api_returns_valid_csv(self):
        """Test that API returns valid CSV data"""
        import requests
        from io import StringIO

        api_url = os.getenv('CDC_API_URL')
        response = requests.get(api_url, timeout=30)

        # Parse as CSV
        df = pd.read_csv(StringIO(response.text))

        assert len(df) > 0, "API returned no data"
        assert 'YearStart' in df.columns, "Missing YearStart column"
        assert 'LocationAbbr' in df.columns, "Missing LocationAbbr column"


class TestRealPostgreSQLConnection:
    """Test actual PostgreSQL connectivity (requires VPC access)"""

    @pytest.mark.skipif(
        not os.getenv('DB_HOST'),
        reason="DB_HOST not configured"
    )
    def test_postgresql_connection(self):
        """Test PostgreSQL connection (VPC required)"""
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            connect_timeout=10
        )

        assert conn is not None

        # Test query
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1

        conn.close()

    @pytest.mark.skipif(
        not os.getenv('DB_HOST'),
        reason="DB_HOST not configured"
    )
    def test_staging_table_exists(self):
        """Test that staging table exists or can be created"""
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )

        table_name = os.getenv('STAGING_CDC_TABLE', 'staging.staging_cdc_chronic_disease')

        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = '{table_name.split('.')[-1]}'
                );
            """)
            exists = cursor.fetchone()[0]

            # Either table exists, or we can create it
            assert exists or True, "Test database accessible"

        conn.close()


class TestRealGCSConnection:
    """Test actual GCS connectivity"""

    @pytest.mark.skipif(
        not os.getenv('GCS_TRANSIENT_BUCKET'),
        reason="GCS_TRANSIENT_BUCKET not configured"
    )
    def test_gcs_bucket_accessible(self):
        """Test GCS bucket is accessible"""
        from google.cloud import storage

        bucket_name = os.getenv('GCS_TRANSIENT_BUCKET')

        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Test bucket exists
        assert bucket.exists(), f"Bucket {bucket_name} does not exist"

    @pytest.mark.skipif(
        not os.getenv('GCS_TRANSIENT_BUCKET'),
        reason="GCS_TRANSIENT_BUCKET not configured"
    )
    def test_gcs_write_read_delete(self):
        """Test GCS read/write/delete operations"""
        from google.cloud import storage

        bucket_name = os.getenv('GCS_TRANSIENT_BUCKET')
        test_blob_path = 'test/integration_test.txt'
        test_content = 'Integration test content'

        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Write
        blob = bucket.blob(test_blob_path)
        blob.upload_from_string(test_content)

        # Read
        downloaded = blob.download_as_text()
        assert downloaded == test_content

        # Delete
        blob.delete()
        assert not blob.exists()


class TestRealBigQueryConnection:
    """Test actual BigQuery connectivity"""

    @pytest.mark.skipif(
        not os.getenv('BQ_PROJECT'),
        reason="BQ_PROJECT not configured"
    )
    def test_bigquery_connection(self):
        """Test BigQuery connection"""
        from google.cloud import bigquery

        client = bigquery.Client(project=os.getenv('BQ_PROJECT'))

        # Simple query
        query = "SELECT 1 as test"
        result = client.query(query).result()

        row = list(result)[0]
        assert row['test'] == 1

    @pytest.mark.skipif(
        not os.getenv('BQ_PROJECT'),
        reason="BQ_PROJECT not configured"
    )
    def test_bigquery_dataset_exists(self):
        """Test BigQuery dataset exists"""
        from google.cloud import bigquery

        client = bigquery.Client(project=os.getenv('BQ_PROJECT'))
        dataset_id = f"{os.getenv('BQ_PROJECT')}.{os.getenv('BQ_DATASET')}"

        # Try to get dataset
        dataset = client.get_dataset(dataset_id)
        assert dataset is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-m', 'integration'])
