"""
Unit Tests for BigQuery Push Logic
Tests for data_loader.py BigQueryWriter class
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import pandas as pd
import sys
import os
from datetime import datetime

# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_loader import BigQueryWriter


class TestBigQueryWriter(unittest.TestCase):
    """Test suite for BigQuery data writing and push logic"""

    def setUp(self):
        """Set up test fixtures"""
        self.project = 'test-project'
        self.dataset = 'test_dataset'
        self.table = 'test_table'

        # Sample test DataFrame
        self.test_df = pd.DataFrame({
            'YearStart': [2015, 2016, 2017],
            'YearEnd': [2015, 2016, 2017],
            'LocationAbbr': ['US', 'CA', 'NY'],
            'Topic': ['Diabetes', 'Asthma', 'Cancer'],
            'DataValue': [9.3, 12.5, 150.2],
            'loaded_at': [datetime.now()] * 3,
            'load_date': [datetime.now().date()] * 3
        })

    def tearDown(self):
        """Clean up after tests"""
        pass

    # ================================================================
    # Test 1: BigQueryWriter Initialization
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_initialization_success(self, mock_client):
        """Test successful BigQueryWriter initialization"""
        mock_client.return_value = MagicMock()

        writer = BigQueryWriter(self.project, self.dataset, self.table)

        self.assertEqual(writer.project, self.project)
        self.assertEqual(writer.dataset, self.dataset)
        self.assertEqual(writer.table, self.table)
        self.assertEqual(writer.full_table_id, f"{self.project}.{self.dataset}.{self.table}")
        mock_client.assert_called_once_with(project=self.project)

    @patch('data_loader.bigquery.Client')
    def test_initialization_failure(self, mock_client):
        """Test handling of initialization failure"""
        mock_client.side_effect = Exception("Authentication failed")

        with self.assertRaises(Exception) as context:
            BigQueryWriter(self.project, self.dataset, self.table)

        self.assertIn("Authentication failed", str(context.exception))

    # ================================================================
    # Test 2: Write Data - Replace Mode
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_replace_mode(self, mock_client):
        """Test writing data with replace (WRITE_TRUNCATE) mode"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        # Mock the load job
        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(self.test_df, if_exists='replace')

        # Verify load_table_from_dataframe was called
        mock_bq_client.load_table_from_dataframe.assert_called_once()

        # Verify job_config has WRITE_TRUNCATE disposition
        call_args = mock_bq_client.load_table_from_dataframe.call_args
        job_config = call_args[0][2]  # Third argument is job_config

        from google.cloud import bigquery
        self.assertEqual(
            job_config.write_disposition,
            bigquery.WriteDisposition.WRITE_TRUNCATE
        )

    # ================================================================
    # Test 3: Write Data - Append Mode
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_append_mode(self, mock_client):
        """Test writing data with append (WRITE_APPEND) mode"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(self.test_df, if_exists='append')

        # Verify job_config has WRITE_APPEND disposition
        call_args = mock_bq_client.load_table_from_dataframe.call_args
        job_config = call_args[0][2]

        from google.cloud import bigquery
        self.assertEqual(
            job_config.write_disposition,
            bigquery.WriteDisposition.WRITE_APPEND
        )

    # ================================================================
    # Test 4: Write Empty DataFrame
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_empty_dataframe(self, mock_client):
        """Test writing empty DataFrame"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        empty_df = pd.DataFrame()

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(empty_df, if_exists='replace')

        # Should still attempt to write (BigQuery handles empty loads)
        mock_bq_client.load_table_from_dataframe.assert_called_once()

    # ================================================================
    # Test 5: Write Large DataFrame
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_large_dataframe(self, mock_client):
        """Test writing large DataFrame (100k rows)"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        # Create large DataFrame
        large_df = pd.DataFrame({
            'YearStart': range(100000),
            'DataValue': [i * 1.5 for i in range(100000)]
        })

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(large_df, if_exists='replace')

        # Verify successful write
        mock_bq_client.load_table_from_dataframe.assert_called_once()
        mock_job.result.assert_called_once()

    # ================================================================
    # Test 6: Schema Auto-detection
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_schema_autodetect(self, mock_client):
        """Test that schema auto-detection is enabled"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(self.test_df, if_exists='replace')

        # Verify autodetect is True in job_config
        call_args = mock_bq_client.load_table_from_dataframe.call_args
        job_config = call_args[0][2]

        self.assertTrue(job_config.autodetect)

    # ================================================================
    # Test 7: Write Failure Handling
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_failure(self, mock_client):
        """Test handling of write failures"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        # Simulate job failure
        mock_job = MagicMock()
        mock_job.result.side_effect = Exception("BigQuery load job failed")
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        writer = BigQueryWriter(self.project, self.dataset, self.table)

        with self.assertRaises(Exception) as context:
            writer.write(self.test_df, if_exists='replace')

        self.assertIn("BigQuery load job failed", str(context.exception))

    # ================================================================
    # Test 8: Write with NULL Values
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_with_null_values(self, mock_client):
        """Test writing DataFrame with NULL values"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        df_with_nulls = pd.DataFrame({
            'YearStart': [2015, 2016, None],
            'DataValue': [9.3, None, 150.2],
            'Topic': ['Diabetes', None, 'Cancer']
        })

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(df_with_nulls, if_exists='replace')

        # Should successfully write (BigQuery handles NULL)
        mock_bq_client.load_table_from_dataframe.assert_called_once()

    # ================================================================
    # Test 9: Write with Mixed Data Types
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_mixed_data_types(self, mock_client):
        """Test writing DataFrame with mixed data types"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        mixed_df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.5, 2.5, 3.5],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True],
            'date_col': pd.date_range('2020-01-01', periods=3)
        })

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(mixed_df, if_exists='replace')

        mock_bq_client.load_table_from_dataframe.assert_called_once()

    # ================================================================
    # Test 10: Table ID Format
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_table_id_format(self, mock_client):
        """Test that table ID is correctly formatted"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(self.test_df, if_exists='replace')

        # Verify table ID format in call
        call_args = mock_bq_client.load_table_from_dataframe.call_args
        table_ref = call_args[0][1]

        self.assertEqual(table_ref, f"{self.dataset}.{self.table}")

    # ================================================================
    # Test 11: Write Job Completion Wait
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_waits_for_job_completion(self, mock_client):
        """Test that write() waits for job to complete"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(self.test_df, if_exists='replace')

        # Verify job.result() was called (blocks until complete)
        mock_job.result.assert_called_once()

    # ================================================================
    # Test 12: Credential Handling
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_default_credentials_used(self, mock_client):
        """Test that default credentials are used"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        writer = BigQueryWriter(self.project, self.dataset, self.table)

        # Verify Client was initialized with project only (using default credentials)
        mock_client.assert_called_once_with(project=self.project)

    # ================================================================
    # Test 13: Row Count Validation
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_row_count_logging(self, mock_client):
        """Test that row count is logged during write"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        writer = BigQueryWriter(self.project, self.dataset, self.table)

        # Capture logs
        import logging
        with self.assertLogs(level='INFO') as log:
            writer.write(self.test_df, if_exists='replace')

        # Verify row count is mentioned in logs
        log_output = ' '.join(log.output)
        self.assertIn(str(len(self.test_df)), log_output)

    # ================================================================
    # Test 14: DataFrame with Special Characters
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_special_characters(self, mock_client):
        """Test writing DataFrame with special characters in data"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        special_df = pd.DataFrame({
            'Topic': ['Diabetes & Obesity', 'Heart Disease (CVD)', "Alzheimer's"],
            'Question': ['Rate > 50%?', 'Is BMI < 25?', 'Age 65+'],
            'DataValue': [9.3, 12.5, 150.2]
        })

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(special_df, if_exists='replace')

        mock_bq_client.load_table_from_dataframe.assert_called_once()

    # ================================================================
    # Test 15: Write with Timestamp Columns
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_with_timestamps(self, mock_client):
        """Test writing DataFrame with timestamp columns"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        timestamp_df = pd.DataFrame({
            'YearStart': [2015, 2016, 2017],
            'loaded_at': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00']),
            'load_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']).date
        })

        writer = BigQueryWriter(self.project, self.dataset, self.table)
        writer.write(timestamp_df, if_exists='replace')

        mock_bq_client.load_table_from_dataframe.assert_called_once()

    # ================================================================
    # Test 16: Connection Timeout Handling
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_timeout(self, mock_client):
        """Test handling of timeout during write"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.side_effect = TimeoutError("BigQuery job timed out")
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        writer = BigQueryWriter(self.project, self.dataset, self.table)

        with self.assertRaises(Exception):
            writer.write(self.test_df, if_exists='replace')

    # ================================================================
    # Test 17: Quota Exceeded Error
    # ================================================================

    @patch('data_loader.bigquery.Client')
    def test_write_quota_exceeded(self, mock_client):
        """Test handling of quota exceeded error"""
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client

        mock_job = MagicMock()
        mock_job.result.side_effect = Exception("Quota exceeded: BigQuery streaming insert quota")
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        writer = BigQueryWriter(self.project, self.dataset, self.table)

        with self.assertRaises(Exception) as context:
            writer.write(self.test_df, if_exists='replace')

        self.assertIn("Quota exceeded", str(context.exception))


# ================================================================
# Test Runner
# ================================================================

if __name__ == '__main__':
    unittest.main(verbosity=2)
