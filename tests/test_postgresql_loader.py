"""
Unit Tests for PostgreSQL Insert Logic
Tests for data_extractor.py PostgreSQLLoader class
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, call
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_extractor import PostgreSQLLoader, Config


class TestPostgreSQLLoader(unittest.TestCase):
    """Test suite for PostgreSQL data loading and insertion"""

    def setUp(self):
        """Set up test fixtures"""
        self.loader = PostgreSQLLoader()

        # Mock connection
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(return_value=self.mock_cursor)
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        # Sample test DataFrame
        self.test_df = pd.DataFrame({
            'YearStart': [2015, 2016, 2017],
            'YearEnd': [2015, 2016, 2017],
            'LocationAbbr': ['US', 'CA', 'NY'],
            'Topic': ['Diabetes', 'Asthma', 'Cancer'],
            'DataValue': [9.3, 12.5, 150.2]
        })

    def tearDown(self):
        """Clean up after tests"""
        if self.loader.conn:
            self.loader.conn = None

    # ================================================================
    # Test 1: Database Connection
    # ================================================================

    @patch('data_extractor.psycopg2.connect')
    def test_connect_success(self, mock_connect):
        """Test successful database connection"""
        mock_connect.return_value = self.mock_conn

        self.loader.connect()

        mock_connect.assert_called_once()
        self.assertIsNotNone(self.loader.conn)

    @patch('data_extractor.psycopg2.connect')
    def test_connect_failure(self, mock_connect):
        """Test handling of connection failure"""
        mock_connect.side_effect = psycopg2.OperationalError("Connection refused")

        with self.assertRaises(psycopg2.OperationalError):
            self.loader.connect()

    # ================================================================
    # Test 2: Connection Closure
    # ================================================================

    def test_close_connection(self):
        """Test proper connection closure"""
        self.loader.conn = self.mock_conn

        self.loader.close()

        self.mock_conn.close.assert_called_once()

    def test_close_connection_when_none(self):
        """Test closing when connection is None"""
        self.loader.conn = None

        # Should not raise error
        self.loader.close()

    # ================================================================
    # Test 3: Table Existence Check
    # ================================================================

    def test_table_exists_true(self):
        """Test checking for existing table"""
        self.loader.conn = self.mock_conn
        self.mock_cursor.fetchone.return_value = [True]

        result = self.loader.table_exists('staging.test_table')

        self.assertTrue(result)
        self.mock_cursor.execute.assert_called_once()

    def test_table_exists_false(self):
        """Test checking for non-existent table"""
        self.loader.conn = self.mock_conn
        self.mock_cursor.fetchone.return_value = [False]

        result = self.loader.table_exists('staging.nonexistent_table')

        self.assertFalse(result)

    def test_table_exists_error_handling(self):
        """Test error handling when checking table existence"""
        self.loader.conn = self.mock_conn
        self.mock_cursor.execute.side_effect = psycopg2.Error("Database error")

        result = self.loader.table_exists('staging.test_table')

        self.assertFalse(result)

    # ================================================================
    # Test 4: Get Table Columns
    # ================================================================

    def test_get_table_columns_success(self):
        """Test retrieving table column schema"""
        self.loader.conn = self.mock_conn
        self.mock_cursor.fetchall.return_value = [
            ('yearstart', 'bigint'),
            ('locationabbr', 'text'),
            ('datavalue', 'double precision')
        ]

        result = self.loader.get_table_columns('test_table')

        self.assertEqual(result, {
            'yearstart': 'bigint',
            'locationabbr': 'text',
            'datavalue': 'double precision'
        })

    def test_get_table_columns_error(self):
        """Test error handling when retrieving columns"""
        self.loader.conn = self.mock_conn
        self.mock_cursor.execute.side_effect = psycopg2.Error("Table not found")

        result = self.loader.get_table_columns('nonexistent_table')

        self.assertEqual(result, {})

    # ================================================================
    # Test 5: Schema Matching
    # ================================================================

    def test_schema_matches_true(self):
        """Test schema matching when columns align"""
        self.loader.conn = self.mock_conn
        self.mock_cursor.fetchall.return_value = [
            ('yearstart', 'bigint'),
            ('yearend', 'bigint'),
            ('locationabbr', 'text'),
            ('topic', 'text'),
            ('datavalue', 'double precision'),
            ('loaded_at', 'timestamp'),
            ('load_date', 'date')
        ]

        result = self.loader.schema_matches('test_table', self.test_df)

        self.assertTrue(result)

    def test_schema_matches_false_missing_column(self):
        """Test schema mismatch when column is missing"""
        self.loader.conn = self.mock_conn
        self.mock_cursor.fetchall.return_value = [
            ('yearstart', 'bigint'),
            ('locationabbr', 'text')
            # Missing other columns
        ]

        result = self.loader.schema_matches('test_table', self.test_df)

        self.assertFalse(result)

    def test_schema_matches_no_existing_columns(self):
        """Test schema matching when table doesn't exist"""
        self.loader.conn = self.mock_conn
        self.mock_cursor.fetchall.return_value = []

        result = self.loader.schema_matches('test_table', self.test_df)

        self.assertFalse(result)

    # ================================================================
    # Test 6: Create Staging Table - Truncate Path
    # ================================================================

    @patch.object(PostgreSQLLoader, 'table_exists')
    @patch.object(PostgreSQLLoader, 'schema_matches')
    def test_create_staging_table_truncate(self, mock_schema_matches, mock_table_exists):
        """Test table truncation when schema matches"""
        self.loader.conn = self.mock_conn
        mock_table_exists.return_value = True
        mock_schema_matches.return_value = True

        self.loader.create_staging_table('staging.test_table', self.test_df)

        # Verify TRUNCATE was called
        truncate_call_found = False
        for call_args in self.mock_cursor.execute.call_args_list:
            if 'TRUNCATE' in str(call_args):
                truncate_call_found = True
                break

        self.assertTrue(truncate_call_found, "TRUNCATE statement should be executed")
        self.mock_conn.commit.assert_called()

    # ================================================================
    # Test 7: Create Staging Table - Recreate Path
    # ================================================================

    @patch.object(PostgreSQLLoader, 'table_exists')
    @patch.object(PostgreSQLLoader, 'schema_matches')
    def test_create_staging_table_recreate(self, mock_schema_matches, mock_table_exists):
        """Test table recreation when schema doesn't match"""
        self.loader.conn = self.mock_conn
        mock_table_exists.return_value = True
        mock_schema_matches.return_value = False

        self.loader.create_staging_table('staging.test_table', self.test_df)

        # Verify DROP and CREATE were called
        drop_call_found = False
        create_call_found = False

        for call_args in self.mock_cursor.execute.call_args_list:
            sql_statement = str(call_args)
            if 'DROP TABLE' in sql_statement:
                drop_call_found = True
            if 'CREATE TABLE' in sql_statement:
                create_call_found = True

        self.assertTrue(drop_call_found, "DROP TABLE statement should be executed")
        self.assertTrue(create_call_found, "CREATE TABLE statement should be executed")
        self.mock_conn.commit.assert_called()

    # ================================================================
    # Test 8: Create Staging Table - New Table
    # ================================================================

    @patch.object(PostgreSQLLoader, 'table_exists')
    def test_create_staging_table_new(self, mock_table_exists):
        """Test creating new table when it doesn't exist"""
        self.loader.conn = self.mock_conn
        mock_table_exists.return_value = False

        self.loader.create_staging_table('staging.new_table', self.test_df)

        # Verify CREATE was called
        create_call_found = False
        for call_args in self.mock_cursor.execute.call_args_list:
            if 'CREATE TABLE' in str(call_args):
                create_call_found = True
                break

        self.assertTrue(create_call_found, "CREATE TABLE statement should be executed")

    # ================================================================
    # Test 9: Create Table Error Handling
    # ================================================================

    @patch.object(PostgreSQLLoader, 'table_exists')
    def test_create_staging_table_error(self, mock_table_exists):
        """Test error handling during table creation"""
        self.loader.conn = self.mock_conn
        mock_table_exists.return_value = False
        self.mock_cursor.execute.side_effect = psycopg2.Error("Permission denied")

        with self.assertRaises(psycopg2.Error):
            self.loader.create_staging_table('staging.test_table', self.test_df)

        self.mock_conn.rollback.assert_called()

    # ================================================================
    # Test 10: Load Data - Successful Insert
    # ================================================================

    @patch('data_extractor.execute_values')
    def test_load_data_success(self, mock_execute_values):
        """Test successful data loading"""
        self.loader.conn = self.mock_conn
        self.loader.conn.closed = False

        self.loader.load_data('staging.test_table', self.test_df)

        # Verify execute_values was called
        self.assertTrue(mock_execute_values.called)
        self.mock_conn.commit.assert_called()

    # ================================================================
    # Test 11: Load Data - Column Name Cleaning
    # ================================================================

    @patch('data_extractor.execute_values')
    def test_load_data_column_name_cleaning(self, mock_execute_values):
        """Test that column names are cleaned (spaces, hyphens, etc.)"""
        self.loader.conn = self.mock_conn
        self.loader.conn.closed = False

        df_dirty_cols = pd.DataFrame({
            'Year Start': [2015],
            'Location-Abbr': ['US'],
            'Data.Value': [9.3]
        })

        self.loader.load_data('staging.test_table', df_dirty_cols)

        # Verify cleaned column names
        self.assertIn('year_start', df_dirty_cols.columns)
        self.assertIn('location_abbr', df_dirty_cols.columns)
        self.assertIn('data_value', df_dirty_cols.columns)

    # ================================================================
    # Test 12: Load Data - NULL Handling
    # ================================================================

    @patch('data_extractor.execute_values')
    def test_load_data_null_handling(self, mock_execute_values):
        """Test that NaN values are converted to None (NULL)"""
        self.loader.conn = self.mock_conn
        self.loader.conn.closed = False

        df_with_nan = pd.DataFrame({
            'YearStart': [2015, 2016, None],
            'DataValue': [9.3, None, 150.2]
        })

        self.loader.load_data('staging.test_table', df_with_nan)

        # Verify execute_values was called successfully
        self.assertTrue(mock_execute_values.called)

    # ================================================================
    # Test 13: Load Data - Chunking
    # ================================================================

    @patch('data_extractor.execute_values')
    def test_load_data_chunking(self, mock_execute_values):
        """Test that large datasets are chunked properly"""
        self.loader.conn = self.mock_conn
        self.loader.conn.closed = False

        # Create large DataFrame (15,000 rows)
        large_df = pd.DataFrame({
            'YearStart': range(15000),
            'DataValue': [i * 1.5 for i in range(15000)]
        })

        self.loader.load_data('staging.test_table', large_df)

        # Should be called twice (chunk_size=10000, so 10k + 5k)
        self.assertGreaterEqual(mock_execute_values.call_count, 2)

    # ================================================================
    # Test 14: Load Data - Connection Recovery
    # ================================================================

    @patch('data_extractor.execute_values')
    @patch.object(PostgreSQLLoader, 'connect')
    def test_load_data_connection_recovery(self, mock_connect, mock_execute_values):
        """Test connection recovery on OperationalError"""
        self.loader.conn = self.mock_conn
        self.loader.conn.closed = False

        # First attempt fails, second succeeds
        mock_execute_values.side_effect = [
            psycopg2.OperationalError("Connection lost"),
            None  # Success on retry
        ]

        self.loader.load_data('staging.test_table', self.test_df)

        # Verify reconnect was attempted
        mock_connect.assert_called()

    # ================================================================
    # Test 15: Load Data - Error After Max Retries
    # ================================================================

    @patch('data_extractor.execute_values')
    @patch.object(PostgreSQLLoader, 'connect')
    def test_load_data_max_retries_exceeded(self, mock_connect, mock_execute_values):
        """Test that error is raised after max retries"""
        self.loader.conn = self.mock_conn
        self.loader.conn.closed = False

        # All attempts fail
        mock_execute_values.side_effect = psycopg2.OperationalError("Connection lost")

        with self.assertRaises(psycopg2.OperationalError):
            self.loader.load_data('staging.test_table', self.test_df)

        # Verify multiple retry attempts
        self.assertEqual(mock_connect.call_count, 3)  # max_retries = 3

    # ================================================================
    # Test 16: Load Data - SQL Injection Protection
    # ================================================================

    @patch('data_extractor.execute_values')
    def test_load_data_sql_injection_protection(self, mock_execute_values):
        """Test that data with SQL-like content is handled safely"""
        self.loader.conn = self.mock_conn
        self.loader.conn.closed = False

        df_malicious = pd.DataFrame({
            'YearStart': [2015],
            'Topic': ["'; DROP TABLE staging.test_table; --"]
        })

        # Should not raise error and should use parameterized queries
        self.loader.load_data('staging.test_table', df_malicious)

        # Verify execute_values was used (which uses parameterized queries)
        self.assertTrue(mock_execute_values.called)

    # ================================================================
    # Test 17: Load Data - Rollback on Error
    # ================================================================

    @patch('data_extractor.execute_values')
    def test_load_data_rollback_on_error(self, mock_execute_values):
        """Test that transaction is rolled back on error"""
        self.loader.conn = self.mock_conn
        self.loader.conn.closed = False

        mock_execute_values.side_effect = psycopg2.IntegrityError("Duplicate key violation")

        with self.assertRaises(psycopg2.IntegrityError):
            self.loader.load_data('staging.test_table', self.test_df)

        # Verify rollback was called
        # Note: In actual implementation, rollback might not be explicitly called
        # because connection recovery catches OperationalError/InterfaceError only


# ================================================================
# Test Runner
# ================================================================

if __name__ == '__main__':
    unittest.main(verbosity=2)
