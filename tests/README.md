# GCP ETL Pipeline - Unit Tests

Comprehensive unit test suite for the CDC Chronic Disease ETL pipeline.

## Test Coverage

### 1. API Response Validation Tests (`test_api_extraction.py`)

Tests for `data_extractor.py` - **13 test cases**

- ✅ Session creation with retry strategy
- ✅ Successful API response parsing
- ✅ Empty response validation
- ✅ HTTP timeout handling
- ✅ HTTP error codes (4xx, 5xx)
- ✅ Network connection errors
- ✅ Malformed CSV response
- ✅ Required column validation
- ✅ Data type validation
- ✅ Minimum row count validation
- ✅ NULL/missing value handling
- ✅ API retry logic
- ✅ Configuration validation

### 2. PostgreSQL Insert Logic Tests (`test_postgresql_loader.py`)

Tests for `data_extractor.py` PostgreSQLLoader - **17 test cases**

- ✅ Database connection success/failure
- ✅ Connection closure
- ✅ Table existence checks
- ✅ Schema retrieval
- ✅ Schema matching validation
- ✅ Table truncation (schema match)
- ✅ Table recreation (schema mismatch)
- ✅ New table creation
- ✅ Error handling with rollback
- ✅ Successful data insertion
- ✅ Column name cleaning
- ✅ NULL value handling
- ✅ Chunked data loading
- ✅ Connection recovery on error
- ✅ Max retry handling
- ✅ SQL injection protection
- ✅ Transaction rollback on error

### 3. BigQuery Push Logic Tests (`test_bigquery_writer.py`)

Tests for `data_loader.py` BigQueryWriter - **17 test cases**

- ✅ BigQuery client initialization
- ✅ Write in replace mode (WRITE_TRUNCATE)
- ✅ Write in append mode (WRITE_APPEND)
- ✅ Empty DataFrame handling
- ✅ Large DataFrame handling (100k rows)
- ✅ Schema auto-detection
- ✅ Write failure handling
- ✅ NULL value handling
- ✅ Mixed data types
- ✅ Table ID formatting
- ✅ Job completion wait
- ✅ Default credential usage
- ✅ Row count validation
- ✅ Special characters in data
- ✅ Timestamp columns
- ✅ Connection timeout handling
- ✅ Quota exceeded errors

## Setup

### 1. Install Test Dependencies

```bash
pip install -r requirements-test.txt
```

### 2. Install Main Dependencies

```bash
pip install -r requirements.txt
```

## Running Tests

### Run All Tests

```bash
pytest
```

### Run with Coverage Report

```bash
pytest --cov=. --cov-report=html
```

Then open `htmlcov/index.html` in your browser.

### Run Specific Test File

```bash
# API tests only
pytest tests/test_api_extraction.py

# PostgreSQL tests only
pytest tests/test_postgresql_loader.py

# BigQuery tests only
pytest tests/test_bigquery_writer.py
```

### Run Specific Test Case

```bash
pytest tests/test_api_extraction.py::TestAPIExtractor::test_extract_cdc_data_success
```

### Run Tests by Marker

```bash
# Run only fast unit tests
pytest -m unit

# Run API-related tests
pytest -m api

# Skip slow tests
pytest -m "not slow"
```

### Run with Verbose Output

```bash
pytest -v
pytest -vv  # Extra verbose
```

### Run Tests in Parallel

```bash
pip install pytest-xdist
pytest -n auto  # Use all CPU cores
```

## Test Structure

```
tests/
├── __init__.py                  # Test package init
├── test_api_extraction.py       # API response validation tests
├── test_postgresql_loader.py    # PostgreSQL insert logic tests
├── test_bigquery_writer.py      # BigQuery push logic tests
└── README.md                    # This file
```

## Key Testing Patterns

### 1. Mocking External Dependencies

```python
@patch('data_extractor.requests.Session.get')
def test_extract_cdc_data_success(self, mock_get):
    mock_response = Mock()
    mock_response.text = "csv_data"
    mock_get.return_value = mock_response

    # Test logic...
```

### 2. Testing Error Conditions

```python
@patch('data_extractor.psycopg2.connect')
def test_connect_failure(self, mock_connect):
    mock_connect.side_effect = psycopg2.OperationalError("Connection refused")

    with self.assertRaises(psycopg2.OperationalError):
        self.loader.connect()
```

### 3. Verifying Function Calls

```python
mock_client.load_table_from_dataframe.assert_called_once()
self.assertEqual(mock_connect.call_count, 3)
```

## Coverage Goals

- **Overall Coverage**: ≥ 70%
- **Critical Paths**: ≥ 90%
  - API extraction logic
  - Database insert operations
  - BigQuery write operations

## Continuous Integration

Add to your CI/CD pipeline:

```yaml
# Example GitHub Actions
- name: Run Tests
  run: |
    pip install -r requirements-test.txt
    pytest --cov=. --cov-report=xml

- name: Upload Coverage
  uses: codecov/codecov-action@v3
  with:
    file: ./coverage.xml
```

## Writing New Tests

### Test Naming Convention

- File: `test_<module_name>.py`
- Class: `Test<ClassName>`
- Method: `test_<what_it_tests>`

### Example Template

```python
def test_feature_name_condition(self):
    """Test description of what is being validated"""
    # Arrange
    test_data = ...

    # Act
    result = function_under_test(test_data)

    # Assert
    self.assertEqual(result, expected_value)
```

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError`:
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Mock Not Working

Ensure you're patching the right location:
```python
# ❌ Wrong: @patch('requests.Session.get')
# ✅ Right: @patch('data_extractor.requests.Session.get')
```

### Database Connection Tests Failing

These tests use mocks and don't require actual database connection. If failing, check that mocks are properly configured.

## Future Enhancements

- [ ] Add integration tests with test database
- [ ] Add performance benchmarks
- [ ] Add mutation testing
- [ ] Add contract testing for API responses
- [ ] Add property-based testing with Hypothesis
