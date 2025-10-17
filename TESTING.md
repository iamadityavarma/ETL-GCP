# Testing Guide - GCP ETL Pipeline

Quick reference for running unit tests for the CDC Chronic Disease ETL pipeline.

## Test Suite Summary

| Component | File | Test Count | Coverage |
|-----------|------|------------|----------|
| **API Response Validation** | `test_api_extraction.py` | 13 tests | API extraction, response parsing, error handling |
| **PostgreSQL Insert Logic** | `test_postgresql_loader.py` | 17 tests | DB connections, schema management, data loading |
| **BigQuery Push Logic** | `test_bigquery_writer.py` | 17 tests | BQ writes, schema detection, error handling |
| **Total** | - | **47 tests** | Full ETL pipeline coverage |

---

## Quick Start

### 1. Install Test Dependencies

```bash
pip install -r requirements-test.txt
```

### 2. Run All Tests

```bash
# Using pytest directly
pytest

# Using test runner script
python run_tests.py --all
```

### 3. View Coverage Report

```bash
python run_tests.py --coverage --html
# Open htmlcov/index.html in browser
```

---

## Common Test Commands

### Run Specific Test Suites

```bash
# API extraction tests only
python run_tests.py --api

# PostgreSQL tests only
python run_tests.py --postgresql

# BigQuery tests only
python run_tests.py --bigquery
```

### Run Individual Test Files

```bash
pytest tests/test_api_extraction.py
pytest tests/test_postgresql_loader.py
pytest tests/test_bigquery_writer.py
```

### Run Specific Test Case

```bash
pytest tests/test_api_extraction.py::TestAPIExtractor::test_extract_cdc_data_success
```

### Verbose Output

```bash
python run_tests.py --all -v
pytest -vv  # Extra verbose
```

---

## Test Coverage Details

### API Response Validation (13 tests)

✅ **Session & Connection**
- Session creation with retry strategy
- Timeout handling
- Connection error handling

✅ **Response Validation**
- Successful CSV parsing
- Empty response detection
- Malformed CSV handling
- Required column validation
- Data type validation

✅ **Error Handling**
- HTTP 4xx/5xx errors
- Network failures
- API retry logic

✅ **Data Quality**
- Row count validation
- NULL value handling
- Configuration validation

### PostgreSQL Insert Logic (17 tests)

✅ **Connection Management**
- Successful connection
- Connection failure handling
- Connection closure
- Connection recovery

✅ **Schema Management**
- Table existence checks
- Schema retrieval
- Schema matching
- Table truncation (matching schema)
- Table recreation (schema mismatch)

✅ **Data Loading**
- Successful insertion
- Column name cleaning
- NULL value conversion
- Chunked loading (large datasets)
- SQL injection protection

✅ **Error Handling**
- Transaction rollback
- Max retry handling
- Error logging

### BigQuery Push Logic (17 tests)

✅ **Client Initialization**
- Successful BQ client creation
- Authentication failure handling
- Default credential usage

✅ **Write Operations**
- Replace mode (WRITE_TRUNCATE)
- Append mode (WRITE_APPEND)
- Empty DataFrame handling
- Large DataFrame (100k rows)

✅ **Schema & Data Types**
- Schema auto-detection
- Mixed data types
- Timestamp columns
- NULL value handling
- Special characters

✅ **Error Handling**
- Write failures
- Timeout errors
- Quota exceeded
- Job completion wait

---

## Coverage Thresholds

Current configuration (`.coveragerc`):

```ini
fail_under = 70  # Minimum 70% coverage required
```

Target coverage by component:
- **API Extraction**: ≥ 85%
- **PostgreSQL Loader**: ≥ 80%
- **BigQuery Writer**: ≥ 80%
- **Overall**: ≥ 70%

---

## Test Execution Examples

### Example 1: Run all tests with coverage

```bash
$ python run_tests.py --coverage

======================================================================
Running: pytest --cov=. --cov-report=term-missing
======================================================================

tests/test_api_extraction.py ............. PASSED         [ 27%]
tests/test_postgresql_loader.py ................. PASSED [ 63%]
tests/test_bigquery_writer.py ................. PASSED   [100%]

---------- coverage: platform win32, python 3.11 -----------
Name                        Stmts   Miss  Cover   Missing
---------------------------------------------------------
data_extractor.py             245     35    86%   78-82, 156-160
data_loader.py                198     28    86%   245-250, 312-315
data_validator.py             156     45    71%   95-100, 180-185
---------------------------------------------------------
TOTAL                         599     108   82%

======================================================================
✅ All tests passed!
======================================================================
```

### Example 2: Run only API tests verbosely

```bash
$ python run_tests.py --api -v

tests/test_api_extraction.py::TestAPIExtractor::test_create_session_success PASSED
tests/test_api_extraction.py::TestAPIExtractor::test_extract_cdc_data_success PASSED
tests/test_api_extraction.py::TestAPIExtractor::test_extract_cdc_data_empty_response PASSED
...

✅ All tests passed!
```

### Example 3: Run specific test with detailed output

```bash
$ pytest tests/test_postgresql_loader.py::TestPostgreSQLLoader::test_load_data_chunking -vv

tests/test_postgresql_loader.py::TestPostgreSQLLoader::test_load_data_chunking
Test that large datasets are chunked properly

PASSED [100%]
```

---

## Continuous Integration

### GitHub Actions Example

```yaml
name: Run Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'

    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install -r requirements-test.txt

    - name: Run tests with coverage
      run: |
        pytest --cov=. --cov-report=xml --cov-report=term

    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
```

---

## Troubleshooting

### Issue: Import errors when running tests

**Solution**: Add project root to PYTHONPATH
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"  # Linux/Mac
set PYTHONPATH=%PYTHONPATH%;%cd%          # Windows
```

### Issue: Mock not patching correctly

**Problem**: `@patch('requests.Session.get')` not working

**Solution**: Patch where it's imported, not where it's defined
```python
# ❌ Wrong
@patch('requests.Session.get')

# ✅ Correct
@patch('data_extractor.requests.Session.get')
```

### Issue: Tests pass individually but fail together

**Problem**: Shared state between tests

**Solution**: Ensure proper cleanup in `tearDown()`
```python
def tearDown(self):
    if self.loader.conn:
        self.loader.conn = None
```

### Issue: Coverage report not generated

**Solution**: Install coverage dependencies
```bash
pip install pytest-cov coverage
```

---

## Best Practices

### 1. Test Naming

```python
# ✅ Good: Descriptive test names
def test_extract_cdc_data_empty_response(self):

# ❌ Bad: Vague test names
def test_extraction(self):
```

### 2. AAA Pattern (Arrange-Act-Assert)

```python
def test_load_data_success(self):
    # Arrange
    self.loader.conn = self.mock_conn

    # Act
    self.loader.load_data('table', self.test_df)

    # Assert
    mock_execute_values.assert_called_once()
```

### 3. Mock External Dependencies

```python
# ✅ Good: Mock external API
@patch('data_extractor.requests.Session.get')
def test_extract(self, mock_get):
    # Test doesn't make actual HTTP requests
```

### 4. Test Edge Cases

```python
# Test with empty data
# Test with NULL values
# Test with special characters
# Test with maximum values
```

---

## Running Tests in Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install -r requirements.txt
RUN pip install -r requirements-test.txt

CMD ["pytest", "--cov=.", "--cov-report=term-missing"]
```

```bash
docker build -t etl-tests .
docker run etl-tests
```

---

## Next Steps

1. ✅ Install test dependencies: `pip install -r requirements-test.txt`
2. ✅ Run all tests: `python run_tests.py --all`
3. ✅ Check coverage: `python run_tests.py --coverage --html`
4. ✅ Review coverage report: Open `htmlcov/index.html`
5. ✅ Add tests to CI/CD pipeline

---

## Additional Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [unittest.mock Documentation](https://docs.python.org/3/library/unittest.mock.html)
- [Coverage.py Documentation](https://coverage.readthedocs.io/)
- [Test README](tests/README.md) - Detailed test documentation
