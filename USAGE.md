# Usage Guide - API Chunking & Transient Files

Quick reference for using the enhanced ETL pipeline with chunking and transient file storage.

---

## Quick Start

### 1. Setup Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your actual credentials
nano .env
```

### 2. Configure Chunking

In your `.env` file:

```bash
# Adjust chunk size based on your data volume
API_CHUNK_SIZE=50000              # 50k rows per chunk (recommended)

# Transient file location
TRANSIENT_DIR=./transient_data    # Local directory

# Keep files for debugging (optional)
KEEP_TRANSIENT_FILES=false        # Auto-delete after success
```

### 3. Run the Pipeline

```bash
python data_extractor.py
```

---

## How It Works

### **Before (Direct Load)**
```
API → DataFrame (all in memory) → PostgreSQL
```
**Problem**: Large datasets cause OOM errors

### **After (Chunked with Transient Files)**
```
API → DataFrame → Chunks → Transient Files → PostgreSQL (one chunk at a time)
```
**Benefit**: Memory efficient + fault tolerant

---

## Configuration Options

### Chunk Size Recommendations

| Dataset Size | Recommended Chunk Size | Number of Chunks |
|--------------|------------------------|------------------|
| < 100k rows  | 50,000 (default)       | 2 chunks         |
| 100k-500k    | 50,000                 | 10 chunks        |
| 500k-1M      | 100,000                | 10 chunks        |
| 1M+          | 100,000-200,000        | 10-20 chunks     |

**Rule of thumb**: Adjust so each chunk takes 10-30 seconds to process

### Memory Considerations

```bash
# Low memory environment (<4GB RAM)
API_CHUNK_SIZE=25000

# Standard environment (8GB+ RAM)
API_CHUNK_SIZE=50000

# High memory environment (16GB+ RAM)
API_CHUNK_SIZE=100000
```

---

## Transient File Management

### Auto-Cleanup (Production)

```bash
# .env
KEEP_TRANSIENT_FILES=false
```

Pipeline will automatically delete transient files after successful completion.

### Manual Cleanup

```bash
# Remove transient directory manually
rm -rf ./transient_data
```

### Keep for Debugging

```bash
# .env
KEEP_TRANSIENT_FILES=true
```

Transient files will be preserved even after success. Useful for:
- Debugging data quality issues
- Auditing pipeline runs
- Replaying failed chunks

---

## Error Recovery

### Scenario 1: API Extraction Fails

```
[ERROR] CDC API request timeout
```

**What happens:**
- No transient files created yet
- Pipeline exits with error code 1
- Retry entire pipeline

**Action:**
```bash
# Just re-run
python data_extractor.py
```

---

### Scenario 2: Database Connection Fails Mid-Load

```
[ERROR] Database connection failed
[INFO] Keeping transient files for debugging (pipeline failed)
```

**What happens:**
- Transient files are preserved
- Can resume from failed chunk

**Action:**
```bash
# Fix database connection issue
# Re-run pipeline
python data_extractor.py
```

The pipeline will:
1. Recreate transient files from API (overwrites old ones)
2. Start fresh database load

**Alternative (Manual Recovery):**
```python
# Load specific chunks manually
from data_extractor import TransientFileManager, PostgreSQLLoader

# Load existing transient files
manager = TransientFileManager('./transient_data')

# Connect and load
loader = PostgreSQLLoader()
loader.connect()
loader.load_from_transient_files('staging.table_name', manager)
```

---

### Scenario 3: Specific Chunk Fails

```
[ERROR] Failed to load chunk cdc_chunk_0005.csv
```

**What happens:**
- All transient files kept
- Pipeline stops

**Action:**
1. Investigate chunk file: `cat transient_data/cdc_chunk_0005.csv | head`
2. Fix data issue (if any)
3. Re-run pipeline

---

## Advanced Usage

### Custom Chunk Processing

```python
from data_extractor import APIExtractor, TransientFileManager, PostgreSQLLoader

# Extract and chunk
extractor = APIExtractor()
transient_manager = extractor.extract_and_chunk_to_transient()

# Custom processing per chunk
for chunk_file in transient_manager.list_chunks():
    df = transient_manager.load_chunk(chunk_file)

    # Your custom logic here
    df_cleaned = custom_cleaning(df)

    # Load to database
    loader.load_data('staging.table', df_cleaned)
```

### Parallel Chunk Loading (Future Enhancement)

```python
# TODO: Not implemented yet
# Future feature to load multiple chunks concurrently
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(load_chunk, chunk_files)
```

---

## Monitoring

### Log Monitoring

Key log messages to watch:

```bash
# Successful chunking
[CHUNKING] Splitting 542,321 rows into 11 chunk(s)
[SUCCESS] All chunks saved to transient storage

# Successful database load
[SUCCESS] All 11 transient file(s) loaded
[SUCCESS] Total rows loaded: 542,321

# Cleanup
[TRANSIENT] Cleaned up transient directory
```

### Error Patterns

```bash
# API issues
[ERROR] CDC API request failed
[ERROR] CDC API request timeout

# Database issues
[ERROR] Database connection failed
[ERROR] Failed to load chunk

# Data issues
[ERROR] CDC API returned empty dataset
```

---

## Performance Optimization

### 1. Adjust Chunk Size

```bash
# If chunks process too slowly (>60s each)
API_CHUNK_SIZE=25000  # Reduce chunk size

# If too many chunks (>20)
API_CHUNK_SIZE=100000  # Increase chunk size
```

### 2. Database Tuning

In `data_extractor.py` → `load_data()` method:

```python
# Increase batch size for faster inserts (requires more memory)
chunk_size = 20000  # Default: 10000

# Increase page size
page_size = 1000  # Default: 500

# Reduce delay between chunks
time.sleep(0.05)  # Default: 0.1
```

### 3. Disk I/O Optimization

```bash
# Use tmpfs (RAM disk) for transient files
TRANSIENT_DIR=/dev/shm/transient_data  # Linux only

# Or SSD storage
TRANSIENT_DIR=/mnt/ssd/transient_data
```

---

## Troubleshooting

### Issue: "Disk space full"

```bash
# Check transient directory size
du -sh ./transient_data

# Cleanup manually
rm -rf ./transient_data

# Reduce chunk size to use less disk
API_CHUNK_SIZE=25000
```

### Issue: "Chunks not being cleaned up"

Check configuration:
```bash
# In .env
KEEP_TRANSIENT_FILES=false  # Ensure this is false
```

### Issue: "Pipeline runs out of memory"

```bash
# Reduce chunk size
API_CHUNK_SIZE=25000

# Reduce DB batch size
# Edit data_extractor.py load_data() method:
chunk_size = 5000
page_size = 250
```

---

## Best Practices

### Production Deployments

```bash
# .env for production
API_CHUNK_SIZE=50000
TRANSIENT_DIR=/tmp/etl_transient
KEEP_TRANSIENT_FILES=false
```

### Development/Testing

```bash
# .env for development
API_CHUNK_SIZE=10000
TRANSIENT_DIR=./transient_data_dev
KEEP_TRANSIENT_FILES=true  # Keep for inspection
```

### CI/CD Pipelines

```yaml
# GitHub Actions example
- name: Run ETL Pipeline
  env:
    API_CHUNK_SIZE: 50000
    KEEP_TRANSIENT_FILES: false
    TRANSIENT_DIR: /tmp/transient
  run: python data_extractor.py
```

---

## FAQ

**Q: Do I need to change existing code?**
A: No! The old `extract_cdc_data()` method still works. The new chunking is used automatically in `run_pipeline()`.

**Q: Can I use this for other APIs?**
A: Yes! Just modify the `extract_cdc_data()` method to call your API.

**Q: What happens if I stop the pipeline mid-run?**
A: Transient files are kept. Re-running will overwrite them with fresh API data.

**Q: Can I manually inspect chunk files?**
A: Yes! They're standard CSV files:
```bash
head transient_data/cdc_chunk_0000.csv
```

**Q: How much disk space do I need?**
A: Approximately same size as the API response. For 500k rows ~500MB.

---

## Examples

### Example 1: Standard Production Run

```bash
# Setup
export API_CHUNK_SIZE=50000
export KEEP_TRANSIENT_FILES=false

# Run
python data_extractor.py

# Output
[SUCCESS] All chunks saved to transient storage
[SUCCESS] Total rows loaded: 542,321
[TRANSIENT] Cleaned up transient directory
```

### Example 2: Debug Mode

```bash
# Setup - keep files for inspection
export KEEP_TRANSIENT_FILES=true

# Run
python data_extractor.py

# After completion, inspect
ls -lh transient_data/
cat transient_data/cdc_chunk_0000.csv | head -20
```

### Example 3: Recovering from Failure

```bash
# Pipeline failed during load
[ERROR] Failed to load chunk cdc_chunk_0005.csv

# Inspect problematic chunk
cat transient_data/cdc_chunk_0005.csv

# Fix issue and re-run
python data_extractor.py
```

---

## Additional Resources

- **Architecture**: See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design
- **Testing**: See [TESTING.md](TESTING.md) for unit tests
- **Configuration**: See [.env.example](.env.example) for all settings
