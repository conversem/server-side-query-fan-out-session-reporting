# Performance Tuning Guide

## Overview

This guide provides recommendations for optimizing ingestion performance, especially for large datasets.

## Batch Size Optimization

### Default Behavior

The default batch size is **1000 records**. This works well for most use cases.

### Tuning Batch Size

**Smaller batches (100-500)**:
- Better for low-memory environments
- More frequent progress updates
- Slower overall (more database transactions)

**Larger batches (5000-10000)**:
- Better for high-memory environments
- Fewer database transactions (faster)
- Less frequent progress updates

**Example**:
```bash
# Small batch size (low memory)
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --batch-size 100

# Large batch size (high memory)
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --batch-size 5000
```

### Finding Optimal Batch Size

1. Start with default (1000)
2. Increase if CPU is idle and memory is available
3. Decrease if memory usage is high
4. Monitor throughput (records/second)

## Time Filtering

### Early Filtering

Filter data as early as possible to reduce processing:

```bash
# Good: Filter before ingestion
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --start-date 2024-01-15 \
  --end-date 2024-01-16

# Bad: Ingest everything then filter
python scripts/ingest_logs.py --provider universal --input logs.csv
# Then filter in database (slower)
```

### Benefits

- **Reduced memory usage**: Only process relevant records
- **Faster processing**: Skip irrelevant data
- **Lower database load**: Insert fewer records

## File Format Selection

### NDJSON (Recommended for Large Files)

**Advantages**:
- Memory-efficient (streaming)
- Fast parsing
- Easy to process incrementally

**Use when**:
- Files > 100MB
- Limited memory
- Need streaming processing

**Example**:
```bash
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.ndjson
```

### CSV

**Advantages**:
- Human-readable
- Easy to edit
- Widely supported

**Use when**:
- Files < 100MB
- Need human inspection
- Simple data structure

### JSON (Array)

**Disadvantages**:
- Loads entire file into memory
- Slower for large files

**Avoid for**:
- Files > 50MB
- Limited memory

## Compression

### Use Gzip Compression

Compressed files are:
- **Smaller on disk**: 70-90% reduction
- **Faster to transfer**: Less I/O
- **Automatically handled**: Adapter decompresses transparently

**Example**:
```bash
# Compress before ingestion
gzip logs.csv

# Ingest compressed file
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv.gz
```

## Directory Processing

### Single File vs Directory

**Single file**:
- Faster for one file
- Simpler error handling
- Better for testing

**Directory**:
- Efficient for multiple files
- Automatic file discovery
- Better for batch processing

**Example**:
```bash
# Process directory (efficient)
python scripts/ingest_logs.py \
  --provider universal \
  --input logs/

# Process single file (faster for one file)
python scripts/ingest_logs.py \
  --provider universal \
  --input logs/file1.csv
```

### Parallel Processing

For very large directories, process files in parallel:

```bash
# Process files in parallel (bash)
find logs/ -name "*.csv" | xargs -P 4 -I {} \
  python scripts/ingest_logs.py --provider universal --input {}
```

**Note**: Use separate database files or ensure database supports concurrent access.

## Database Optimization

### SQLite Configuration

The backend uses optimized SQLite settings:
- WAL mode (Write-Ahead Logging)
- Normal synchronous mode
- Optimized PRAGMA settings

### Database Location

**Local SSD**: Fastest
- Use local SSD for database
- Avoid network storage

**Example**:
```bash
# Local SSD
python scripts/ingest_logs.py \
  --db-path /tmp/fast-ssd/llm-bot-logs.db \
  --input logs.csv

# Network storage (slower)
python scripts/ingest_logs.py \
  --db-path /mnt/nfs/llm-bot-logs.db \
  --input logs.csv
```

## Memory Management

### Monitor Memory Usage

```bash
# Monitor during ingestion
watch -n 1 'ps aux | grep ingest_logs.py'
```

### Reduce Memory Usage

1. **Use NDJSON format**: More memory-efficient
2. **Process files individually**: Don't load entire directory
3. **Use smaller batch sizes**: 100-500 records
4. **Filter early**: Use time filters
5. **Process compressed files**: Smaller on disk

**Example**:
```bash
# Low memory configuration
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.ndjson.gz \
  --batch-size 100 \
  --start-date 2024-01-15 \
  --end-date 2024-01-16
```

## Bot Filtering Performance

### Filter Early

Bot filtering is applied during ingestion (efficient) and is **enabled by default**:

```bash
# Good: Filter during ingestion (default behavior)
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv

# Bad: Filter after ingestion (slower)
python scripts/ingest_logs.py --provider universal --input logs.csv --no-filter-bots
# Then filter in database (slower)
```

### Performance Impact

- **Minimal overhead**: Filtering is fast
- **Reduces database size**: Only store bot traffic
- **Faster queries**: Smaller dataset

## Cloudflare API Optimization

### Batch API Calls

The adapter batches API calls automatically for efficiency.

### Time Range Selection

Use smallest time range needed:

```bash
# Good: Specific time range
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input api://zone_id \
  --start-date 2024-01-15T00:00:00 \
  --end-date 2024-01-15T23:59:59

# Bad: Large time range (slower)
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input api://zone_id \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

### Use Logpush for Large Volumes

For large data volumes, use Logpush files instead of API:

```bash
# Better for large volumes
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input logpush.csv

# Slower for large volumes
python scripts/ingest_logs.py \
  --provider cloudflare \
  --input api://zone_id \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

## Performance Benchmarks

### Typical Throughput

- **CSV**: 500-1000 records/second
- **NDJSON**: 800-1500 records/second
- **JSON**: 300-600 records/second (array format)
- **W3C**: 400-800 records/second

### Factors Affecting Performance

1. **File format**: NDJSON > CSV > JSON array
2. **Batch size**: Larger batches = faster (up to limit)
3. **Compression**: Compressed files are faster to transfer
4. **Database location**: Local SSD > network storage
5. **Record complexity**: More fields = slower

## Monitoring Performance

### Progress Reporting

The CLI reports progress every 5 seconds:

```
Processed 1,000 records...
Processed 2,000 records...
Processed 3,000 records...
```

### Summary Statistics

After completion:

```
Ingestion complete!
  Records processed: 5,000
  Duration: 12.5 seconds
  Throughput: 400 records/second
```

### Verbose Mode

Enable verbose logging for detailed performance info:

```bash
python scripts/ingest_logs.py \
  --provider universal \
  --input logs.csv \
  --verbose
```

## Best Practices Summary

1. **Use NDJSON for large files**: Most memory-efficient
2. **Compress files**: Faster transfer and processing
3. **Filter early**: Use time filters to reduce data
4. **Optimize batch size**: Balance memory and speed
5. **Process directories**: Efficient for multiple files
6. **Use local storage**: Faster database access
7. **Monitor performance**: Watch throughput and adjust

## Troubleshooting Performance Issues

### Slow Ingestion

1. Check file format (use NDJSON)
2. Reduce batch size if memory-constrained
3. Use compression
4. Filter data early
5. Check database location (use local SSD)

### High Memory Usage

1. Use NDJSON format
2. Process files individually
3. Reduce batch size
4. Use compression
5. Filter data early

### Database Locked

1. Close other connections
2. Use separate database files for parallel processing
3. Wait for other processes to finish

See [Troubleshooting Guide](troubleshooting.md) for more solutions.

## See Also

- [CLI Usage Reference](cli-usage.md)
- [Troubleshooting Guide](troubleshooting.md)
- [Provider Guides](providers/)

