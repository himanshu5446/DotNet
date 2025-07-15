# Arrow IPC Streaming Guide

## Overview

The ReportBuilder API provides high-performance Arrow IPC streaming capabilities for large-scale data analytics. This feature allows you to execute SQL queries against CSV, Parquet, and Arrow files using DuckDB and stream results in Apache Arrow IPC format.

## Key Features

- **Memory-efficient streaming**: Direct Arrow IPC format without JSON serialization overhead
- **Multiple file format support**: CSV, Parquet, and Arrow files with auto-detection
- **DuckDB integration**: Leverages DuckDB's powerful SQL engine and file readers
- **Cancellation support**: Proper handling of client cancellation and timeouts
- **Comprehensive metadata**: Query execution info, performance metrics, and schema details
- **Safety features**: SQL injection prevention and resource limits

## Endpoints

### 1. Execute Arrow Stream Query

**POST** `/api/ArrowStream/execute`

Executes a SQL query and streams results in Arrow IPC format.

**Request Body:**
```json
{
  "sqlQuery": "SELECT * FROM data_table WHERE amount > 100",
  "filePath": "/path/to/data.csv",
  "fileFormat": "Csv",
  "timeoutSeconds": 300,
  "includeMetadata": true,
  "enableMemoryLogging": false,
  "projectionColumns": ["customer_id", "amount"],
  "whereClause": "amount > 100",
  "orderBy": "amount DESC",
  "limitRows": 10000
}
```

**Response:**
- Content-Type: `application/vnd.apache.arrow.stream`
- Body: Arrow IPC stream data
- Headers: Metadata about query execution

### 2. Validate Arrow Query

**POST** `/api/ArrowStream/validate`

Validates a query and returns metadata without executing.

**Response:**
```json
{
  "success": true,
  "metadata": {
    "originalQuery": "SELECT * FROM data_table",
    "executedQuery": "SELECT * FROM data_table ORDER BY 1",
    "filePath": "/path/to/data.csv",
    "fileFormat": "Csv",
    "fileSize": 1048576,
    "columns": [
      {
        "name": "customer_id",
        "dataType": "INTEGER",
        "isNullable": false,
        "index": 0
      }
    ],
    "estimatedRows": 10000
  },
  "estimatedSizeInfo": {
    "fileSize": 1048576,
    "estimatedRows": 10000,
    "columns": 5,
    "estimatedArrowSizeKB": 1024
  },
  "recommendations": [
    "Consider adding WHERE clauses to filter data"
  ]
}
```

### 3. Get Supported Formats

**GET** `/api/ArrowStream/formats`

Returns information about supported file formats.

## Usage Examples

### Python Client

```python
from python_arrow_client import ArrowStreamClient

# Initialize client
client = ArrowStreamClient("http://localhost:5000")

# Execute query and get pandas DataFrame
df = client.execute_query_to_dataframe(
    "SELECT * FROM data_table WHERE amount > 100 LIMIT 1000",
    "/path/to/data.csv",
    "Csv"
)

# Stream results in batches
for batch in client.stream_query_batches(
    "SELECT customer_id, amount FROM data_table ORDER BY amount DESC",
    "/path/to/data.csv"
):
    print(f"Batch shape: {batch.num_rows} rows")
    batch_df = batch.to_pandas()
    # Process batch...
```

### JavaScript Client

```javascript
const { ArrowStreamClient } = require('./javascript_arrow_client');

// Initialize client
const client = new ArrowStreamClient('http://localhost:5000');

// Execute query and get Arrow table
const table = await client.executeQueryToArrow(
    'SELECT * FROM data_table WHERE amount > 100 LIMIT 1000',
    '/path/to/data.csv',
    'Csv'
);

// Convert to JavaScript objects
const objects = await client.executeQueryToObjects(
    'SELECT customer_id, amount FROM data_table ORDER BY amount DESC LIMIT 10',
    '/path/to/data.csv'
);
```

### cURL Examples

```bash
# Validate query
curl -X POST "http://localhost:5000/api/ArrowStream/validate" \
  -H "Content-Type: application/json" \
  -d '{
    "sqlQuery": "SELECT * FROM data_table LIMIT 10",
    "filePath": "/path/to/data.csv",
    "fileFormat": "Csv"
  }'

# Execute query (saves Arrow data to file)
curl -X POST "http://localhost:5000/api/ArrowStream/execute" \
  -H "Content-Type: application/json" \
  -H "Accept: application/vnd.apache.arrow.stream" \
  -d '{
    "sqlQuery": "SELECT * FROM data_table WHERE amount > 100",
    "filePath": "/path/to/data.csv",
    "fileFormat": "Csv",
    "timeoutSeconds": 300
  }' \
  --output results.arrow
```

## Performance Considerations

### Memory Usage
- Arrow streaming uses minimal memory (~10MB typical)
- Memory monitoring with automatic GC triggering
- Configurable memory limits (500MB default)

### Query Performance
- CSV files: ~1-2 seconds loading time for 100MB files
- Parquet files: ~500ms loading time for 100MB files
- Query execution: ~200-500ms for typical analytical queries

### File Size Recommendations
- **CSV**: Up to 500MB per file
- **Parquet**: Up to 2GB per file (compressed)
- **Arrow**: Up to 1GB per file

## SQL Query Guidelines

### Supported Operations
- SELECT with projections, filters, aggregations
- JOIN operations (use separate JOIN endpoints for cross-file joins)
- ORDER BY for consistent results
- LIMIT for result size control
- Window functions and CTEs

### Prohibited Operations
- INSERT, UPDATE, DELETE, DROP
- CREATE, ALTER, TRUNCATE
- Dynamic SQL execution (EXEC)

### Best Practices
```sql
-- Good: Use projections to select only needed columns
SELECT customer_id, amount, order_date FROM data_table WHERE amount > 100

-- Good: Use ORDER BY for consistent pagination
SELECT * FROM data_table ORDER BY created_date DESC LIMIT 1000

-- Good: Use aggregations for summary data
SELECT region, COUNT(*) as orders, SUM(amount) as total_sales 
FROM data_table GROUP BY region

-- Avoid: SELECT * on very wide tables
-- Avoid: Large result sets without LIMIT
-- Avoid: Complex joins (use dedicated JOIN endpoints)
```

## Error Handling

### Common Errors

1. **File Not Found**
   - Check file path and permissions
   - Ensure file exists on server

2. **SQL Syntax Error**
   - Verify column names exist
   - Check for proper table references (use 'data_table')
   - Validate SQL syntax

3. **Memory Limit Exceeded**
   - Add LIMIT clause to reduce result set
   - Use WHERE clauses to filter data
   - Consider column projection

4. **Timeout Error**
   - Increase timeout value
   - Optimize query with better filters
   - Break large queries into smaller chunks

### Response Headers

The API includes helpful headers in Arrow responses:

```
X-Arrow-Schema-Version: 1.0
X-Content-Format: arrow-ipc
X-Query-Columns: 5
X-Estimated-Rows: 10000
X-File-Format: Csv
X-File-Size: 1048576
X-Stream-Start-Time: 2024-01-15T10:30:00.000Z
```

## Client Libraries

### Python Requirements
```bash
pip install pyarrow pandas requests
```

### JavaScript Requirements
```bash
npm install apache-arrow node-fetch
```

### .NET Requirements
```bash
# For .NET clients
dotnet add package Apache.Arrow
dotnet add package System.Net.Http
```

## Architecture

```
[Client] -> [ArrowStreamController] -> [ArrowStreamService] -> [DuckDB] -> [File System]
                                                    |
                                                    v
[Arrow IPC Stream] <- [Temp Arrow File] <- [COPY TO Arrow Format]
```

The service:
1. Validates the SQL query for safety
2. Loads the file into DuckDB using appropriate reader
3. Executes the query with `COPY TO` Arrow format
4. Streams the resulting Arrow file to the client
5. Cleans up temporary files

## Limitations

1. **Single file queries**: Use dedicated JOIN endpoints for multi-file operations
2. **File size limits**: 500MB for CSV, 2GB for Parquet
3. **Memory constraints**: 500MB peak memory usage
4. **Concurrent requests**: Limited by server capacity
5. **Temporary files**: Requires disk space for Arrow export

## Monitoring

The API provides comprehensive performance metrics:
- Memory usage (initial, peak, final)
- Query execution time
- Data loading time
- Streaming rate (MB/s)
- Garbage collection statistics

Use these metrics to optimize queries and monitor system performance.

## Security

- SQL injection prevention through query validation
- File path validation and sandboxing
- Memory limits to prevent resource exhaustion
- Timeout controls for long-running queries
- No write operations allowed