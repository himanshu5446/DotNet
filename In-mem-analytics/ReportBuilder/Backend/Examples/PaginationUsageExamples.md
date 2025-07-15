# DuckDB Pagination System - Usage Examples

## Overview
The pagination system provides memory-efficient, offset-based paging for large DuckDB result sets with comprehensive metadata and performance monitoring.

## Available Endpoints

### 1. GET Request with Query Parameters
```bash
GET /api/PaginatedQuery/execute?sqlQuery={query}&filePath={path}&pageNumber={page}&pageSize={size}
```

### 2. POST Request with JSON Body
```bash
POST /api/PaginatedQuery/execute
Content-Type: application/json
```

### 3. Pagination Information
```bash
GET /api/PaginatedQuery/pages/info?sqlQuery={query}&filePath={path}&pageSize={size}
```

## Usage Examples

### Basic Pagination (GET)
```bash
curl -X GET "http://localhost:5000/api/PaginatedQuery/execute" \
  -G \
  --data-urlencode "sqlQuery=SELECT * FROM table WHERE amount > 100" \
  --data-urlencode "filePath=/data/sales.csv" \
  --data-urlencode "fileFormat=Csv" \
  --data-urlencode "pageNumber=1" \
  --data-urlencode "pageSize=1000" \
  --data-urlencode "includeTotalCount=true" \
  --data-urlencode "orderBy=amount DESC"
```

### Advanced Pagination (POST)
```bash
curl -X POST "http://localhost:5000/api/PaginatedQuery/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "sqlQuery": "SELECT customer_id, order_date, amount FROM table",
    "filePath": "/data/orders.parquet",
    "fileFormat": "Parquet",
    "pageNumber": 5,
    "pageSize": 500,
    "includeTotalCount": true,
    "enableMemoryLogging": true,
    "timeoutSeconds": 60,
    "projectionColumns": ["customer_id", "order_date", "amount"],
    "whereClause": "amount > 50 AND order_date >= '2023-01-01'",
    "orderBy": "order_date DESC, amount DESC"
  }'
```

### Get Pagination Info
```bash
curl -X GET "http://localhost:5000/api/PaginatedQuery/pages/info" \
  -G \
  --data-urlencode "sqlQuery=SELECT * FROM table" \
  --data-urlencode "filePath=/data/large_dataset.parquet" \
  --data-urlencode "fileFormat=Parquet" \
  --data-urlencode "pageSize=1000"
```

## Response Format

### Successful Response
```json
{
  "success": true,
  "pagination": {
    "currentPage": 1,
    "pageSize": 1000,
    "totalRows": 15420,
    "totalPages": 16,
    "hasNextPage": true,
    "hasPreviousPage": false,
    "startRow": 1,
    "endRow": 1000,
    "rowsInCurrentPage": 1000
  },
  "query": {
    "originalQuery": "SELECT * FROM table WHERE amount > 100",
    "executedQuery": "SELECT * FROM data_table WHERE amount > 100 ORDER BY 1 LIMIT 1000 OFFSET 0",
    "filePath": "/data/sales.csv",
    "fileFormat": "Csv",
    "fileSize": 2048576,
    "columns": ["customer_id", "order_date", "amount"],
    "queryStartTime": "2024-01-15T10:30:00Z",
    "queryEndTime": "2024-01-15T10:30:02.150Z",
    "duration": "00:00:02.150"
  },
  "performance": {
    "initialMemoryUsage": 104857600,
    "finalMemoryUsage": 125829120,
    "peakMemoryUsage": 130023424,
    "memoryDelta": 20971520,
    "queryExecutionTime": "00:00:01.200",
    "dataLoadTime": "00:00:00.950",
    "memoryOptimized": true,
    "garbageCollections": 2
  },
  "data": [
    {
      "customer_id": 12345,
      "order_date": "2023-12-01",
      "amount": 299.99
    },
    // ... more rows
  ],
  "metadata": {
    "actualRowsReturned": 1000,
    "isPartialPage": false,
    "nextPageUrl": "/api/PaginatedQuery/execute?pageNumber=2&pageSize=1000",
    "previousPageUrl": null
  }
}
```

### Error Response
```json
{
  "success": false,
  "errorMessage": "File not found: /data/missing.csv",
  "errorType": "FileNotFound",
  "memoryUsage": 104857600,
  "validationErrors": [],
  "suggestions": [
    "Verify the file path is correct and accessible",
    "Check file permissions",
    "Ensure the file exists on the server"
  ],
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Pagination Info Response
```json
{
  "totalRows": 15420,
  "totalPages": 16,
  "pageSize": 1000,
  "fileInfo": {
    "filePath": "/data/large_dataset.parquet",
    "fileFormat": "Parquet",
    "fileSizeKB": 2048,
    "columns": ["id", "name", "amount", "date"]
  },
  "performance": {
    "queryExecutionTimeMs": 1200,
    "dataLoadTimeMs": 950,
    "memoryUsageMB": 120
  },
  "suggestedPageSizes": [500, 1000, 2500, 5000],
  "navigationHints": {
    "firstPage": 1,
    "lastPage": 16,
    "maxRecommendedPage": 16,
    "deepPaginationWarning": null
  }
}
```

## File Format Support

### CSV Files
```bash
# Auto-detection
"fileFormat": "Auto"

# Explicit CSV
"fileFormat": "Csv"
```

### Parquet Files  
```bash
"fileFormat": "Parquet"
```

### Arrow Files
```bash
"fileFormat": "Arrow"
```

## Memory Optimization Features

### 1. Projection Pushdown
```json
{
  "projectionColumns": ["customer_id", "amount"],
  "sqlQuery": "SELECT customer_id, amount FROM table"
}
```

### 2. Filter Pushdown
```json
{
  "whereClause": "amount > 100 AND region = 'West'",
  "sqlQuery": "SELECT * FROM table"
}
```

### 3. Memory Monitoring
```json
{
  "enableMemoryLogging": true
}
```

## Best Practices

### 1. Optimal Page Sizes
- **Small datasets** (< 1K rows): 50-100 rows per page
- **Medium datasets** (1K-10K rows): 100-500 rows per page  
- **Large datasets** (10K-100K rows): 500-1000 rows per page
- **Very large datasets** (> 100K rows): 1000-2500 rows per page

### 2. Efficient Queries
```sql
-- Good: Use WHERE clauses to filter data
SELECT customer_id, amount FROM table WHERE amount > 100

-- Good: Use ORDER BY for consistent pagination
SELECT * FROM table ORDER BY created_date DESC

-- Avoid: SELECT * on very wide tables without projection
-- Avoid: Deep pagination (page > 1000) without filters
```

### 3. Error Handling
```javascript
// Check for pagination warnings
if (response.warnings && response.warnings.length > 0) {
  console.warn('Pagination warnings:', response.warnings);
}

// Handle deep pagination
if (response.pagination.currentPage > 1000) {
  console.warn('Consider adding filters for better performance');
}

// Check memory usage
if (response.performance.peakMemoryUsage > 400 * 1024 * 1024) {
  console.warn('High memory usage detected');
}
```

### 4. Client-Side Navigation
```javascript
// Navigate to next page
const nextPage = response.pagination.currentPage + 1;
if (response.pagination.hasNextPage) {
  fetchPage(nextPage);
}

// Calculate total pages
const totalPages = Math.ceil(response.pagination.totalRows / response.pagination.pageSize);

// Jump to specific page
const targetPage = Math.min(Math.max(1, userInput), totalPages);
```

## Performance Considerations

### Memory Usage
- Each page uses maximum ~10MB memory for 1000 rows
- Memory is released immediately after streaming
- GC is triggered automatically on high usage

### Query Performance
- First page: Includes file loading time (~1-2 seconds)
- Subsequent pages: Query execution only (~200-500ms)
- Total count query: Parallel execution (~500ms additional)

### File Size Recommendations
- **CSV**: Up to 500MB per file
- **Parquet**: Up to 2GB per file (compressed)
- **Arrow**: Up to 1GB per file

### Deep Pagination Limits
- Pages beyond 1000 may have degraded performance
- Consider using time-based or ID-based pagination for large datasets
- Use filters to reduce total result set size