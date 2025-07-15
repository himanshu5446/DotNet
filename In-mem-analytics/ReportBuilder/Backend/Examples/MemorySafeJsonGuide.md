# Memory-Safe JSON Streaming Guide

This implementation provides memory-safe JSON serialization for large DuckDB query results in .NET 8, avoiding memory crashes when processing massive datasets.

## Key Features

### 1. Row-by-Row Serialization
- Processes each database row individually using `IAsyncEnumerable<T>`
- Never buffers entire result set in memory
- Uses `System.Text.Json` with streaming approach

### 2. Exception Handling
- **ValueTooLarge**: Automatically truncates large values (>100KB strings, >1MB blobs)
- **Cyclic References**: Uses `ReferenceHandler.IgnoreCycles` and safe fallbacks
- **OutOfMemory**: Creates minimal error representations instead of crashing
- **General Errors**: Graceful error handling with context preservation

### 3. Format Options
- **JSON Array**: Standard `[{...},{...}]` format for compatibility
- **NDJSON**: Newline-delimited JSON for streaming parsers
- **Chunked**: HTTP chunked transfer encoding for better client responsiveness

### 4. Performance Monitoring
- Per-row serialization latency measurement
- Memory usage tracking during processing
- Throughput metrics (MB/s)
- Progress logging every 1000 rows

## API Endpoints

### `/api/MemorySafeJson/stream-json`
```json
{
  "datasetPath": "data.csv",
  "query": "SELECT * FROM dataset LIMIT 1000",
  "pageSize": 100
}
```
- Returns: JSON array format
- Content-Type: `application/json`
- Manual row-by-row streaming

### `/api/MemorySafeJson/stream-ndjson`
```json
{
  "datasetPath": "data.csv", 
  "query": "SELECT * FROM dataset",
  "pageSize": 50
}
```
- Returns: Newline-delimited JSON
- Content-Type: `application/x-ndjson`
- Each line is a complete JSON object

### `/api/MemorySafeJson/stream-json-chunked`
- Uses HTTP chunked transfer encoding
- Better for real-time client processing
- Automatic chunk boundaries every 100 items

### `/api/MemorySafeJson/test-large-dataset`
```
GET /api/MemorySafeJson/test-large-dataset?rowCount=10000&fieldCount=20&includeBlobs=true
```
- Generates synthetic test data
- Tests memory handling under load
- Includes binary data and large strings

## Configuration

Add to `appsettings.json`:

```json
{
  "JsonStreamingSettings": {
    "MeasureSerializationLatency": true,
    "EnablePropertyNameCaseInsensitive": true,
    "WriteIndented": false,
    "MaxDepth": 64,
    "AllowTrailingCommas": true,
    "DefaultBufferSize": 16384
  }
}
```

## Memory Safety Features

### 1. Value Sanitization
```csharp
// Large strings (>100KB) are truncated
if (str.Length > 100000) {
    return new { __type = "large_string", __size = str.Length, __preview = str[..1000] };
}

// Large binary data (>1MB) is summarized
if (byteArray.Length > 1024 * 1024) {
    return new { __type = "large_binary", __size = byteArray.Length };
}
```

### 2. Exception Recovery
```csharp
// Cyclic reference handling
catch (JsonException ex) when (ex.Message.Contains("cycle")) {
    return new { __error = "cyclic_reference", __original_keys = row.Keys };
}

// Out of memory recovery
catch (OutOfMemoryException) {
    return new { __error = "out_of_memory", __field_count = row.Count };
}
```

### 3. Memory Monitoring
- Checks memory usage every 1000 rows
- Triggers GC every 5000 rows
- Automatic cancellation at 500MB limit

## Performance Optimization

### Serialization Options
```csharp
var options = new JsonSerializerOptions
{
    ReferenceHandler = ReferenceHandler.IgnoreCycles, // Prevent cycles
    MaxDepth = 64,                                   // Limit recursion depth
    NumberHandling = JsonNumberHandling.AllowReadingFromString
};
```

### Memory Stream Usage
```csharp
// Controlled memory usage with buffer size limits
using var memoryStream = new MemoryStream(bufferSize);
await JsonSerializer.SerializeAsync(memoryStream, row, options, cancellationToken);
```

## Error Handling Examples

### Large Value Error
```json
{
  "__error": "value_too_large",
  "__row_summary": {
    "field_count": 25,
    "field_names": ["id", "name", "description", "..."],
    "has_more_fields": true
  }
}
```

### Cyclic Reference Error
```json
{
  "__error": "cyclic_reference",
  "__original_keys": ["id", "parent", "children"]
}
```

### Memory Error
```json
{
  "__error": "out_of_memory",
  "__field_count": 150
}
```

## Usage Examples

### Standard JSON Streaming
```bash
curl -X POST "https://localhost:7034/api/MemorySafeJson/stream-json" \
  -H "Content-Type: application/json" \
  -d '{"datasetPath": "data.csv", "query": "SELECT * FROM dataset LIMIT 1000"}'
```

### NDJSON Streaming
```bash
curl -X POST "https://localhost:7034/api/MemorySafeJson/stream-ndjson" \
  -H "Content-Type: application/json" \
  -d '{"datasetPath": "data.csv", "query": "SELECT * FROM dataset"}'
```

### Client-Side NDJSON Processing
```javascript
// Process NDJSON stream line by line
fetch('/api/MemorySafeJson/stream-ndjson', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ datasetPath: 'data.csv', query: 'SELECT * FROM dataset' })
})
.then(response => response.body.getReader())
.then(reader => {
    const decoder = new TextDecoder();
    
    function processChunk({ done, value }) {
        if (done) return;
        
        const chunk = decoder.decode(value);
        const lines = chunk.split('\n');
        
        for (const line of lines) {
            if (line.trim()) {
                const row = JSON.parse(line);
                console.log('Received row:', row);
            }
        }
        
        return reader.read().then(processChunk);
    }
    
    return reader.read().then(processChunk);
});
```

## Monitoring and Logging

Watch logs for:
- `Processed {RowCount} rows in {ElapsedMs}ms` - Progress tracking
- `Avg serialization: {LatencyMs}ms/row` - Serialization performance
- `Throughput: {ThroughputMbps} MB/s` - Network performance
- `Memory limit exceeded` - Memory protection triggered
- `Error serializing row {RowNumber}` - Individual row errors

## Best Practices

1. **Use NDJSON** for large datasets and streaming clients
2. **Monitor memory usage** in production environments
3. **Set appropriate buffer sizes** based on available memory
4. **Handle client cancellation** gracefully
5. **Test with large datasets** before production deployment
6. **Use chunked encoding** for real-time applications