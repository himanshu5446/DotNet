# Backpressure Streaming Implementation Guide

This implementation provides a .NET 8 Web API that streams large DuckDB query results with backpressure control using `IAsyncEnumerable<T>` and `Channel<T>`.

## Key Features

### 1. Backpressure Control
- Uses `Channel<T>` with bounded capacity to prevent async reader from buffering ahead
- Channel blocks producer when consumer can't keep up
- Configurable buffer sizes via `BackpressureSettings`

### 2. Rate Limiting
- Configurable throttle delays between rows
- Manual response stream flushing after each row (optional)
- Two endpoints: normal and throttled streaming

### 3. Queue Threshold Monitoring
- Cancels query if channel queue exceeds max threshold (default: 10K items)
- Prevents memory issues from queue buildup
- Logs warnings when buffer gets full

### 4. Comprehensive Logging
- Logs throttling events when buffer reaches 80% capacity
- Memory usage monitoring during query execution
- Progress logging every 1000 rows

## Configuration

Add to `appsettings.json`:

```json
{
  "BackpressureSettings": {
    "MaxChannelBufferSize": 10000,
    "ThrottleDelayMs": 10,
    "FlushAfterEachRow": true,
    "EnableThrottling": true,
    "MaxQueueThreshold": 10000
  }
}
```

## API Endpoints

### `/api/BackpressureStreaming/stream`
- Normal streaming with backpressure control
- Respects `FlushAfterEachRow` setting
- Uses standard throttle delay

### `/api/BackpressureStreaming/stream-with-throttle`
- Simulates slow consumer with double throttle delay
- Always flushes after each row
- Includes additional logging for throttling events

### `/api/BackpressureStreaming/test-slow-consumer`
- Simple test endpoint that streams data with delays
- Useful for testing client-side cancellation

## Testing Scenarios

1. **Normal Load**: Small datasets with standard settings
2. **High Volume**: Large datasets to trigger memory monitoring
3. **Slow Consumer**: Use throttled endpoint to simulate bandwidth limitations
4. **Cancellation**: Cancel requests mid-stream to test cleanup
5. **Memory Pressure**: Large queries to test memory limits and GC

## Implementation Details

### Channel Configuration
```csharp
var channel = Channel.CreateBounded<Dictionary<string, object?>>(
    new BoundedChannelOptions(_settings.MaxChannelBufferSize)
    {
        FullMode = BoundedChannelFullMode.Wait,  // Blocks producer
        SingleReader = true,
        SingleWriter = true
    });
```

### Backpressure Mechanism
- Producer (DB reader) writes to channel
- Consumer (HTTP response) reads from channel
- When channel is full, producer blocks automatically
- This prevents memory buildup from fast DB reads vs slow HTTP writes

### Memory Management
- Periodic GC calls every 5000 rows
- Memory monitoring every 1000 rows
- Automatic cancellation on memory limits

## Example Usage

```bash
# Normal streaming
curl -X POST "https://localhost:7034/api/BackpressureStreaming/stream" \
  -H "Content-Type: application/json" \
  -d '{"datasetPath": "data.csv", "query": "SELECT * FROM dataset LIMIT 1000"}'

# Throttled streaming (slow consumer simulation)  
curl -X POST "https://localhost:7034/api/BackpressureStreaming/stream-with-throttle" \
  -H "Content-Type: application/json" \
  -d '{"datasetPath": "data.csv", "query": "SELECT * FROM dataset LIMIT 100"}'
```

## Monitoring

Watch logs for:
- `Channel buffer is X% full` - Indicates backpressure activation
- `Queue threshold exceeded` - Automatic cancellation
- `Memory limit exceeded` - Memory protection triggered
- `Throttling active` - Rate limiting in effect

## Performance Tuning

- **MaxChannelBufferSize**: Larger = more memory, better throughput
- **ThrottleDelayMs**: Smaller = faster, more CPU usage
- **FlushAfterEachRow**: False = better performance, larger client buffers
- **MaxQueueThreshold**: Should match or exceed MaxChannelBufferSize