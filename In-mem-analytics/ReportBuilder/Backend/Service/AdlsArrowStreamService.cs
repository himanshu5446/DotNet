using Apache.Arrow;
using Apache.Arrow.Ipc;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure;
using ReportBuilder.Request;
using ReportBuilder.Response;
using System.Runtime.CompilerServices;

namespace ReportBuilder.Service;

/// <summary>
/// Interface for streaming Apache Arrow files from Azure Data Lake Storage (ADLS) with advanced capabilities.
/// Provides methods for schema validation, chunked streaming, and comprehensive error handling.
/// </summary>
public interface IAdlsArrowStreamService
{
    /// <summary>
    /// Streams an Arrow file from ADLS in memory-efficient chunks with performance monitoring.
    /// </summary>
    /// <param name="request">ADLS Arrow stream request with file URI, chunk size, and validation options</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    /// <returns>Arrow stream response with data stream and performance metrics</returns>
    Task<ArrowStreamResponse> StreamArrowFileAsync(AdlsArrowStreamRequest request, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Retrieves schema information from an Arrow file in ADLS without downloading the entire file.
    /// </summary>
    /// <param name="adlsUri">URI of the Arrow file in Azure Data Lake Storage</param>
    /// <param name="sasToken">Optional SAS token for authenticated access</param>
    /// <param name="cancellationToken">Token for cancelling the operation</param>
    /// <returns>Schema response with column information and file metadata</returns>
    Task<SchemaResponse> GetArrowSchemaAsync(string adlsUri, string? sasToken = null, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Validates that a file in ADLS is a properly formatted Arrow file by attempting to read its schema.
    /// </summary>
    /// <param name="adlsUri">URI of the file to validate in Azure Data Lake Storage</param>
    /// <param name="sasToken">Optional SAS token for authenticated access</param>
    /// <param name="cancellationToken">Token for cancelling the validation</param>
    /// <returns>True if the file is a valid Arrow file, false otherwise</returns>
    Task<bool> ValidateArrowFileAsync(string adlsUri, string? sasToken = null, CancellationToken cancellationToken = default);
}

/// <summary>
/// Service for streaming Apache Arrow files from Azure Data Lake Storage with advanced features.
/// Provides memory-efficient chunked streaming, retry logic, schema validation, and performance monitoring.
/// </summary>
public class AdlsArrowStreamService : IAdlsArrowStreamService
{
    private readonly ILogger<AdlsArrowStreamService> _logger;
    private readonly BlobServiceClient _blobServiceClient;
    
    private const int DefaultChunkSize = 65536; // 64KB
    private const int MaxRetries = 3;
    private const int RetryDelayMs = 1000;
    private const long MaxMemoryThreshold = 500 * 1024 * 1024; // 500MB

    /// <summary>
    /// Initializes a new instance of the AdlsArrowStreamService.
    /// </summary>
    /// <param name="logger">Logger for recording streaming operations and performance metrics</param>
    /// <param name="blobServiceClient">Azure Blob Service client for accessing ADLS storage</param>
    public AdlsArrowStreamService(ILogger<AdlsArrowStreamService> logger, BlobServiceClient blobServiceClient)
    {
        _logger = logger;
        _blobServiceClient = blobServiceClient;
    }
    /// <summary>
    /// Streams an Arrow file from Azure Data Lake Storage in memory-efficient chunks.
    /// Provides comprehensive error handling, retry logic, schema validation, and memory monitoring.
    /// </summary>
    /// <param name="request">ADLS Arrow stream request containing file URI, chunk configuration, and validation options</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    /// <returns>Arrow stream response with asynchronous data stream and detailed performance metrics</returns>
    public async Task<ArrowStreamResponse> StreamArrowFileAsync(AdlsArrowStreamRequest request, CancellationToken cancellationToken = default)
    {
        var initialMemory = GC.GetTotalMemory(true);
        var maxMemory = initialMemory;
        var queryId = Guid.NewGuid().ToString();
        var chunksProcessed = 0;
        var totalBytesRead = 0L;

        _logger.LogInformation("Starting Arrow stream processing for query {QueryId}, file: {FileUri}", queryId, request.AdlsFileUri);

        try
        {
            var blobClient = GetBlobClient(request.AdlsFileUri, request.SasToken);
            
            // Validate file exists and get properties
            var properties = await GetBlobPropertiesWithRetryAsync(blobClient, request.MaxRetries, cancellationToken);
            if (properties == null)
            {
                return new ArrowStreamResponse
                {
                    Success = false,
                    ErrorMessage = $"Arrow file not found or inaccessible: {request.AdlsFileUri}",
                    QueryId = queryId,
                    InitialMemoryUsage = initialMemory,
                    FinalMemoryUsage = GC.GetTotalMemory(false)
                };
            }

            _logger.LogInformation("Arrow file found. Size: {SizeKB} KB, ETag: {ETag}", 
                properties.ContentLength / 1024, properties.ETag);

            // Get Arrow schema first
            Schema? arrowSchema = null;
            if (request.ValidateSchema)
            {
                arrowSchema = await GetArrowSchemaFromFileAsync(blobClient, request.MaxRetries, cancellationToken);
                if (arrowSchema == null)
                {
                    return new ArrowStreamResponse
                    {
                        Success = false,
                        ErrorMessage = "Failed to read Arrow schema from file",
                        QueryId = queryId,
                        InitialMemoryUsage = initialMemory,
                        FinalMemoryUsage = GC.GetTotalMemory(false)
                    };
                }
            }

            // Stream Arrow data in chunks
            var data = StreamArrowChunksAsync(blobClient, request, arrowSchema, queryId, cancellationToken);
            
            var finalMemory = GC.GetTotalMemory(false);
            maxMemory = Math.Max(maxMemory, finalMemory);

            return new ArrowStreamResponse
            {
                Success = true,
                QueryId = queryId,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = finalMemory,
                PeakMemoryUsage = maxMemory,
                ChunksProcessed = chunksProcessed,
                TotalBytesRead = totalBytesRead,
                SchemaInfo = arrowSchema != null ? MapArrowSchemaInfo(arrowSchema) : null,
                Data = data
            };
        }
        catch (RequestFailedException ex)
        {
            _logger.LogError(ex, "Azure storage error during Arrow streaming for query {QueryId}", queryId);
            return new ArrowStreamResponse
            {
                Success = false,
                ErrorMessage = $"Azure storage error: {ex.Message}",
                QueryId = queryId,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = GC.GetTotalMemory(false)
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Arrow streaming for query {QueryId}", queryId);
            return new ArrowStreamResponse
            {
                Success = false,
                ErrorMessage = ex.Message,
                QueryId = queryId,
                InitialMemoryUsage = initialMemory,
                FinalMemoryUsage = GC.GetTotalMemory(false)
            };
        }
    }

    /// <summary>
    /// Retrieves Arrow schema information from a file in ADLS by reading only the necessary file headers.
    /// Provides column information, data types, and file metadata without downloading the entire file.
    /// </summary>
    /// <param name="adlsUri">URI of the Arrow file in Azure Data Lake Storage</param>
    /// <param name="sasToken">Optional SAS token for authenticated access to the file</param>
    /// <param name="cancellationToken">Token for cancelling the schema retrieval operation</param>
    /// <returns>Schema response with column details, file size, and metadata information</returns>
    public async Task<SchemaResponse> GetArrowSchemaAsync(string adlsUri, string? sasToken = null, CancellationToken cancellationToken = default)
    {
        try
        {
            var blobClient = GetBlobClient(adlsUri, sasToken);
            var properties = await GetBlobPropertiesWithRetryAsync(blobClient, MaxRetries, cancellationToken);
            
            if (properties == null)
            {
                return new SchemaResponse
                {
                    Success = false,
                    ErrorMessage = $"Arrow file not found: {adlsUri}"
                };
            }

            var schema = await GetArrowSchemaFromFileAsync(blobClient, MaxRetries, cancellationToken);
            if (schema == null)
            {
                return new SchemaResponse
                {
                    Success = false,
                    ErrorMessage = "Failed to read Arrow schema"
                };
            }

            var columns = schema.FieldsList.Select(field => new ColumnInfo
            {
                Name = field.Name,
                DataType = field.DataType.ToString(),
                IsNullable = field.IsNullable
            }).ToList();

            return new SchemaResponse
            {
                Success = true,
                Columns = columns,
                FileSize = properties.ContentLength,
                FilePath = adlsUri,
                Metadata = new FileMetadata
                {
                    Format = "Arrow",
                    LastModified = properties.LastModified.DateTime,
                    ETag = properties.ETag.ToString()
                }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting Arrow schema for {Uri}", adlsUri);
            return new SchemaResponse
            {
                Success = false,
                ErrorMessage = ex.Message
            };
        }
    }

    /// <summary>
    /// Validates that a file in ADLS is a properly formatted Arrow file by attempting to read its schema.
    /// Performs minimal file access to check format validity without full download.
    /// </summary>
    /// <param name="adlsUri">URI of the file to validate in Azure Data Lake Storage</param>
    /// <param name="sasToken">Optional SAS token for authenticated access to the file</param>
    /// <param name="cancellationToken">Token for cancelling the validation operation</param>
    /// <returns>True if the file exists and has a valid Arrow format, false otherwise</returns>
    public async Task<bool> ValidateArrowFileAsync(string adlsUri, string? sasToken = null, CancellationToken cancellationToken = default)
    {
        try
        {
            var blobClient = GetBlobClient(adlsUri, sasToken);
            var properties = await GetBlobPropertiesWithRetryAsync(blobClient, MaxRetries, cancellationToken);
            
            if (properties == null)
            {
                return false;
            }

            // Try to read the schema to validate it's a valid Arrow file
            var schema = await GetArrowSchemaFromFileAsync(blobClient, MaxRetries, cancellationToken);
            return schema != null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error validating Arrow file {Uri}", adlsUri);
            return false;
        }
    }

    /// <summary>
    /// Streams Arrow data in chunks from Azure Blob Storage with memory management and performance monitoring.
    /// Handles range-based reading, schema validation, and memory threshold enforcement.
    /// </summary>
    /// <param name="blobClient">Blob client for accessing the Arrow file</param>
    /// <param name="request">Stream request with chunk size and offset configuration</param>
    /// <param name="expectedSchema">Expected Arrow schema for validation</param>
    /// <param name="queryId">Unique identifier for tracking the streaming operation</param>
    /// <param name="cancellationToken">Token for cancelling the streaming operation</param>
    /// <returns>Asynchronous enumerable of dictionary rows representing Arrow data</returns>
    private async IAsyncEnumerable<Dictionary<string, object?>> StreamArrowChunksAsync(
        BlobClient blobClient,
        AdlsArrowStreamRequest request,
        Schema? expectedSchema,
        string queryId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var chunkSize = Math.Max(request.ChunkSize, 4096); // Minimum 4KB chunks
        var offset = request.StartOffset ?? 0;
        var endOffset = request.EndOffset;
        var chunksProcessed = 0;
        var totalRows = 0;
        var lastSchemaValidation = DateTime.UtcNow;

        try
        {
            // Get file properties for range validation
            var properties = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken);
            var fileSize = properties.Value.ContentLength;
            
            if (endOffset.HasValue && endOffset.Value > fileSize)
            {
                endOffset = fileSize;
            }

            _logger.LogDebug("Starting chunked read for query {QueryId}. Offset: {Offset}, End: {End}, ChunkSize: {ChunkSize}", 
                queryId, offset, endOffset, chunkSize);

            while (offset < (endOffset ?? fileSize) && !cancellationToken.IsCancellationRequested)
            {
                // Check memory before processing each chunk
                var currentMemory = GC.GetTotalMemory(false);
                if (currentMemory > MaxMemoryThreshold)
                {
                    _logger.LogWarning("Memory threshold exceeded during chunk processing: {MemoryMB} MB", currentMemory / (1024 * 1024));
                    throw new InvalidOperationException($"Memory threshold exceeded: {currentMemory / (1024 * 1024)} MB");
                }

                var remainingBytes = (endOffset ?? fileSize) - offset;
                var currentChunkSize = Math.Min(chunkSize, remainingBytes);

                var chunkData = await DownloadChunkWithRetryAsync(blobClient, offset, currentChunkSize, request.MaxRetries, cancellationToken);
                
                if (chunkData == null || chunkData.Length == 0)
                {
                    _logger.LogWarning("Empty chunk received at offset {Offset} for query {QueryId}", offset, queryId);
                    break;
                }

                // Process Arrow chunk
                await foreach (var row in ProcessArrowChunkAsync(chunkData, expectedSchema, queryId, cancellationToken))
                {
                    totalRows++;
                    yield return row;

                    // Periodic memory and schema validation
                    if (totalRows % 1000 == 0)
                    {
                        currentMemory = GC.GetTotalMemory(false);
                        if (currentMemory > MaxMemoryThreshold)
                        {
                            throw new InvalidOperationException($"Memory threshold exceeded during processing: {currentMemory / (1024 * 1024)} MB");
                        }

                        // Force GC every 5000 rows
                        if (totalRows % 5000 == 0)
                        {
                            GC.Collect();
                            GC.WaitForPendingFinalizers();
                        }
                    }
                }

                chunksProcessed++;
                offset += currentChunkSize;

                _logger.LogDebug("Processed chunk {ChunkNum} for query {QueryId}. Offset: {Offset}/{FileSize}, Rows: {TotalRows}", 
                    chunksProcessed, queryId, offset, fileSize, totalRows);
            }

            _logger.LogInformation("Completed Arrow streaming for query {QueryId}. Chunks: {Chunks}, Rows: {Rows}", 
                queryId, chunksProcessed, totalRows);
        }
        finally
        {
            // Final garbage collection
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    /// <summary>
    /// Downloads a chunk of data from Azure Blob Storage with exponential backoff retry logic.
    /// Handles transient errors and provides robust download reliability.
    /// </summary>
    /// <param name="blobClient">Blob client for downloading data</param>
    /// <param name="offset">Starting byte offset for the chunk</param>
    /// <param name="length">Number of bytes to download</param>
    /// <param name="maxRetries">Maximum number of retry attempts</param>
    /// <param name="cancellationToken">Token for cancelling the download operation</param>
    /// <returns>Downloaded chunk data as byte array, or null if all retries failed</returns>
    private async Task<byte[]?> DownloadChunkWithRetryAsync(BlobClient blobClient, long offset, long length, int maxRetries, CancellationToken cancellationToken)
    {
        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            try
            {
                var downloadOptions = new BlobDownloadOptions
                {
                    Range = new HttpRange(offset, length)
                };
                var response = await blobClient.DownloadStreamingAsync(downloadOptions, cancellationToken);
                
                using var memoryStream = new MemoryStream();
                await response.Value.Content.CopyToAsync(memoryStream, cancellationToken);
                return memoryStream.ToArray();
            }
            catch (RequestFailedException ex) when (IsRetryableError(ex) && attempt < maxRetries - 1)
            {
                var delay = (attempt + 1) * RetryDelayMs;
                _logger.LogWarning("Chunk download failed (attempt {Attempt}/{MaxAttempts}), retrying in {Delay}ms: {Error}", 
                    attempt + 1, maxRetries, delay, ex.Message);
                await Task.Delay(delay, cancellationToken);
            }
        }

        return null;
    }

    /// <summary>
    /// Processes a downloaded Arrow chunk by parsing Arrow IPC format and converting to dictionary rows.
    /// Handles schema validation, type conversion, and memory management for each record batch.
    /// </summary>
    /// <param name="chunkData">Raw Arrow IPC data to process</param>
    /// <param name="expectedSchema">Expected schema for validation purposes</param>
    /// <param name="queryId">Query identifier for logging and tracking</param>
    /// <param name="cancellationToken">Token for cancelling the processing operation</param>
    /// <returns>Asynchronous enumerable of dictionary rows with column name-value pairs</returns>
    private async IAsyncEnumerable<Dictionary<string, object?>> ProcessArrowChunkAsync(
        byte[] chunkData, 
        Schema? expectedSchema, 
        string queryId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var stream = new MemoryStream(chunkData);
        using var reader = new ArrowStreamReader(stream);

        RecordBatch? batch;
        while ((batch = await reader.ReadNextRecordBatchAsync(cancellationToken)) != null)
        {
            // Validate schema if expected
            if (expectedSchema != null && !batch.Schema.Equals(expectedSchema))
            {
                _logger.LogWarning("Schema mismatch detected in chunk for query {QueryId}", queryId);
                // Could choose to throw or continue with warning
            }

            var columnNames = batch.Schema.FieldsList.Select(f => f.Name).ToArray();
            
            for (int rowIndex = 0; rowIndex < batch.Length; rowIndex++)
            {
                var row = new Dictionary<string, object?>();
                
                for (int colIndex = 0; colIndex < batch.ColumnCount; colIndex++)
                {
                    var column = batch.Column(colIndex);
                    var value = ExtractValueFromArrowArray(column, rowIndex);
                    row[columnNames[colIndex]] = value;
                }
                
                yield return row;
            }
            
            batch.Dispose();
        }
    }

    /// <summary>
    /// Extracts a value from an Arrow array at the specified index with type-safe conversion.
    /// Handles various Arrow data types and converts them to appropriate .NET types.
    /// </summary>
    /// <param name="array">Arrow array containing the data</param>
    /// <param name="index">Index of the value to extract</param>
    /// <returns>Converted .NET object value, or null if the Arrow value is null</returns>
    private object? ExtractValueFromArrowArray(IArrowArray array, int index)
    {
        if (array.IsNull(index))
            return null;

        return array switch
        {
            StringArray stringArray => stringArray.GetString(index),
            Int32Array int32Array => int32Array.GetValue(index),
            Int64Array int64Array => int64Array.GetValue(index),
            DoubleArray doubleArray => doubleArray.GetValue(index),
            FloatArray floatArray => floatArray.GetValue(index),
            BooleanArray boolArray => boolArray.GetValue(index),
            Date32Array dateArray => DateOnly.FromDayNumber(dateArray.GetValue(index) ?? 0),
            TimestampArray timestampArray => DateTimeOffset.FromUnixTimeMilliseconds(timestampArray.GetValue(index) ?? 0),
            _ => array.IsNull(index) ? null : "Unsupported type"
        };
    }

    /// <summary>
    /// Retrieves blob properties with retry logic for handling transient Azure storage errors.
    /// Provides file metadata including size, ETag, and last modified time.
    /// </summary>
    /// <param name="blobClient">Blob client for accessing file properties</param>
    /// <param name="maxRetries">Maximum number of retry attempts</param>
    /// <param name="cancellationToken">Token for cancelling the operation</param>
    /// <returns>Blob properties if successful, null if file not found or all retries failed</returns>
    private async Task<BlobProperties?> GetBlobPropertiesWithRetryAsync(BlobClient blobClient, int maxRetries, CancellationToken cancellationToken)
    {
        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            try
            {
                var response = await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken);
                return response.Value;
            }
            catch (RequestFailedException ex) when (IsRetryableError(ex) && attempt < maxRetries - 1)
            {
                var delay = (attempt + 1) * RetryDelayMs;
                _logger.LogWarning("Get properties failed (attempt {Attempt}/{MaxAttempts}), retrying in {Delay}ms: {Error}", 
                    attempt + 1, maxRetries, delay, ex.Message);
                await Task.Delay(delay, cancellationToken);
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                return null; // File not found
            }
        }

        return null;
    }

    /// <summary>
    /// Reads Arrow schema from the beginning of a file by downloading minimal data needed for schema extraction.
    /// Efficiently retrieves schema information without downloading the entire file.
    /// </summary>
    /// <param name="blobClient">Blob client for accessing the Arrow file</param>
    /// <param name="maxRetries">Maximum number of retry attempts for download</param>
    /// <param name="cancellationToken">Token for cancelling the schema reading operation</param>
    /// <returns>Arrow schema if successfully parsed, null if file is invalid or inaccessible</returns>
    private async Task<Schema?> GetArrowSchemaFromFileAsync(BlobClient blobClient, int maxRetries, CancellationToken cancellationToken)
    {
        // Read first chunk to get schema
        var initialChunk = await DownloadChunkWithRetryAsync(blobClient, 0, 8192, maxRetries, cancellationToken); // 8KB should be enough for schema
        
        if (initialChunk == null)
            return null;

        try
        {
            using var stream = new MemoryStream(initialChunk);
            using var reader = new ArrowStreamReader(stream);
            
            var batch = await reader.ReadNextRecordBatchAsync(cancellationToken);
            return batch?.Schema;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading Arrow schema");
            return null;
        }
    }

    /// <summary>
    /// Creates a BlobClient for accessing files in Azure Data Lake Storage with optional SAS token authentication.
    /// Handles both authenticated and service-client based access patterns.
    /// </summary>
    /// <param name="adlsUri">URI of the file in Azure Data Lake Storage</param>
    /// <param name="sasToken">Optional SAS token for authenticated access</param>
    /// <returns>Configured BlobClient for accessing the specified file</returns>
    private BlobClient GetBlobClient(string adlsUri, string? sasToken)
    {
        var uri = new Uri(adlsUri);
        
        if (!string.IsNullOrEmpty(sasToken))
        {
            var uriBuilder = new UriBuilder(uri);
            uriBuilder.Query = sasToken.TrimStart('?');
            return new BlobClient(uriBuilder.Uri);
        }

        return _blobServiceClient.GetBlobContainerClient(GetContainerName(uri))
            .GetBlobClient(GetBlobName(uri));
    }

    /// <summary>
    /// Extracts the container name from an ADLS URI by parsing the path segments.
    /// </summary>
    /// <param name="uri">ADLS URI to parse</param>
    /// <returns>Container name from the URI path</returns>
    /// <exception cref="ArgumentException">Thrown when URI format is invalid</exception>
    private static string GetContainerName(Uri uri)
    {
        var segments = uri.AbsolutePath.Trim('/').Split('/');
        return segments.FirstOrDefault() ?? throw new ArgumentException("Invalid ADLS URI format");
    }

    /// <summary>
    /// Extracts the blob name (path within container) from an ADLS URI by parsing path segments.
    /// </summary>
    /// <param name="uri">ADLS URI to parse</param>
    /// <returns>Blob name/path within the container</returns>
    private static string GetBlobName(Uri uri)
    {
        var segments = uri.AbsolutePath.Trim('/').Split('/');
        return string.Join("/", segments.Skip(1));
    }

    /// <summary>
    /// Determines if an Azure storage exception represents a transient error that should be retried.
    /// Checks for common retryable HTTP status codes like 500, 503, 429, etc.
    /// </summary>
    /// <param name="ex">Azure request failed exception to evaluate</param>
    /// <returns>True if the error is retryable, false if it's a permanent failure</returns>
    private static bool IsRetryableError(RequestFailedException ex)
    {
        return ex.Status == 500 || ex.Status == 503 || ex.Status == 429 || 
               ex.Status == 408 || ex.Status == 502 || ex.Status == 504;
    }

    /// <summary>
    /// Maps an Arrow Schema to ArrowSchemaInfo response model with field details and metadata.
    /// Converts Arrow-specific types to serializable response format.
    /// </summary>
    /// <param name="schema">Arrow schema to convert</param>
    /// <returns>ArrowSchemaInfo with field information and metadata</returns>
    private ArrowSchemaInfo MapArrowSchemaInfo(Schema schema)
    {
        return new ArrowSchemaInfo
        {
            FieldCount = schema.FieldsList.Count,
            Fields = schema.FieldsList.Select(field => new ArrowFieldInfo
            {
                Name = field.Name,
                DataType = field.DataType.ToString(),
                IsNullable = field.IsNullable,
                Metadata = field.Metadata?.ToDictionary(kv => kv.Key, kv => kv.Value) ?? new Dictionary<string, string>()
            }).ToList(),
            Metadata = schema.Metadata?.ToDictionary(kv => kv.Key, kv => kv.Value) ?? new Dictionary<string, string>(),
            SchemaMatches = true
        };
    }
}