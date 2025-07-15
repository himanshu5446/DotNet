# DuckDB UDF Usage Guide

## Overview

The ReportBuilder API provides custom User-Defined Functions (UDFs) for processing large CSV files with DuckDB. This system is designed to handle files with millions of rows while maintaining memory efficiency and providing comprehensive monitoring.

## Key Features

- **Memory-safe processing**: Monitors memory usage and cancels queries that exceed 500MB
- **Streaming results**: Process 5M+ rows without loading everything into memory
- **Performance tracking**: Measures processing rate per 10,000 rows
- **Custom UDFs**: Text normalization, phone cleaning, email processing, and more
- **Cancellation support**: Safe query cancellation when clients disconnect

## Available UDFs

### 1. NormalizeText(text)
Comprehensive text normalization: lowercase, trim, remove special characters.

```sql
-- Basic usage
SELECT NormalizeText(description) FROM input_data

-- With other columns
SELECT customer_id, NormalizeText(company_name) AS clean_name FROM input_data
```

### 2. CleanPhone(phone_text)
Phone number normalization - extracts only digits.

```sql
-- Clean phone numbers
SELECT customer_id, CleanPhone(phone_number) AS clean_phone FROM input_data

-- Filter valid phone numbers (10+ digits)
SELECT * FROM input_data WHERE LENGTH(CleanPhone(phone_number)) >= 10
```

### 3. ExtractDomain(email)
Extract domain from email addresses.

```sql
-- Get email domains
SELECT ExtractDomain(email) AS domain, COUNT(*) AS count 
FROM input_data 
GROUP BY ExtractDomain(email)

-- Filter business emails
SELECT * FROM input_data WHERE ExtractDomain(email) NOT LIKE '%gmail.com'
```

### 4. SafeSubstring(text, start, length)
Safe substring with bounds checking.

```sql
-- Extract first 10 characters safely
SELECT SafeSubstring(description, 1, 10) AS short_desc FROM input_data

-- Extract area code from phone
SELECT SafeSubstring(CleanPhone(phone), 1, 3) AS area_code FROM input_data
```

### 5. WordCount(text)
Count words in text.

```sql
-- Filter by word count
SELECT * FROM input_data WHERE WordCount(description) > 5

-- Get average word count by category
SELECT category, AVG(WordCount(description)) AS avg_words FROM input_data GROUP BY category
```

## API Endpoints

### Execute UDF Query

**POST** `/api/UdfQuery/execute`

```json
{
  "csvFilePath": "/path/to/large_file.csv",
  "sqlQuery": "SELECT customer_id, NormalizeText(company_name) AS clean_name FROM input_data WHERE WordCount(description) > 3",
  "batchSize": 10000,
  "timeoutSeconds": 600,
  "enableMemoryLogging": true,
  "enablePerformanceLogging": true,
  "responseFormat": "Streaming",
  "maxRows": 0
}
```

### Text Normalization

**POST** `/api/UdfQuery/normalize-text`

```json
{
  "csvFilePath": "/path/to/data.csv",
  "textColumnName": "company_name",
  "additionalColumns": ["customer_id", "phone"],
  "whereClause": "company_name IS NOT NULL",
  "orderBy": "customer_id",
  "batchSize": 10000,
  "normalizationType": "Full"
}
```

### Get Available UDFs

**GET** `/api/UdfQuery/udfs`

Returns information about all available UDFs with examples.

### Validate Query

**POST** `/api/UdfQuery/validate`

Validates the query and file before execution.

## Usage Examples

### Example 1: Basic Text Normalization

```bash
curl -X POST "http://localhost:5000/api/UdfQuery/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "csvFilePath": "/data/customers.csv",
    "sqlQuery": "SELECT customer_id, NormalizeText(company_name) AS clean_name FROM input_data LIMIT 1000",
    "batchSize": 1000,
    "responseFormat": "Streaming"
  }'
```

### Example 2: Complex Data Cleaning

```bash
curl -X POST "http://localhost:5000/api/UdfQuery/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "csvFilePath": "/data/contacts.csv",
    "sqlQuery": "SELECT customer_id, NormalizeText(name) AS clean_name, CleanPhone(phone) AS clean_phone, ExtractDomain(email) AS domain FROM input_data WHERE WordCount(description) > 2 AND LENGTH(CleanPhone(phone)) >= 10",
    "batchSize": 10000,
    "enableMemoryLogging": true,
    "responseFormat": "Streaming"
  }'
```

### Example 3: Large File Processing with Memory Monitoring

```bash
curl -X POST "http://localhost:5000/api/UdfQuery/normalize-text" \
  -H "Content-Type: application/json" \
  -d '{
    "csvFilePath": "/data/large_dataset_5M_rows.csv",
    "textColumnName": "description",
    "additionalColumns": ["id", "category", "timestamp"],
    "whereClause": "description IS NOT NULL AND LENGTH(description) > 10",
    "batchSize": 25000,
    "normalizationType": "Full",
    "enableMemoryLogging": true
  }'
```

## Performance Characteristics

### Memory Usage
- **Peak Memory**: Typically 100-300MB for 5M row processing
- **Memory Monitoring**: Every 10,000 rows
- **Automatic Cancellation**: If memory exceeds 500MB
- **Garbage Collection**: Forced every 100,000 rows

### Processing Speed
- **CSV Loading**: ~1-2 seconds for 100MB files
- **UDF Processing**: 10,000-50,000 rows/second depending on UDF complexity
- **Text Normalization**: ~25,000 rows/second
- **Phone Cleaning**: ~40,000 rows/second

### File Size Recommendations
- **Small files** (< 10MB): Any batch size
- **Medium files** (10-100MB): Batch size 5,000-10,000
- **Large files** (100MB-1GB): Batch size 10,000-25,000
- **Very large files** (> 1GB): Batch size 25,000-50,000

## Monitoring and Logging

### Console Output
```
[UDF-a1b2c3d4] Starting UDF query execution
[UDF-a1b2c3d4] Memory usage before: 45 MB
[UDF-a1b2c3d4] Processing CSV file: /data/large.csv, Size: 524288 KB
[UDF-a1b2c3d4] Memory usage after CSV load: 156 MB
[UDF-a1b2c3d4] CSV loaded successfully. Rows: 5,234,567, Load time: 2.45s
[UDF] Starting to stream results. Columns: 4, Batch size: 10000
[UDF] Processed 10,000 rows. Batch time: 0.42s, Rate: 23,810 rows/sec, Memory: 162 MB (Δ+6 MB)
[UDF] Processed 20,000 rows. Batch time: 0.38s, Rate: 26,316 rows/sec, Memory: 165 MB (Δ+3 MB)
[UDF] Forced GC at 100,000 rows. Memory: 148 MB
[UDF] Streaming completed. Total rows: 5,234,567, Duration: 186.32s, Overall rate: 28,087 rows/sec, Final memory: 167 MB
```

### Response Headers
```
X-Content-Format: udf-stream
X-UDF-Type: Full
X-Text-Column: description
```

## Best Practices

### 1. Query Optimization
```sql
-- Good: Use WHERE clauses to filter early
SELECT NormalizeText(name) FROM input_data WHERE name IS NOT NULL

-- Good: Use column selection
SELECT id, NormalizeText(name) FROM input_data

-- Avoid: Processing all columns unnecessarily
-- SELECT * FROM input_data
```

### 2. Batch Size Tuning
- **Small files** (< 1M rows): 5,000-10,000
- **Large files** (1M-5M rows): 10,000-25,000
- **Very large files** (> 5M rows): 25,000-50,000

### 3. Memory Management
- Monitor logs for memory warnings
- Use streaming response format for large results
- Enable garbage collection for very large files
- Consider processing in chunks for 1GB+ files

### 4. Error Handling
```bash
# Check validation before processing
curl -X POST "http://localhost:5000/api/UdfQuery/validate" \
  -H "Content-Type: application/json" \
  -d '{
    "csvFilePath": "/data/test.csv",
    "sqlQuery": "SELECT NormalizeText(name) FROM input_data",
    "batchSize": 10000
  }'
```

## Advanced Examples

### Multi-UDF Processing
```sql
SELECT 
  customer_id,
  NormalizeText(company_name) AS clean_company,
  CleanPhone(phone) AS clean_phone,
  ExtractDomain(email) AS email_domain,
  WordCount(description) AS desc_words,
  SafeSubstring(notes, 1, 50) AS short_notes
FROM input_data 
WHERE 
  WordCount(description) > 5 
  AND LENGTH(CleanPhone(phone)) >= 10
  AND ExtractDomain(email) IS NOT NULL
ORDER BY customer_id
```

### Data Quality Analysis
```sql
SELECT 
  COUNT(*) AS total_rows,
  COUNT(CASE WHEN NormalizeText(name) != name THEN 1 END) AS names_normalized,
  COUNT(CASE WHEN LENGTH(CleanPhone(phone)) >= 10 THEN 1 END) AS valid_phones,
  COUNT(CASE WHEN ExtractDomain(email) LIKE '%.com' THEN 1 END) AS com_emails,
  AVG(WordCount(description)) AS avg_description_words
FROM input_data
```

### Performance Comparison
```sql
-- Before normalization
SELECT COUNT(DISTINCT company_name) AS unique_companies_raw FROM input_data;

-- After normalization  
SELECT COUNT(DISTINCT NormalizeText(company_name)) AS unique_companies_clean FROM input_data;
```

## Troubleshooting

### Common Issues

1. **Memory Limit Exceeded**
   - Reduce batch size
   - Add WHERE clauses to filter data
   - Use column selection instead of SELECT *

2. **Slow Processing**
   - Check file size vs batch size ratio
   - Verify UDF complexity
   - Monitor system resources

3. **File Not Found**
   - Verify absolute file paths
   - Check file permissions
   - Ensure file exists on server

4. **Query Timeout**
   - Increase timeout value
   - Optimize query with filters
   - Consider processing in smaller chunks

### Performance Tuning

1. **For 5M+ row files**:
   - Batch size: 25,000-50,000
   - Enable memory logging
   - Use streaming response
   - Monitor memory usage

2. **For complex UDFs**:
   - Reduce batch size to 5,000-10,000
   - Add WHERE clauses early
   - Process only necessary columns

3. **For memory-constrained systems**:
   - Batch size: 5,000-10,000
   - Enable forced garbage collection
   - Monitor peak memory usage
   - Consider multiple smaller queries