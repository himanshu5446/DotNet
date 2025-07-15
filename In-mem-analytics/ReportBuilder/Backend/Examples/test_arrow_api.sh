#!/bin/bash

# Test script for Arrow Streaming API
# Make sure the API is running on localhost:5000

API_BASE="http://localhost:5000"
TEST_DATA_PATH="/tmp/test_data.csv"

echo "=== Arrow Stream API Test Script ==="
echo "API Base URL: $API_BASE"
echo ""

# Create test data file
echo "Creating test data file..."
cat > "$TEST_DATA_PATH" << 'EOF'
customer_id,amount,order_date,region
1,150.50,2023-01-15,West
2,75.25,2023-01-16,East
3,200.00,2023-01-17,West
4,90.75,2023-01-18,Central
5,125.00,2023-01-19,East
6,300.25,2023-01-20,West
7,45.50,2023-01-21,Central
8,180.00,2023-01-22,East
9,250.75,2023-01-23,West
10,95.25,2023-01-24,Central
EOF

echo "Test data created at: $TEST_DATA_PATH"
echo ""

# Test 1: Get supported formats
echo "=== Test 1: Get Supported Formats ==="
curl -s -X GET "$API_BASE/api/ArrowStream/formats" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 2: Validate query
echo "=== Test 2: Validate Query ==="
curl -s -X POST "$API_BASE/api/ArrowStream/validate" \
  -H "Content-Type: application/json" \
  -d "{
    \"sqlQuery\": \"SELECT * FROM data_table WHERE amount > 100\",
    \"filePath\": \"$TEST_DATA_PATH\",
    \"fileFormat\": \"Csv\"
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 3: Execute query and save Arrow data
echo "=== Test 3: Execute Query (save to file) ==="
ARROW_OUTPUT="/tmp/test_results.arrow"
curl -s -X POST "$API_BASE/api/ArrowStream/execute" \
  -H "Content-Type: application/json" \
  -H "Accept: application/vnd.apache.arrow.stream" \
  -d "{
    \"sqlQuery\": \"SELECT customer_id, amount, region FROM data_table WHERE amount > 100 ORDER BY amount DESC\",
    \"filePath\": \"$TEST_DATA_PATH\",
    \"fileFormat\": \"Csv\",
    \"timeoutSeconds\": 30,
    \"includeMetadata\": true
  }" \
  --output "$ARROW_OUTPUT"

if [ -f "$ARROW_OUTPUT" ]; then
    echo "Arrow data saved to: $ARROW_OUTPUT"
    echo "File size: $(ls -lh "$ARROW_OUTPUT" | awk '{print $5}')"
    echo "File type: $(file "$ARROW_OUTPUT")"
else
    echo "Failed to save Arrow data"
fi
echo ""

# Test 4: Test with aggregation query
echo "=== Test 4: Aggregation Query ==="
curl -s -X POST "$API_BASE/api/ArrowStream/execute" \
  -H "Content-Type: application/json" \
  -H "Accept: application/vnd.apache.arrow.stream" \
  -d "{
    \"sqlQuery\": \"SELECT region, COUNT(*) as count, AVG(amount) as avg_amount, SUM(amount) as total_amount FROM data_table GROUP BY region ORDER BY total_amount DESC\",
    \"filePath\": \"$TEST_DATA_PATH\",
    \"fileFormat\": \"Csv\",
    \"timeoutSeconds\": 30
  }" \
  --output "/tmp/aggregation_results.arrow"

if [ -f "/tmp/aggregation_results.arrow" ]; then
    echo "Aggregation results saved to: /tmp/aggregation_results.arrow"
    echo "File size: $(ls -lh /tmp/aggregation_results.arrow | awk '{print $5}')"
else
    echo "Failed to save aggregation results"
fi
echo ""

# Test 5: Test error handling (invalid file)
echo "=== Test 5: Error Handling (Invalid File) ==="
curl -s -X POST "$API_BASE/api/ArrowStream/validate" \
  -H "Content-Type: application/json" \
  -d "{
    \"sqlQuery\": \"SELECT * FROM data_table\",
    \"filePath\": \"/non/existent/file.csv\",
    \"fileFormat\": \"Csv\"
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 6: Test error handling (invalid SQL)
echo "=== Test 6: Error Handling (Invalid SQL) ==="
curl -s -X POST "$API_BASE/api/ArrowStream/validate" \
  -H "Content-Type: application/json" \
  -d "{
    \"sqlQuery\": \"DELETE FROM data_table\",
    \"filePath\": \"$TEST_DATA_PATH\",
    \"fileFormat\": \"Csv\"
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

echo "=== Test Results Summary ==="
echo "Test data file: $TEST_DATA_PATH"
echo "Arrow results: $ARROW_OUTPUT"
echo "Aggregation results: /tmp/aggregation_results.arrow"
echo ""
echo "To view Arrow data, you can use:"
echo "  Python: import pyarrow as pa; table = pa.ipc.open_file('$ARROW_OUTPUT').read_all()"
echo "  JavaScript: const table = require('apache-arrow').tableFromIPC(fs.readFileSync('$ARROW_OUTPUT'))"
echo ""
echo "Test completed!"

# Cleanup option
echo ""
read -p "Remove test files? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -f "$TEST_DATA_PATH" "$ARROW_OUTPUT" "/tmp/aggregation_results.arrow"
    echo "Test files removed."
fi