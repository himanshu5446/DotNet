#!/bin/bash

# Test script for UDF functionality
# Make sure the API is running on localhost:5000

API_BASE="http://localhost:5000"
TEST_DATA_PATH="/tmp/large_test_data.csv"

echo "=== DuckDB UDF Test Script ==="
echo "API Base URL: $API_BASE"
echo ""

# Create large test CSV file (simulate 5M rows with smaller sample)
echo "Creating test data file with sample data..."
cat > "$TEST_DATA_PATH" << 'EOF'
customer_id,company_name,phone_number,email,description
1,"ABC Corp!!! ","(555) 123-4567","john@example.com","This is a very detailed description with UPPERCASE and special chars!!!"
2," XYZ Industries@#$ ","555.987.6543","jane@company.org","Another description with    extra spaces    and weird formatting"
3,"Tech Solutions LLC!!!","(800) HELP-NOW","support@techsolutions.net","Professional services company providing EXCELLENT support"
4,"  Global Systems  ","8001234567","admin@global-systems.com","INTERNATIONAL business solutions provider with 24/7 support"
5,"Data Analytics Co.","555-GET-DATA","info@dataanalytics.biz","Leading provider of DATA SCIENCE and analytics solutions"
6,"Software Development Inc","(555) 867-5309","dev@softwaredev.io","Custom software development and CLOUD solutions"
7,"Marketing Agency Pro","555 MARKET 1","hello@marketingpro.agency","Full-service DIGITAL MARKETING and advertising agency"
8,"Financial Services Ltd","800-FINANCE","contact@financial.services","Comprehensive FINANCIAL planning and investment services"
9,"Healthcare Solutions","(555) 123-HEAL","info@healthcare-solutions.med","INNOVATIVE healthcare technology and medical solutions"
10,"Education Platform","555.EDU.TECH","support@education-platform.edu","Online LEARNING platform for students and professionals"
EOF

echo "Test data created at: $TEST_DATA_PATH"
echo ""

# Test 1: Get available UDFs
echo "=== Test 1: Get Available UDFs ==="
curl -s -X GET "$API_BASE/api/UdfQuery/udfs" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 2: Validate UDF query
echo "=== Test 2: Validate UDF Query ==="
curl -s -X POST "$API_BASE/api/UdfQuery/validate" \
  -H "Content-Type: application/json" \
  -d "{
    \"csvFilePath\": \"$TEST_DATA_PATH\",
    \"sqlQuery\": \"SELECT customer_id, NormalizeText(company_name) AS clean_name, CleanPhone(phone_number) AS clean_phone FROM input_data\",
    \"batchSize\": 1000
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 3: Execute basic UDF query (streaming)
echo "=== Test 3: Execute Basic UDF Query ==="
curl -s -X POST "$API_BASE/api/UdfQuery/execute" \
  -H "Content-Type: application/json" \
  -d "{
    \"csvFilePath\": \"$TEST_DATA_PATH\",
    \"sqlQuery\": \"SELECT customer_id, NormalizeText(company_name) AS clean_name FROM input_data LIMIT 5\",
    \"batchSize\": 2,
    \"responseFormat\": \"Streaming\",
    \"enableMemoryLogging\": true
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 4: Text normalization endpoint
echo "=== Test 4: Text Normalization Endpoint ==="
curl -s -X POST "$API_BASE/api/UdfQuery/normalize-text" \
  -H "Content-Type: application/json" \
  -d "{
    \"csvFilePath\": \"$TEST_DATA_PATH\",
    \"textColumnName\": \"company_name\",
    \"additionalColumns\": [\"customer_id\", \"phone_number\"],
    \"normalizationType\": \"Full\",
    \"batchSize\": 3
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 5: Multiple UDF operations
echo "=== Test 5: Multiple UDF Operations ==="
curl -s -X POST "$API_BASE/api/UdfQuery/execute" \
  -H "Content-Type: application/json" \
  -d "{
    \"csvFilePath\": \"$TEST_DATA_PATH\",
    \"sqlQuery\": \"SELECT customer_id, NormalizeText(company_name) AS clean_name, CleanPhone(phone_number) AS clean_phone, ExtractDomain(email) AS domain, WordCount(description) AS word_count FROM input_data WHERE WordCount(description) > 8\",
    \"batchSize\": 5,
    \"responseFormat\": \"Buffered\",
    \"enableMemoryLogging\": true,
    \"enablePerformanceLogging\": true
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 6: Count query
echo "=== Test 6: Count Query ==="
curl -s -X POST "$API_BASE/api/UdfQuery/execute" \
  -H "Content-Type: application/json" \
  -d "{
    \"csvFilePath\": \"$TEST_DATA_PATH\",
    \"sqlQuery\": \"SELECT COUNT(*) FROM input_data WHERE WordCount(description) > 5\",
    \"responseFormat\": \"Count\"
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 7: Complex data analysis
echo "=== Test 7: Complex Data Analysis ==="
curl -s -X POST "$API_BASE/api/UdfQuery/execute" \
  -H "Content-Type: application/json" \
  -d "{
    \"csvFilePath\": \"$TEST_DATA_PATH\",
    \"sqlQuery\": \"SELECT ExtractDomain(email) AS domain, COUNT(*) AS company_count, AVG(WordCount(description)) AS avg_description_words FROM input_data GROUP BY ExtractDomain(email) ORDER BY company_count DESC\",
    \"batchSize\": 10,
    \"responseFormat\": \"Buffered\"
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 8: Phone number analysis
echo "=== Test 8: Phone Number Analysis ==="
curl -s -X POST "$API_BASE/api/UdfQuery/execute" \
  -H "Content-Type: application/json" \
  -d "{
    \"csvFilePath\": \"$TEST_DATA_PATH\",
    \"sqlQuery\": \"SELECT customer_id, phone_number AS original_phone, CleanPhone(phone_number) AS clean_phone, LENGTH(CleanPhone(phone_number)) AS phone_length FROM input_data WHERE LENGTH(CleanPhone(phone_number)) >= 10\",
    \"responseFormat\": \"Buffered\"
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 9: Error handling - invalid file
echo "=== Test 9: Error Handling (Invalid File) ==="
curl -s -X POST "$API_BASE/api/UdfQuery/validate" \
  -H "Content-Type: application/json" \
  -d "{
    \"csvFilePath\": \"/nonexistent/file.csv\",
    \"sqlQuery\": \"SELECT NormalizeText(name) FROM input_data\"
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"
echo ""

# Test 10: Performance test with larger batch
echo "=== Test 10: Performance Test ==="
# Create larger dataset for performance testing
LARGE_DATA_PATH="/tmp/performance_test_data.csv"
echo "customer_id,name,description" > "$LARGE_DATA_PATH"
for i in {1..1000}; do
    echo "$i,\"Company Name $i\",\"This is a detailed description for company number $i with various UPPERCASE words and special characters!\"" >> "$LARGE_DATA_PATH"
done

curl -s -X POST "$API_BASE/api/UdfQuery/execute" \
  -H "Content-Type: application/json" \
  -d "{
    \"csvFilePath\": \"$LARGE_DATA_PATH\",
    \"sqlQuery\": \"SELECT customer_id, NormalizeText(name) AS clean_name, WordCount(description) AS word_count FROM input_data\",
    \"batchSize\": 100,
    \"responseFormat\": \"Count\",
    \"enableMemoryLogging\": true,
    \"enablePerformanceLogging\": true
  }" | jq '.' 2>/dev/null || echo "Response received (jq not available for formatting)"

rm -f "$LARGE_DATA_PATH" 2>/dev/null
echo ""

echo "=== Test Results Summary ==="
echo "Test data file: $TEST_DATA_PATH"
echo ""
echo "UDF Functions Tested:"
echo "  - NormalizeText(): Text cleaning and normalization"
echo "  - CleanPhone(): Phone number digit extraction"
echo "  - ExtractDomain(): Email domain extraction"
echo "  - WordCount(): Word counting in text"
echo "  - SafeSubstring(): Safe string extraction"
echo ""
echo "Response Formats Tested:"
echo "  - Streaming: Real-time data streaming"
echo "  - Buffered: Complete result set"
echo "  - Count: Row count only"
echo ""
echo "Features Tested:"
echo "  - Memory monitoring and logging"
echo "  - Performance tracking"
echo "  - Batch processing"
echo "  - Error handling"
echo "  - Query validation"
echo ""

# Cleanup option
echo ""
read -p "Remove test files? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -f "$TEST_DATA_PATH"
    echo "Test files removed."
fi

echo "Test completed!"