/**
 * JavaScript client for consuming Arrow IPC streams from the ReportBuilder API
 * Requires: npm install apache-arrow node-fetch
 * 
 * Usage:
 * const client = new ArrowStreamClient('http://localhost:5000');
 * const table = await client.executeQueryToArrow('SELECT * FROM data_table LIMIT 10', '/path/to/data.csv');
 */

const { tableFromIPC } = require('apache-arrow');
const fetch = require('node-fetch');

class ArrowStreamClient {
    constructor(baseUrl = 'http://localhost:5000') {
        this.baseUrl = baseUrl;
        this.defaultHeaders = {
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.apache.arrow.stream'
        };
    }

    /**
     * Validate query and get metadata without executing
     */
    async validateQuery(sqlQuery, filePath, fileFormat = 'Auto') {
        const url = `${this.baseUrl}/api/ArrowStream/validate`;
        
        const payload = {
            sqlQuery,
            filePath,
            fileFormat,
            includeMetadata: true
        };

        const response = await fetch(url, {
            method: 'POST',
            headers: this.defaultHeaders,
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`Validation failed: ${response.status} ${response.statusText}`);
        }

        return await response.json();
    }

    /**
     * Execute query and return results as Arrow Table
     */
    async executeQueryToArrow(sqlQuery, filePath, fileFormat = 'Auto', timeoutSeconds = 300) {
        const url = `${this.baseUrl}/api/ArrowStream/execute`;
        
        const payload = {
            sqlQuery,
            filePath,
            fileFormat,
            timeoutSeconds,
            includeMetadata: true,
            enableMemoryLogging: true
        };

        console.log(`Executing Arrow query: ${sqlQuery}`);
        console.log(`File: ${filePath}, Format: ${fileFormat}`);

        const response = await fetch(url, {
            method: 'POST',
            headers: this.defaultHeaders,
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`Query failed: ${response.status} ${response.statusText}`);
        }

        // Check for error response
        const contentType = response.headers.get('Content-Type');
        if (contentType && contentType.startsWith('application/json')) {
            const errorData = await response.json();
            throw new Error(`Query failed: ${errorData.errorMessage || 'Unknown error'}`);
        }

        // Log response headers
        console.log('Response headers:', Object.fromEntries(response.headers.entries()));

        // Read Arrow stream
        const arrowData = await response.arrayBuffer();
        
        // Create Arrow table from IPC data
        const table = tableFromIPC(arrowData);
        
        console.log(`Arrow table shape: ${table.numRows} rows, ${table.numCols} columns`);
        console.log(`Arrow table schema:`, table.schema.fields.map(f => ({ name: f.name, type: f.type.toString() })));
        
        return table;
    }

    /**
     * Execute query and return results as JavaScript objects
     */
    async executeQueryToObjects(sqlQuery, filePath, fileFormat = 'Auto', timeoutSeconds = 300) {
        const table = await this.executeQueryToArrow(sqlQuery, filePath, fileFormat, timeoutSeconds);
        
        // Convert Arrow table to JavaScript objects
        const objects = [];
        for (let i = 0; i < table.numRows; i++) {
            const row = {};
            for (let j = 0; j < table.numCols; j++) {
                const field = table.schema.fields[j];
                row[field.name] = table.getChildAt(j).get(i);
            }
            objects.push(row);
        }
        
        return objects;
    }

    /**
     * Stream query results and process batches
     */
    async streamQueryBatches(sqlQuery, filePath, fileFormat = 'Auto', timeoutSeconds = 300, batchCallback) {
        const table = await this.executeQueryToArrow(sqlQuery, filePath, fileFormat, timeoutSeconds);
        
        // For this example, we'll process the entire table as one batch
        // In a real streaming scenario, the server would send multiple batches
        const batchSize = 1000;
        let startIndex = 0;
        
        while (startIndex < table.numRows) {
            const endIndex = Math.min(startIndex + batchSize, table.numRows);
            const batch = table.slice(startIndex, endIndex);
            
            // Convert batch to JavaScript objects
            const batchObjects = [];
            for (let i = 0; i < batch.numRows; i++) {
                const row = {};
                for (let j = 0; j < batch.numCols; j++) {
                    const field = batch.schema.fields[j];
                    row[field.name] = batch.getChildAt(j).get(i);
                }
                batchObjects.push(row);
            }
            
            await batchCallback(batchObjects, startIndex);
            startIndex = endIndex;
        }
    }

    /**
     * Get supported file formats
     */
    async getSupportedFormats() {
        const url = `${this.baseUrl}/api/ArrowStream/formats`;
        const response = await fetch(url);
        
        if (!response.ok) {
            throw new Error(`Failed to get formats: ${response.status} ${response.statusText}`);
        }
        
        return await response.json();
    }
}

// Example usage
async function main() {
    const client = new ArrowStreamClient('http://localhost:5000');
    
    try {
        // Example 1: Get supported formats
        console.log('Getting supported formats...');
        const formats = await client.getSupportedFormats();
        console.log('Supported formats:', JSON.stringify(formats, null, 2));
        
        // Example 2: Validate query
        console.log('\nValidating query...');
        const sqlQuery = 'SELECT * FROM data_table LIMIT 10';
        const filePath = '/path/to/your/data.csv'; // Update this path
        
        const validation = await client.validateQuery(sqlQuery, filePath, 'Csv');
        console.log('Validation result:', JSON.stringify(validation, null, 2));
        
        // Example 3: Execute query and get Arrow table
        console.log('\nExecuting query to Arrow table...');
        const table = await client.executeQueryToArrow(
            'SELECT * FROM data_table WHERE amount > 100 LIMIT 1000',
            filePath,
            'Csv'
        );
        
        console.log(`Table shape: ${table.numRows} rows, ${table.numCols} columns`);
        console.log('Table schema:', table.schema.fields.map(f => ({ name: f.name, type: f.type.toString() })));
        
        // Example 4: Execute query and get JavaScript objects
        console.log('\nExecuting query to JavaScript objects...');
        const objects = await client.executeQueryToObjects(
            'SELECT customer_id, amount FROM data_table ORDER BY amount DESC LIMIT 5',
            filePath,
            'Csv'
        );
        
        console.log('Query results as objects:', objects);
        
        // Example 5: Stream query results in batches
        console.log('\nStreaming query results...');
        let totalRows = 0;
        
        await client.streamQueryBatches(
            'SELECT customer_id, amount FROM data_table ORDER BY amount DESC LIMIT 5000',
            filePath,
            'Csv',
            300,
            async (batch, startIndex) => {
                totalRows += batch.length;
                console.log(`Received batch starting at index ${startIndex} with ${batch.length} rows`);
                
                // Process batch as needed
                if (batch.length > 0) {
                    console.log('Sample row from batch:', batch[0]);
                }
                
                // Stop after processing a few batches for demo
                if (totalRows > 2000) {
                    return;
                }
            }
        );
        
        console.log(`Total rows processed: ${totalRows}`);
        
        // Example 6: Aggregation query
        console.log('\nExecuting aggregation query...');
        const aggregationTable = await client.executeQueryToArrow(
            'SELECT COUNT(*) as total_count, AVG(amount) as avg_amount FROM data_table',
            filePath,
            'Csv'
        );
        
        const aggregationObjects = await client.executeQueryToObjects(
            'SELECT COUNT(*) as total_count, AVG(amount) as avg_amount FROM data_table',
            filePath,
            'Csv'
        );
        
        console.log('Aggregation results:', aggregationObjects[0]);
        
    } catch (error) {
        console.error('Error:', error.message);
    }
}

// Export for use as a module
module.exports = { ArrowStreamClient };

// Run examples if this script is executed directly
if (require.main === module) {
    main();
}