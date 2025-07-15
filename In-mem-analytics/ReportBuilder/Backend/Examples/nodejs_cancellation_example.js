/**
 * Node.js example demonstrating query cancellation with AbortController
 * Run with: node nodejs_cancellation_example.js
 */

const fetch = require('node-fetch');
const AbortController = require('abort-controller');

const API_BASE = 'http://localhost:5000';

class CancellableQueryClient {
    constructor(baseUrl = API_BASE) {
        this.baseUrl = baseUrl;
        this.activeQueries = new Map();
    }

    async executeArrowQuery(request, options = {}) {
        const queryId = this.generateQueryId();
        const controller = new AbortController();
        
        this.activeQueries.set(queryId, controller);

        try {
            console.log(`[${queryId}] Starting Arrow query...`);
            
            // Set up timeout if specified
            let timeoutId;
            if (options.timeout) {
                timeoutId = setTimeout(() => {
                    console.log(`[${queryId}] Query timed out after ${options.timeout}ms`);
                    controller.abort();
                }, options.timeout);
            }

            const response = await fetch(`${this.baseUrl}/api/ArrowStream/execute`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/vnd.apache.arrow.stream'
                },
                body: JSON.stringify(request),
                signal: controller.signal
            });

            if (timeoutId) {
                clearTimeout(timeoutId);
            }

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`HTTP ${response.status}: ${errorText}`);
            }

            console.log(`[${queryId}] Query completed successfully`);
            console.log(`[${queryId}] Response headers:`, Object.fromEntries(response.headers.entries()));

            // Read response data
            const buffer = await response.buffer();
            console.log(`[${queryId}] Received ${buffer.length} bytes of Arrow data`);

            return {
                queryId,
                success: true,
                data: buffer,
                headers: Object.fromEntries(response.headers.entries())
            };

        } catch (error) {
            if (error.name === 'AbortError') {
                console.log(`[${queryId}] [CANCELLED] Query was cancelled`);
                return {
                    queryId,
                    success: false,
                    cancelled: true,
                    error: '[CANCELLED] Query aborted by client'
                };
            } else {
                console.error(`[${queryId}] Query failed:`, error.message);
                return {
                    queryId,
                    success: false,
                    cancelled: false,
                    error: error.message
                };
            }
        } finally {
            this.activeQueries.delete(queryId);
        }
    }

    async executePaginatedQuery(request, options = {}) {
        const queryId = this.generateQueryId();
        const controller = new AbortController();
        
        this.activeQueries.set(queryId, controller);

        try {
            console.log(`[${queryId}] Starting paginated query...`);

            // Set up timeout if specified
            let timeoutId;
            if (options.timeout) {
                timeoutId = setTimeout(() => {
                    console.log(`[${queryId}] Query timed out after ${options.timeout}ms`);
                    controller.abort();
                }, options.timeout);
            }

            const response = await fetch(`${this.baseUrl}/api/PaginatedQuery/execute`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(request),
                signal: controller.signal
            });

            if (timeoutId) {
                clearTimeout(timeoutId);
            }

            const result = await response.json();

            if (response.ok) {
                console.log(`[${queryId}] Paginated query completed successfully`);
                return {
                    queryId,
                    success: true,
                    data: result
                };
            } else {
                throw new Error(result.errorMessage || 'Unknown error');
            }

        } catch (error) {
            if (error.name === 'AbortError') {
                console.log(`[${queryId}] [CANCELLED] Paginated query was cancelled`);
                return {
                    queryId,
                    success: false,
                    cancelled: true,
                    error: '[CANCELLED] Query aborted by client'
                };
            } else {
                console.error(`[${queryId}] Paginated query failed:`, error.message);
                return {
                    queryId,
                    success: false,
                    cancelled: false,
                    error: error.message
                };
            }
        } finally {
            this.activeQueries.delete(queryId);
        }
    }

    cancelQuery(queryId) {
        const controller = this.activeQueries.get(queryId);
        if (controller) {
            console.log(`[${queryId}] Cancelling query...`);
            controller.abort();
            return true;
        }
        return false;
    }

    cancelAllQueries() {
        console.log(`Cancelling all active queries (${this.activeQueries.size})...`);
        for (const [queryId, controller] of this.activeQueries) {
            console.log(`[${queryId}] Cancelling...`);
            controller.abort();
        }
    }

    getActiveQueryCount() {
        return this.activeQueries.size;
    }

    generateQueryId() {
        return Math.random().toString(36).substring(2, 10);
    }
}

// Demo functions
async function demonstrateCancellation() {
    console.log('=== Query Cancellation Demonstration ===\n');

    const client = new CancellableQueryClient();

    // Example 1: Quick cancellation
    console.log('1. Quick Cancellation Example');
    console.log('Starting query and cancelling it immediately...');
    
    const queryPromise = client.executeArrowQuery({
        sqlQuery: 'SELECT * FROM data_table',
        filePath: '/tmp/test_data.csv',
        fileFormat: 'Csv',
        timeoutSeconds: 30
    });

    // Cancel after 100ms
    setTimeout(() => {
        console.log('Cancelling query after 100ms...');
        client.cancelAllQueries();
    }, 100);

    const result1 = await queryPromise;
    console.log('Result:', result1);
    console.log('');

    // Example 2: Timeout cancellation
    console.log('2. Timeout Cancellation Example');
    console.log('Starting query with 2-second timeout...');

    const result2 = await client.executeArrowQuery({
        sqlQuery: 'SELECT COUNT(*) FROM data_table',
        filePath: '/tmp/test_data.csv',
        fileFormat: 'Csv',
        timeoutSeconds: 30
    }, { timeout: 2000 });

    console.log('Result:', result2);
    console.log('');

    // Example 3: Multiple concurrent queries with selective cancellation
    console.log('3. Multiple Concurrent Queries Example');
    console.log('Starting 3 concurrent queries...');

    const queries = [
        client.executeArrowQuery({
            sqlQuery: 'SELECT COUNT(*) FROM data_table',
            filePath: '/tmp/test_data.csv',
            fileFormat: 'Csv'
        }),
        client.executePaginatedQuery({
            sqlQuery: 'SELECT * FROM data_table ORDER BY amount DESC',
            filePath: '/tmp/test_data.csv',
            fileFormat: 'Csv',
            pageNumber: 1,
            pageSize: 100
        }),
        client.executeArrowQuery({
            sqlQuery: 'SELECT AVG(amount) FROM data_table',
            filePath: '/tmp/test_data.csv',
            fileFormat: 'Csv'
        })
    ];

    // Cancel all after 1 second
    setTimeout(() => {
        console.log(`Cancelling all queries (${client.getActiveQueryCount()} active)...`);
        client.cancelAllQueries();
    }, 1000);

    const results = await Promise.allSettled(queries);
    results.forEach((result, index) => {
        console.log(`Query ${index + 1}:`, result.value || result.reason);
    });

    console.log('\n=== Demonstration Complete ===');
}

// Process handling for graceful shutdown
process.on('SIGINT', () => {
    console.log('\nReceived SIGINT. Cancelling all queries and exiting...');
    
    // If there's a global client instance, cancel all queries
    if (typeof client !== 'undefined') {
        client.cancelAllQueries();
    }
    
    process.exit(0);
});

// Interactive mode
async function interactiveMode() {
    const readline = require('readline');
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    const client = new CancellableQueryClient();
    let activeQueryPromise = null;

    console.log('=== Interactive Query Cancellation Demo ===');
    console.log('Commands:');
    console.log('  1: Start Arrow query');
    console.log('  2: Start paginated query');
    console.log('  c: Cancel all queries');
    console.log('  s: Show active query count');
    console.log('  q: Quit');
    console.log('');

    const askCommand = () => {
        rl.question('Enter command: ', async (command) => {
            switch (command.toLowerCase()) {
                case '1':
                    console.log('Starting Arrow query...');
                    activeQueryPromise = client.executeArrowQuery({
                        sqlQuery: 'SELECT * FROM data_table WHERE amount > 100',
                        filePath: '/tmp/test_data.csv',
                        fileFormat: 'Csv',
                        timeoutSeconds: 60
                    });
                    
                    activeQueryPromise.then(result => {
                        console.log('Arrow query result:', result);
                        askCommand();
                    });
                    break;

                case '2':
                    console.log('Starting paginated query...');
                    activeQueryPromise = client.executePaginatedQuery({
                        sqlQuery: 'SELECT customer_id, amount FROM data_table ORDER BY amount DESC',
                        filePath: '/tmp/test_data.csv',
                        fileFormat: 'Csv',
                        pageNumber: 1,
                        pageSize: 50,
                        includeTotalCount: true
                    });
                    
                    activeQueryPromise.then(result => {
                        console.log('Paginated query result:', result);
                        askCommand();
                    });
                    break;

                case 'c':
                    console.log('Cancelling all queries...');
                    client.cancelAllQueries();
                    askCommand();
                    break;

                case 's':
                    console.log(`Active queries: ${client.getActiveQueryCount()}`);
                    askCommand();
                    break;

                case 'q':
                    console.log('Cancelling all queries and exiting...');
                    client.cancelAllQueries();
                    rl.close();
                    process.exit(0);
                    break;

                default:
                    console.log('Unknown command. Try again.');
                    askCommand();
                    break;
            }
        });
    };

    askCommand();
}

// Main execution
if (require.main === module) {
    const mode = process.argv[2];
    
    if (mode === 'interactive' || mode === '-i') {
        interactiveMode();
    } else {
        demonstrateCancellation().catch(console.error);
    }
}

module.exports = { CancellableQueryClient };