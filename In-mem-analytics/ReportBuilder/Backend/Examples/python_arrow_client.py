#!/usr/bin/env python3
"""
Python client for consuming Arrow IPC streams from the ReportBuilder API
Requires: pip install pyarrow pandas requests
"""
import json
import requests
import pyarrow as pa
import pandas as pd
from io import BytesIO
from typing import Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ArrowStreamClient:
    """Client for consuming Arrow IPC streams from ReportBuilder API"""
    
    def __init__(self, base_url: str = "http://localhost:5000"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.apache.arrow.stream'
        })
    
    def validate_query(self, sql_query: str, file_path: str, file_format: str = "Auto") -> Dict[str, Any]:
        """Validate query and get metadata without executing"""
        url = f"{self.base_url}/api/ArrowStream/validate"
        
        payload = {
            "sqlQuery": sql_query,
            "filePath": file_path,
            "fileFormat": file_format,
            "includeMetadata": True
        }
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def execute_query_to_dataframe(self, sql_query: str, file_path: str, 
                                 file_format: str = "Auto", 
                                 timeout_seconds: int = 300) -> pd.DataFrame:
        """Execute query and return results as pandas DataFrame"""
        arrow_table = self.execute_query_to_arrow(sql_query, file_path, file_format, timeout_seconds)
        return arrow_table.to_pandas()
    
    def execute_query_to_arrow(self, sql_query: str, file_path: str, 
                             file_format: str = "Auto", 
                             timeout_seconds: int = 300) -> pa.Table:
        """Execute query and return results as Arrow Table"""
        url = f"{self.base_url}/api/ArrowStream/execute"
        
        payload = {
            "sqlQuery": sql_query,
            "filePath": file_path,
            "fileFormat": file_format,
            "timeoutSeconds": timeout_seconds,
            "includeMetadata": True,
            "enableMemoryLogging": True
        }
        
        logger.info(f"Executing Arrow query: {sql_query}")
        logger.info(f"File: {file_path}, Format: {file_format}")
        
        response = self.session.post(url, json=payload, stream=True)
        response.raise_for_status()
        
        # Check for error response
        if response.headers.get('Content-Type', '').startswith('application/json'):
            error_data = response.json()
            raise Exception(f"Query failed: {error_data.get('errorMessage', 'Unknown error')}")
        
        # Log response headers
        logger.info(f"Response headers: {dict(response.headers)}")
        
        # Read Arrow stream
        arrow_data = BytesIO(response.content)
        
        # Create Arrow IPC stream reader
        reader = pa.ipc.open_stream(arrow_data)
        
        # Read all batches into a table
        table = reader.read_all()
        
        logger.info(f"Arrow table shape: {table.num_rows} rows, {table.num_columns} columns")
        logger.info(f"Arrow table schema: {table.schema}")
        
        return table
    
    def stream_query_batches(self, sql_query: str, file_path: str, 
                           file_format: str = "Auto", 
                           timeout_seconds: int = 300):
        """Stream query results as Arrow record batches"""
        url = f"{self.base_url}/api/ArrowStream/execute"
        
        payload = {
            "sqlQuery": sql_query,
            "filePath": file_path,
            "fileFormat": file_format,
            "timeoutSeconds": timeout_seconds,
            "includeMetadata": True,
            "enableMemoryLogging": True
        }
        
        logger.info(f"Streaming Arrow query: {sql_query}")
        
        response = self.session.post(url, json=payload, stream=True)
        response.raise_for_status()
        
        # Check for error response
        if response.headers.get('Content-Type', '').startswith('application/json'):
            error_data = response.json()
            raise Exception(f"Query failed: {error_data.get('errorMessage', 'Unknown error')}")
        
        # Read Arrow stream
        arrow_data = BytesIO(response.content)
        
        # Create Arrow IPC stream reader
        reader = pa.ipc.open_stream(arrow_data)
        
        # Yield each batch
        for batch in reader:
            yield batch
    
    def get_supported_formats(self) -> Dict[str, Any]:
        """Get supported file formats"""
        url = f"{self.base_url}/api/ArrowStream/formats"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

def main():
    """Example usage of the Arrow Stream Client"""
    
    # Initialize client
    client = ArrowStreamClient("http://localhost:5000")
    
    # Example 1: Get supported formats
    try:
        formats = client.get_supported_formats()
        print("Supported formats:", json.dumps(formats, indent=2))
    except Exception as e:
        print(f"Error getting formats: {e}")
    
    # Example 2: Validate query
    try:
        sql_query = "SELECT * FROM data_table LIMIT 10"
        file_path = "/path/to/your/data.csv"  # Update this path
        
        validation = client.validate_query(sql_query, file_path, "Csv")
        print("Validation result:", json.dumps(validation, indent=2))
    except Exception as e:
        print(f"Error validating query: {e}")
    
    # Example 3: Execute query and get DataFrame
    try:
        sql_query = "SELECT * FROM data_table WHERE amount > 100 LIMIT 1000"
        file_path = "/path/to/your/data.csv"  # Update this path
        
        df = client.execute_query_to_dataframe(sql_query, file_path, "Csv")
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {df.columns.tolist()}")
        print(f"DataFrame head:\n{df.head()}")
    except Exception as e:
        print(f"Error executing query: {e}")
    
    # Example 4: Stream query results in batches
    try:
        sql_query = "SELECT customer_id, amount FROM data_table ORDER BY amount DESC LIMIT 5000"
        file_path = "/path/to/your/data.csv"  # Update this path
        
        total_rows = 0
        for batch in client.stream_query_batches(sql_query, file_path, "Csv"):
            total_rows += len(batch)
            print(f"Received batch with {len(batch)} rows")
            
            # Process batch as needed
            batch_df = batch.to_pandas()
            print(f"Batch columns: {batch_df.columns.tolist()}")
            
            # Stop after processing a few batches for demo
            if total_rows > 2000:
                break
        
        print(f"Total rows streamed: {total_rows}")
    except Exception as e:
        print(f"Error streaming query: {e}")
    
    # Example 5: Working with Arrow Table directly
    try:
        sql_query = "SELECT COUNT(*) as total_count, AVG(amount) as avg_amount FROM data_table"
        file_path = "/path/to/your/data.csv"  # Update this path
        
        arrow_table = client.execute_query_to_arrow(sql_query, file_path, "Csv")
        
        print(f"Arrow table schema: {arrow_table.schema}")
        print(f"Arrow table data: {arrow_table.to_pydict()}")
        
        # Convert to pandas for analysis
        df = arrow_table.to_pandas()
        print(f"Analysis results: {df.iloc[0].to_dict()}")
    except Exception as e:
        print(f"Error with Arrow table: {e}")

if __name__ == "__main__":
    main()