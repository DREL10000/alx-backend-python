#!/usr/bin/python3
# 1-batch_processing.py

import mysql.connector
from typing import Dict, Generator, List
from mysql.connector import Error

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Add your MySQL password here if needed
    'database': 'ALX_prodev',
    'port': 3306
}


def stream_users_in_batches(batch_size: int) -> Generator[List[Dict[str, any]], None, None]:
    """
    Generator function that fetches rows from user_data table in batches.
    
    Args:
        batch_size: Number of rows to fetch in each batch
        
    Yields:
        List of dictionaries containing user data for each batch
    """
    connection = None
    cursor = None
    
    try:
        # Connect to the database
        connection = mysql.connector.connect(**DB_CONFIG)
        
        if not connection.is_connected():
            raise ConnectionError("Failed to connect to database")
        
        # Create a cursor with dictionary=True to get rows as dictionaries
        cursor = connection.cursor(dictionary=True)
        
        # Execute query to select all users
        query = "SELECT user_id, name, email, age FROM user_data"
        cursor.execute(query)
        
        # Loop 1: Fetch and yield batches
        while True:
            # Fetch batch_size rows
            batch = cursor.fetchmany(batch_size)
            
            # If no more rows, break the loop
            if not batch:
                break
            
            # Yield the current batch
            yield batch
            
    except Error as e:
        print(f"Database error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        # Clean up resources
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def batch_processing(batch_size: int = 50) -> None:
    """
    Processes batches of users, filtering those over age 25.
    
    Args:
        batch_size: Number of rows to process in each batch
    """
    try:
        # Loop 2: Iterate through batches from the generator
        for batch in stream_users_in_batches(batch_size):
            # Loop 3: Process each user in the current batch
            for user in batch:
                # Filter users over age 25
                if user['age'] > 25:
                    print(user)
                    
    except KeyboardInterrupt:
        # Handle keyboard interrupt gracefully
        pass
    except Exception as e:
        print(f"Processing error: {e}", file=sys.stderr)


# Alternative implementation with exactly 3 loops total
def batch_processing_concise(batch_size: int = 50) -> None:
    """
    Alternative implementation with exactly 3 loops total.
    
    Args:
        batch_size: Number of rows to process in each batch
    """
    connection = None
    cursor = None
    
    try:
        # Connect to the database
        connection = mysql.connector.connect(**DB_CONFIG)
        
        if not connection.is_connected():
            return
        
        # Create dictionary cursor
        cursor = connection.cursor(dictionary=True)
        
        # Execute query
        query = "SELECT user_id, name, email, age FROM user_data"
        cursor.execute(query)
        
        # Loop 1: Fetch batches
        while True:
            # Fetch a batch
            batch = cursor.fetchmany(batch_size)
            
            if not batch:
                break
            
            # Loop 2: Process users in batch
            for user in batch:
                # Filter users over age 25
                if user['age'] > 25:
                    print(user)
                    
    except Error as e:
        print(f"Database error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


# For backward compatibility and testing
if __name__ == "__main__":
    import sys
    
    # Test the functions
    if len(sys.argv) > 1:
        batch_size = int(sys.argv[1])
    else:
        batch_size = 50
    
    batch_processing(batch_size)