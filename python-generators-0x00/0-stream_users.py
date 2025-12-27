#!/usr/bin/python3
# 0-stream_users.py (concise version)

import mysql.connector
from typing import Dict, Generator
from mysql.connector import Error

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Add your MySQL password here if needed
    'database': 'ALX_prodev',
    'port': 3306
}


def stream_users() -> Generator[Dict[str, any], None, None]:
    """
    Generator function that streams rows one by one from the user_data table.
    Uses only one loop as required.
    
    Yields:
        Dictionary containing user data for one row at a time
    """
    try:
        # Connect to the database
        connection = mysql.connector.connect(**DB_CONFIG)
        
        if not connection.is_connected():
            return  # Early return if connection fails
        
        # Create dictionary cursor
        with connection.cursor(dictionary=True) as cursor:
            # Execute query
            cursor.execute("SELECT * FROM user_data")
            
            # Single loop with yield - this is the key requirement
            for row in cursor:
                yield row
                
    except Error as e:
        print(f"Database error: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()


# Minimal version that strictly follows requirements
def stream_users_minimal() -> Generator[Dict[str, any], None, None]:
    """
    Minimal implementation with exactly one loop as required.
    
    Yields:
        Dictionary containing user data for one row at a time
    """
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM user_data")
    
    # Exactly one loop as required
    row = cursor.fetchone()
    while row:
        yield row
        row = cursor.fetchone()
    
    cursor.close()
    connection.close()