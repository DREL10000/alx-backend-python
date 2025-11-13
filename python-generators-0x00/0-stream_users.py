#!/usr/bin/python3
"""
Module for streaming user data from MySQL database using generators.
"""
import mysql.connector
from mysql.connector import Error


def stream_users():
    """
    Generator function that fetches rows one by one from the user_data table.
    
    Yields:
        dict: A dictionary containing user data with keys: user_id, name, email, age
    """
    connection = None
    cursor = None
    
    try:
        # Connect to the ALX_prodev database
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',  # Update with your MySQL password
            database='ALX_prodev'
        )
        
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            
            # Execute query to fetch all rows
            cursor.execute("SELECT * FROM user_data")
            
            # Yield rows one by one
            for row in cursor:
                yield row
                
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    
    finally:
        # Clean up resources
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()