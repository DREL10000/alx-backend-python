#!/usr/bin/python3
"""
Module for streaming and processing user data in batches from MySQL database.
"""
import mysql.connector
from mysql.connector import Error


def stream_users_in_batches(batch_size):
    """
    Generator function that fetches rows in batches from the user_data table.
    
    Args:
        batch_size (int): Number of rows to fetch in each batch
        
    Yields:
        list: A list of dictionaries containing user data for each batch
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
            
            # Fetch and yield rows in batches
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                yield batch
                
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    
    finally:
        # Clean up resources
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def batch_processing(batch_size):
    """
    Processes each batch to filter users over the age of 25.
    
    Args:
        batch_size (int): Number of rows to fetch in each batch
    """
    # Loop 1: Iterate through batches
    for batch in stream_users_in_batches(batch_size):
        # Loop 2: Iterate through users in each batch
        for user in batch:
            # Filter users over age 25
            if user['age'] > 25:
                print(user)