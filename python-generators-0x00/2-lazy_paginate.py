#!/usr/bin/python3
"""
Module for lazy pagination of user data from MySQL database.
"""
import mysql.connector
from mysql.connector import Error


def paginate_users(page_size, offset):
    """
    Fetches a page of users from the database at a specific offset.
    
    Args:
        page_size (int): Number of rows to fetch per page
        offset (int): Starting position for fetching rows
        
    Returns:
        list: List of dictionaries containing user data for the page
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
            cursor.execute(f"SELECT * FROM user_data LIMIT {page_size} OFFSET {offset}")
            rows = cursor.fetchall()
            return rows
            
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return []
    
    finally:
        # Clean up resources
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def lazy_pagination(page_size):
    """
    Generator function that lazily loads pages of user data.
    Fetches the next page only when needed.
    
    Args:
        page_size (int): Number of rows to fetch per page
        
    Yields:
        list: A list of dictionaries containing user data for each page
    """
    offset = 0
    
    # Single loop: continue fetching pages until no more data
    while True:
        page = paginate_users(page_size, offset)
        
        # Stop if no more rows are returned
        if not page:
            break
            
        yield page
        offset += page_size