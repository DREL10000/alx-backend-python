#!/usr/bin/python3
"""
Module for computing memory-efficient aggregate functions using generators.
"""
import mysql.connector
from mysql.connector import Error


def stream_user_ages():
    """
    Generator function that yields user ages one by one from the database.
    
    Yields:
        int: Age of each user
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
            cursor = connection.cursor()
            
            # Execute query to fetch only ages
            cursor.execute("SELECT age FROM user_data")
            
            # Loop 1: Yield ages one by one
            for (age,) in cursor:
                yield age
                
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    
    finally:
        # Clean up resources
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def calculate_average_age():
    """
    Calculates the average age of users without loading the entire dataset into memory.
    Uses the stream_user_ages generator to process ages one at a time.
    """
    total_age = 0
    count = 0
    
    # Loop 2: Iterate through ages from generator
    for age in stream_user_ages():
        total_age += age
        count += 1
    
    # Calculate and print average
    if count > 0:
        average_age = total_age / count
        print(f"Average age of users: {average_age:.2f}")
    else:
        print("No users found in database")


# Execute when script is run directly
if __name__ == "__main__":
    calculate_average_age()