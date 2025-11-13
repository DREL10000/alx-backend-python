#!/usr/bin/python3
"""
Module for setting up MySQL database and populating with user data.
"""
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import csv
import uuid
import os

load_dotenv()

def connect_db():
    """
    Connects to the MySQL database server.
    
    Returns:
        connection: MySQL connection object or None if connection fails
    """
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password= os.getenv("password")
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def create_database(connection):
    """
    Creates the database ALX_prodev if it does not exist.
    
    Args:
        connection: MySQL connection object
    """
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        cursor.close()
    except Error as e:
        print(f"Error creating database: {e}")


def connect_to_prodev():
    """
    Connects to the ALX_prodev database in MySQL.
    
    Returns:
        connection: MySQL connection object to ALX_prodev or None if connection fails
    """
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password=os.getenv("password"),
            database='ALX_prodev'
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to ALX_prodev: {e}")
        return None


def create_table(connection):
    """
    Creates a table user_data if it does not exist with the required fields.
    
    Args:
        connection: MySQL connection object
    """
    try:
        cursor = connection.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_data (
            user_id CHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            age DECIMAL(3, 0) NOT NULL,
            INDEX idx_user_id (user_id)
        )
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        print("Table user_data created successfully")
        cursor.close()
    except Error as e:
        print(f"Error creating table: {e}")


def insert_data(connection, csv_file):
    """
    Inserts data into the database if it does not exist.
    
    Args:
        connection: MySQL connection object
        csv_file: Path to the CSV file containing user data
    """
    try:
        cursor = connection.cursor()
        
        # Read CSV file and insert data
        with open(csv_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            
            insert_query = """
            INSERT IGNORE INTO user_data (user_id, name, email, age)
            VALUES (%s, %s, %s, %s)
            """
            
            for row in csv_reader:
                # Validate and prepare data
                user_id = row.get('user_id', str(uuid.uuid4()))
                name = row.get('name', '')
                email = row.get('email', '')
                age = row.get('age', 0)
                
                cursor.execute(insert_query, (user_id, name, email, age))
            
            connection.commit()
        
        cursor.close()
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found")
    except Error as e:
        print(f"Error inserting data: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")