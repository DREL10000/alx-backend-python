# seed.py
import mysql.connector
import csv
import uuid
from mysql.connector import Error
from typing import Optional, List, Tuple, Any, Dict

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Add your MySQL password here if needed
    'port': 3306
}

DB_NAME = 'ALX_prodev'


def connect_db() -> Optional[mysql.connector.MySQLConnection]:
    """
    Connects to the MySQL database server.
    
    Returns:
        MySQLConnection object if successful, None otherwise
    """
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    return None


def create_database(connection: mysql.connector.MySQLConnection) -> None:
    """
    Creates the database ALX_prodev if it does not exist.
    
    Args:
        connection: MySQL connection object
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        print(f"Database {DB_NAME} created or already exists")
    except Error as e:
        print(f"Error creating database: {e}")
    finally:
        cursor.close()


def connect_to_prodev() -> Optional[mysql.connector.MySQLConnection]:
    """
    Connects to the ALX_prodev database in MySQL.
    
    Returns:
        MySQLConnection object if successful, None otherwise
    """
    try:
        config = DB_CONFIG.copy()
        config['database'] = DB_NAME
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to {DB_NAME} database: {e}")
    return None


def create_table(connection: mysql.connector.MySQLConnection) -> None:
    """
    Creates a table user_data if it does not exist with the required fields.
    
    Args:
        connection: MySQL connection object
    """
    cursor = connection.cursor()
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_data (
            user_id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            age DECIMAL(5, 2) NOT NULL,
            INDEX idx_user_id (user_id)
        )
        """
        cursor.execute(create_table_query)
        print("Table user_data created successfully")
    except Error as e:
        print(f"Error creating table: {e}")
    finally:
        cursor.close()


def insert_data(connection: mysql.connector.MySQLConnection, csv_file: str) -> None:
    """
    Inserts data in the database if it does not exist.
    
    Args:
        connection: MySQL connection object
        csv_file: Path to the CSV file containing user data
    """
    cursor = connection.cursor()
    
    try:
        # Read data from CSV file
        with open(csv_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            rows_to_insert = []
            
            for row in csv_reader:
                # Generate UUID if not present in CSV, otherwise use existing
                user_id = row.get('user_id', str(uuid.uuid4()))
                name = row['name']
                email = row['email']
                age = float(row['age'])
                
                rows_to_insert.append((user_id, name, email, age))
        
        # Insert data in batches
        insert_query = """
        INSERT IGNORE INTO user_data (user_id, name, email, age)
        VALUES (%s, %s, %s, %s)
        """
        
        # Use executemany for batch insert
        cursor.executemany(insert_query, rows_to_insert)
        connection.commit()
        
        print(f"Inserted {cursor.rowcount} rows into user_data table")
        
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found")
    except KeyError as e:
        print(f"Error: Missing required column in CSV file: {e}")
    except Error as e:
        print(f"Error inserting data: {e}")
        connection.rollback()
    finally:
        cursor.close()


# Generator function for streaming rows one by one
def stream_rows(connection: mysql.connector.MySQLConnection, 
                query: str, 
                params: Tuple = None) -> Any:
    """
    Generator that streams rows from an SQL database one by one.
    
    Args:
        connection: MySQL connection object
        query: SQL query to execute
        params: Parameters for the query (optional)
    
    Yields:
        One row at a time from the query result
    """
    cursor = None
    try:
        # Use a server-side cursor for streaming
        cursor = connection.cursor(buffered=False)
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Stream rows one by one
        row = cursor.fetchone()
        while row is not None:
            yield row
            row = cursor.fetchone()
            
    except Error as e:
        print(f"Error streaming rows: {e}")
    finally:
        if cursor:
            cursor.close()


# Additional utility function to demonstrate the generator
def get_all_users_generator(connection: mysql.connector.MySQLConnection):
    """
    Example usage of the generator to stream all users.
    
    Args:
        connection: MySQL connection object
    
    Yields:
        One user row at a time
    """
    query = "SELECT * FROM user_data"
    return stream_rows(connection, query)


if __name__ == "__main__":
    # For testing the module directly
    connection = connect_db()
    if connection:
        create_database(connection)
        connection.close()
        print("Connection successful")
        
        connection = connect_to_prodev()
        if connection:
            create_table(connection)
            # Note: You'll need to update the path to your CSV file
            insert_data(connection, 'user_data.csv')
            
            # Test the generator
            print("\nStreaming first 5 users using generator:")
            user_generator = get_all_users_generator(connection)
            
            for i, user in enumerate(user_generator):
                print(user)
                if i >= 4:  # Show only first 5
                    break
            
            connection.close()