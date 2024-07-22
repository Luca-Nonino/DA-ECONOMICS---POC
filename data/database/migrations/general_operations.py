# data/database/migrations/populate_country.py
import sqlite3
import os

def add_country_column():
    # Define the explicit path for the database file
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
    
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Add the country column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE documents_table ADD COLUMN country VARCHAR")
        print("Column 'country' added.")
    except sqlite3.OperationalError as e:
        if 'duplicate column name' in str(e):
            print("Column 'country' already exists. Skipping add.")
        else:
            raise

    # Close the connection
    conn.close()

def populate_country():
    # Define the explicit path for the database file
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Update all entries to set the country to "US"
    try:
        cursor.execute("UPDATE documents_table SET country = 'US'")
        conn.commit()
        print("Country column populated with 'US' for all records.")
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()

if __name__ == "__main__":
    add_country_column()  # Ensure the column is added
    populate_country()    # Populate the column with "US"
