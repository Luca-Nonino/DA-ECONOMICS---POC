import sqlite3
import pandas as pd
import os

def export_sqlite_tables_to_csv(db_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)

    try:
        # Get a list of all tables in the database
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]

        # Iterate through each table and export to CSV
        for table_name in tables:
            print(f"Exporting table: {table_name}")
            
            # Read the table into a pandas DataFrame
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            
            # Create the CSV filename
            csv_filename = f"{table_name}.csv"
            
            # Export the DataFrame to CSV
            df.to_csv(csv_filename, index=False)
            
            print(f"Table '{table_name}' exported successfully to {csv_filename}")

    finally:
        # Close the connection
        conn.close()

# Specify the path to your SQLite database
db_path = r"data\database\database.sqlite"

# Call the function to export tables
export_sqlite_tables_to_csv(db_path)

print("All tables have been exported to CSV files.")
