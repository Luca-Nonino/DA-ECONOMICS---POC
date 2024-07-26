import sqlite3
import os

def migrate_dates_to_integer():
    # Define the explicit path for the database file
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
    
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Rename the existing table
    cursor.execute("ALTER TABLE documents_table RENAME TO documents_table_backup")

    # Create the new table with Integer date fields
    cursor.execute("""
    CREATE TABLE documents_table (
        document_id INTEGER PRIMARY KEY,
        path TEXT,
        reference_month TEXT,
        pipe_name TEXT,
        pipe_id INTEGER,
        freq TEXT,
        document_name TEXT,
        source_name TEXT,
        escope TEXT,
        current_release_date INTEGER,
        hash TEXT,
        next_release_date INTEGER,
        next_release_time INTEGER
    )
    """)

    # Copy data from the old table to the new table
    cursor.execute("""
    INSERT INTO documents_table (document_id, path, reference_month, pipe_name, pipe_id, freq, document_name, source_name, escope, current_release_date, hash, next_release_date, next_release_time)
    SELECT document_id, path, reference_month, pipe_name, pipe_id, freq, document_name, source_name, escope, current_release_date, hash, next_release_date, next_release_time
    FROM documents_table_backup
    """)

    # Drop the backup table
    cursor.execute("DROP TABLE documents_table_backup")

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    migrate_dates_to_integer()
