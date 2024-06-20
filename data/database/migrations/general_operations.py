import sqlite3
import os

def inspect_document():
    # Define the explicit path for the database file
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
    
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Execute the query
    cursor.execute("SELECT document_id, current_release_date, hash FROM documents_table WHERE document_id = 3")
    row = cursor.fetchone()

    if row:
        document_id, current_release_date, hash_value = row
        print(f"Document ID: {document_id}")
        print(f"Current Release Date: {current_release_date} (Type: {type(current_release_date)})")
    else:
        print("No document found with document_id = 3")

    # Close the connection
    conn.close()

#if __name__ == "__main__":
    #inspect_document()

import sqlite3
import os

def update_document_path(document_id, new_path):
    # Define the explicit path for the database file
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Update the path for the specified document_id
        cursor.execute("UPDATE documents_table SET path = ? WHERE document_id = ?", (new_path, document_id))
        
        # Commit the transaction
        conn.commit()
        print(f"Document ID {document_id} path updated to {new_path}")
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()




import sys
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure the root of the project is in the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import all models before querying
from data.database.documents_table import DocumentsTable
from data.database.prompts_table import PromptsTable
from data.database.summary_table import SummaryTable
from data.database.keytakeaways_table import KeyTakeawaysTable
from data.database.analysis_table import AnalysisTable
from data.database.users_table import User, Base

def create_users_table_if_not_exists():
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)

def delete_and_update_records():
    # Define the explicit path for the database file
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
    engine = create_engine(f'sqlite:///{db_path}')
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Delete records from key_takeaways_table where document_id = 1
        session.query(KeyTakeawaysTable).filter_by(document_id=1).delete()

        # Delete records from summary_table where document_id = 1
        session.query(SummaryTable).filter_by(document_id=1).delete()

        # Delete records from analysis_table where document_id = 1
        session.query(AnalysisTable).filter_by(document_id=1).delete()

        # Delete records from users_table where id = 1
        session.query(User).filter_by(id=1).delete()

        # Clear the values of current_release_date and hash in documents_table where document_id = 1
        document = session.query(DocumentsTable).filter_by(document_id=1).first()
        if document:
            # Print current values
            print(f"Current Release Date (before clearing): {document.current_release_date} (Type: {type(document.current_release_date)})")
            # Ensure the date is cleared
            document.current_release_date = None
            # Print values after clearing
            print(f"Current Release Date (after clearing): {document.current_release_date}")

        session.commit()
        print("Records deleted and updated successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()



if __name__ == "__main__":
    document_id = 5
    new_path = "https://www.fhfa.gov/data/hpi"
    update_document_path(document_id, new_path)