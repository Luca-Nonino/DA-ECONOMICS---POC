import sqlite3
import os

def query_ids():
    # Define the explicit path for the database file
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
    
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query all document_ids from documents_table
    cursor.execute("SELECT document_id FROM documents_table")
    document_ids = cursor.fetchall()
    print("Document IDs in documents_table:")
    for doc_id in document_ids:
        print(doc_id[0])

    # Query all prompt_ids from prompts_table
    cursor.execute("SELECT prompt_id FROM prompts_table")
    prompt_ids = cursor.fetchall()
    print("\nPrompt IDs in prompts_table:")
    for prompt_id in prompt_ids:
        print(prompt_id[0])

    # Close the connection
    conn.close()

if __name__ == "__main__":
    query_ids()
