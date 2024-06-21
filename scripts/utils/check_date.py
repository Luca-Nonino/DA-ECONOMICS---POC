import os
import sqlite3

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

def get_current_release_date(document_id, db_path=os.path.join(BASE_DIR, 'data/database/database.sqlite')):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT current_release_date FROM documents_table WHERE document_id = ?
    """, (document_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def update_release_date(document_id, new_date, db_path=os.path.join(BASE_DIR, 'data/database/database.sqlite')):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE documents_table SET current_release_date = ? WHERE document_id = ?
    """, (new_date, document_id))
    conn.commit()
    conn.close()

def check_and_update_release_date(document_id, new_date, db_path=os.path.join(BASE_DIR, 'data/database/database.sqlite')):
    current_date = get_current_release_date(document_id, db_path)
    
    if current_date is None or current_date != new_date:
        update_release_date(document_id, new_date, db_path)
        print(f"Release date updated to {new_date} for document ID {document_id}")
        return True
    else:
        print(f"Release date for document ID {document_id} is already up to date.")
        return False




############################# Test Examples #################################


# Example usage
document_id = 4  # Replace with actual document_id for testing
new_date = '20240527'  # Replace with actual new date for testing
#check_and_update_release_date(document_id, new_date)
