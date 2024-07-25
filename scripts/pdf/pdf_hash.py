import hashlib
import sqlite3
import os
import sys
import shutil
import re
import json
from datetime import datetime

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from scripts.utils.completions_general import extract_release_date

def get_previous_hash(document_id, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT hash FROM documents_table WHERE document_id = ?
        """, (document_id,))
        result = cursor.fetchone()
        conn.close()
        print(f"Fetched previous hash for document_id {document_id}: {result[0] if result else 'None'}")
        return result[0] if result else None
    except sqlite3.Error as e:
        print(f"Database error occurred while fetching previous hash: {e}")
        return None

def update_hash(document_id, new_hash, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE documents_table SET hash = ? WHERE document_id = ?
        """, (new_hash, document_id))
        conn.commit()
        conn.close()
        print(f"Updated hash for document_id {document_id} to {new_hash}")
    except sqlite3.Error as e:
        print(f"Database error occurred while updating hash: {e}")

def calculate_pdf_hash(pdf_path, num_chars=300):
    pdf_path = os.path.abspath(pdf_path)
    try:
        with open(pdf_path, 'rb') as file:
            content = file.read(num_chars)
            pdf_hash = hashlib.sha256(content).hexdigest()
            print(f"Generated hash for PDF ({pdf_path}): {pdf_hash}")
            return pdf_hash
    except Exception as e:
        print(f"Failed to calculate PDF hash: {e}")
        return None

def update_current_release_date(document_id, release_date, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE documents_table SET current_release_date = ? WHERE document_id = ?
        """, (release_date, document_id))
        conn.commit()
        conn.close()
        print(f"Updated current release date for document_id {document_id} to {release_date}")
    except sqlite3.Error as e:
        print(f"Database error occurred while updating current release date: {e}")

def check_hash_and_extract_release_date(pdf_path, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    pdf_path = os.path.abspath(pdf_path)

    # Extract document_id from the pdf_path
    match = re.search(r'(\d+)_(\d+)_(\d{8})\.pdf$', pdf_path.replace('\\', '/'))
    if not match:
        print("Invalid PDF path format. Expected format: 'data/raw/pdf/{document_id}_{pipe_id}_{release_date}.pdf'")
        return json.dumps({"status": "error", "message": "Invalid PDF path format"})

    document_id, pipe_id, release_date = match.groups()

    previous_hash = get_previous_hash(document_id, db_path)
    new_hash = calculate_pdf_hash(pdf_path)

    if new_hash is None:
        print("Failed to calculate new hash.")
        return json.dumps({"status": "error", "message": "Failed to calculate new hash"})

    print(f"Previous hash: {previous_hash}")
    print(f"New hash: {new_hash}")

    if previous_hash is None or previous_hash != new_hash:
        update_hash(document_id, new_hash, db_path)
        release_date = extract_release_date(pdf_path)

        if release_date:
            print(f"Extracted release date: {release_date}")
            valid_release_date = release_date.strip("*")
            new_file_name = os.path.join(os.path.dirname(pdf_path), f"{document_id}_{pipe_id}_{valid_release_date}.pdf")

            if not os.path.exists(os.path.dirname(new_file_name)):
                os.makedirs(os.path.dirname(new_file_name))

            shutil.move(pdf_path, new_file_name)
            print(f"File renamed to: {new_file_name}")
            update_current_release_date(document_id, valid_release_date, db_path)
            return json.dumps({"status": "success", "release_date": valid_release_date, "updated_pdf_path": new_file_name})
        else:
            print("Failed to extract release date.")
            return json.dumps({"status": "error", "message": "Failed to extract release date"})
    else:
        print("Hash matches the previous one. No update needed.")
        return json.dumps({"status": "no_update", "message": "Hash matches the previous one. No update needed."})
