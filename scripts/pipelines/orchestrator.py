import os
import sys
from datetime import datetime
import json
import sqlite3
import re
import io
from contextlib import redirect_stdout

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from scripts.pdf.pdf_download import execute_pdf_download, execute_pdf_download_with_url
from scripts.utils.completions_general import generate_output
from scripts.utils.parse_load import parse_and_load
from scripts.pdf.pdf_hash import check_hash_and_extract_release_date
from scripts.utils.check_date import check_and_update_release_date
from scripts.html_scraping.adp_html import process_adp_html
from scripts.html_scraping.conf_html import process_conference_board_html
from scripts.html_scraping.ny_html import process_ny_html
from scripts.link_scraping.bea_link import process_bea_link
from scripts.link_scraping.fhfa_link import process_fhfa_link
from scripts.link_scraping.nar_link import process_nar_link
from scripts.link_scraping.sca_link import process_sca_link

# List of allowed document IDs
ALLOWED_DOCUMENT_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]

# Mapping of document_id to processing functions
PROCESSING_FUNCTIONS = {
    1: process_conference_board_html,
    2: process_conference_board_html,
    3: process_sca_link,
    5: process_fhfa_link,
    11: process_nar_link,
    12: process_bea_link,
    13: process_bea_link,
    14: process_bea_link,
    17: process_ny_html,
    18: process_adp_html,
}

def get_document_details(document_id, db_path='data/database/database.sqlite'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pipe_id, path FROM documents_table WHERE document_id = ?
    """, (document_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def is_already_processed(document_id, release_date, db_path='data/database/database.sqlite'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM summary_table WHERE document_id = ? AND release_date = ?
    """, (document_id, release_date))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

def process_html_content(process_func, url, document_id, pipe_id):
    with io.StringIO() as buf, redirect_stdout(buf):
        process_func(url, document_id, pipe_id)
        log_message = buf.getvalue()

    # Determine the correct path based on document_id
    if document_id in [3, 5]:
        file_path_match = re.search(r'data/raw/pdf[\\/](\d+_\d+_\d{8}\.pdf)', log_message)
        base_path = "data/raw/pdf"
    else:
        file_path_match = re.search(r'data/raw/txt[\\/](\d+_\d+_\d{8}\.txt)', log_message)
        base_path = "data/raw/txt"

    if file_path_match:
        file_name = file_path_match.group(1)
        file_path = f"{base_path}/{file_name}"
        release_date_match = re.search(r'_(\d{8})\.(txt|pdf)$', file_name)
        if release_date_match:
            release_date = release_date_match.group(1)
            return file_path, release_date
        else:
            raise ValueError(f"Failed to extract release date from {file_name}")
    else:
        raise ValueError(f"Failed to extract file path from log message: {log_message}")

def process_pdf_content(document_id, url, pipe_id):
    with io.StringIO() as buf, redirect_stdout(buf):
        if document_id in [3, 5]:
            # For document IDs 3 and 5, use the specific link scraping functions
            if document_id == 3:
                process_sca_link(url, document_id, pipe_id)
            elif document_id == 5:
                process_fhfa_link(url, document_id, pipe_id)
        else:
            execute_pdf_download(document_id)
        log_message = buf.getvalue()

    print(f"Log message for document_id {document_id}: {log_message}")  # Debugging: Print the log message

    if document_id in [3, 5]:
        # Adjust the regex to handle backslashes for document IDs 3 and 5
        pdf_path_match = re.search(r'PDF renamed and saved to (data[\\/]raw[\\/]pdf[\\/]\d+_\d+_\d{8}\.pdf)', log_message)
    else:
        # Original regex for other document IDs
        pdf_path_match = re.search(r'PDF downloaded successfully: (data[\\/]raw[\\/]pdf[\\/]\d+_\d+_\d{8}\.pdf)', log_message)

    if pdf_path_match:
        pdf_path = pdf_path_match.group(1).replace('\\', '/')  # Normalize path to use forward slashes
        release_date_match = re.search(r'_(\d{8})\.pdf$', pdf_path)
        if release_date_match:
            release_date = release_date_match.group(1)
            return pdf_path, release_date, None
        else:
            raise ValueError(f"Failed to extract release date from {pdf_path}")
    else:
        raise ValueError(f"Failed to extract PDF path from log message: {log_message}")

def run_pipeline(document_id):
    if document_id not in ALLOWED_DOCUMENT_IDS:
        return "Document ID not allowed"

    details = get_document_details(document_id)
    if not details:
        return f"No details found for document_id {document_id}"

    pipe_id, url = details
    release_date = None
    txt_path = None
    pdf_path = None

    try:
        if document_id in PROCESSING_FUNCTIONS:
            txt_path, release_date = process_html_content(PROCESSING_FUNCTIONS[document_id], url, document_id, pipe_id)
        elif document_id in [3, 5]:
            pdf_path, release_date, message = process_pdf_content(document_id, url, pipe_id)
            if message:
                return message
        elif document_id in [4, 6, 7, 8, 9, 10, 15, 16, 19, 20, 21]:
            pdf_path, release_date, message = process_pdf_content(document_id, url, pipe_id)
            if message:
                return message

            # Step 2: Check hash and extract release date for PDFs
            result = check_hash_and_extract_release_date(pdf_path)
            
            if "Hash matches the previous one. No update needed." in result:
                return "Hash matches the previous one. No update needed."

            try:
                response = json.loads(result)
            except json.JSONDecodeError as e:
                return f"Failed to parse JSON output: {e}. Output was: {result}"

            if response["status"] == "no_update":
                return response["message"]

            if response["status"] == "error":
                return response["message"]

            release_date = response.get("release_date")
            pdf_path = response.get("updated_pdf_path")

            if not release_date or not pdf_path:
                return "Failed to extract release date or updated PDF path"

        # Check if the content has already been processed
        if release_date and is_already_processed(document_id, release_date):
            return "Content already up-to-date. No processing needed."

        # Check and update release date
        if release_date and not check_and_update_release_date(document_id, release_date):
            return "Execution interrupted due to release date mismatch."

        # Generate output
        if pdf_path:
            generate_output(pdf_path)
        elif txt_path:
            generate_output(txt_path)

        # Parse and load
        processed_file_path = f"data/processed/{document_id}_{pipe_id}_{release_date}.txt"
        parse_and_load(processed_file_path)

        return "Pipeline executed successfully"
    except Exception as e:
        return f"Error occurred: {e}"

if __name__ == "__main__":
    # Example usage
    document_ids = range(1, 6)  # Adjusted to include document IDs 3 to 5 for testing
    statuses = []
    for document_id in document_ids:
        try:
            result = run_pipeline(document_id)
            status = "success" if "successfully" in result or "No update needed" in result or "No processing needed" in result else "failed"
        except Exception as e:
            result = f"Error occurred: {e}"
            status = "failed"
        print(f"Result for document_id {document_id}: {result}")
        statuses.append({"document_id": document_id, "status": status})
    print(statuses)
