import os
import sys
from datetime import datetime
import json
import sqlite3
import re
import io
from contextlib import redirect_stdout
import logging

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(filename=os.path.join(project_root, 'app', 'logs', 'orchestrator_br.log'), 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Importing script functions
from scripts.html_scraping.mdic_html import process_balança_comercial_html
from scripts.link_scraping.ibge_link import process_ibge_link
from scripts.link_scraping.anfavea_link import process_anfavea_link
from scripts.api_scraping.bcb_api import process_bcb_api
from scripts.html_scraping.bcb_html import process_bcb_html
from scripts.pdf.pdf_download import execute_pdf_download, execute_pdf_download_with_url
from scripts.utils.completions_general import generate_output
from scripts.utils.parse_load import parse_and_load
from scripts.pdf.pdf_hash import check_hash_and_extract_release_date
from scripts.utils.check_date import check_and_update_release_date

# Mapping document IDs to processing functions
PROCESSING_FUNCTIONS = {
    22: process_anfavea_link,
    23: process_bcb_api,
    24: process_bcb_html,
    25: process_ibge_link,
    26: process_ibge_link,
    27: process_ibge_link,
    28: process_ibge_link,
    29: process_ibge_link,
    30: process_balança_comercial_html
}

def get_document_details(document_id, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pipe_id, path FROM documents_table WHERE document_id = ?
    """, (document_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def process_output(file_path, document_id, pipe_id, release_date):
    if file_path.endswith('.txt'):
        # Handle TXT output
        try:
            generate_output(file_path)
            processed_file_path = os.path.join(project_root, f"data/processed/{document_id}_{pipe_id}_{release_date}.txt")
            parse_and_load(processed_file_path)
        except Exception as e:
            logger.error(f"Error processing TXT file for document_id {document_id}: {e}", exc_info=True)
    elif file_path.endswith('.pdf'):
        # Handle PDF output
        try:
            result = check_hash_and_extract_release_date(file_path)
            response = json.loads(result)
            if response.get("status") == "success":
                updated_pdf_path = response.get("updated_pdf_path")
                generate_output(updated_pdf_path)
                processed_file_path = os.path.join(project_root, f"data/processed/{document_id}_{pipe_id}_{release_date}.txt")
                parse_and_load(processed_file_path)
            else:
                logger.error(f"Error processing PDF file for document_id {document_id}: {response.get('message')}")
        except Exception as e:
            logger.error(f"Error processing PDF file for document_id {document_id}: {e}", exc_info=True)

def run_pipeline(document_id):
    if document_id not in PROCESSING_FUNCTIONS:
        return f"Document ID {document_id} not supported in this orchestrator."

    details = get_document_details(document_id)
    if not details:
        return f"No details found for document_id {document_id}"

    pipe_id, url = details
    try:
        process_func = PROCESSING_FUNCTIONS[document_id]
        if document_id in [22, 23]:
            # Specific processing for PDF generating functions
            file_path, release_date = process_func(document_id, pipe_id)
        else:
            # Generic processing for HTML scraping functions
            file_path, release_date, error_message = process_func(url, document_id, pipe_id)
        
        if file_path and release_date:
            # Ensure release_date is in YYYYMMDD format
            try:
                # Try parsing as YYYY-MM-DD first
                parsed_date = datetime.strptime(release_date, "%Y-%m-%d")
            except ValueError:
                try:
                    # If that fails, try parsing as YYYYMMDD
                    parsed_date = datetime.strptime(release_date, "%Y%m%d")
                except ValueError:
                    # If both fail, log an error and return
                    logger.error(f"Invalid date format for document_id {document_id}: {release_date}")
                    return f"Failed to process document_id {document_id}: Invalid date format"

            # Convert to YYYYMMDD format
            formatted_release_date = parsed_date.strftime("%Y%m%d")
            process_output(file_path, document_id, pipe_id, formatted_release_date)
        else:
            return f"Failed to process document_id {document_id}"
    except Exception as e:
        logger.error(f"Error in run_pipeline for document_id {document_id}: {e}", exc_info=True)
        return f"Error occurred: {e}"

    return f"Pipeline executed successfully for document_id {document_id}"

if __name__ == "__main__":
    # Example usage
    # document_ids = [22, 23, 25, 26, 27, 28, 29, 30]
    document_ids = [22,23,24]
    statuses = []
    for document_id in document_ids:
        try:
            result = run_pipeline(document_id)
            status = "success" if "successfully" in result else "failed"
        except Exception as e:
            logger.error(f"Exception occurred while processing document_id {document_id}: {e}", exc_info=True)
            result = f"Error occurred: {e}"
            status = "failed"
        print(f"Result for document_id {document_id}: {result}")
        statuses.append({"document_id": document_id, "status": status})
    print(statuses)
