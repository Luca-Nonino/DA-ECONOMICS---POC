import requests
import os
import sqlite3
from datetime import datetime
import time
from fake_useragent import UserAgent
import logging
import sys

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(filename=os.path.join(project_root, 'app', 'logs', 'errors.log'),
                    level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection function
def get_document_details(document_id, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT pipe_id, document_id, path
            FROM documents_table
            WHERE document_id = ?
        """, (document_id,))
        result = cursor.fetchone()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error accessing database file at path: {db_path}. Error: {e}", exc_info=True)
        raise

# Generic PDF download function
def download_pdf(url, save_path):
    save_path = os.path.abspath(save_path)
    ua = UserAgent()
    headers = {
        'User-Agent': ua.random,
        'Referer': url,
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Accept': 'application/pdf',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
    }

    try:
        with requests.Session() as session:
            session.headers.update(headers)
            time.sleep(2)  # Add a 2-second delay before making the request
            response = session.get(url)
            response.raise_for_status()  # Check if the request was successful
            with open(save_path, 'wb') as file:
                file.write(response.content)
            print(f"PDF downloaded successfully: {os.path.normpath(save_path)}")
    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code == 403:
            print("HTTP 403 Forbidden error. Retrying with different headers.")
            headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            try:
                response = session.get(url, headers=headers)
                response.raise_for_status()
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                print(f"PDF downloaded successfully: {os.path.normpath(save_path)}")
            except Exception as e:
                logger.error(f"Failed to download PDF on retry. URL: {url}, Save Path: {save_path}. Error: {e}", exc_info=True)
        else:
            logger.error(f"HTTP error occurred. URL: {url}, Save Path: {save_path}. HTTP Error: {http_err}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to download PDF. URL: {url}, Save Path: {save_path}. Error: {e}", exc_info=True)

# Function for the first pipeline type
def execute_pdf_download(document_id, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    # Fetch document details
    details = get_document_details(document_id, db_path)
    if not details:
        print(f"No document found with ID {document_id}")
        return

    pipe_id, document_id, url = details

    # Define save path and file name
    current_date = datetime.now().strftime("%Y%m%d")
    save_dir = os.path.abspath(os.path.join(project_root, 'data', 'raw', 'pdf'))
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{document_id}_{pipe_id}_{current_date}.pdf")

    # Download the PDF
    download_pdf(url, save_path)

# Function for the second pipeline type
def execute_pdf_download_with_url(document_id, url, current_release_date, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    # Fetch document details
    details = get_document_details(document_id, db_path)
    if not details:
        print(f"No document found with ID {document_id}")
        return

    pipe_id, document_id, _ = details  # We ignore the URL from the database in this case

    # Define save path and file name
    save_dir = os.path.abspath(os.path.join(project_root, 'data', 'raw', 'pdf'))
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{document_id}_{pipe_id}_{current_release_date}.pdf")

    # Download the PDF
    download_pdf(url, save_path)
