## sca_link.py

```python
import requests
from bs4 import BeautifulSoup
import os
import sys
from datetime import datetime

# Define project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the PDF link and release date from the page
def extract_pdf_link_and_release_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    link_element = soup.find('a', href=True, string=lambda x: x and "Sources of Economic News and Information for Consumers" in x)
    if link_element:
        pdf_link = link_element['href']
        date_text = link_element.next_sibling.strip("() ")
        try:
            date_obj = datetime.strptime(date_text, "%B %d, %Y")
            release_date = date_obj.strftime("%Y%m%d")
            return pdf_link, release_date
        except ValueError as e:
            print(f"Error parsing date: {date_text}, Error: {e}")
            return None, None
    return None, None

# Main function to process the URL, extract PDF link and release date, and download the PDF
def process_sca_link(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        pdf_link, release_date = extract_pdf_link_and_release_date(html_content)
        if pdf_link and release_date:
            if not pdf_link.startswith("http"):
                pdf_link = f"https://data.sca.isr.umich.edu{pdf_link}"
            print(f"PDF Link: {pdf_link}")
            print(f"Release Date: {release_date}")
            execute_pdf_download_with_url(document_id, pipe_id, pdf_link, release_date)
        else:
            print("Failed to extract PDF link or release date.")
    else:
        print("Failed to fetch HTML content.")

# Function for the second pipeline type
def execute_pdf_download_with_url(document_id, pipe_id, url, current_release_date):
    # Define save path and file name
    save_dir = os.path.join(PROJECT_ROOT, 'data/raw/pdf')
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{document_id}_{pipe_id}_{current_release_date}.pdf")

    # Download the PDF
    download_pdf(url, save_path)

# PDF download function
def download_pdf(url, save_path):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"PDF renamed and saved to {save_path}")
    except Exception as e:
        print(f"Failed to download PDF: {e}")

############################# Test Examples #################################

# Example usage
url = "http://www.sca.isr.umich.edu/"
document_id = 3  # Replace with actual document_id
pipe_id = 1
#process_sca_link(url, document_id, pipe_id)

```

## orchestrator.py

```python
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
from scripts.link_scraping.nar_link import process_nar_link

# Import the new modules for IDs 3 and 5
from scripts.pipelines.modules.sca import process_sca_logic
from scripts.pipelines.modules.fhfa import process_fhfa_logic

# List of allowed document IDs
ALLOWED_DOCUMENT_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]

# Mapping of document_id to processing functions
PROCESSING_FUNCTIONS = {
    1: process_conference_board_html,
    2: process_conference_board_html,
    11: process_nar_link,
    12: process_bea_link,
    13: process_bea_link,
    14: process_bea_link,
    17: process_ny_html,
    18: process_adp_html,
}

def get_document_details(document_id, db_path=os.path.join(project_root, 'data/database/database.sqlite')):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pipe_id, path FROM documents_table WHERE document_id = ?
    """, (document_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def is_already_processed(document_id, release_date, db_path=os.path.join(project_root, 'data/database/database.sqlite')):
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

    print(f"Log message for document_id {document_id}: {log_message}")  # Debugging: Print the log message

    file_path_match = re.search(r'data/raw/txt[\\/](\d+_\d+_\d{8}\.txt)', log_message)
    base_path = os.path.join(project_root, "data/raw/txt")

    if file_path_match:
        file_name = file_path_match.group(1)
        file_path = os.path.join(base_path, file_name)
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
        execute_pdf_download(document_id)
        log_message = buf.getvalue()

    print(f"Log message for document_id {document_id}: {log_message}")  # Debugging: Print the log message

    pdf_path_match = re.search(r'PDF downloaded successfully: (data[\\/]raw[\\/]pdf[\\/]\d+_\d+_\d{8}\.pdf)', log_message)
    if pdf_path_match:
        pdf_path = os.path.join(project_root, pdf_path_match.group(1).replace('\\', '/'))  # Normalize path to use forward slashes
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
        elif document_id == 3:
            # Redirect logic to the relevant script for ID 3
            txt_path, release_date = process_sca_logic(document_id, url, pipe_id)
        elif document_id == 5:
            # Redirect logic to the relevant script for ID 5
            txt_path, release_date = process_fhfa_logic(document_id, url, pipe_id)
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
        processed_file_path = os.path.join(project_root, f"data/processed/{document_id}_{pipe_id}_{release_date}.txt")
        parse_and_load(processed_file_path)

        return "Pipeline executed successfully"
    except Exception as e:
        return f"Error occurred: {e}"

if __name__ == "__main__":
    # Example usage
    document_ids = [5] # Adjusted to include document IDs 3 to 5 for testing
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

```

## fhfa.py

```python
import os
import re
import io
from contextlib import redirect_stdout
from scripts.link_scraping.fhfa_link import process_fhfa_link

def process_fhfa_logic(document_id, url, pipe_id):
    print(f"Searching for document ID: {document_id}, URL: {url}, Pipe ID: {pipe_id}")  # Log what we are searching for
    with io.StringIO() as buf, redirect_stdout(buf):
        process_fhfa_link(url, document_id, pipe_id)
        captured_message = buf.getvalue()
        print(f"Captured log message for FHFA link (ID {document_id}): {captured_message}")  # Debugging: Print the captured log message

    # Update the regex to match the expected log message format
    release_date_match = re.search(r'Release Date: (\d{8})', captured_message)
    save_path_match = re.search(r'Save Path: (.+?\.pdf)', captured_message)

    if release_date_match and save_path_match:
        release_date = release_date_match.group(1)
        file_path = save_path_match.group(1)
        print(f"Extracted file path: {file_path}, release date: {release_date}")  # Log extracted file path and release date
        return file_path, release_date
    else:
        error_message = f"Failed to extract release date or file path from log message: {captured_message}"
        print(error_message)  # Log the error
        raise ValueError(error_message)

```

