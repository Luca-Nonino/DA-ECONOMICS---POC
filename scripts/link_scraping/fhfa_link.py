import requests
from bs4 import BeautifulSoup
import os
import re
import fitz  # PyMuPDF
from datetime import datetime

# Determine project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the PDF link from the HTML
def extract_pdf_link(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    link_element = soup.find('a', href=re.compile(r'/sites/default/files/\d{4}-\d{2}/FHFA-HPI-Monthly-\d{8}.pdf'))
    if link_element:
        return "https://www.fhfa.gov" + link_element['href']
    return None

# Function to download the PDF
def download_pdf(url, save_path):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        os.makedirs(os.path.dirname(save_path), exist_ok=True)  # Ensure the directory exists
        with open(save_path, 'wb') as file:
            file.write(response.content)
        log_message = f"PDF downloaded successfully: {save_path}"
        print(log_message)
        return True, log_message
    except Exception as e:
        log_message = f"Failed to download PDF: {e}"
        print(log_message)
        return False, log_message

# Adjusted function to extract the release date from the PDF using PyMuPDF
def extract_release_date_from_pdf(pdf_path):
    try:
        pdf_document = fitz.open(pdf_path)
        first_page = pdf_document[0]
        text = first_page.get_text()
        # Adjusted regex to capture the full date format
        date_match = re.search(r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}\b', text)
        if date_match:
            date_str = date_match.group(0)
            date_obj = datetime.strptime(date_str, '%B %d, %Y')
            return date_obj.strftime('%Y%m%d')
    except Exception as e:
        print(f"Failed to extract release date from PDF: {e}")
        return None

# Main function to process the URL, extract PDF link, download PDF, and save it with the release date
def process_fhfa_link(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        pdf_link = extract_pdf_link(html_content)
        if pdf_link:
            print(f"PDF Link: {pdf_link}")
            pdf_save_path = os.path.join(project_root, "data/raw/pdf", f"{document_id}_{pipe_id}.pdf")
            download_success, download_log = download_pdf(pdf_link, pdf_save_path)
            if download_success:
                release_date = extract_release_date_from_pdf(pdf_save_path)
                if release_date:
                    final_save_path = os.path.join(project_root, "data/raw/pdf", f"{document_id}_{pipe_id}_{release_date}.pdf")
                    if os.path.exists(final_save_path):
                        os.remove(final_save_path)
                    os.rename(pdf_save_path, final_save_path)
                    log_message = f"Release Date: {release_date}\nSave Path: {final_save_path}"
                    print(log_message)
                    return log_message
                else:
                    log_message = "Failed to extract release date from PDF."
                    print(log_message)
                    return log_message
            else:
                return download_log
        else:
            log_message = "Failed to extract PDF link."
            print(log_message)
            return log_message
    else:
        log_message = "Failed to fetch HTML content."
        print(log_message)
        return log_message

############################# Test Examples #################################

# Example usage
url = "https://www.fhfa.gov/data/hpi"
document_id = 5  # Replace with actual document_id
pipe_id = 3
#process_fhfa_link(url, document_id, pipe_id)
