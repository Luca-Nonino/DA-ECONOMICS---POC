import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime

# Define project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the PDF link and release date
def extract_pdf_link_and_release_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    pdf_link_element = soup.find('a', class_='btn btn-sm btn-primary')
    if pdf_link_element and 'href' in pdf_link_element.attrs:
        pdf_link = pdf_link_element['href']
        # Find the release date
        date_element = soup.find('h3', class_='cormorant mb-1 _ngcontent-hyf-c169')
        if date_element:
            date_str = date_element.get_text(strip=True)
            try:
                # Extracting the date part "19/07/2024" from "Relatório de Mercado - 19/07/2024"
                date_str = date_str.split('-')[1].strip()
                release_date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y%m%d")
                return pdf_link, release_date
            except ValueError as e:
                print(f"Error parsing date: {e}")
    return None, None

# Function to download the PDF
def download_pdf(url, save_path):
    try:
        response = requests.get(url)
        response.raise_for_status()
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"PDF downloaded successfully: {save_path}")
        return True
    except Exception as e:
        print(f"Failed to download PDF: {e}")
        return False

# Main function to process the URL, extract PDF link, download the PDF, and save it with the release date
def process_bcb_link(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        pdf_link, release_date = extract_pdf_link_and_release_date(html_content)
        if pdf_link and release_date:
            pdf_link = "https://www.bcb.gov.br" + pdf_link  # Construct full URL if necessary
            save_path = os.path.join(PROJECT_ROOT, 'data/raw/pdf', f"{document_id}_{pipe_id}_{release_date}.pdf")
            if download_pdf(pdf_link, save_path):
                print(f"PDF saved to {save_path}")
            else:
                print("Failed to download PDF.")
        else:
            print("Failed to extract PDF link or release date.")
    else:
        print("Failed to fetch HTML content.")

# Example usage
url = "https://www.bcb.gov.br/publicacoes/focus"
document_id = 22  # Example document ID
pipe_id = "1"
process_bcb_link(url, document_id, pipe_id)
