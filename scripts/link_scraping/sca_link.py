import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime

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
    save_dir = 'data/raw/pdf'
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
