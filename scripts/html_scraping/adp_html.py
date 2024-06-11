import requests
from bs4 import BeautifulSoup
import os
import sys
from datetime import datetime

# Define project root directory
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the release date in YYYYMMDD format
def extract_release_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    date_element = soup.find('li', class_='current-report')
    if date_element:
        date_text = date_element.get_text(strip=True)
        # Correct the parsing of the date
        try:
            date_obj = datetime.strptime(date_text, "%B %Y")
            formatted_date = date_obj.strftime("%Y%m") + "01"  # Assuming the release is on the 1st day of the month
            return formatted_date
        except ValueError:
            return datetime.now().strftime("%Y%m%d")  # Fallback to current date if parsing fails
    return datetime.now().strftime("%Y%m%d")

# Function to save the page content as a .txt file
def save_page_content(html_content, document_id, pipe_id, release_date):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract relevant sections based on their HTML structure
    sections = []

    # Main report overview
    main_overview = soup.select_one('.report-overview.NER')
    if main_overview:
        sections.append(main_overview.get_text(separator='\n', strip=True))

    # Change by establishment size
    establishment_size = soup.select_one('.report-section.NER.biz-size')
    if establishment_size:
        sections.append(establishment_size.get_text(separator='\n', strip=True))

    # Change by industry
    industry_section = soup.select_one('.report-section.NER')
    if industry_section:
        sections.append(industry_section.get_text(separator='\n', strip=True))

    # About this report
    about_report = soup.select_one('.prefooter')
    if about_report:
        sections.append(about_report.get_text(separator='\n', strip=True))

    main_content = "\n\n".join(sections)

    save_dir = os.path.join(PROJECT_ROOT, 'data/raw/txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(main_content)
    print(f"Page content saved to {save_path}")

# Main function to process the URL and perform both extraction and saving
def process_adp_html(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        release_date = extract_release_date(html_content)
        if release_date:
            save_page_content(html_content, document_id, pipe_id, release_date)
        else:
            print("Failed to extract release date.")
    else:
        print("Failed to fetch HTML content.")

# Example usage
url = "https://adpemploymentreport.com/"
document_id = 18  # Replace with actual document_id
pipe_id = "1"
#process_adp_html(url, document_id, pipe_id)

# Example usage
url = "https://adpemploymentreport.com/"
document_id = 18  # Replace with actual document_id
pipe_id = "1"
#process_adp_html(url, document_id, pipe_id)

