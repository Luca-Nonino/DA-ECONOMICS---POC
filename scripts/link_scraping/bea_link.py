import os
import sys
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

# Define project root directory
project_root = os.path.abspath(os.path.dirname(__file__))

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
    date_element = soup.find('div', class_='field--name-field-description')
    if date_element:
        date_text = date_element.get_text(strip=True)
        date_match = re.search(r'Current release:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})', date_text)
        if date_match:
            date_str = date_match.group(1)
            date_obj = datetime.strptime(date_str, "%B %d, %Y")
            formatted_date = date_obj.strftime("%Y%m%d")
            return formatted_date
    return None

# Function to extract the specific publication link
def extract_publication_link(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    # Locate the specific anchor tag based on the href attribute pattern
    anchor_tag = soup.find('a', href=re.compile(r'/news/\d{4}/.+'))
    if anchor_tag:
        return "https://www.bea.gov" + anchor_tag['href']
    return None

# Function to extract the relevant content from the report page
def extract_report_content(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    content_sections = soup.select("p, div.field--name-body")
    main_content = "\n\n".join([section.get_text(separator='\n', strip=True) for section in content_sections])
    return main_content

# Function to save the page content as a .txt file
def save_page_content(content, document_id, pipe_id, release_date):
    save_dir = os.path.join(project_root, 'data', 'raw', 'txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(content)
    print(f"Page content saved to {save_path}")

# Function to process a single URL and perform both extraction and saving
def process_bea_link(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        release_date = extract_release_date(html_content)
        publication_link = extract_publication_link(html_content)
        if release_date and publication_link:
            print(f"Release Date: {release_date}")
            print(f"Publication Link: {publication_link}")

            # Fetch content from the publication link
            publication_content = fetch_html_content(publication_link)
            if publication_content:
                report_content = extract_report_content(publication_content)
                save_page_content(report_content, document_id, pipe_id, release_date)
            else:
                print("Failed to fetch publication content.")
        else:
            print("Failed to extract release date or publication link.")
    else:
        print("Failed to fetch HTML content.")

############################# Test Examples #################################

# Example usage for the first URL
url_1 = "https://www.bea.gov/data/gdp/gross-domestic-product"
document_id_1 = "12"
pipe_id_1 = 3
#process_bea_link(url_1, document_id_1, pipe_id_1)

# Example usage for the second URL
url_2 = "https://www.bea.gov/data/consumer-spending/main"
document_id_2 = "13"
pipe_id_2 = 3
#process_bea_link(url_2, document_id_2, pipe_id_2)

# Example usage for the third URL
url_3 = "https://www.bea.gov/data/income-saving/personal-income"
document_id_3 = "14"
pipe_id_3 = 3
#process_bea_link(url_3, document_id_3, pipe_id_3)
