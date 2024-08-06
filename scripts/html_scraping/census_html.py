import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Determine project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        logger.error(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the release date in YYYYMMDD format
def extract_release_date(soup):
    date_element = soup.find('time', class_='uscb-sub-heading-2 uscb-color-secondary-1 uscb-uppercase')
    if date_element:
        date_text = date_element.get_text(strip=True)
        try:
            date_obj = datetime.strptime(date_text, "%B %d, %Y")
            formatted_date = date_obj.strftime("%Y%m%d")
            return formatted_date
        except ValueError as e:
            logger.error(f"Failed to parse date: {e}")
            return None
    return None

# Function to extract the relevant parts of the HTML
def extract_relevant_content(soup):
    relevant_content = []
    content_div = soup.find('div', class_='richtext')
    if content_div:
        text = content_div.get_text(separator='\n', strip=True)
        relevant_content.append(text)
    return "\n\n".join(relevant_content)

# Function to save the page content as a .txt file
def save_page_content(relevant_content, document_id, pipe_id, release_date):
    save_dir = os.path.join(project_root, 'data/raw/txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)
    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(relevant_content)
    logger.info(f"Page content saved to {save_path}")
    print(f"Page content saved to {save_path}")  # Ensure this matches the format expected by the orchestrator

# Main function to process the URL and perform both extraction and saving
def process_census_html(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        release_date = extract_release_date(soup)
        if release_date:
            relevant_content = extract_relevant_content(soup)
            if relevant_content:
                save_page_content(relevant_content, document_id, pipe_id, release_date)
                print(f"Release date: {release_date}")  # Ensure this matches the format expected by the orchestrator
            else:
                logger.error("Failed to extract relevant content.")
        else:
            logger.error("Failed to extract release date.")
    else:
        logger.error("Failed to fetch HTML content.")

# Example usage
if __name__ == "__main__":
    url = "https://www.census.gov/manufacturing/m3/current/index.html"
    document_id = 15  # Replace with actual document_id
    pipe_id = "1"
    process_census_html(url, document_id, pipe_id)
