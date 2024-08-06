import requests
from bs4 import BeautifulSoup
import os
import sys
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

# Function to extract the release date in YYYYMMDD format
def extract_release_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    date_element = soup.find('strong', string=lambda text: text and 'Latest estimate' in text)
    if date_element:
        date_text = date_element.text.split('--')[1].strip()
        try:
            date_obj = datetime.strptime(date_text, "%B %d, %Y")
            return date_obj.strftime("%Y%m%d")
        except ValueError:
            print(f"Failed to parse date: {date_text}")
    return None

# Function to extract the relevant content
def extract_relevant_content(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the strong element containing "Latest estimate"
    latest_estimate = soup.find('strong', string=lambda text: text and 'Latest estimate' in text)
    
    if latest_estimate:
        # Find the next paragraph
        content_paragraph = latest_estimate.find_next('p')
        
        if content_paragraph:
            return f"{latest_estimate.text}\n\n{content_paragraph.text}"
    
    return None

# Function to save the page content as a .txt file
def save_page_content(content, document_id, pipe_id, release_date):
    save_dir = os.path.join(project_root, 'data/raw/txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(content)
    print(f"Page content saved to {save_path}")

# Main function to process the URL and perform both extraction and saving
def process_gdpnow_html(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        release_date = extract_release_date(html_content)
        if release_date:
            relevant_content = extract_relevant_content(html_content)
            if relevant_content:
                save_page_content(relevant_content, document_id, pipe_id, release_date)
            else:
                print("Failed to extract relevant content.")
        else:
            print("Failed to extract release date.")
    else:
        print("Failed to fetch HTML content.")

# Example usage
url = "https://www.atlantafed.org/cqer/research/gdpnow"
document_id = 41
pipe_id = "1"
#process_gdpnow_html(url, document_id, pipe_id)
