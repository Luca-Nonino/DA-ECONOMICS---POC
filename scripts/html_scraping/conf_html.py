import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime

project_root = os.path.dirname(os.path.abspath(__file__))

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
def extract_release_date(soup):
    date_element = soup.find('p', class_='date')
    if date_element:
        date_text = date_element.get_text(strip=True).replace("Updated: ", "")
        date_obj = datetime.strptime(date_text, "%A, %B %d, %Y")
        formatted_date = date_obj.strftime("%Y%m%d")
        return formatted_date
    return None

# Function to extract the relevant parts of the HTML
def extract_relevant_content(soup):
    relevant_content = []
    # Identify the main content section based on your HTML structure
    main_content = soup.find('div', {'id': 'mainContainer'})

    if main_content:
        elements = main_content.find_all(['h2', 'h3', 'p'])
        for element in elements:
            text = element.get_text(strip=True)
            if text:
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
    print(f"Page content saved to {save_path}")

# Main function to process the URL and perform both extraction and saving
def process_conference_board_html(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        release_date = extract_release_date(soup)
        if release_date:
            relevant_content = extract_relevant_content(soup)
            save_page_content(relevant_content, document_id, pipe_id, release_date)
        else:
            print("Failed to extract release date.")
    else:
        print("Failed to fetch HTML content.")

############################# Test Examples #################################





# Example usage
url_1 = "https://www.conference-board.org/topics/us-leading-indicators"
document_id_1 = 1
pipe_id_1 = "1"
#process_conference_board_html(url_1, document_id_1, pipe_id_1)

url_2 = "https://www.conference-board.org/topics/consumer-confidence"
document_id_2 = 2
pipe_id_2 = "1"
#process_conference_board_html(url_2, document_id_2, pipe_id_2)