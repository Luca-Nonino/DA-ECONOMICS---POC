from fake_useragent import UserAgent
import httpx
from bs4 import BeautifulSoup
import os
from datetime import datetime

# Initialize UserAgent
ua = UserAgent()

# Determine project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to fetch and parse HTML content from the URL using httpx
def fetch_html_content(url):
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
        with httpx.Client(timeout=30, verify=False) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            return response.content
    except httpx.RequestError as e:
        print(f"Failed to fetch content: {e}")
        return None

# Function to extract the release date in YYYYMMDD format
def extract_release_date(soup):
    title_element = soup.find('div', class_='page_title')
    if title_element:
        title_text = title_element.get_text(strip=True)
        try:
            date_obj = datetime.strptime(title_text, "Final Results for %B %Y")
            formatted_date = date_obj.strftime("%Y%m%d")
            return formatted_date
        except ValueError as e:
            print(f"Failed to parse date: {e}")
            return None
    return None

# Function to extract and format the relevant parts of the HTML
def extract_relevant_content(soup):
    relevant_content = []

    # Extract the table with id 'front_table'
    table = soup.find('table', id='front_table')
    if table:
        relevant_content.append(format_table(table))

    # Extract the paragraphs under the 'richnote' class
    richnote_div = soup.find('div', id='richnote')
    if richnote_div:
        paragraphs = richnote_div.find_all('div')
        for paragraph in paragraphs:
            relevant_content.append(paragraph.get_text(separator='\n', strip=True))

    return "\n\n".join(relevant_content)

# Function to format table data for better readability
def format_table(table):
    formatted_table = ""
    headers = []
    rows = []

    # Extracting header rows
    header_rows = table.find_all('tr')
    if header_rows:
        for tr in header_rows[:2]:  # First two rows are headers
            header = [th.get_text(strip=True) for th in tr.find_all('td')]
            headers.append("\t".join(header))
        formatted_table += "\n".join(headers) + "\n"

    # Extracting data rows
    data_rows = header_rows[2:]
    if data_rows:
        for tr in data_rows:
            row = [td.get_text(strip=True) for td in tr.find_all('td')]
            rows.append("\t".join(row))

    formatted_table += "\n".join(rows)
    return formatted_table

# Function to save the page content as a .txt file
def save_page_content(relevant_content, document_id, pipe_id, release_date):
    save_dir = os.path.join(project_root, 'data', 'raw', 'txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(relevant_content)
    print(f"Page content saved to {save_path}")
    return save_path

# Main function to process the URL and perform both extraction and saving
def process_sca_html(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        release_date = extract_release_date(soup)
        if release_date:
            print(f"Release date: {release_date}")
            relevant_content = extract_relevant_content(soup)
            file_path = save_page_content(relevant_content, document_id, pipe_id, release_date)
            return file_path, release_date, None  # Return None as the third value
        else:
            print("Failed to extract release date.")
            return None, None, "Failed to extract release date."
    else:
        print("Failed to fetch HTML content.")
        return None, None, "Failed to fetch HTML content."

# Example usage
url = "http://www.sca.isr.umich.edu/"
document_id = 3  # Replace with actual document_id
pipe_id = "1"
#process_sca_html(url, document_id, pipe_id)
