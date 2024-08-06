import os
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

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

# Function to get the current release date from the database
def get_current_release_date(document_id, db_path=os.path.join(PROJECT_ROOT, 'data', 'database', 'database.sqlite')):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT current_release_date FROM documents_table WHERE document_id = ?", (document_id,))
    result = cursor.fetchone()
    conn.close()
    return str(result[0]) if result else None  # Convert result to string if it exists

# Function to update the release date in the database
def update_release_date(document_id, new_date, db_path=os.path.join(PROJECT_ROOT, 'data', 'database', 'database.sqlite')):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("UPDATE documents_table SET current_release_date = ? WHERE document_id = ?", (new_date, document_id))
    conn.commit()
    conn.close()

# Function to check and update the release date
def check_and_update_release_date(document_id, new_date, db_path=os.path.join(PROJECT_ROOT, 'data', 'database', 'database.sqlite')):
    current_date = get_current_release_date(document_id, db_path)
    if current_date is None or current_date[:6] < new_date[:6]:  # Compare year and month
        update_release_date(document_id, new_date, db_path)
        return True
    else:
        return False

# Function to extract the relevant content
def extract_relevant_content(soup):
    relevant_content = []
    for section in soup.find_all('div', class_='mb-4'):
        title = section.find('h3')
        if title:
            title_text = title.get_text(strip=True)
            relevant_content.append(f"{title_text}\n{'=' * len(title_text)}")
        paragraphs = section.find_all('p')
        for p in paragraphs:
            relevant_content.append(p.get_text(strip=True))
        table = section.find('table')
        if table:
            table_text = format_table(table)
            relevant_content.append(table_text)
    return "\n\n".join(relevant_content)

# Function to format table data for better readability
def format_table(table):
    formatted_table = ""
    headers = []
    rows = []

    thead = table.find('thead')
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all('th')]
        formatted_table += "\t".join(headers) + "\n"

    tbody = table.find('tbody')
    if tbody:
        for tr in tbody.find_all('tr'):
            row = [td.get_text(strip=True) for td in tr.find_all('td')]
            rows.append("\t".join(row))

    formatted_table += "\n".join(rows)
    return formatted_table

# Function to save the page content as a .txt file
def save_page_content(content, document_id, pipe_id, release_date):
    save_dir = os.path.join(PROJECT_ROOT, 'data', 'raw', 'txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(content)
    return save_path

# Function to check if the current release date exists in the summary table
def check_summary_for_release_date(document_id, release_date, db_path=os.path.join(PROJECT_ROOT, 'data', 'database', 'database.sqlite')):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM summary_table WHERE document_id = ? AND release_date = ?", (document_id, release_date))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

# Main function to process the URL and perform both extraction and saving
def process_ism_html(base_url, document_id, pipe_id):
    # Calculate the previous and two months ago
    current_date = datetime.now()
    last_month = (current_date.replace(day=1) - timedelta(days=1)).strftime('%B').lower()
    two_months_ago = (current_date.replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)
    two_months_ago = two_months_ago.strftime('%B').lower()
    
    urls_to_try = [
        f"{base_url}/{last_month}/",
        f"{base_url}/{two_months_ago}/"
    ]

    html_content = None
    for url in urls_to_try:
        html_content = fetch_html_content(url)
        if html_content:
            break

    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        release_date = current_date.strftime('%Y%m%d')  # Use current date as release date
        relevant_content = extract_relevant_content(soup)

        if check_and_update_release_date(document_id, release_date):
            file_path = save_page_content(relevant_content, document_id, pipe_id, release_date)
            print(f"Release Date Updated: {release_date}")
            print(f"Page content saved to {file_path}")
        else:
            # Extra check if "No New Releases"
            if not check_summary_for_release_date(document_id, release_date):
                file_path = save_page_content(relevant_content, document_id, pipe_id, release_date)
                print(f"Release Date Updated: {release_date}")
                print(f"Page content saved to {file_path}")
            else:
                print("No New Releases")
    else:
        print("Failed to fetch content for both last month and two months ago URLs.")

# Example usage
url_pmi = "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi"
document_id_pmi = 44
pipe_id = "1"
#process_ism_html(url_pmi, document_id_pmi, pipe_id)

url_services = "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/services"
document_id_services = 45
pipe_id = "1"
#process_ism_html(url_services, document_id_services, pipe_id)
