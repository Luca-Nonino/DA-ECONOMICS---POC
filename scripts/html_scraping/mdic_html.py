from fake_useragent import UserAgent
import httpx
from bs4 import BeautifulSoup
import os
import sys
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
    date_element = soup.find('h4', class_='date')
    if date_element:
        date_text = date_element.get_text(strip=True).replace("Atualizado em ", "")
        try:
            date_obj = datetime.strptime(date_text, "%d/%m/%Y")
            formatted_date = date_obj.strftime("%Y%m%d")
            return formatted_date
        except ValueError as e:
            print(f"Failed to parse date: {e}")
            return None
    return None

# Function to extract and format the relevant parts of the HTML
def extract_relevant_content(soup):
    relevant_content = []

    # Extract the updated date
    updated_date = soup.find('h4', class_='date')
    if updated_date:
        relevant_content.append(updated_date.get_text(strip=True) + '\n')

    # Extract main results
    main_results_section = soup.find('div', {'id': 'totais---principais-resultados'})
    if main_results_section:
        relevant_content.append(format_section(main_results_section, "Totais - Principais Resultados"))

    # Extract highlights
    highlights_section = soup.find('div', {'id': 'destaques'})
    if highlights_section:
        relevant_content.append(format_section(highlights_section, "Destaques"))

    # Extract totals section
    totals_section = soup.find('div', {'id': 'totais'})
    if totals_section:
        relevant_content.append(format_section(totals_section, "Totais"))

    # Extract sectors and products
    sectors_products_section = soup.find('div', {'id': 'setores-e-produtos'})
    if sectors_products_section:
        relevant_content.append(format_section(sectors_products_section, "Setores e Produtos"))

    return "\n\n".join(relevant_content)

# Function to format each section with proper headings and tables
def format_section(section, heading):
    formatted_section = f"{heading}\n{'=' * len(heading)}\n"
    if section:
        tables = section.find_all('table')
        if tables:
            for table in tables:
                formatted_section += format_table(table) + '\n\n'
        else:
            formatted_section += section.get_text(separator='\n', strip=True) + '\n'
    return formatted_section

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
def process_balança_comercial_html(url, document_id, pipe_id):
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
url = "https://balanca.economia.gov.br/balanca/pg_principal_bc/principais_resultados.html"
document_id = 30  # Replace with actual document_id
pipe_id = "1"
process_balança_comercial_html(url, document_id, pipe_id)
