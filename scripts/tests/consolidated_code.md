## mdic_html.py

```python
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

```

## adp_html.py

```python
import requests
from bs4 import BeautifulSoup
import os
import sys
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


```

## conf_html.py

```python
import requests
from bs4 import BeautifulSoup
import os
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
```

## ny_html.py

```python
import requests
from bs4 import BeautifulSoup
import os
import sys


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

# Function to extract the most recent release date in YYYYMMDD format
def extract_release_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', class_='greyborder')

    if not table:
        print("Table not found. Please check the HTML structure.")
        return None

    dates = []
    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    year = "2024"  # Hardcoded year as per the provided example

    # Find all rows in the table
    rows = table.find_all('tr', {'valign': 'top'})

    # Define the starting index of months
    month_idx = 0

    for row in rows:
        cells = row.find_all('td', {'class': 'dirCol'})
        for cell in cells:
            # Find the div that contains the date
            date_div = cell.find('div')
            if date_div:
                link = date_div.find('a', {'class': 'pdf'})
                if link:
                    # Construct the full date
                    if month_idx < len(months):
                        month = months[month_idx]
                        date_text = date_div.get_text(strip=True).split()[0]
                        full_date = f"{year} {month} {date_text}"
                        dates.append(full_date)
            # Increment month index only if a valid month cell is processed
            month_idx += 1

    # Extract the last date and format it
    if dates:
        last_date_str = dates[-1]
        parts = last_date_str.split()
        formatted_date = f"{parts[0]}{months.index(parts[1]) + 1:02d}{parts[2]}"
        return formatted_date[:8]
    else:
        return None

# Function to save specific content sections as a .txt file
def save_page_content(html_content, document_id, pipe_id, release_date):
    soup = BeautifulSoup(html_content, 'html.parser')
    content_sections = soup.select("p, div.hidden")
    main_content = "\n\n".join([section.get_text(separator='\n', strip=True) for section in content_sections])

    save_dir = os.path.join(project_root, 'data/raw/txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(main_content)
    print(f"Page content saved to {save_path}")

# Main function to process the URL and extract both release date and content sections
def process_ny_html(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        release_date = extract_release_date(html_content)
        if release_date:
            save_page_content(html_content, document_id, pipe_id, release_date)
        else:
            print("Failed to extract release date.")
    else:
        print("Failed to fetch HTML content.")

############################# Test Examples #################################

# Example usage
url = "https://www.newyorkfed.org/survey/empire/empiresurvey_overview"
document_id = 17  # Replace with actual document_id
pipe_id = "1"
process_ny_html(url, document_id, pipe_id)

```

