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
#process_ny_html(url, document_id, pipe_id)

```

## bea_link.py

```python
import os
import sys
import requests
from bs4 import BeautifulSoup
import re
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

```

## fhfa_link.py

```python
import requests
from bs4 import BeautifulSoup
import os
import re
import fitz  # PyMuPDF
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

# Function to extract the PDF link from the HTML
def extract_pdf_link(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    link_element = soup.find('a', href=re.compile(r'/sites/default/files/\d{4}-\d{2}/HPI_\d{4}Q\d.pdf'))
    if link_element:
        return "https://www.fhfa.gov" + link_element['href']
    return None

# Function to download the PDF
def download_pdf(url, save_path):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        os.makedirs(os.path.dirname(save_path), exist_ok=True)  # Ensure the directory exists
        with open(save_path, 'wb') as file:
            file.write(response.content)
        log_message = f"PDF downloaded successfully: {save_path}"
        print(log_message)
        return True, log_message
    except Exception as e:
        log_message = f"Failed to download PDF: {e}"
        print(log_message)
        return False, log_message

# Adjusted function to extract the release date from the PDF using PyMuPDF
def extract_release_date_from_pdf(pdf_path):
    try:
        pdf_document = fitz.open(pdf_path)
        first_page = pdf_document[0]
        text = first_page.get_text()
        # Adjusted regex to capture the full date format
        date_match = re.search(r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}\b', text)
        if date_match:
            date_str = date_match.group(0)
            date_obj = datetime.strptime(date_str, '%B %d, %Y')
            return date_obj.strftime('%Y%m%d')
    except Exception as e:
        print(f"Failed to extract release date from PDF: {e}")
        return None

# Main function to process the URL, extract PDF link, download PDF, and save it with the release date
def process_fhfa_link(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        pdf_link = extract_pdf_link(html_content)
        if pdf_link:
            print(f"PDF Link: {pdf_link}")
            pdf_save_path = os.path.join(project_root, "data/raw/pdf", f"{document_id}_{pipe_id}.pdf")
            download_success, download_log = download_pdf(pdf_link, pdf_save_path)
            if download_success:
                release_date = extract_release_date_from_pdf(pdf_save_path)
                if release_date:
                    final_save_path = os.path.join(project_root, "data/raw/pdf", f"{document_id}_{pipe_id}_{release_date}.pdf")
                    if os.path.exists(final_save_path):
                        os.remove(final_save_path)
                    os.rename(pdf_save_path, final_save_path)
                    log_message = f"Release Date: {release_date}\nSave Path: {final_save_path}"
                    print(log_message)
                    return log_message
                else:
                    log_message = "Failed to extract release date from PDF."
                    print(log_message)
                    return log_message
            else:
                return download_log
        else:
            log_message = "Failed to extract PDF link."
            print(log_message)
            return log_message
    else:
        log_message = "Failed to fetch HTML content."
        print(log_message)
        return log_message

############################# Test Examples #################################

# Example usage
url = "https://www.fhfa.gov/data/hpi"
document_id = 5  # Replace with actual document_id
pipe_id = 3
#process_fhfa_link(url, document_id, pipe_id)

```

## nar_link.py

```python
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime

# Define the project root directory
PROJECT_ROOT= os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the secondary link from the initial page
def extract_secondary_link(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    link_element = soup.find('a', href=True, string=lambda x: x and "Read the full news release" in x)
    if link_element:
        return link_element['href']
    return None

# Function to extract the release date in YYYYMMDD format from the secondary page
def extract_release_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    date_element = soup.find('span', property='dc:date dc:created')
    if date_element:
        date_str = date_element['content']
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
            formatted_date = date_obj.strftime("%Y%m%d")
            return formatted_date
        except ValueError as e:
            print(f"Error parsing date: {date_str}, Error: {e}")
            return None
    print("Date element not found.")
    return None

# Function to extract the relevant content from the report page
def extract_report_content(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    content_sections = soup.find_all('div', class_='field field--body')
    main_content = "\n\n".join([section.get_text(separator='\n', strip=True) for section in content_sections])
    return main_content

# Function to save the page content as a .txt file
def save_page_content(content, document_id, pipe_id, release_date):
    save_dir = os.path.join(PROJECT_ROOT, 'data/raw/txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(content)
    print(f"Page content saved to {save_path}")

# Main function to process the URL and extract both release date and content
def process_nar_link(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        secondary_link = extract_secondary_link(html_content)
        if secondary_link:
            if not secondary_link.startswith("http"):
                secondary_link = f"https://www.nar.realtor{secondary_link}"
            print(f"Secondary link extracted: {secondary_link}")
            secondary_html_content = fetch_html_content(secondary_link)
            if secondary_html_content:
                release_date = extract_release_date(secondary_html_content)
                if release_date:
                    report_content = extract_report_content(secondary_html_content)
                    if report_content.strip():  # Check if content is not empty
                        save_page_content(report_content, document_id, pipe_id, release_date)
                    else:
                        print("Extracted report content is empty.")
                else:
                    print("Failed to extract release date.")
            else:
                print("Failed to fetch secondary HTML content.")
        else:
            print("Failed to extract secondary link.")
    else:
        print("Failed to fetch HTML content.")

############################# Test Examples #################################

# Example usage
url = "https://www.nar.realtor/research-and-statistics/housing-statistics/existing-home-sales"
document_id = 11  # Replace with actual document_id
pipe_id = 3
#process_nar_link(url, document_id, pipe_id)
```

## sca_link.py

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
    save_dir = os.path.join(PROJECT_ROOT, 'data/raw/pdf')
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

```

## pdf_download.py

```python
import requests
import os
import sqlite3
from datetime import datetime
import time
from fake_useragent import UserAgent
import logging
import sys

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(filename=os.path.join(project_root, 'app', 'logs', 'errors.log'),
                    level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection function
def get_document_details(document_id, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT pipe_id, document_id, path
            FROM documents_table
            WHERE document_id = ?
        """, (document_id,))
        result = cursor.fetchone()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error accessing database file at path: {db_path}. Error: {e}", exc_info=True)
        raise

# Generic PDF download function
def download_pdf(url, save_path):
    save_path = os.path.abspath(save_path)
    ua = UserAgent()
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
        with requests.Session() as session:
            session.headers.update(headers)
            time.sleep(2)  # Add a 2-second delay before making the request
            response = session.get(url)
            response.raise_for_status()  # Check if the request was successful
            with open(save_path, 'wb') as file:
                file.write(response.content)
            print(f"PDF downloaded successfully: {os.path.normpath(save_path)}")
    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code == 403:
            print("HTTP 403 Forbidden error. Retrying with different headers.")
            headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            try:
                response = session.get(url, headers=headers)
                response.raise_for_status()
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                print(f"PDF downloaded successfully: {os.path.normpath(save_path)}")
            except Exception as e:
                logger.error(f"Failed to download PDF on retry. URL: {url}, Save Path: {save_path}. Error: {e}", exc_info=True)
        else:
            logger.error(f"HTTP error occurred. URL: {url}, Save Path: {save_path}. HTTP Error: {http_err}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to download PDF. URL: {url}, Save Path: {save_path}. Error: {e}", exc_info=True)

# Function for the first pipeline type
def execute_pdf_download(document_id, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    # Fetch document details
    details = get_document_details(document_id, db_path)
    if not details:
        print(f"No document found with ID {document_id}")
        return

    pipe_id, document_id, url = details

    # Define save path and file name
    current_date = datetime.now().strftime("%Y%m%d")
    save_dir = os.path.abspath(os.path.join(project_root, 'data', 'raw', 'pdf'))
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{document_id}_{pipe_id}_{current_date}.pdf")

    # Download the PDF
    download_pdf(url, save_path)

# Function for the second pipeline type
def execute_pdf_download_with_url(document_id, url, current_release_date, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    # Fetch document details
    details = get_document_details(document_id, db_path)
    if not details:
        print(f"No document found with ID {document_id}")
        return

    pipe_id, document_id, _ = details  # We ignore the URL from the database in this case

    # Define save path and file name
    save_dir = os.path.abspath(os.path.join(project_root, 'data', 'raw', 'pdf'))
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{document_id}_{pipe_id}_{current_release_date}.pdf")

    # Download the PDF
    download_pdf(url, save_path)

```

## pdf_hash.py

```python
import hashlib
import sqlite3
import os
import sys
import shutil
import re
import json
from datetime import datetime

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from scripts.utils.completions_general import extract_release_date

def get_previous_hash(document_id, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT hash FROM documents_table WHERE document_id = ?
        """, (document_id,))
        result = cursor.fetchone()
        conn.close()
        print(f"Fetched previous hash for document_id {document_id}: {result[0] if result else 'None'}")
        return result[0] if result else None
    except sqlite3.Error as e:
        print(f"Database error occurred while fetching previous hash: {e}")
        return None

def update_hash(document_id, new_hash, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE documents_table SET hash = ? WHERE document_id = ?
        """, (new_hash, document_id))
        conn.commit()
        conn.close()
        print(f"Updated hash for document_id {document_id} to {new_hash}")
    except sqlite3.Error as e:
        print(f"Database error occurred while updating hash: {e}")

def calculate_pdf_hash(pdf_path, num_chars=300):
    pdf_path = os.path.abspath(pdf_path)
    try:
        with open(pdf_path, 'rb') as file:
            content = file.read(num_chars)
            pdf_hash = hashlib.sha256(content).hexdigest()
            print(f"Generated hash for PDF ({pdf_path}): {pdf_hash}")
            return pdf_hash
    except Exception as e:
        print(f"Failed to calculate PDF hash: {e}")
        return None

def update_current_release_date(document_id, release_date, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE documents_table SET current_release_date = ? WHERE document_id = ?
        """, (release_date, document_id))
        conn.commit()
        conn.close()
        print(f"Updated current release date for document_id {document_id} to {release_date}")
    except sqlite3.Error as e:
        print(f"Database error occurred while updating current release date: {e}")

def check_hash_and_extract_release_date(pdf_path, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    pdf_path = os.path.abspath(pdf_path)

    # Extract document_id from the pdf_path
    match = re.search(r'(\d+)_(\d+)_(\d{8})\.pdf$', pdf_path.replace('\\', '/'))
    if not match:
        print("Invalid PDF path format. Expected format: 'data/raw/pdf/{document_id}_{pipe_id}_{release_date}.pdf'")
        return json.dumps({"status": "error", "message": "Invalid PDF path format"})

    document_id, pipe_id, release_date = match.groups()

    previous_hash = get_previous_hash(document_id, db_path)
    new_hash = calculate_pdf_hash(pdf_path)

    if new_hash is None:
        print("Failed to calculate new hash.")
        return json.dumps({"status": "error", "message": "Failed to calculate new hash"})

    print(f"Previous hash: {previous_hash}")
    print(f"New hash: {new_hash}")

    if previous_hash is None or previous_hash != new_hash:
        update_hash(document_id, new_hash, db_path)
        release_date = extract_release_date(pdf_path)

        if release_date:
            print(f"Extracted release date: {release_date}")
            valid_release_date = release_date.strip("*")
            new_file_name = os.path.join(os.path.dirname(pdf_path), f"{document_id}_2_{valid_release_date}.pdf")

            if not os.path.exists(os.path.dirname(new_file_name)):
                os.makedirs(os.path.dirname(new_file_name))

            shutil.move(pdf_path, new_file_name)
            print(f"File renamed to: {new_file_name}")
            update_current_release_date(document_id, valid_release_date, db_path)
            return json.dumps({"status": "success", "release_date": valid_release_date, "updated_pdf_path": new_file_name})
        else:
            print("Failed to extract release date.")
            return json.dumps({"status": "error", "message": "Failed to extract release date"})
    else:
        print("Hash matches the previous one. No update needed.")
        return json.dumps({"status": "no_update", "message": "Hash matches the previous one. No update needed."})

```

## orchestrator.py

```python
import os
import sys
from datetime import datetime
import json
import sqlite3
import re
import io
from contextlib import redirect_stdout
import logging

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(filename=os.path.join(project_root, 'app', 'logs', 'errors.log'), 
                    level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from scripts.pdf.pdf_download import execute_pdf_download, execute_pdf_download_with_url
from scripts.utils.completions_general import generate_output
from scripts.utils.parse_load import parse_and_load
from scripts.pdf.pdf_hash import check_hash_and_extract_release_date
from scripts.utils.check_date import check_and_update_release_date
from scripts.html_scraping.adp_html import process_adp_html
from scripts.html_scraping.conf_html import process_conference_board_html
from scripts.html_scraping.ny_html import process_ny_html
from scripts.link_scraping.bea_link import process_bea_link
from scripts.link_scraping.nar_link import process_nar_link

# Import the new modules for IDs 3 and 5
from scripts.pipelines.modules.sca import process_sca_logic
from scripts.pipelines.modules.fhfa import process_fhfa_logic

# List of allowed document IDs
ALLOWED_DOCUMENT_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]

# Mapping of document_id to processing functions
PROCESSING_FUNCTIONS = {
    1: process_conference_board_html,
    2: process_conference_board_html,
    11: process_nar_link,
    12: process_bea_link,
    13: process_bea_link,
    14: process_bea_link,
    17: process_ny_html,
    18: process_adp_html,
}

def get_document_details(document_id, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pipe_id, path FROM documents_table WHERE document_id = ?
    """, (document_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def is_already_processed(document_id, release_date, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM summary_table WHERE document_id = ? AND release_date = ?
    """, (document_id, release_date))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

def process_html_content(process_func, url, document_id, pipe_id):
    with io.StringIO() as buf, redirect_stdout(buf):
        process_func(url, document_id, pipe_id)
        log_message = buf.getvalue()

    print(f"Log message for document_id {document_id}: {log_message}")  # Debugging: Print the log message

    # Update the regex pattern to match the actual file path format in the log message
    file_path_match = re.search(r'Page content saved to (.+?\.txt)', log_message)
    base_path = os.path.abspath(os.path.join(project_root, "data", "raw", "txt"))

    if file_path_match:
        file_path = os.path.normpath(os.path.join(base_path, file_path_match.group(1).replace('\\', '/')))
        release_date_match = re.search(r'_(\d{8})\.txt$', file_path)
        if release_date_match:
            release_date = release_date_match.group(1)
            return file_path, release_date, None
        else:
            error_message = f"Failed to extract release date from {file_path}"
            print(error_message)  # Debugging: Log the error
            return None, None, error_message
    else:
        error_message = f"Failed to extract file path from log message: {log_message}"
        print(error_message)  # Debugging: Log the error
        return None, None, error_message

def process_pdf_content(document_id, url, pipe_id):
    with io.StringIO() as buf, redirect_stdout(buf):
        execute_pdf_download(document_id)
        log_message = buf.getvalue()

    print(f"Log message for document_id {document_id}: {log_message}")  # Debugging: Print the log message

    pdf_path_match = re.search(r'PDF downloaded successfully: (.+?\.pdf)', log_message.replace('\\', '/'))

    if pdf_path_match:
        pdf_path = os.path.normpath(os.path.join(project_root, pdf_path_match.group(1)))
        release_date_match = re.search(r'_(\d{8})\.pdf$', pdf_path)
        if release_date_match:
            release_date = release_date_match.group(1)
            return pdf_path, release_date, None
        else:
            error_message = f"Failed to extract release date from {pdf_path}"
            print(error_message)  # Debugging: Log the error
            return None, None, error_message
    else:
        error_message = f"Failed to extract PDF path from log message: {log_message}"
        print(error_message)  # Debugging: Log the error
        return None, None, error_message

def run_pipeline(document_id):
    if document_id not in ALLOWED_DOCUMENT_IDS:
        return "Document ID not allowed"

    details = get_document_details(document_id)
    if not details:
        return f"No details found for document_id {document_id}"

    pipe_id, url = details
    release_date = None
    txt_path = None
    pdf_path = None

    try:
        if document_id in PROCESSING_FUNCTIONS:
            txt_path, release_date, error_message = process_html_content(PROCESSING_FUNCTIONS[document_id], url, document_id, pipe_id)
        elif document_id == 3:
            # Redirect logic to the relevant script for ID 3
            txt_path, release_date, error_message = process_sca_logic(document_id, url, pipe_id)
        elif document_id == 5:
            # Redirect logic to the relevant script for ID 5
            txt_path, release_date, error_message = process_fhfa_logic(document_id, url, pipe_id)
        elif document_id in [4, 6, 7, 8, 9, 10, 15, 16, 19, 20, 21]:
            pdf_path, release_date, error_message = process_pdf_content(document_id, url, pipe_id)

            if not error_message:
                # Step 2: Check hash and extract release date for PDFs
                result = check_hash_and_extract_release_date(pdf_path)

                if "Hash matches the previous one. No update needed." in result:
                    return "Hash matches the previous one. No update needed."

                try:
                    response = json.loads(result)
                except json.JSONDecodeError as e:
                    return f"Failed to parse JSON output: {e}. Output was: {result}"

                if response["status"] == "no_update":
                    return response["message"]

                if response["status"] == "error":
                    return response["message"]

                release_date = response.get("release_date")
                pdf_path = response.get("updated_pdf_path")

                if not release_date or not pdf_path:
                    return "Failed to extract release date or updated PDF path"

        if error_message:
            return f"Error occurred: {error_message}"

        # Check if the content has already been processed
        if release_date and is_already_processed(document_id, release_date):
            return "Content already up-to-date. No processing needed."

        # Check and update release date
        if release_date and not check_and_update_release_date(document_id, release_date):
            return "Execution interrupted due to release date mismatch."

        # Generate output
        if pdf_path:
            generate_output(pdf_path)
        elif txt_path:
            generate_output(txt_path)

        # Parse and load
        processed_file_path = os.path.join(project_root, f"data/processed/{document_id}_{pipe_id}_{release_date}.txt")
        parse_and_load(processed_file_path)

        return "Pipeline executed successfully"
    except Exception as e:
        logger.error(f"Error in run_pipeline for document_id {document_id}: {e}", exc_info=True)
        return f"Error occurred: {e}"

if __name__ == "__main__":
    # Example usage
    document_ids = [1,11]
    #document_ids = [3,5]
    #document_ids = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
    statuses = []
    for document_id in document_ids:
        try:
            result = run_pipeline(document_id)
            status = "success" if "successfully" in result or "No update needed" in result or "No processing needed" in result else "failed"
        except Exception as e:
            logger.error(f"Exception occurred while processing document_id {document_id}: {e}", exc_info=True)
            result = f"Error occurred: {e}"
            status = "failed"
        print(f"Result for document_id {document_id}: {result}")
        statuses.append({"document_id": document_id, "status": status})
    print(statuses)
```

## completions_general.py

```python
import os
import sqlite3
import PyPDF2
import re
import time
from datetime import datetime
from openai import OpenAI

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
from app.config import api_key

client = OpenAI(base_url="https://api.openai.com/v1", api_key=api_key)

# Function to convert PDF to text
def convert_pdf_to_text(pdf_path):
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text += page.extract_text()
    except Exception as e:
        raise RuntimeError(f"Failed to convert PDF to text: {e}")

    return text

# Function to convert TXT to text
def read_txt_file(txt_path):
    text = ""
    try:
        with open(txt_path, 'r', encoding='utf-8') as file:
            text = file.read()
    except Exception as e:
        raise RuntimeError(f"Failed to read TXT file: {e}")

    return text

def extract_release_date(pdf_path, num_chars=500, retries=3, timeout=20):
    def make_request(content, prompt):
        history = [
            {
                "role": "system",
                "content": prompt
            },
            {"role": "user", "content": content},
        ]

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=history,
            temperature=0.1,
            stream=True,
        )

        full_response = ""
        for chunk in response:
            if chunk.choices[0].delta.content:
                full_response += chunk.choices[0].delta.content

        return full_response

    def extract_date_from_response(response):
        match = re.search(r'\*\*(\d{8})\*\*', response)
        if match:
            return match.group(1)
        else:
            fallback_match = re.search(r'(\d{8})', response)
            if fallback_match:
                print("Warning: Release date extracted without asterisks.")
                return fallback_match.group(1)
        return None

    prompt = (
        "You are an assistant designed to replace a python function in an application. "
        "Your task is to analyze the provided text and identify the release date within it. "
        "The release date should be formatted as **YYYYMMDD**. "
        "Please ensure your response strictly follows this format. "
        "Examples: "
        "- Given 'The product was launched on May 30, 2024.', your response should be '**20240530**'. "
        "- Given 'Release Date: 2024-05-30', your response should still be '**20240530**'."
        "OUTPUT FORMAT: **YYYYMMDD** -> DO NOT OUTPUT THE LOGIC USED, ONLY THE DATE, REMEMBER, THIS INFERENCE HAS THE GOAL OF REPLACING A PYTHON FUNCTION"
    )

    for attempt in range(retries):
        try:
            content = convert_pdf_to_text(pdf_path)[:num_chars]
            full_response = make_request(content, prompt)
            print("Full response:", full_response)

            release_date = extract_date_from_response(full_response)
            if release_date:
                return release_date

            if attempt == retries - 1:
                num_chars = 1000
                current_year = datetime.now().year
                prompt = (
                    f"You are an assistant designed to replace a python function in an application. "
                    f"Your task is to analyze the provided text and identify the release date within it. "
                    f"The release date should be formatted as **YYYYMMDD**. "
                    f"Please ensure your response strictly follows this format. "
                    f"Examples: "
                    f"- Given 'The product was launched in May 2024.', your response should be '**20240501**'. "
                    f"- Given 'Release Date: May 2024', your response should still be '**20240501**'."
                    f"OUTPUT FORMAT: **YYYYMMDD** -> DO NOT OUTPUT THE LOGIC USED, ONLY THE DATE, REMEMBER, THIS INFERENCE HAS THE GOAL OF REPLACING A PYTHON FUNCTION"
                )

            time.sleep(timeout)
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(timeout)

    print("Failed to extract release date after multiple attempts.")
    return None

def get_prompt(document_id, db_path=os.path.join(BASE_DIR, 'data/database/database.sqlite')):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            persona_expertise, persona_tone, format_input, format_output_overview_title,
            format_output_overview_description, format_output_overview_enclosure,
            format_output_overview_title_enclosure, format_output_key_takeaways_title,
            format_output_key_takeaways_description, format_output_key_takeaways_enclosure,
            format_output_key_takeaways_title_enclosure, format_output_macro_environment_impacts_title,
            format_output_macro_environment_impacts_description, format_output_macro_environment_impacts_enclosure,
            tasks_1, tasks_2, tasks_3, tasks_4, tasks_5, audience, objective,
            constraints_language_usage, constraints_language_style, constraints_search_tool_use
        FROM prompts_table
        WHERE document_id =?
    """, (document_id,))
    row = cursor.fetchone()
    conn.close()

    prompt_dict = {
        "PERSONA": row[0],
        "PERSONA_TONE": row[1],
        "FORMAT_INPUT": row[2],
        "OVERVIEW_TITLE": row[3],
        "OVERVIEW_DESCRIPTION": row[4],
        "OVERVIEW_ENCLOSURE": row[5],
        "OVERVIEW_TITLE_ENCLOSURE": row[6],
        "KEY_TAKEAWAYS_TITLE": row[7],
        "KEY_TAKEAWAYS_DESCRIPTION": row[8],
        "KEY_TAKEAWAYS_ENCLOSURE": row[9],
        "KEY_TAKEAWAYS_TITLE_ENCLOSURE": row[10],
        "MACRO_ENVIRONMENT_IMPACTS_TITLE": row[11],
        "MACRO_ENVIRONMENT_IMPACTS_DESCRIPTION": row[12],
        "MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE": row[13],
        "TASKS_1": row[14],
        "TASKS_2": row[15],
        "TASKS_3": row[16],
        "TASKS_4": row[17],
        "TASKS_5": row[18],
        "AUDIENCE": row[19],
        "OBJECTIVE": row[20],
        "CONSTRAINTS_LANGUAGE_USAGE": row[21],
        "CONSTRAINTS_LANGUAGE_STYLE": row[22],
        "CONSTRAINTS_SEARCH_TOOL_USE": row[23]
    }

    formatted_prompt = (
        f"#PERSONA:\n{prompt_dict['PERSONA']}\n\n"
        f"#PERSONA_TONE:\n{prompt_dict['PERSONA_TONE']}\n\n"
        f"#AUDIENCE:\n{prompt_dict['AUDIENCE']}\n\n"
        f"#OBJECTIVE:\n{prompt_dict['OBJECTIVE']}\n\n"
        f"#FORMAT_INPUT:\n{prompt_dict['FORMAT_INPUT']}\n\n"
        f"#OVERVIEW:\n{prompt_dict['OVERVIEW_ENCLOSURE']}{prompt_dict['OVERVIEW_TITLE_ENCLOSURE']}{prompt_dict['OVERVIEW_TITLE']}{prompt_dict['OVERVIEW_TITLE_ENCLOSURE']}{prompt_dict['OVERVIEW_DESCRIPTION']}{prompt_dict['OVERVIEW_ENCLOSURE']}\n\n"
        f"#KEY_TAKEAWAYS:\n{prompt_dict['KEY_TAKEAWAYS_ENCLOSURE']}{prompt_dict['KEY_TAKEAWAYS_TITLE_ENCLOSURE']}{prompt_dict['KEY_TAKEAWAYS_TITLE']}{prompt_dict['KEY_TAKEAWAYS_TITLE_ENCLOSURE']}{prompt_dict['KEY_TAKEAWAYS_DESCRIPTION']}{prompt_dict['KEY_TAKEAWAYS_ENCLOSURE']}\n\n"
        f"#MACRO_ENVIRONMENT_IMPACTS:\n{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE']}{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_TITLE']}{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE']}{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_DESCRIPTION']}{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE']}\n\n"
        "#TASKS:\n"
        f"1. {prompt_dict['TASKS_1']}\n"
        f"2. {prompt_dict['TASKS_2']}\n"
        f"3. {prompt_dict['TASKS_3']}\n"
        f"4. {prompt_dict['TASKS_4']}\n"
        f"5. {prompt_dict['TASKS_5']}\n\n"
        f"#CONSTRAINTS_LANGUAGE_USAGE:\n{prompt_dict['CONSTRAINTS_LANGUAGE_USAGE']}\n\n"
        f"#CONSTRAINTS_LANGUAGE_STYLE:\n{prompt_dict['CONSTRAINTS_LANGUAGE_STYLE']}\n\n"
        f"#CONSTRAINTS_SEARCH_TOOL_USE:\n{prompt_dict['CONSTRAINTS_SEARCH_TOOL_USE']}"
    )

    example_file_path = os.path.join(BASE_DIR, "data/examples/processed_ex.txt")
    try:
        with open(example_file_path, 'r') as ex_file:
            prompt_example = ex_file.read()
            formatted_prompt += "\n\n#EXAMPLES:\n" + prompt_example
    except FileNotFoundError:
        formatted_prompt += "\n\n#EXAMPLES:\nNo example available for this prompt ID."

    print(f"Formatted prompt for document_id {document_id}:\n{formatted_prompt}")
    return formatted_prompt

def generate_output(file_path, db_path=os.path.join(BASE_DIR, 'data/database/database.sqlite'), stream_timeout=None):
    def request_inference(history, retries=3, timeout=20):
        for attempt in range(retries):
            try:
                response_stream = client.chat.completions.create(
                    model="gpt-4o",
                    messages=history,
                    temperature=0.1,
                    stream=True,
                    timeout=stream_timeout
                )
                return response_stream
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(timeout)
        return None

    match = re.search(r'(\d+)_(\d+)_(\d{8})\.(txt|pdf)$', file_path)
    if not match:
        print("Invalid file path format. Expected format: 'data/raw/{file_type}/{document_id}_{pipe_id}_{release_date}.txt'")
        return

    document_id, pipe_id, release_date, file_type = match.groups()

    if file_type == 'pdf':
        print("Starting PDF to text conversion...")
        content = convert_pdf_to_text(file_path)
    else:
        print("Starting TXT file reading...")
        content = read_txt_file(file_path)

    if not content:
        print("Failed to read content.")
        return

    print("Retrieving formatted prompt...")
    formatted_prompt = get_prompt(document_id, db_path)
    if not formatted_prompt:
        print(f"No prompt found for document_id {document_id}")
        return

    print("Appending content to the prompt...")
    full_prompt = f"{formatted_prompt}\n\n{content}"

    print("Initializing OpenAI client...")
    history = [
        {
            "role": "system",
            "content": (
                "You are an assistant designed to generate a comprehensive analysis based on the provided document. "
                "Your task is to analyze the document content and create a structured response that adheres to the following prompt format. "
                "Please ensure your response is detailed and follows the guidelines provided."
            )
        },
        {"role": "user", "content": full_prompt},
    ]

    print("Performing LLM inference...")
    response_stream = request_inference(history)
    if not response_stream:
        print("Failed to generate output after multiple attempts.")
        return

    output = ""

    for chunk in response_stream:
        if chunk.choices[0].delta.content:
            print(chunk.choices[0].delta.content, end="", flush=True)
            output += chunk.choices[0].delta.content

    print("\nStreaming completed.")

    print("Saving generated output...")
    save_dir = os.path.join(BASE_DIR, 'data/processed')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(output)

    print(f"Generated output saved to {save_path}")

def generate_short_summaries(file_path, prompt_path=os.path.join(BASE_DIR, "data/prompts/short_summary.txt")):
    def make_request(input_text, prompt):
        history = [
            {
                "role": "system",
                "content": (
                    "You are an assistant designed to generate concise summaries. "
                    "Your task is to analyze the provided text and create a short summary. "
                    "The summary should be clear and concise, capturing the key points of the input text."
                )
            },
            {"role": "user", "content": prompt},
        ]

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=history,
            temperature=0.1,
            max_tokens=500,
            stream=True,
        )

        full_response = ""
        for chunk in response:
            if chunk.choices[0].delta.content:
                full_response += chunk.choices[0].delta.content

        return full_response

    for attempt in range(3):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                input_text = file.read()

            with open(prompt_path, 'r', encoding='utf-8') as file:
                prompt = file.read().replace('{input}', input_text)

            full_response = make_request(input_text, prompt)

            print("Full response:", full_response)

            summary = full_response.strip()
            return summary

        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(20)

    print("Failed to generate short summaries after multiple attempts.")
    return None


############################# Test Functions - #################################

def test_get_prompt():
    document_id = 6  # Example document_id for testing
    db_path = 'data/database/database.sqlite'  # Adjust the path according to your setup
    prompt = get_prompt(document_id, db_path)

def test_generate_output():
    pdf_path = "data/raw/pdf/4_2_20240607.pdf"  # Replace with your PDF path
    db_path = 'data/database/database.sqlite'  # Adjust the path according to your setup
    generate_output(pdf_path, db_path)

def test_extract_date():
    pdf_path = "data/raw/pdf/6_2_20240606.pdf"
    release_date = extract_release_date(pdf_path)
    print(f"Extracted release date: {release_date}")

def test_pdf():
    pdf_path = "data/raw/pdf/6_2_20240606.pdf"
    convert_pdf_to_text(pdf_path)

def test_generate_short_summaries():
    file_path = "data/processed/6_2_20240607.txt"  # Replace with your test file path
    summaries = generate_short_summaries(file_path)
    print("Generated Summaries:")
    print(summaries)

# Uncomment the following lines to run the tests individually -> all tests performed
# test_get_prompt()
# test_generate_output()
# test_pdf()
# test_extract_date()
# test_generate_short_summaries()

```

## parse_load.py

```python
import os
import sys
import sqlite3
import re
from scripts.utils.completions_general import generate_short_summaries

# Define project root directory
project_root  = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

def read_processed_file(file_path):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

    return content

def parse_content(content):
    data = {}
    sections = content.split("\n\n")
    print("Sections found:", sections)  # Debugging: Print all sections to check if they are correctly split

    for section in sections:
        print("Processing section:", section[:20])  # Debugging: Print the start of each section for inspection
        try:
            if section.startswith("**Title:**"):
                data['title'] = section.split("{")[1].split("}")[0]
            elif section.startswith("**Overview:**"):
                data['overview'] = section.split("||")[1].split("||")[0]
            elif section.startswith("**Key Takeaways:**"):
                key_takeaways = section.split("\n")
                takeaways = []
                for takeaway in key_takeaways[1:]:
                    if takeaway.strip():
                        topic = takeaway.split("{**")[1].split("**}")[0]
                        content = takeaway.split("[")[1].split("]")[0]
                        takeaways.append((topic, content))
                data['key_takeaways'] = takeaways
            elif section.startswith("**Macro Environment Impacts**"):
                try:
                    data['macro_environment_impacts'] = section.split("||")[1].split("||")[0]
                except IndexError:
                    print("Error parsing Macro Environment Impacts section without colon.")
            elif section.startswith("**Macro Environment Impacts:**"):
                try:
                    data['macro_environment_impacts'] = section.split("||")[1].split("||")[0]
                except IndexError:
                    print("Error parsing Macro Environment Impacts section with colon.")
        except Exception as e:
            print(f"Error parsing section: {e}")

    # Debugging: Print the parsed data to check if all sections are correctly parsed
    print("Parsed data:", data)

    return data

def insert_data_to_tables(document_id, release_date, data, file_path, db_path=None):
    if db_path is None:
        db_path = os.path.join(project_root, 'data/database/database.sqlite')

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return

    try:
        # Insert into SummaryTable
        cursor.execute("""
            INSERT INTO summary_table (document_id, release_date, style, content)
            VALUES (?,?,?,?)
        """, (document_id, release_date, 'Overview', data['overview']))

        # Insert into KeyTakeawaysTable
        for topic, content in data['key_takeaways']:
            cursor.execute("""
                INSERT INTO key_takeaways_table (document_id, release_date, title, content)
                VALUES (?,?,?,?)
            """, (document_id, release_date, topic, content))

        # Insert into AnalysisTable
        macro_environment_impacts = data.get('macro_environment_impacts', 'No data available')
        cursor.execute("""
            INSERT INTO analysis_table (document_id, release_date, topic, content)
            VALUES (?,?,?,?)
        """, (document_id, release_date, 'Macro Environment Impacts', macro_environment_impacts))

        # Generate short summaries
        short_summaries = generate_short_summaries(file_path)
        print("Generated Summaries:", short_summaries)  # Debugging: Print the generated summaries

        if short_summaries:
            en_match = re.search(r'\[EN\]\n\{(.+?)\}', short_summaries)
            pt_match = re.search(r'\[PT\]\n\{(.+?)\}', short_summaries)

            if en_match and pt_match:
                en_summary = en_match.group(1)
                pt_summary = pt_match.group(1)
                cursor.execute("""
                    UPDATE summary_table
                    SET en_summary = ?, pt_summary = ?
                    WHERE document_id = ? AND release_date = ?
                """, (en_summary, pt_summary, document_id, release_date))
            else:
                print("Error: Could not find the expected summary format in the generated summaries.")
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error with database operation: {e}")
    finally:
        conn.close()
        print(f"Data inserted into tables for document_id {document_id} and release_date {release_date}.")

def parse_and_load(file_path, db_path=None):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return

    # Extract document_id, pipe_id, and release_date from the file name
    base_name = os.path.basename(file_path)
    try:
        document_id, pipe_id, release_date = os.path.splitext(base_name)[0].split('_')
    except ValueError as e:
        print(f"Error parsing filename {base_name}: {e}")
        return

    content = read_processed_file(file_path)

    if content:
        data = parse_content(content)
        insert_data_to_tables(document_id, release_date, data, file_path, db_path)
        print(f"Data parsed and loaded for document_id {document_id} and release_date {release_date}.")
    else:
        print(f"No content found in file {file_path}.")
```

