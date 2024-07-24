import os
import time
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from webdriver_manager.microsoft import EdgeChromiumDriverManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

def setup_driver():
    edge_options = Options()
    edge_options.add_argument("--headless")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_argument("--disable-dev-shm-usage")
    
    driver_path = EdgeChromiumDriverManager().install()
    logger.debug(f"Using EdgeDriver path: {driver_path}")
    
    service = Service(driver_path)
    driver = webdriver.Edge(service=service, options=edge_options)
    return driver

def fetch_dynamic_content(url):
    driver = setup_driver()
    try:
        logger.info(f"Attempting to fetch content from URL: {url}")
        driver.get(url)
        
        # Wait for a specific element that you know exists on the page after it's loaded
        # Replace 'specific-element-id' with an actual ID from the page
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "specific-element-id"))
        )
        
        logger.debug("Page loaded, getting page source")
        page_source = driver.page_source
        logger.info("Successfully fetched dynamic content")
        return page_source
    except Exception as e:
        logger.error(f"Failed to fetch dynamic content: {e}", exc_info=True)
        return None
    finally:
        driver.quit()

def extract_section_paragraphs(soup):
    sections = []
    section_headers = soup.find_all(['h2', 'h3', 'h4'])
    for section in section_headers:
        section_title = section.get_text(strip=True)
        logger.info(f"Found section title: {section_title}")
        paragraphs = section.find_next_siblings('p')
        section_content = "\n".join(p.get_text(strip=True) for p in paragraphs)
        sections.append(f"{section_title}\n{section_content}")
    return "\n\n".join(sections)

def save_extracted_content(content, document_id, pipe_id, release_date):
    save_dir = os.path.join(PROJECT_ROOT, 'data/raw/txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(content)
    logger.info(f"Content saved to {save_path}")

def process_bcb_html(url, document_id, pipe_id):
    logger.info(f"Starting process for URL: {url} with document_id: {document_id} and pipe_id: {pipe_id}")
    html_content = fetch_dynamic_content(url)
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        release_date = datetime.now().strftime("%Y%m%d")  # Use current date as fallback
        content = extract_section_paragraphs(soup)
        if content.strip():
            save_extracted_content(content, document_id, pipe_id, release_date)
        else:
            logger.error("Failed to extract content.")
    else:
        logger.error("Failed to fetch HTML content.")

# Example usage
if __name__ == "__main__":
    url = "https://www.bcb.gov.br/estatisticas/estatisticasfiscais"
    document_id = 24
    pipe_id = "1"
    logger.info(f"Running process for URL: {url} with document_id: {document_id} and pipe_id: {pipe_id}")
    process_bcb_html(url, document_id, pipe_id)
