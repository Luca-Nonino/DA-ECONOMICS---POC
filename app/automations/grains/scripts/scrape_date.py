import requests
import pandas as pd
import logging
from bs4 import BeautifulSoup
import os

# Set BASE_DIR to point to the root of the 'app' folder
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Define the correct paths for the grains module
GRAINS_DIR = os.path.join(BASE_DIR, 'automations', 'grains')
DATA_DIR = os.path.join(GRAINS_DIR, 'data')
LOGS_DIR = os.path.join(GRAINS_DIR, 'logs')

# Set up logging
log_file = os.path.join(LOGS_DIR, 'scrape_date.log')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(level=logging.DEBUG, filename=log_file, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_release_date(url):
    """
    Scrapes the release date from the provided URL.
    
    Args:
        url (str): The URL of the webpage containing the release date.
    
    Returns:
        str: The scraped release date as a string, or None if not found.
    """
    try:
        logging.info(f"Scraping release date from {url}.")
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        date_element = soup.find('span', class_='value')
        
        if date_element:
            release_date = date_element.text.strip()
            logging.info(f"Release date found: {release_date}")
            return release_date
        else:
            logging.warning("Release date element not found.")
            return None
    
    except requests.RequestException as e:
        logging.error(f"Error occurred while scraping the release date: {str(e)}")
        return None

def get_most_recent_co_ano_co_mes(file_path):
    """
    Finds the most recent CO_ANO and CO_MES combination in the dataset.
    
    Args:
        file_path (str): The path to the final enriched CSV file.
    
    Returns:
        tuple: The most recent (CO_ANO, CO_MES) combination as a tuple, or None if not found.
    """
    try:
        logging.info(f"Querying the most recent CO_ANO and CO_MES from {file_path}.")
        df = pd.read_csv(file_path, delimiter=';', dtype=str)
        
        df['CO_ANO'] = pd.to_numeric(df['CO_ANO'], errors='coerce')
        df['CO_MES'] = pd.to_numeric(df['CO_MES'], errors='coerce')

        # Drop rows with NaN in CO_ANO or CO_MES
        df.dropna(subset=['CO_ANO', 'CO_MES'], inplace=True)

        # Find the most recent CO_ANO and CO_MES combination
        most_recent_row = df.sort_values(['CO_ANO', 'CO_MES'], ascending=[False, False]).iloc[0]
        most_recent_combination = (int(most_recent_row['CO_ANO']), int(most_recent_row['CO_MES']))
        
        logging.info(f"Most recent CO_ANO and CO_MES found: {most_recent_combination}")
        return most_recent_combination
    
    except Exception as e:
        logging.error(f"Error occurred while querying the most recent CO_ANO and CO_MES: {str(e)}")
        return None

if __name__ == "__main__":
    # URL for scraping the release date
    release_date_url = "https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"

    # Path to the final enriched CSV file
    enriched_file_path = os.path.join(DATA_DIR, "COMEX_FINAL_ENRICHED.csv")

    # Step 1: Scrape the release date
    release_date = scrape_release_date(release_date_url)
    
    if release_date:
        print(f"Release Date: {release_date}")
    else:
        print("Failed to scrape the release date.")

    # Step 2: Get the most recent CO_ANO and CO_MES combination
    most_recent_co_ano_co_mes = get_most_recent_co_ano_co_mes(enriched_file_path)
    
    if most_recent_co_ano_co_mes:
        print(f"Most Recent CO_ANO and CO_MES: {most_recent_co_ano_co_mes}")
    else:
        print("Failed to query the most recent CO_ANO and CO_MES.")
