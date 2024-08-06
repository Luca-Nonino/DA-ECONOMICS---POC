import os
import requests
import json
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# BLS API endpoint (using v1 which doesn't require registration)
BLS_API_URL = "https://api.bls.gov/publicAPI/v1/timeseries/data/"

def fetch_jolts_data(series_id, start_year, end_year):
    headers = {'Content-type': 'application/json'}
    data = json.dumps({
        "seriesid": [series_id],
        "startyear": start_year,
        "endyear": end_year
    })

    try:
        response = requests.post(BLS_API_URL, data=data, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from BLS API: {e}")
        return None

def process_jolts_data(data, document_id, pipe_id):
    if not data or 'Results' not in data or 'series' not in data['Results']:
        logger.error(f"Invalid or empty data received from BLS API: {data}")
        return None

    series = data['Results']['series'][0]
    processed_data = []

    for item in series['data']:
        processed_data.append(f"{item['year']}-{item['period'][1:]}: {item['value']}")

    content = f"Series ID: {series['seriesID']}\n"
    content += "\n".join(processed_data)

    save_path = save_jolts_data(content, document_id, pipe_id)
    return save_path

def save_jolts_data(content, document_id, pipe_id):
    save_dir = os.path.join(PROJECT_ROOT, 'data', 'raw', 'txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{datetime.now().strftime('%Y%m%d')}.txt"
    save_path = os.path.join(save_dir, file_name)

    try:
        with open(save_path, 'w', encoding='utf-8') as file:
            file.write(content)
        logger.info(f"JOLTS data saved to {save_path}")
        return save_path
    except IOError as e:
        logger.error(f"Error saving JOLTS data: {e}")
        return None

def retrieve_jolts_survey(document_id, pipe_id):
    # JOLTS series ID for job openings, total nonfarm
    series_id = "JTS000000000000000JOL"
    start_year = "2022"  # Adjust as needed
    end_year = str(datetime.now().year)

    logger.info(f"Retrieving JOLTS data for series {series_id} from {start_year} to {end_year}")
    
    data = fetch_jolts_data(series_id, start_year, end_year)
    if data:
        logger.info(f"Received data: {json.dumps(data, indent=2)}")
        save_path = process_jolts_data(data, document_id, pipe_id)
        if save_path:
            logger.info(f"JOLTS data successfully retrieved and saved to {save_path}")
            return save_path
    
    logger.error("Failed to retrieve or process JOLTS data")
    return None

# Example usage
if __name__ == "__main__":
    document_id = 31
    pipe_id = "1"
    retrieve_jolts_survey(document_id, pipe_id)
