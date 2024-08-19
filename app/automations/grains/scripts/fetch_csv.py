import os
import aiohttp
import asyncio
import aiofiles
import logging
from datetime import datetime
import random

# Set BASE_DIR to point to the root of the 'app' folder
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Define the correct paths for the grains module
GRAINS_DIR = os.path.join(BASE_DIR, 'automations', 'grains')
DATA_DIR = os.path.join(GRAINS_DIR, 'data')
LOGS_DIR = os.path.join(GRAINS_DIR, 'logs')

# Set up logging
log_file = os.path.join(LOGS_DIR, 'fetch_csv.log')
os.makedirs(LOGS_DIR, exist_ok=True)  # Ensure the logs directory is created
logging.basicConfig(level=logging.DEBUG, filename=log_file, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/{}_{}.csv"
START_YEAR = 1997
CURRENT_YEAR = datetime.now().year  # Dynamically determine the current year
MAX_CONCURRENT_REQUESTS = 8  # Adjust based on network/server capacity
RETRY_LIMIT = 5  # Increase retries
BATCH_SIZE = 4  # Number of files to download simultaneously in each batch

async def fetch_and_save_csv(session, year, dataset_type, retries=0):
    """
    Asynchronously fetch and save the CSV file for the given year with retry logic.
    
    Args:
        session (aiohttp.ClientSession): The aiohttp session object.
        year (int): The year for which to fetch the CSV.
        dataset_type (str): Dataset type ('COMEX-EXP' or 'COMEX-IMP').
        retries (int): The current retry attempt.
    """
    url = BASE_URL.format(dataset_type.split("-")[1], year)
    os.makedirs(DATA_DIR, exist_ok=True)  # Ensure the data directory is created
    
    csv_filepath = os.path.join(DATA_DIR, f"{dataset_type}_{year}.csv")
    
    # Check if file already exists (caching mechanism)
    if os.path.exists(csv_filepath):
        logging.info(f"File already exists, skipping download for {year}.")
        return

    try:
        logging.info(f"Fetching CSV for {year} from {url}...")
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
            if response.status == 200:
                content = await response.read()
                async with aiofiles.open(csv_filepath, 'wb') as f:
                    await f.write(content)
                logging.info(f"Saved CSV for {year} to {csv_filepath}.")
            else:
                logging.error(f"Failed to fetch CSV for {year}. Status code: {response.status}")
                if retries < RETRY_LIMIT:
                    backoff_time = (2 ** retries) + random.uniform(0, 1)
                    logging.info(f"Retrying {year} (Attempt {retries + 1}/{RETRY_LIMIT}) after {backoff_time:.2f} seconds...")
                    await asyncio.sleep(backoff_time)
                    await fetch_and_save_csv(session, year, dataset_type, retries=retries + 1)
    except Exception as e:
        logging.error(f"An error occurred while fetching {year}: {str(e)}")
        if retries < RETRY_LIMIT:
            backoff_time = (2 ** retries) + random.uniform(0, 1)
            logging.info(f"Retrying {year} (Attempt {retries + 1}/{RETRY_LIMIT}) after {backoff_time:.2f} seconds...")
            await asyncio.sleep(backoff_time)
            await fetch_and_save_csv(session, year, dataset_type, retries=retries + 1)

async def fetch_csv_batch(session, years, dataset_type, semaphore):
    """
    Fetch a batch of CSV files concurrently, limited by a semaphore for concurrency control.
    
    Args:
        session (aiohttp.ClientSession): The aiohttp session object.
        years (list): The years for which to fetch the CSVs.
        dataset_type (str): Dataset type ('COMEX-EXP' or 'COMEX-IMP').
        semaphore (asyncio.Semaphore): Semaphore to limit concurrency.
    """
    tasks = []
    async with semaphore:
        for year in years:
            task = asyncio.create_task(fetch_and_save_csv(session, year, dataset_type))
            tasks.append(task)
        await asyncio.gather(*tasks)

async def fetch_all_csvs(dataset_type):
    """
    Asynchronously fetch CSVs from START_YEAR to CURRENT_YEAR in batches with concurrency limit.
    
    Args:
        dataset_type (str): Dataset type ('COMEX-EXP' or 'COMEX-IMP').
    """
    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_REQUESTS)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # Limit concurrent requests

    async with aiohttp.ClientSession(connector=connector) as session:
        years = list(range(START_YEAR, CURRENT_YEAR + 1))
        
        # Process in batches to avoid overwhelming the network or server
        for i in range(0, len(years), BATCH_SIZE):
            batch_years = years[i:i+BATCH_SIZE]
            await fetch_csv_batch(session, batch_years, dataset_type, semaphore)

if __name__ == "__main__":
    # Start the async event loop and fetch all CSV files for the specified dataset
    dataset_type = "COMEX-EXP"  # This could be parameterized
    asyncio.run(fetch_all_csvs(dataset_type))
