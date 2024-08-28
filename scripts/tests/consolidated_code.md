## add_grupo_datagro.py

```python
import os
import pandas as pd
import logging
import asyncio

# Set up logging
# Set BASE_DIR to point to the root of the 'app' folder
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Define the correct paths for the grains module
GRAINS_DIR = os.path.join(BASE_DIR, 'automations', 'grains')
DATA_DIR = os.path.join(GRAINS_DIR, 'data')
LOGS_DIR = os.path.join(GRAINS_DIR, 'logs')

# Set up logging
log_file = os.path.join(LOGS_DIR, 'add_grupo_datagro.log')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(level=logging.DEBUG, filename=log_file, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

async def add_grupo_datagro_column(input_file, output_file, ncm_to_grupo_datagro):
    """
    Adds a new column 'GRUPO_DATAGRO' before 'CO_ANO' in the dataset based on the provided NCM to GRUPO_DATAGRO mapping.
    
    Args:
        input_file (str): Path to the filtered CSV file.
        output_file (str): Path to save the enriched CSV file.
        ncm_to_grupo_datagro (dict): Mapping of NCM codes to GRUPO_DATAGRO values.
    """
    try:
        logging.info(f"Processing file: {input_file}")
        
        # Check if the input file exists
        if not os.path.exists(input_file):
            logging.error(f"Input file not found: {input_file}")
            return
        
        # Read the filtered CSV file with handling for different column structures
        try:
            df = pd.read_csv(input_file, delimiter=';', dtype={'CO_NCM': str})
            logging.info(f"Successfully read {len(df)} rows from {input_file}.")
        except pd.errors.ParserError as e:
            logging.error(f"ParserError: {str(e)}. Attempting to handle column mismatch.")
            
            # Skip bad lines when reading the file
            df = pd.read_csv(input_file, delimiter=';', dtype={'CO_NCM': str}, on_bad_lines='skip')
            logging.info(f"Read {len(df)} rows with bad lines skipped from {input_file}.")

        # Add the GRUPO_DATAGRO column based on the mapping
        df['GRUPO_DATAGRO'] = df['CO_NCM'].map(ncm_to_grupo_datagro).fillna('Unknown')

        # Identify if we are dealing with the IMP dataset (with extra columns)
        if 'VL_FRETE' in df.columns and 'VL_SEGURO' in df.columns:
            logging.info("Detected COMEX-IMP dataset structure with additional columns.")
            expected_columns = [
                'GRUPO_DATAGRO', 'CO_ANO', 'CO_MES', 'CO_NCM', 'NO_NCM_POR', 'NO_SH6_POR', 
                'NO_SH4_POR', 'NO_SH2_POR', 'NO_SEC_POR', 'CO_UNID', 'NO_UNID', 'QT_ESTAT', 
                'KG_LIQUIDO', 'VL_FOB', 'VL_FRETE', 'VL_SEGURO', 'CO_PAIS', 'NO_PAIS', 
                'SG_UF_NCM', 'CO_VIA', 'NO_VIA', 'CO_URF'
            ]
        else:
            logging.info("Detected COMEX-EXP dataset structure.")
            expected_columns = [
                'GRUPO_DATAGRO', 'CO_ANO', 'CO_MES', 'CO_NCM', 'NO_NCM_POR', 'NO_SH6_POR', 
                'NO_SH4_POR', 'NO_SH2_POR', 'NO_SEC_POR', 'CO_UNID', 'NO_UNID', 'QT_ESTAT', 
                'KG_LIQUIDO', 'VL_FOB', 'CO_PAIS', 'NO_PAIS', 'SG_UF_NCM', 'CO_VIA', 
                'NO_VIA', 'CO_URF'
            ]

        # Ensure only the expected columns are in the final output
        df = df[[col for col in expected_columns if col in df.columns]]

        # Save the enriched data to a new CSV file
        df.to_csv(output_file, index=False, sep=';')
        logging.info(f"Enriched data saved to {output_file}.")
    
    except Exception as e:
        logging.error(f"An error occurred while adding GRUPO_DATAGRO to {input_file}: {str(e)}")

if __name__ == "__main__":
    # Example file paths for COMEX-IMP
    input_file_imp = os.path.join(DATA_DIR, "COMEX-IMP_FILTERED.csv")
    output_file_imp = os.path.join(DATA_DIR, "COMEX-IMP_ENRICHED.csv")
    
    # Example file paths for COMEX-EXP
    input_file_exp = os.path.join(DATA_DIR, "COMEX-EXP_FILTERED.csv")
    output_file_exp = os.path.join(DATA_DIR, "COMEX-EXP_ENRICHED.csv")
    
    # Example mapping provided by the user
    ncm_to_grupo_datagro = {
        '10051000': 'Milho',
        '10059010': 'Milho',
        '10059090': 'Milho',
        '23040010': 'Farelo de Soja',
        '23040090': 'Farelo de Soja',
        '12010090': 'Soja em Grãos',
        '12011000': 'Soja em Grãos',
        '12019000': 'Soja em Grãos',
        '12010010': 'Soja em Grãos',
        '15071000': 'Óleo de Soja',
        '15079011': 'Óleo de Soja',
        '15079019': 'Óleo de Soja',
        '15079090': 'Óleo de Soja',
        '11010010': 'Farelo de Trigo',
        '11010020': 'Farelo de Trigo',
        '10011100': 'Trigo em Grãos',
        '10011900': 'Trigo em Grãos',
        '10019100': 'Trigo em Grãos',
        '10019900': 'Trigo em Grãos',
        '10011090': 'Trigo em Grãos',
        '10019010': 'Trigo em Grãos',
        '10019090': 'Trigo em Grãos'
    }
    
    # Run the task to add GRUPO_DATAGRO for COMEX-IMP
    asyncio.run(add_grupo_datagro_column(input_file_imp, output_file_imp, ncm_to_grupo_datagro))
    
    # Run the task to add GRUPO_DATAGRO for COMEX-EXP
    asyncio.run(add_grupo_datagro_column(input_file_exp, output_file_exp, ncm_to_grupo_datagro))

```

## clean_data.py

```python
import os
import logging

# Set up logging
# Set BASE_DIR to point to the root of the 'app' folder
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Define the correct paths for the grains module
GRAINS_DIR = os.path.join(BASE_DIR, 'automations', 'grains')
DATA_DIR = os.path.join(GRAINS_DIR, 'data')
LOGS_DIR = os.path.join(GRAINS_DIR, 'logs')

# Set up logging
log_file = os.path.join(LOGS_DIR, 'clean_data.log')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(level=logging.DEBUG, filename=log_file, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def clean_temp_files():
    """
    Cleans up temporary CSV files in the 'data/' directory, keeping only the final enriched CSV and auxiliary files.
    """
    try:
        logging.info("Cleaning up temporary files...")
        
        # Define the files to keep
        keep_files = {"TABELAS_AUXILIARES.xlsx", "COMEX-EXP_FINAL_ENRICHED.csv", "COMEX-IMP_FINAL_ENRICHED.csv"}
        
        # Iterate over the files in the 'data/' directory
        for filename in os.listdir(DATA_DIR):
            file_path = os.path.join(DATA_DIR, filename)
            if filename not in keep_files and filename.endswith(".csv"):
                os.remove(file_path)
                logging.info(f"Deleted temporary file: {file_path}")
        
        logging.info("Temporary files cleaned up successfully.")
    
    except Exception as e:
        logging.error(f"An error occurred during cleanup: {str(e)}")
        raise

if __name__ == "__main__":
    clean_temp_files()

```

## fetch_csv.py

```python
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
    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_REQUESTS, ssl=False)  # Disable SSL verification
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

```

## filter_ncm.py

```python
import os
import pandas as pd
import asyncio
import logging

print(pd.__version__)

# Set BASE_DIR to point to the root of the 'app' folder
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Define the correct paths for the grains module
GRAINS_DIR = os.path.join(BASE_DIR, 'automations', 'grains')
DATA_DIR = os.path.join(GRAINS_DIR, 'data')
LOGS_DIR = os.path.join(GRAINS_DIR, 'logs')

# Set up logging
log_file = os.path.join(LOGS_DIR, 'filter_ncm.log')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(level=logging.DEBUG, filename=log_file, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def safely_convert_to_str(series):
    """
    Safely converts a pandas Series to string type, handling any conversion issues.
    
    Args:
        series (pd.Series): The pandas Series to convert.
        
    Returns:
        pd.Series: The converted Series as strings.
    """
    return series.astype(str).str.strip()

def safely_convert_to_numeric(series):
    """
    Safely converts a pandas Series to numeric type, handling errors gracefully.
    
    Args:
        series (pd.Series): The pandas Series to convert.
        
    Returns:
        pd.Series: The converted Series as numeric (or original if conversion fails).
    """
    return pd.to_numeric(series, errors='coerce')

async def filter_large_csv_by_ncm_in_chunks(input_file, output_file, ncm_list, chunk_size=100000):
    """
    Asynchronously filters a large CSV file in chunks by a list of NCM codes and appends the filtered data to an output file.
    
    Args:
        input_file (str): Path to the large CSV file.
        output_file (str): Path to append the filtered CSV data.
        ncm_list (list): List of NCM codes to filter by.
        chunk_size (int): Number of rows per chunk to read from the large file.
    """
    try:
        logging.info(f"Starting filtering of {input_file} by NCM list.")
        
        # Determine if the dataset is IMP or EXP based on the filename
        is_imp = "COMEX-IMP" in input_file
        
        # Define the expected columns based on dataset type
        if is_imp:
            expected_columns = [
                "CO_ANO", "CO_MES", "CO_NCM", "CO_UNID", "CO_PAIS", "SG_UF_NCM",
                "CO_VIA", "CO_URF", "QT_ESTAT", "KG_LIQUIDO", "VL_FOB", "VL_FRETE", "VL_SEGURO"
            ]
        else:
            expected_columns = [
                "CO_ANO", "CO_MES", "CO_NCM", "CO_UNID", "CO_PAIS", "SG_UF_NCM",
                "CO_VIA", "CO_URF", "QT_ESTAT", "KG_LIQUIDO", "VL_FOB"
            ]
        
        # Read and filter CSV in chunks
        with pd.read_csv(input_file, delimiter=';', chunksize=chunk_size, dtype={'CO_NCM': str}) as reader:
            for i, chunk in enumerate(reader):
                logging.info(f"Processing chunk {i + 1} of {input_file}.")
                
                # Reorder columns to match the expected structure
                chunk = chunk[[col for col in expected_columns if col in chunk.columns]]
                
                # Convert columns safely to appropriate types using .loc[]
                chunk.loc[:, 'CO_NCM'] = safely_convert_to_str(chunk['CO_NCM'])
                for col in ["CO_ANO", "CO_MES", "QT_ESTAT", "KG_LIQUIDO", "VL_FOB", "VL_FRETE", "VL_SEGURO"]:
                    if col in chunk.columns:
                        chunk.loc[:, col] = safely_convert_to_numeric(chunk[col])
                
                filtered_chunk = chunk[chunk['CO_NCM'].isin(ncm_list)]
                
                if not filtered_chunk.empty:
                    # Append filtered chunk to the output file
                    write_header = i == 0 and not os.path.exists(output_file)
                    filtered_chunk.to_csv(output_file, mode='a', index=False, sep=';', header=write_header)
                    
        logging.info(f"Finished filtering {input_file}. Filtered data appended to {output_file}.")
    
    except Exception as e:
        logging.error(f"An error occurred during filtering of {input_file}: {str(e)}")
        raise

async def filter_all_csvs_in_directory(directory, output_file_imp, output_file_exp, ncm_list, chunk_size=100000):
    """
    Asynchronously filters all CSV files in the given directory in chunks and consolidates the results into separate output files for IMP and EXP.
    
    Args:
        directory (str): Path to the directory containing the CSV files.
        output_file_imp (str): Path to save the consolidated filtered IMP CSV.
        output_file_exp (str): Path to save the consolidated filtered EXP CSV.
        ncm_list (list): List of NCM codes to filter by.
        chunk_size (int): Number of rows per chunk to read from each CSV file.
    """
    try:
        # Clear the output files if they already exist
        if output_file_imp and os.path.exists(output_file_imp):
            os.remove(output_file_imp)
        if output_file_exp and os.path.exists(output_file_exp):
            os.remove(output_file_exp)

        tasks = []
        for filename in os.listdir(directory):
            if filename.endswith(".csv"):
                input_file = os.path.join(directory, filename)
                if "COMEX-IMP" in filename and output_file_imp:
                    task = filter_large_csv_by_ncm_in_chunks(input_file, output_file_imp, ncm_list, chunk_size)
                    tasks.append(task)
                elif "COMEX-EXP" in filename and output_file_exp:
                    task = filter_large_csv_by_ncm_in_chunks(input_file, output_file_exp, ncm_list, chunk_size)
                    tasks.append(task)

        # Ensure that all tasks are awaited and executed
        if tasks:
            await asyncio.gather(*tasks)

    except Exception as e:
        logging.error(f"An error occurred during directory processing: {str(e)}")
        raise

if __name__ == "__main__":
    # Correct directory paths for input and output
    directory = DATA_DIR
    output_file_imp = os.path.join(DATA_DIR, "COMEX-IMP_FILTERED.csv")
    output_file_exp = os.path.join(DATA_DIR, "COMEX-EXP_FILTERED.csv")
    ncm_list = [
        '10051000', '10059010', '10059090', '12010010', '12010090',
        '12011000', '12019000', '15071000', '15079011'
    ]
    
    asyncio.run(filter_all_csvs_in_directory(directory, output_file_imp, output_file_exp, ncm_list))

```

## join_aux.py

```python
import os
import pandas as pd
import logging
import asyncio

# Set up logging
# Set BASE_DIR to point to the root of the 'app' folder
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Define the correct paths for the grains module
GRAINS_DIR = os.path.join(BASE_DIR, 'automations', 'grains')
DATA_DIR = os.path.join(GRAINS_DIR, 'data')
LOGS_DIR = os.path.join(GRAINS_DIR, 'logs')

# Set up logging
log_file = os.path.join(LOGS_DIR, 'join_aux.log')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(level=logging.DEBUG, filename=log_file, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def convert_columns_to_str(df, columns):
    """
    Convert the specified columns in a DataFrame to string type.
    
    Args:
        df (pd.DataFrame): DataFrame to modify.
        columns (list): List of column names to convert to strings.
    
    Returns:
        pd.DataFrame: DataFrame with the specified columns converted to string type.
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df

def handle_duplicate_columns(df, column_base_names):
    """
    Handles duplicate columns generated by merging (e.g., _x and _y suffixes).
    
    Args:
        df (pd.DataFrame): The DataFrame containing potential duplicates.
        column_base_names (list): The base names of the columns to check for duplicates.
    
    Returns:
        pd.DataFrame: DataFrame with duplicates handled.
    """
    for col in column_base_names:
        if f"{col}_x" in df.columns and f"{col}_y" in df.columns:
            # If both _x and _y columns exist, combine them, and drop the original columns
            df[col] = df[f"{col}_x"].combine_first(df[f"{col}_y"])  # Prefer _x, fallback to _y if NaN
            df.drop([f"{col}_x", f"{col}_y"], axis=1, inplace=True)
        elif f"{col}_x" in df.columns:
            df.rename(columns={f"{col}_x": col}, inplace=True)
        elif f"{col}_y" in df.columns:
            df.rename(columns={f"{col}_y": col}, inplace=True)
    return df

async def enrich_data_chunked(main_file, aux_tables_file, output_file, chunk_size=50000):
    """
    Enrich the dataset by joining auxiliary tables in chunks.
    
    Args:
        main_file (str): Path to the main CSV file (e.g., the enriched file with GRUPO_DATAGRO).
        aux_tables_file (str): Path to the Excel file containing auxiliary tables.
        output_file (str): Path to save the enriched CSV file.
        chunk_size (int): Number of rows per chunk to read from the main file.
    """
    try:
        logging.info(f"Starting enrichment of {main_file} using {aux_tables_file}.")
        
        # Load the auxiliary tables
        aux_tables = pd.read_excel(aux_tables_file, sheet_name=None)
        logging.info("Auxiliary tables loaded successfully.")
        
        # Extract relevant tables and drop duplicates
        table_1 = convert_columns_to_str(aux_tables['1'].drop_duplicates(subset=['CO_NCM']), ['CO_NCM'])
        table_10 = convert_columns_to_str(aux_tables['10'].drop_duplicates(subset=['CO_PAIS']), ['CO_PAIS'])
        table_14 = convert_columns_to_str(aux_tables['14'].drop_duplicates(subset=['CO_VIA']), ['CO_VIA'])
        table_6 = convert_columns_to_str(aux_tables['6'].drop_duplicates(subset=['CO_UNID']), ['CO_UNID'])

        chunk_idx = 0
        with pd.read_csv(main_file, delimiter=';', chunksize=chunk_size, dtype=str) as reader:
            for chunk in reader:
                logging.info(f"Processing chunk {chunk_idx + 1}. Columns before merge: {list(chunk.columns)}")

                # Convert relevant columns to strings for consistent merging
                chunk = convert_columns_to_str(chunk, ['CO_NCM', 'CO_PAIS', 'CO_VIA', 'CO_UNID'])

                # Perform the sequential merging with deduplicated tables
                chunk = chunk.merge(table_1[['CO_NCM', 'NO_NCM_POR', 'NO_SH6_POR', 'NO_SH4_POR', 'NO_SH2_POR', 'NO_SEC_POR']], on='CO_NCM', how='left')
                logging.info(f"Columns after merging with table_1: {list(chunk.columns)}")

                chunk = chunk.merge(table_10[['CO_PAIS', 'NO_PAIS']], on='CO_PAIS', how='left')
                logging.info(f"Columns after merging with table_10: {list(chunk.columns)}")

                chunk = chunk.merge(table_14[['CO_VIA', 'NO_VIA']], on='CO_VIA', how='left')
                logging.info(f"Columns after merging with table_14: {list(chunk.columns)}")

                chunk = chunk.merge(table_6[['CO_UNID', 'NO_UNID']], on='CO_UNID', how='left')
                logging.info(f"Columns after merging with table_6: {list(chunk.columns)}")

                # Handle duplicated columns (_x, _y suffixes)
                chunk = handle_duplicate_columns(chunk, ['NO_NCM_POR', 'NO_SH6_POR', 'NO_SH4_POR', 'NO_SH2_POR', 'NO_SEC_POR', 'NO_UNID', 'NO_PAIS', 'NO_VIA'])

                logging.info(f"Columns after handling duplicates: {list(chunk.columns)}")

                # Reorder columns as needed
                if 'VL_FRETE' in chunk.columns:
                    # IMP-specific column order
                    chunk = chunk[['GRUPO_DATAGRO', 'CO_ANO', 'CO_MES', 'CO_NCM', 'NO_NCM_POR', 'NO_SH6_POR', 'NO_SH4_POR', 'NO_SH2_POR', 'NO_SEC_POR', 
                                   'CO_UNID', 'NO_UNID', 'QT_ESTAT', 'KG_LIQUIDO', 'VL_FOB', 'VL_FRETE', 'VL_SEGURO', 
                                   'CO_PAIS', 'NO_PAIS', 'SG_UF_NCM', 'CO_VIA', 'NO_VIA', 'CO_URF']]
                else:
                    # EXP-specific column order
                    chunk = chunk[['GRUPO_DATAGRO', 'CO_ANO', 'CO_MES', 'CO_NCM', 'NO_NCM_POR', 'NO_SH6_POR', 'NO_SH4_POR', 'NO_SH2_POR', 'NO_SEC_POR', 
                                   'CO_UNID', 'NO_UNID', 'QT_ESTAT', 'KG_LIQUIDO', 'VL_FOB', 
                                   'CO_PAIS', 'NO_PAIS', 'SG_UF_NCM', 'CO_VIA', 'NO_VIA', 'CO_URF']]

                # Write the chunk to the output file
                mode = 'w' if chunk_idx == 0 else 'a'
                header = True if chunk_idx == 0 else False
                chunk.to_csv(output_file, mode=mode, index=False, sep=';', header=header)
                
                chunk_idx += 1
        
        logging.info(f"Enrichment complete. Data saved to {output_file}.")
    
    except Exception as e:
        logging.error(f"An error occurred during enrichment: {str(e)}")
        raise

if __name__ == "__main__":
    # Update file paths for both IMP and EXP
    main_file = os.path.join(DATA_DIR, "COMEX-EXP_ENRICHED.csv")  # Adjust this based on dataset type (e.g., COMEX-EXP_ENRICHED.csv)
    aux_tables_file = os.path.join(DATA_DIR, "TABELAS_AUXILIARES.xlsx")
    output_file = os.path.join(DATA_DIR, "COMEX-EXP_FINAL_ENRICHED.csv")  # Adjust this based on dataset type
    
    asyncio.run(enrich_data_chunked(main_file, aux_tables_file, output_file))

```

## scrape_date.py

```python
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

```

