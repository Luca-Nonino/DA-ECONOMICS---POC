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
