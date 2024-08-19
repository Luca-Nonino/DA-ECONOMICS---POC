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
