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
