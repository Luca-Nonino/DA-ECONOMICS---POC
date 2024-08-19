import os
import logging
import asyncio
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from app.automations.grains.scripts.fetch_csv import fetch_all_csvs
from app.automations.grains.scripts.filter_ncm import filter_all_csvs_in_directory
from app.automations.grains.scripts.add_grupo_datagro import add_grupo_datagro_column
from app.automations.grains.scripts.join_aux import enrich_data_chunked
from app.automations.grains.scripts.scrape_date import scrape_release_date, get_most_recent_co_ano_co_mes
from app.automations.grains.scripts.clean_data import clean_temp_files

router = APIRouter()

# Adjusted base path setup
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
AUTOMATIONS_DIR = os.path.join(PROJECT_ROOT, 'app', 'automations', 'grains')
DATA_DIR = os.path.join(AUTOMATIONS_DIR, 'data')
LOGS_DIR = os.path.join(AUTOMATIONS_DIR, 'logs')

# Set up logging
log_file = os.path.join(LOGS_DIR, 'automation.log')
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, filename=log_file, format='%(asctime)s - %(levelname)s - %(message)s')

# Define constants
PASSWORD = "Giovana@443251"
AVAILABLE_DATASETS = ["COMEX-EXP", "COMEX-IMP"]

class DatasetRequest(BaseModel):
    password: str
    dataset: str
    ncm_to_grupo_datagro: dict

@router.post("/automations/grains-db/")
async def process_grains_dataset(request: DatasetRequest):
    if request.password != PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if request.dataset not in AVAILABLE_DATASETS:
        raise HTTPException(status_code=400, detail="Invalid dataset. Use 'COMEX-EXP' or 'COMEX-IMP'.")

    try:
        logging.info(f"Processing dataset: {request.dataset}")

        # Step 1: Fetch and merge datasets
        await fetch_all_csvs(request.dataset)
        logging.info(f"Fetched dataset: {request.dataset}")

        # Step 2: Filter data based on CO_NCM list
        if request.dataset == "COMEX-IMP":
            output_file_imp = os.path.join(DATA_DIR, "COMEX-IMP_FILTERED.csv")
            await filter_all_csvs_in_directory(DATA_DIR, output_file_imp, None, list(request.ncm_to_grupo_datagro.keys()))
            filtered_file = output_file_imp
        elif request.dataset == "COMEX-EXP":
            output_file_exp = os.path.join(DATA_DIR, "COMEX-EXP_FILTERED.csv")
            await filter_all_csvs_in_directory(DATA_DIR, None, output_file_exp, list(request.ncm_to_grupo_datagro.keys()))
            filtered_file = output_file_exp
        logging.info(f"Filtered data for dataset: {request.dataset}")

        # Step 3: Add GRUPO_DATAGRO column
        enriched_file = os.path.join(DATA_DIR, f"{request.dataset}_ENRICHED.csv")
        await add_grupo_datagro_column(filtered_file, enriched_file, request.ncm_to_grupo_datagro)
        logging.info(f"Added GRUPO_DATAGRO column for dataset: {request.dataset}")

        # Step 4: Enrich with auxiliary tables
        final_enriched_file = os.path.join(DATA_DIR, f"{request.dataset}_FINAL_ENRICHED.csv")
        await enrich_data_chunked(enriched_file, os.path.join(DATA_DIR, "TABELAS_AUXILIARES.xlsx"), final_enriched_file)
        logging.info(f"Enriched data with auxiliary tables for dataset: {request.dataset}")

        # Step 5: Scrape release date
        release_date = scrape_release_date("https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta")
        if not release_date:
            logging.warning(f"Failed to scrape release date for dataset: {request.dataset}")
            release_date = "Unknown"
        logging.info(f"Scraped release date: {release_date}")

        # Step 6: Get most recent CO_ANO and CO_MES combination
        most_recent_combination = get_most_recent_co_ano_co_mes(final_enriched_file)
        if not most_recent_combination:
            logging.warning(f"Failed to get most recent CO_ANO and CO_MES for dataset: {request.dataset}")
            most_recent_combination = ("Unknown", "Unknown")
        logging.info(f"Most recent CO_ANO and CO_MES combination: {most_recent_combination}")

        # Step 7: Clean up temporary files
        clean_temp_files()
        logging.info(f"Cleaned up temporary files for dataset: {request.dataset}")

        # Step 8: Construct download link for the final CSV
        download_link = f"/download/{os.path.basename(final_enriched_file)}"
        logging.info(f"Generated download link: {download_link}")

        # Return JSON response with the required data and download link
        return JSONResponse(content={
            "release_date": release_date,
            "most_recent_co_ano_co_mes": most_recent_combination,
            "csv_download_link": download_link
        })

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

