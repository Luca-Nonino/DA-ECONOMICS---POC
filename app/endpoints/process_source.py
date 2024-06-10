# app/endpoints/process_source.py
import logging
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from scripts.pipelines.orchestrator import run_pipeline

logger = logging.getLogger(__name__)
process_source_app = FastAPI()

logging.basicConfig(filename='app/logs/errors.log', level=logging.DEBUG)

@process_source_app.get("/", response_class=HTMLResponse)
async def update_source(id: int = Query(...)):
    try:
        logger.debug(f"Fetching document with ID: {id}")
        result = run_pipeline(id)
        logger.debug(f"Pipeline result for document ID {id}: {result}")
        return HTMLResponse(content=f"<html><body><h1>Update Result</h1><p>{result}</p></body></html>")
    except Exception as e:
        logger.error(f"Error updating source for document ID {id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")