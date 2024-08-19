import sys
import os
import logging
from fastapi import FastAPI, Request, HTTPException, Depends, Query
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
import sqlite3
from typing import Optional

# Get the directory of the current file
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the root directory to the Python path
sys.path.append(BASE_DIR)

from scripts.utils.auth import get_current_user
from scripts.pipelines.orchestrator import run_pipeline
from app.endpoints.api import api_app
from app.endpoints.automation import router as automation_router  # Import the automation router

# Configure logging
log_directory = os.path.join(BASE_DIR, 'app', 'logs')
os.makedirs(log_directory, exist_ok=True)
log_file = os.path.join(log_directory, 'errors.log')

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
db_path = os.path.join(BASE_DIR, 'data/database/database.sqlite')
logger.info(f"File permissions for {db_path}: {oct(os.stat(db_path).st_mode)[-3:]}")
app = FastAPI()

# Mount static files for serving downloads from automation data directory
AUTOMATIONS_DATA_DIR = os.path.join(BASE_DIR, 'app', 'automations', 'grains', 'data')
app.mount("/download", StaticFiles(directory=AUTOMATIONS_DATA_DIR), name="download")

# Mount the static directory for serving static assets
static_directory = os.path.join(BASE_DIR, 'app', 'static')
app.mount("/static", StaticFiles(directory=static_directory), name="static")

app.mount("/indicators/api", api_app)

# Include the automation router
app.include_router(automation_router)

# Configure Jinja2 templates
templates_directory = os.path.join(BASE_DIR, 'app', 'templates')
templates = Jinja2Templates(directory=templates_directory)

@app.get("", response_class=HTMLResponse)
async def redirect_to_list():
    return RedirectResponse(url="/indicators/list")

@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse(url="/indicators/list")

@app.get("/indicators/list", response_class=HTMLResponse)
async def indicators_list(request: Request, user: dict = Depends(get_current_user)):
    try:
        db_path = os.path.join(BASE_DIR, 'data', 'database', 'database.sqlite')
        logger.info(f"Connecting to database at {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Fetch unique document names and IDs for the sidebar
        cursor.execute("SELECT document_name, document_id, country, source_name FROM documents_table")
        documents = cursor.fetchall()

        # Structure data into a nested dictionary
        country_sources_documents = {}
        for document_name, document_id, country, source_name in documents:
            if country not in country_sources_documents:
                country_sources_documents[country] = {}
            if source_name not in country_sources_documents[country]:
                country_sources_documents[country][source_name] = []
            country_sources_documents[country][source_name].append({
                "document_name": document_name,
                "document_id": document_id
            })

        conn.close()
        logger.info("Returning template response")
        return templates.TemplateResponse("base.html", {
            "request": request,
            "country_sources_documents": country_sources_documents
        })
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    except Exception as e:
        logger.error(f"Error fetching indicators list: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.get("/indicators/query/{doc_id}", response_class=JSONResponse)
async def query_source(request: Request, doc_id: int, date: Optional[str] = None):
    conn = None
    try:
        db_path = os.path.join(BASE_DIR, 'data', 'database', 'database.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT document_id, document_name, source_name, country, path FROM documents_table WHERE document_id = ?", (doc_id,))
        document = cursor.fetchone()
        if not document:
            logger.error(f"Document with ID {doc_id} not found")
            raise HTTPException(status_code=404, detail="Document not found")

        document_id, document_name, source_name, country, path = document

        if date is None:
            cursor.execute("SELECT release_date FROM summary_table WHERE document_id = ? ORDER BY release_date DESC LIMIT 1", (document_id,))
            date = cursor.fetchone()[0]

        cursor.execute("SELECT en_summary, pt_summary FROM summary_table WHERE document_id = ? AND release_date = ?", (document_id, date))
        summary = cursor.fetchone()
        en_summary, pt_summary = summary if summary else ("No summary available", "No summary available")

        cursor.execute("SELECT title, content FROM key_takeaways_table WHERE document_id = ? AND release_date = ?", (document_id, date))
        key_takeaways = cursor.fetchall()

        cursor.execute("SELECT DISTINCT release_date FROM summary_table WHERE document_id = ?", (document_id,))
        release_dates = [row[0] for row in cursor.fetchall()]

        data = {
            "document_id": document_id,
            "document_name": document_name,
            "source_name": source_name,
            "country": country,
            "path": path,
            "en_summary": en_summary,
            "pt_summary": pt_summary,
            "key_takeaways": [{"title": kt[0], "content": kt[1]} for kt in key_takeaways],
            "release_dates": release_dates
        }

        return JSONResponse(data)
    except Exception as e:
        logger.error(f"Error querying source: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if conn:
            conn.close()

@app.post("/indicators/api/update_field")
async def update_field(data: dict):
    try:
        field = data['field']
        value = data['value']
        db_path = os.path.join(BASE_DIR, 'data', 'database', 'database.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"UPDATE prompts_table SET {field} = ? WHERE prompt_id = ?", (value, data['prompt_id']))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error updating field {field}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if conn:
            conn.close()

@app.post("/indicators/api/update_tasks")
async def update_tasks(data: dict):
    try:
        tasks = data['tasks']
        db_path = os.path.join(BASE_DIR, 'data', 'database', 'database.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("UPDATE prompts_table SET tasks_1 = ?, tasks_2 = ?, tasks_3 = ?, tasks_4 = ?, tasks_5 = ? WHERE prompt_id = ?",
                       (*tasks, data['prompt_id']))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error updating tasks: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if conn:
            conn.close()

@app.get("/indicators/update", response_class=HTMLResponse)
async def update_source(id: int = Query(...)):
    logger.info(f"Received request to update source with ID: {id}")
    try:
        result = run_pipeline(id)
        logger.info(f"Pipeline execution result for ID {id}: {result}")
        return HTMLResponse(content=f"<html><body><h1>Update Result</h1><p>{result}</p></body></html>")
    except Exception as e:
        logger.error(f"Error updating source with ID {id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
