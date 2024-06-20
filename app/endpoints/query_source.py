import logging
import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from starlette.templating import Jinja2Templates
import sqlite3
from typing import Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app/templates"))

query_source_app = FastAPI()

# Set up file handler for logging errors
file_handler = logging.FileHandler(os.path.join(BASE_DIR, 'app/logs/errors.log'))
file_handler.setLevel(logging.ERROR)
logger.addHandler(file_handler)

def log_all_documents(cursor):
    cursor.execute("SELECT document_id, document_name FROM documents_table")
    documents = cursor.fetchall()
    logger.info(f"All documents: {documents}")

@query_source_app.get("/{document_id}", response_class=HTMLResponse)
async def query_source(request: Request, document_id: int, date: Optional[str] = None):
    conn = None
    try:
        db_path = os.path.join(BASE_DIR, 'data/database/database.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        log_all_documents(cursor)

        cursor.execute("SELECT document_id, document_name, source_name, path FROM documents_table WHERE document_id = ?", (document_id,))
        document = cursor.fetchone()
        if not document:
            logger.error(f"Document with ID {document_id} not found")
            raise HTTPException(status_code=404, detail="Document not found")

        document_id, document_name, source_name, path = document

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
            "path": path,
            "en_summary": en_summary,
            "pt_summary": pt_summary,
            "key_takeaways": [{"title": kt[0], "content": kt[1]} for kt in key_takeaways],
            "release_dates": release_dates
        }

        logger.debug(f"Data to be passed to template: {data}")

        return templates.TemplateResponse("indicator_details.html", {"request": request, **data})
    except Exception as e:
        logger.error(f"Error querying source: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if conn:
            conn.close()
