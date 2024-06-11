import logging
import os
from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.templating import Jinja2Templates
import sqlite3
from typing import Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app/templates"))

api_app = FastAPI()

# Set up file handler for logging errors
file_handler = logging.FileHandler(os.path.join(BASE_DIR, 'app/logs/errors.log'))
file_handler.setLevel(logging.ERROR)
logger.addHandler(file_handler)

def log_all_documents(cursor):
    cursor.execute("SELECT document_id, document_name FROM documents_table")
    documents = cursor.fetchall()
    logger.info(f"All documents: {documents}")

@api_app.get("/", response_class=JSONResponse)
async def query_source_api(document_id: int, date: str):
    conn = None
    try:
        db_path = os.path.join(BASE_DIR, 'data/database/database.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        log_all_documents(cursor)

        logger.info(f"Querying document with ID: {document_id}")

        cursor.execute("SELECT document_id, document_name, source_name, path FROM documents_table WHERE document_id = ?", (document_id,))
        document = cursor.fetchone()
        if not document:
            logger.error(f"Document with ID {document_id} not found")
            raise HTTPException(status_code=404, detail="Document not found")

        document_id, document_name, source_name, path = document
        logger.info(f"Found document: {document_name}, Source: {source_name}, Path: {path}")

        logger.info(f"Using release date: {date}")

        cursor.execute("SELECT en_summary, pt_summary FROM summary_table WHERE document_id = ? AND release_date = ?", (document_id, date))
        summary = cursor.fetchone()
        if not summary:
            en_summary, pt_summary = "No summary available", "No summary available"
            logger.warning(f"No summaries found for document ID {document_id} and release date {date}")
        else:
            en_summary, pt_summary = summary

        cursor.execute("SELECT title, content FROM key_takeaways_table WHERE document_id = ? AND release_date = ?", (document_id, date))
        key_takeaways = cursor.fetchall()
        if not key_takeaways:
            logger.warning(f"No key takeaways found for document ID {document_id} and release date {date}")

        data = {
            "release_date": date,
            "document_id": document_id,
            "document_name": document_name,
            "source_name": source_name,
            "path": path,
            "en_summary": en_summary,
            "pt_summary": pt_summary,
            "key_takeaways": [{"title": kt[0], "content": kt[1]} for kt in key_takeaways]
        }

        logger.info(f"Successfully retrieved data for document ID {document_id}")
        return JSONResponse(content=data)
    except sqlite3.Error as db_error:
        logger.error(f"Database error: {db_error}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        logger.error(f"Error querying source: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if conn:
            conn.close()
