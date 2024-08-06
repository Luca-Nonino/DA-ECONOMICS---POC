import logging
import os
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse
from starlette.templating import Jinja2Templates
import sqlite3
from scripts.utils.auth import get_current_user

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)
logging.basicConfig(filename=os.path.join(BASE_DIR, 'app/logs/errors.log'), level=logging.DEBUG)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app/templates"))

indicators_list_app = FastAPI()

@indicators_list_app.get("/", response_class=HTMLResponse)
async def indicators_list(request: Request, user: dict = Depends(get_current_user)):
    try:
        db_path = os.path.join(BASE_DIR, 'data/database/database.sqlite')
        logger.info(f"Connecting to database at {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Fetch unique document names and IDs for the sidebar
        cursor.execute("SELECT DISTINCT document_name, document_id, country, source_name FROM documents_table")
        document_names = cursor.fetchall()
        if document_names:
            logger.debug(f"Fetched document names: {document_names}")
        else:
            logger.debug("No document names fetched")

        conn.close()
        logger.info("Returning template response")
        return templates.TemplateResponse("base.html", {"request": request, "document_names": document_names})
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    except Exception as e:
        logger.error(f"Error fetching indicators list: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")

