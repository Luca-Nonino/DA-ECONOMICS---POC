## main.py

```python
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

# Mount static files and API
static_directory = os.path.join(BASE_DIR, 'app', 'static')
app.mount("/static", StaticFiles(directory=static_directory), name="static")

app.mount("/indicators/api", api_app)

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
        cursor.execute("SELECT DISTINCT document_name, document_id, country, source_name FROM documents_table")
        document_names = cursor.fetchall()

        # Fetch data for the main content
        cursor.execute("SELECT document_id, document_name, source_name, path FROM documents_table")
        data = cursor.fetchall()

        conn.close()
        logger.info("Returning template response")
        return templates.TemplateResponse("base.html", {"request": request, "document_names": document_names, "data": data})
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


```

## api.py

```python
import logging
import os
import sqlite3
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime
import time

logger = logging.getLogger(__name__)
import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import BASE_DIR, client

api_app = FastAPI()

# Set up file handler for logging errors
file_handler = logging.FileHandler(os.path.join(BASE_DIR, 'app/logs/errors.log'))
file_handler.setLevel(logging.ERROR)
logger.addHandler(file_handler)

def log_all_documents(cursor):
    cursor.execute("SELECT document_id, document_name FROM documents_table")
    documents = cursor.fetchall()
    logger.info(f"All documents: {documents}")

def fetch_release_dates(cursor, document_id):
    cursor.execute("SELECT DISTINCT CAST(release_date AS INTEGER) FROM key_takeaways_table WHERE document_id = ? ORDER BY release_date DESC", (document_id,))
    return [row[0] for row in cursor.fetchall()]

@api_app.get("/", response_class=JSONResponse)
async def query_source_api(document_id: int, date: str):
    conn = None
    try:
        db_path = os.path.join(BASE_DIR, 'data/database/database.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        log_all_documents(cursor)

        logger.info(f"Querying document with ID: {document_id}")

        cursor.execute("SELECT document_id, document_name, source_name, country, path FROM documents_table WHERE document_id = ?", (document_id,))
        document = cursor.fetchone()
        if not document:
            logger.error(f"Document with ID {document_id} not found")
            raise HTTPException(status_code=404, detail="Document not found")

        document_id, document_name, source_name, country, path = document
        logger.info(f"Found document: {document_name}, Source: {source_name}, Country: {country}, Path: {path}")

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
            "country": country,
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

@api_app.post("/update_all")
async def update_all(data: dict):
    try:
        db_path = os.path.join(BASE_DIR, 'data/database/database.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE prompts_table
            SET format_output_macro_environment_impacts_description = ?,
                audience = ?,
                objective = ?,
                constraints_language_usage = ?,
                constraints_language_style = ?,
                tasks_1 = ?,
                tasks_2 = ?,
                tasks_3 = ?,
                tasks_4 = ?,
                tasks_5 = ?
            WHERE prompt_id = ?
        """, (
            data['macro_env_desc'],
            data['audience'],
            data['objective'],
            data['constraints_lang_usage'],
            data['constraints_lang_style'],
            data['tasks'][0] if len(data['tasks']) > 0 else None,
            data['tasks'][1] if len(data['tasks']) > 1 else None,
            data['tasks'][2] if len(data['tasks']) > 2 else None,
            data['tasks'][3] if len(data['tasks']) > 3 else None,
            data['tasks'][4] if len(data['tasks']) > 4 else None,
            data['prompt_id']
        ))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error updating all fields: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if conn:
            conn.close()

@api_app.get("/prompts/{doc_id}", response_class=JSONResponse)
async def get_prompts(doc_id: int):
    conn = None
    try:
        db_path = os.path.join(BASE_DIR, 'data/database/database.sqlite')
        logger.info(f"Attempting to connect to database at: {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        logger.info(f"Querying prompt for document ID: {doc_id}")
        cursor.execute("SELECT prompt_id FROM prompts_table WHERE document_id = ?", (doc_id,))
        prompt_id = cursor.fetchone()
        if not prompt_id:
            logger.error(f"Prompt with Document ID {doc_id} not found")
            raise HTTPException(status_code=404, detail="Prompt not found")

        prompt_id = prompt_id[0]
        logger.info(f"Found prompt_id: {prompt_id}")

        cursor.execute("SELECT format_output_macro_environment_impacts_description, audience, objective, constraints_language_usage, constraints_language_style, tasks_1, tasks_2, tasks_3, tasks_4, tasks_5 FROM prompts_table WHERE prompt_id = ?", (prompt_id,))
        prompt = cursor.fetchone()
        if not prompt:
            logger.error(f"Prompt with ID {prompt_id} not found")
            raise HTTPException(status_code=404, detail="Prompt not found")

        format_output_macro_environment_impacts_description, audience, objective, constraints_language_usage, constraints_language_style, tasks_1, tasks_2, tasks_3, tasks_4, tasks_5 = prompt

        data = {
            "prompt_id": prompt_id,
            "format_output_macro_environment_impacts_description": format_output_macro_environment_impacts_description,
            "audience": audience,
            "objective": objective,
            "constraints_language_usage": constraints_language_usage,
            "constraints_language_style": constraints_language_style,
            "tasks": [tasks_1, tasks_2, tasks_3, tasks_4, tasks_5]
        }

        return JSONResponse(data)
    except sqlite3.Error as db_error:
        logger.error(f"Database error: {db_error}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(db_error)}")
    except Exception as e:
        logger.error(f"Error querying prompts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    finally:
        if conn:
            conn.close()

@api_app.post("/generate_pt_summary")
async def generate_pt_summary(request: Request):
    data = await request.json()
    doc_id = data['doc_id']
    release_date = data['release_date']
    en_summary = data['en_summary']
    pt_summary = data['pt_summary']
    key_takeaways = data['key_takeaways']
    prompt_data = data['prompt_data']

    try:
        db_path = os.path.join(BASE_DIR, 'data/database/database.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Fetch release dates
        release_dates = fetch_release_dates(cursor, doc_id)
        if not release_dates:
            raise HTTPException(status_code=404, detail="No release dates found for the document")

        most_recent_date = release_dates[0]
        second_most_recent_date = release_dates[1] if len(release_dates) > 1 else None

        logger.info(f"Most recent date: {most_recent_date}")
        if second_most_recent_date:
            logger.info(f"Second most recent date: {second_most_recent_date}")

        # Fetch data for the most recent and second most recent dates
        def fetch_data_for_date(cursor, doc_id, date):
            cursor.execute("SELECT en_summary, pt_summary FROM summary_table WHERE document_id = ? AND release_date = ?", (doc_id, date))
            summary = cursor.fetchone()
            if not summary:
                logger.warning("Summary not found for date: %s", date)
                return None, None, None
            en_summary, pt_summary = summary

            cursor.execute("SELECT title, content FROM key_takeaways_table WHERE document_id = ? AND release_date = ?", (doc_id, date))
            key_takeaways = cursor.fetchall()

            return en_summary, pt_summary, key_takeaways

        most_recent_en_summary, most_recent_pt_summary, most_recent_key_takeaways = fetch_data_for_date(cursor, doc_id, most_recent_date)
        second_most_recent_en_summary, second_most_recent_pt_summary, second_most_recent_key_takeaways = (None, None, None)

        if second_most_recent_date:
            second_most_recent_en_summary, second_most_recent_pt_summary, second_most_recent_key_takeaways = fetch_data_for_date(cursor, doc_id, second_most_recent_date)

        logger.info("Most recent summaries fetched")
        if second_most_recent_date:
            logger.info("Second most recent summaries fetched")

        if not most_recent_en_summary:
            raise HTTPException(status_code=404, detail="Summary data not found for the most recent date")

        # Generate the prompt
        prompt = (
            f"Translate the following economic report summary into Portuguese and format it for easy sharing on WhatsApp.\n\n"
            f"Summary for {most_recent_date}:\n{most_recent_en_summary}\n\n"
            f"Key Takeaways:\n" + "\n".join([f"{kt[0]}: {kt[1]}" for kt in most_recent_key_takeaways]) + "\n\n"
        )

        if second_most_recent_date and second_most_recent_en_summary:
            prompt += (
                f"Comparison with previous release ({second_most_recent_date}):\n{second_most_recent_en_summary}\n\n"
                f"Key Takeaways:\n" + "\n".join([f"{kt[0]}: {kt[1]}" for kt in second_most_recent_key_takeaways]) + "\n\n"
            )

        prompt += (
            f"Prompt Data:\n"
            f"Macro Environment Description: {prompt_data['macro_env_desc']}\n"
            f"Audience: {prompt_data['audience']}\n"
            f"Objective: {prompt_data['objective']}\n"
            f"Language Usage Constraints: {prompt_data['constraints_lang_usage']}\n"
            f"Language Style Constraints: {prompt_data['constraints_lang_style']}\n"
            f"Tasks: " + "\n".join([f"{i + 1}. {task}" for i, task in enumerate(prompt_data['tasks'])])
        )

        example_file_path = os.path.join(BASE_DIR, "data/examples/summarys_pt.txt")
        try:
            with open(example_file_path, 'r', encoding='utf-8') as ex_file:
                prompt_example = ex_file.read()
                prompt += "\n\n#EXAMPLES:\n" + prompt_example
        except FileNotFoundError:
            prompt += "\n\n#EXAMPLES:\nNo example available for this prompt ID."

        logger.info("Prompt generated")

        # Make the request to the Azure OpenAI API
        history = [
            {"role": "system", "content": "You are an assistant specialized in translating and formatting economic summaries from English to Brazilian Portuguese. Your task is to ensure the translated summaries are clear, accurate, and well-formatted."},
            {"role": "user", "content": prompt}
        ]

        def request_inference(history, retries=3, timeout=20):
            for attempt in range(retries):
                try:
                    response_stream = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=history,
                        temperature=0.1,
                        stream=True,
                        timeout=timeout
                    )
                    logger.info("Request attempt %s successful", attempt + 1)
                    return response_stream
                except Exception as e:
                    logger.error(f"Attempt {attempt + 1} failed: {e}")
                    time.sleep(timeout)
            return None

        response_stream = request_inference(history)
        if not response_stream:
            raise HTTPException(status_code=500, detail="Failed to generate output after multiple attempts")

        pt_summary = ""
        for chunk in response_stream:
            if chunk.choices and chunk.choices[0].delta.content:
                pt_summary += chunk.choices[0].delta.content

        pt_summary = pt_summary.strip()

        logger.info("PT summary generated successfully")

        return JSONResponse({"pt_summary": pt_summary})
    except Exception as e:
        logger.error(f"Error generating PT summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error generating PT summary: {e}")
    finally:
        if conn:
            conn.close()

```

## indicators_list.py

```python
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


```

## process_source.py

```python
import logging
import os
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from scripts.pipelines.orchestrator import run_pipeline

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)
process_source_app = FastAPI()

logging.basicConfig(filename=os.path.join(BASE_DIR, 'app/logs/errors.log'), level=logging.DEBUG)

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

```

## query_source.py

```python
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

        cursor.execute("SELECT document_id, document_name, source_name, country, path FROM documents_table WHERE document_id = ?", (document_id,))
        document = cursor.fetchone()
        if not document:
            logger.error(f"Document with ID {document_id} not found")
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

        logger.debug(f"Data to be passed to template: {data}")

        return templates.TemplateResponse("indicator_details.html", {"request": request, **data})
    except Exception as e:
        logger.error(f"Error querying source: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if conn:
            conn.close()

```

## mdic_html.py

```python
from fake_useragent import UserAgent
import httpx
from bs4 import BeautifulSoup
import os
import sys
from datetime import datetime

# Initialize UserAgent
ua = UserAgent()

# Determine project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to fetch and parse HTML content from the URL using httpx
def fetch_html_content(url):
    headers = {
        'User-Agent': ua.random,
        'Referer': url,
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Accept': 'application/pdf',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
    }
    try:
        with httpx.Client(timeout=30, verify=False) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            return response.content
    except httpx.RequestError as e:
        print(f"Failed to fetch content: {e}")
        return None

# Function to extract the release date in YYYYMMDD format
def extract_release_date(soup):
    date_element = soup.find('h4', class_='date')
    if date_element:
        date_text = date_element.get_text(strip=True).replace("Atualizado em ", "")
        try:
            date_obj = datetime.strptime(date_text, "%d/%m/%Y")
            formatted_date = date_obj.strftime("%Y%m%d")
            return formatted_date
        except ValueError as e:
            print(f"Failed to parse date: {e}")
            return None
    return None

# Function to extract and format the relevant parts of the HTML
def extract_relevant_content(soup):
    relevant_content = []

    # Extract the updated date
    updated_date = soup.find('h4', class_='date')
    if updated_date:
        relevant_content.append(updated_date.get_text(strip=True) + '\n')

    # Extract main results
    main_results_section = soup.find('div', {'id': 'totais---principais-resultados'})
    if main_results_section:
        relevant_content.append(format_section(main_results_section, "Totais - Principais Resultados"))

    # Extract highlights
    highlights_section = soup.find('div', {'id': 'destaques'})
    if highlights_section:
        relevant_content.append(format_section(highlights_section, "Destaques"))

    # Extract totals section
    totals_section = soup.find('div', {'id': 'totais'})
    if totals_section:
        relevant_content.append(format_section(totals_section, "Totais"))

    # Extract sectors and products
    sectors_products_section = soup.find('div', {'id': 'setores-e-produtos'})
    if sectors_products_section:
        relevant_content.append(format_section(sectors_products_section, "Setores e Produtos"))

    return "\n\n".join(relevant_content)

# Function to format each section with proper headings and tables
def format_section(section, heading):
    formatted_section = f"{heading}\n{'=' * len(heading)}\n"
    if section:
        tables = section.find_all('table')
        if tables:
            for table in tables:
                formatted_section += format_table(table) + '\n\n'
        else:
            formatted_section += section.get_text(separator='\n', strip=True) + '\n'
    return formatted_section

# Function to format table data for better readability
def format_table(table):
    formatted_table = ""
    headers = []
    rows = []

    thead = table.find('thead')
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all('th')]
        formatted_table += "\t".join(headers) + "\n"

    tbody = table.find('tbody')
    if tbody:
        for tr in tbody.find_all('tr'):
            row = [td.get_text(strip=True) for td in tr.find_all('td')]
            rows.append("\t".join(row))

    formatted_table += "\n".join(rows)
    return formatted_table

# Function to save the page content as a .txt file
def save_page_content(relevant_content, document_id, pipe_id, release_date):
    save_dir = os.path.join(project_root, 'data', 'raw', 'txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(relevant_content)
    print(f"Page content saved to {save_path}")
    return save_path

# Main function to process the URL and perform both extraction and saving
def process_balança_comercial_html(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        release_date = extract_release_date(soup)
        if release_date:
            print(f"Release date: {release_date}")
            relevant_content = extract_relevant_content(soup)
            file_path = save_page_content(relevant_content, document_id, pipe_id, release_date)
            return file_path, release_date, None  # Return None as the third value
        else:
            print("Failed to extract release date.")
            return None, None, "Failed to extract release date."
    else:
        print("Failed to fetch HTML content.")
        return None, None, "Failed to fetch HTML content."


# Example usage
url = "https://balanca.economia.gov.br/balanca/pg_principal_bc/principais_resultados.html"
document_id = 30  # Replace with actual document_id
pipe_id = "1"
#process_balança_comercial_html(url, document_id, pipe_id)

```

## ibge_link.py

```python
import os
import sys
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

# Determine project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the release date and secondary link
def extract_release_date_and_link(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    release_element = soup.select_one('.lista-noticias__item .lista-noticias__texto')
    if release_element:
        secondary_link_element = release_element.select_one('a')
        release_date_element = release_element.select_one('.lista-noticias__data')
        if secondary_link_element and release_date_element:
            secondary_link = secondary_link_element['href']
            if not secondary_link.startswith('http'):
                secondary_link = f"https://www.ibge.gov.br{secondary_link}"
            release_date_text = release_date_element.get_text(strip=True)
            try:
                release_date = datetime.strptime(release_date_text, "%d/%m/%Y").strftime("%Y%m%d")
            except ValueError as e:
                print(f"Error parsing date: {release_date_text}, Error: {e}")
                return None, None
            return release_date, secondary_link
    return None, None

# Function to extract publication content from the secondary link
def extract_publication_content(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    content_sections = soup.find_all('p')
    main_content = "\n\n".join([section.get_text(separator='\n', strip=True) for section in content_sections])
    return main_content

# Function to save the page content as a .txt file
def save_page_content(content, document_id, pipe_id, release_date):
    save_dir = os.path.join(project_root, 'data', 'raw', 'txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(content)
    print(f"Page content saved to {save_path}")

# Main function to process the URL, extract release date, and save publication content
def process_ibge_link(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        release_date, secondary_link = extract_release_date_and_link(html_content)
        if release_date and secondary_link:
            print(f"Release Date: {release_date}")
            print(f"Secondary Link: {secondary_link}")

            # Fetch content from the secondary link
            secondary_html_content = fetch_html_content(secondary_link)
            if secondary_html_content:
                publication_content = extract_publication_content(secondary_html_content)
                save_page_content(publication_content, document_id, pipe_id, release_date)
                file_path = os.path.join(project_root, 'data', 'raw', 'txt', f"{document_id}_{pipe_id}_{release_date}.txt")
                return file_path, release_date, None
            else:
                error_message = "Failed to fetch secondary HTML content."
                print(error_message)
                return None, None, error_message
        else:
            error_message = "Failed to extract release date or secondary link."
            print(error_message)
            return None, None, error_message
    else:
        error_message = "Failed to fetch HTML content."
        print(error_message)
        return None, None, error_message

############################# Test Examples #################################

# Example usage
if __name__ == "__main__":
    urls = [
        "https://www.ibge.gov.br/estatisticas/economicas/industria/9294-pesquisa-industrial-mensal-producao-fisica-brasil.html?=&t=destaques",
        "https://www.ibge.gov.br/estatisticas/economicas/precos-e-custos/9256-indice-nacional-de-precos-ao-consumidor-amplo.html?=&t=noticias-e-releases",
        "https://www.ibge.gov.br/estatisticas/economicas/precos-e-custos/9260-indice-nacional-de-precos-ao-consumidor-amplo-15.html?=&t=noticias-e-releases",
        "https://www.ibge.gov.br/estatisticas/economicas/comercio/9227-pesquisa-mensal-de-comercio.html?=&t=noticias-e-releases",
        "https://www.ibge.gov.br/estatisticas/economicas/servicos/9229-pesquisa-mensal-de-servicos.html?=&t=noticias-e-releases"
    ]
    document_ids = [25, 26, 27, 28, 29]
    pipe_id = 3
    for url, document_id in zip(urls, document_ids):
        process_ibge_link(url, document_id, pipe_id)

```

## anfavea_link.py

```python
import requests
import os
import sys
from datetime import datetime
import re
import fitz  # PyMuPDF

# Define project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to download the PDF
def download_pdf(url, save_path):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/pdf',
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"PDF downloaded successfully: {save_path}")
        return True
    except Exception as e:
        print(f"Failed to download PDF: {e}")
        return False

# Function to extract the release date from the PDF
def extract_release_date_from_pdf(pdf_path):
    try:
        pdf_document = fitz.open(pdf_path)
        first_page = pdf_document[0]
        text = first_page.get_text()
        
        # Look for a date pattern in the first 1000 characters
        date_pattern = r'\b(\d{1,2})\s+de\s+(janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)\s+de\s+(\d{4})\b'
        date_match = re.search(date_pattern, text[:1000], re.IGNORECASE)
        
        if date_match:
            day, month, year = date_match.groups()
            # Convert Portuguese month names to numbers
            month_dict = {
                'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04',
                'maio': '05', 'junho': '06', 'julho': '07', 'agosto': '08',
                'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12'
            }
            month_num = month_dict[month.lower()]
            return f"{year}{month_num}{day.zfill(2)}"
    except Exception as e:
        print(f"Failed to extract release date from PDF: {e}")
    return None

# Function to generate the PDF URL based on the current month and year
def generate_pdf_url():
    current_date = datetime.now()
    month_dict = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }
    month_name = month_dict[current_date.month]
    year = current_date.year
    return f"https://anfavea.com.br/site/wp-content/uploads/{year}/{current_date.month:02d}/Release_{month_name}{str(year)[-2:]}.pdf"

# Main function to process the URL, download PDF, and save it with the release date
def process_anfavea_link(document_id, pipe_id):
    url = generate_pdf_url()
    print(f"Generated URL: {url}")
    pdf_save_path = os.path.join(PROJECT_ROOT, "data/raw/pdf", f"{document_id}_{pipe_id}.pdf")
    if download_pdf(url, pdf_save_path):
        release_date = extract_release_date_from_pdf(pdf_save_path)
        if release_date:
            final_save_path = os.path.join(PROJECT_ROOT, "data/raw/pdf", f"{document_id}_{pipe_id}_{release_date}.pdf")
            if os.path.exists(final_save_path):
                os.remove(final_save_path)
            os.rename(pdf_save_path, final_save_path)
            print(f"Release Date: {release_date}")
            print(f"Save Path: {final_save_path}")
            return final_save_path, release_date
        else:
            print("Failed to extract release date from PDF.")
    else:
        print("Failed to download PDF.")
    return None, None

# Example usage
if __name__ == "__main__":
    document_id = 22
    pipe_id = 3
    result = process_anfavea_link(document_id, pipe_id)
    print(result)

```

## bcb_api.py

```python
import os
import sys
from datetime import datetime, timedelta
import subprocess

# Define project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
print(f"PROJECT_ROOT: {PROJECT_ROOT}")

# Calculate the last Friday or return the current date if it's Friday
def get_last_friday(date):
    weekday = date.weekday()
    if weekday == 4:
        return date
    days_to_last_friday = (weekday - 4) % 7
    return date - timedelta(days_to_last_friday)

def execute_script(script_path):
    try:
        result = subprocess.run(['python', script_path], check=True, capture_output=True, text=True)
        print(f"Executed {script_path} successfully.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_path}: {e}")
        print(e.stdout)
        print(e.stderr)

def process_bcb_api(document_id, pipe_id):
    script_ema = os.path.join(PROJECT_ROOT, 'scripts', 'api_scraping', 'bcb', 'modules', 'db_read_ema.py')
    script_emm = os.path.join(PROJECT_ROOT, 'scripts', 'api_scraping', 'bcb', 'modules', 'db_read_emm.py')
    execute_script(script_ema)
    execute_script(script_emm)
    mensais_summary_path = os.path.join(PROJECT_ROOT, 'scripts', 'api_scraping', 'bcb', 'modules', 'ExpectativaMercadoMensais_summary_output.txt')
    anuais_summary_path = os.path.join(PROJECT_ROOT, 'scripts', 'api_scraping', 'bcb', 'modules', 'ExpectativasMercadoAnuais_summary_output.txt')
    if not os.path.exists(mensais_summary_path):
        print(f"File not found: {mensais_summary_path}")
        return None, None
    if not os.path.exists(anuais_summary_path):
        print(f"File not found: {anuais_summary_path}")
        return None, None
    with open(mensais_summary_path, 'r', encoding='utf-8') as mensais_file:
        mensais_content = mensais_file.read()
    with open(anuais_summary_path, 'r', encoding='utf-8') as anuais_file:
        anuais_content = anuais_file.read()
    current_date = datetime.now()
    last_friday = get_last_friday(current_date)
    formatted_date = last_friday.strftime('%Y-%m-%d')
    mensais_title = f"Relatório Focus - Dados de expectativas Mensais por indicador - {formatted_date}"
    anuais_title = f"Relatório Focus - Dados de expectativas Anuais por indicador - {formatted_date}"
    consolidated_content = f"{mensais_title}\n\n{mensais_content}\n\n{anuais_title}\n\n{anuais_content}"
    save_dir = os.path.join(PROJECT_ROOT, 'data', 'raw', 'txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{last_friday.strftime('%Y%m%d')}.txt"
    save_path = os.path.join(save_dir, file_name)
    with open(save_path, 'w', encoding='utf-8') as output_file:
        output_file.write(consolidated_content)
    print(f"Consolidated content saved to {save_path}")
    return save_path, formatted_date

# Example usage
if __name__ == "__main__":
    document_id = 23
    pipe_id = 4
    process_bcb_api(document_id, pipe_id)

```

## adp_html.py

```python
import requests
from bs4 import BeautifulSoup
import os
import sys
from datetime import datetime

# Define project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the release date in YYYYMMDD format
def extract_release_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    date_element = soup.find('li', class_='current-report')
    if date_element:
        date_text = date_element.get_text(strip=True)
        # Correct the parsing of the date
        try:
            date_obj = datetime.strptime(date_text, "%B %Y")
            formatted_date = date_obj.strftime("%Y%m") + "01"  # Assuming the release is on the 1st day of the month
            return formatted_date
        except ValueError:
            return datetime.now().strftime("%Y%m%d")  # Fallback to current date if parsing fails
    return datetime.now().strftime("%Y%m%d")

# Function to save the page content as a .txt file
def save_page_content(html_content, document_id, pipe_id, release_date):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract relevant sections based on their HTML structure
    sections = []

    # Main report overview
    main_overview = soup.select_one('.report-overview.NER')
    if main_overview:
        sections.append(main_overview.get_text(separator='\n', strip=True))

    # Change by establishment size
    establishment_size = soup.select_one('.report-section.NER.biz-size')
    if establishment_size:
        sections.append(establishment_size.get_text(separator='\n', strip=True))

    # Change by industry
    industry_section = soup.select_one('.report-section.NER')
    if industry_section:
        sections.append(industry_section.get_text(separator='\n', strip=True))

    # About this report
    about_report = soup.select_one('.prefooter')
    if about_report:
        sections.append(about_report.get_text(separator='\n', strip=True))

    main_content = "\n\n".join(sections)

    save_dir = os.path.join(PROJECT_ROOT, 'data/raw/txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(main_content)
    print(f"Page content saved to {save_path}")

# Main function to process the URL and perform both extraction and saving
def process_adp_html(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        release_date = extract_release_date(html_content)
        if release_date:
            save_page_content(html_content, document_id, pipe_id, release_date)
        else:
            print("Failed to extract release date.")
    else:
        print("Failed to fetch HTML content.")

# Example usage
url = "https://adpemploymentreport.com/"
document_id = 18  # Replace with actual document_id
pipe_id = "1"
#process_adp_html(url, document_id, pipe_id)

# Example usage
url = "https://adpemploymentreport.com/"
document_id = 18  # Replace with actual document_id
pipe_id = "1"
#process_adp_html(url, document_id, pipe_id)


```

## conf_html.py

```python
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime


# Determine project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the release date in YYYYMMDD format
def extract_release_date(soup):
    date_element = soup.find('p', class_='date')
    if date_element:
        date_text = date_element.get_text(strip=True).replace("Updated: ", "")
        date_obj = datetime.strptime(date_text, "%A, %B %d, %Y")
        formatted_date = date_obj.strftime("%Y%m%d")
        return formatted_date
    return None

# Function to extract the relevant parts of the HTML
def extract_relevant_content(soup):
    relevant_content = []
    # Identify the main content section based on your HTML structure
    main_content = soup.find('div', {'id': 'mainContainer'})

    if main_content:
        elements = main_content.find_all(['h2', 'h3', 'p'])
        for element in elements:
            text = element.get_text(strip=True)
            if text:
                relevant_content.append(text)

    return "\n\n".join(relevant_content)

# Function to save the page content as a .txt file
def save_page_content(relevant_content, document_id, pipe_id, release_date):
    save_dir = os.path.join(project_root, 'data/raw/txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(relevant_content)
    print(f"Page content saved to {save_path}")

# Main function to process the URL and perform both extraction and saving
def process_conference_board_html(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        release_date = extract_release_date(soup)
        if release_date:
            relevant_content = extract_relevant_content(soup)
            save_page_content(relevant_content, document_id, pipe_id, release_date)
        else:
            print("Failed to extract release date.")
    else:
        print("Failed to fetch HTML content.")

############################# Test Examples #################################





# Example usage
url_1 = "https://www.conference-board.org/topics/us-leading-indicators"
document_id_1 = 1
pipe_id_1 = "1"
#process_conference_board_html(url_1, document_id_1, pipe_id_1)

url_2 = "https://www.conference-board.org/topics/consumer-confidence"
document_id_2 = 2
pipe_id_2 = "1"
#process_conference_board_html(url_2, document_id_2, pipe_id_2)
```

## ny_html.py

```python
import requests
from bs4 import BeautifulSoup
import os
import sys


# Determine project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the most recent release date in YYYYMMDD format
def extract_release_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', class_='greyborder')

    if not table:
        print("Table not found. Please check the HTML structure.")
        return None

    dates = []
    months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    year = "2024"  # Hardcoded year as per the provided example

    # Find all rows in the table
    rows = table.find_all('tr', {'valign': 'top'})

    # Define the starting index of months
    month_idx = 0

    for row in rows:
        cells = row.find_all('td', {'class': 'dirCol'})
        for cell in cells:
            # Find the div that contains the date
            date_div = cell.find('div')
            if date_div:
                link = date_div.find('a', {'class': 'pdf'})
                if link:
                    # Construct the full date
                    if month_idx < len(months):
                        month = months[month_idx]
                        date_text = date_div.get_text(strip=True).split()[0]
                        full_date = f"{year} {month} {date_text}"
                        dates.append(full_date)
            # Increment month index only if a valid month cell is processed
            month_idx += 1

    # Extract the last date and format it
    if dates:
        last_date_str = dates[-1]
        parts = last_date_str.split()
        formatted_date = f"{parts[0]}{months.index(parts[1]) + 1:02d}{parts[2]}"
        return formatted_date[:8]
    else:
        return None

# Function to save specific content sections as a .txt file
def save_page_content(html_content, document_id, pipe_id, release_date):
    soup = BeautifulSoup(html_content, 'html.parser')
    content_sections = soup.select("p, div.hidden")
    main_content = "\n\n".join([section.get_text(separator='\n', strip=True) for section in content_sections])

    save_dir = os.path.join(project_root, 'data/raw/txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(main_content)
    print(f"Page content saved to {save_path}")

# Main function to process the URL and extract both release date and content sections
def process_ny_html(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        release_date = extract_release_date(html_content)
        if release_date:
            save_page_content(html_content, document_id, pipe_id, release_date)
        else:
            print("Failed to extract release date.")
    else:
        print("Failed to fetch HTML content.")

############################# Test Examples #################################

# Example usage
url = "https://www.newyorkfed.org/survey/empire/empiresurvey_overview"
document_id = 17  # Replace with actual document_id
pipe_id = "1"
#process_ny_html(url, document_id, pipe_id)

```

## bea_link.py

```python
import os
import sys
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

# Determine project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the release date in YYYYMMDD format
def extract_release_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    date_element = soup.find('div', class_='field--name-field-description')
    if date_element:
        date_text = date_element.get_text(strip=True)
        date_match = re.search(r'Current [Rr]elease:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})', date_text)
        if date_match:
            date_str = date_match.group(1)
            date_obj = datetime.strptime(date_str, "%B %d, %Y")
            formatted_date = date_obj.strftime("%Y%m%d")
            return formatted_date
    return None


# Function to extract the specific publication link
def extract_publication_link(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    # Locate the specific anchor tag based on the href attribute pattern
    anchor_tag = soup.find('a', href=re.compile(r'/news/\d{4}/.+'))
    if anchor_tag:
        return "https://www.bea.gov" + anchor_tag['href']
    return None

# Function to extract the relevant content from the report page
def extract_report_content(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    content_sections = soup.select("p, div.field--name-body")
    main_content = "\n\n".join([section.get_text(separator='\n', strip=True) for section in content_sections])
    return main_content

# Function to save the page content as a .txt file
def save_page_content(content, document_id, pipe_id, release_date):
    save_dir = os.path.join(project_root, 'data', 'raw', 'txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(content)
    print(f"Page content saved to {save_path}")

# Function to process a single URL and perform both extraction and saving
def process_bea_link(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        release_date = extract_release_date(html_content)
        publication_link = extract_publication_link(html_content)
        if release_date and publication_link:
            print(f"Release Date: {release_date}")
            print(f"Publication Link: {publication_link}")

            # Fetch content from the publication link
            publication_content = fetch_html_content(publication_link)
            if publication_content:
                report_content = extract_report_content(publication_content)
                save_page_content(report_content, document_id, pipe_id, release_date)
            else:
                print("Failed to fetch publication content.")
        else:
            print("Failed to extract release date or publication link.")
    else:
        print("Failed to fetch HTML content.")

############################# Test Examples #################################

# Example usage for the first URL
url_1 = "https://www.bea.gov/data/gdp/gross-domestic-product"
document_id_1 = "12"
pipe_id_1 = 3
#process_bea_link(url_1, document_id_1, pipe_id_1)

# Example usage for the second URL
url_2 = "https://www.bea.gov/data/consumer-spending/main"
document_id_2 = "13"
pipe_id_2 = 3
#process_bea_link(url_2, document_id_2, pipe_id_2)

# Example usage for the third URL
url_3 = "https://www.bea.gov/data/intl-trade-investment/international-trade-goods-and-services"
document_id_3 = "43"
pipe_id_3 = 3
#process_bea_link(url_3, document_id_3, pipe_id_3)

```

## fhfa_link.py

```python
import requests
from bs4 import BeautifulSoup
import os
import re
import fitz  # PyMuPDF
from datetime import datetime

# Determine project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the PDF link from the HTML
def extract_pdf_link(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    link_element = soup.find('a', href=re.compile(r'/sites/default/files/\d{4}-\d{2}/FHFA-HPI-Monthly-\d{8}.pdf'))
    if link_element:
        return "https://www.fhfa.gov" + link_element['href']
    return None

# Function to download the PDF
def download_pdf(url, save_path):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        os.makedirs(os.path.dirname(save_path), exist_ok=True)  # Ensure the directory exists
        with open(save_path, 'wb') as file:
            file.write(response.content)
        log_message = f"PDF downloaded successfully: {save_path}"
        print(log_message)
        return True, log_message
    except Exception as e:
        log_message = f"Failed to download PDF: {e}"
        print(log_message)
        return False, log_message

# Adjusted function to extract the release date from the PDF using PyMuPDF
def extract_release_date_from_pdf(pdf_path):
    try:
        pdf_document = fitz.open(pdf_path)
        first_page = pdf_document[0]
        text = first_page.get_text()
        # Adjusted regex to capture the full date format
        date_match = re.search(r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}\b', text)
        if date_match:
            date_str = date_match.group(0)
            date_obj = datetime.strptime(date_str, '%B %d, %Y')
            return date_obj.strftime('%Y%m%d')
    except Exception as e:
        print(f"Failed to extract release date from PDF: {e}")
        return None

# Main function to process the URL, extract PDF link, download PDF, and save it with the release date
def process_fhfa_link(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        pdf_link = extract_pdf_link(html_content)
        if pdf_link:
            print(f"PDF Link: {pdf_link}")
            pdf_save_path = os.path.join(project_root, "data/raw/pdf", f"{document_id}_{pipe_id}.pdf")
            download_success, download_log = download_pdf(pdf_link, pdf_save_path)
            if download_success:
                release_date = extract_release_date_from_pdf(pdf_save_path)
                if release_date:
                    final_save_path = os.path.join(project_root, "data/raw/pdf", f"{document_id}_{pipe_id}_{release_date}.pdf")
                    if os.path.exists(final_save_path):
                        os.remove(final_save_path)
                    os.rename(pdf_save_path, final_save_path)
                    log_message = f"Release Date: {release_date}\nSave Path: {final_save_path}"
                    print(log_message)
                    return log_message
                else:
                    log_message = "Failed to extract release date from PDF."
                    print(log_message)
                    return log_message
            else:
                return download_log
        else:
            log_message = "Failed to extract PDF link."
            print(log_message)
            return log_message
    else:
        log_message = "Failed to fetch HTML content."
        print(log_message)
        return log_message

############################# Test Examples #################################

# Example usage
url = "https://www.fhfa.gov/data/hpi"
document_id = 5  # Replace with actual document_id
pipe_id = 3
#process_fhfa_link(url, document_id, pipe_id)

```

## nar_link.py

```python
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime

# Define the project root directory
PROJECT_ROOT= os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


# Function to fetch and parse HTML content from the URL
def fetch_html_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

# Function to extract the secondary link from the initial page
def extract_secondary_link(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    link_element = soup.find('a', href=True, string=lambda x: x and "Read the full news release" in x)
    if link_element:
        return link_element['href']
    return None

# Function to extract the release date in YYYYMMDD format from the secondary page
def extract_release_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    date_element = soup.find('span', property='dc:date dc:created')
    if date_element:
        date_str = date_element['content']
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
            formatted_date = date_obj.strftime("%Y%m%d")
            return formatted_date
        except ValueError as e:
            print(f"Error parsing date: {date_str}, Error: {e}")
            return None
    print("Date element not found.")
    return None

# Function to extract the relevant content from the report page
def extract_report_content(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    content_sections = soup.find_all('div', class_='field field--body')
    main_content = "\n\n".join([section.get_text(separator='\n', strip=True) for section in content_sections])
    return main_content

# Function to save the page content as a .txt file
def save_page_content(content, document_id, pipe_id, release_date):
    save_dir = os.path.join(PROJECT_ROOT, 'data/raw/txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(content)
    print(f"Page content saved to {save_path}")

# Main function to process the URL and extract both release date and content
def process_nar_link(url, document_id, pipe_id):
    html_content = fetch_html_content(url)
    if html_content:
        secondary_link = extract_secondary_link(html_content)
        if secondary_link:
            if not secondary_link.startswith("http"):
                secondary_link = f"https://www.nar.realtor{secondary_link}"
            print(f"Secondary link extracted: {secondary_link}")
            secondary_html_content = fetch_html_content(secondary_link)
            if secondary_html_content:
                release_date = extract_release_date(secondary_html_content)
                if release_date:
                    report_content = extract_report_content(secondary_html_content)
                    if report_content.strip():  # Check if content is not empty
                        save_page_content(report_content, document_id, pipe_id, release_date)
                    else:
                        print("Extracted report content is empty.")
                else:
                    print("Failed to extract release date.")
            else:
                print("Failed to fetch secondary HTML content.")
        else:
            print("Failed to extract secondary link.")
    else:
        print("Failed to fetch HTML content.")

############################# Test Examples #################################

# Example usage
url = "https://www.nar.realtor/research-and-statistics/housing-statistics/existing-home-sales"
document_id = 11  # Replace with actual document_id
pipe_id = 3
#process_nar_link(url, document_id, pipe_id)
```

## pdf_download.py

```python
import requests
import os
import sqlite3
from datetime import datetime
import time
from fake_useragent import UserAgent
import logging
import sys

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(filename=os.path.join(project_root, 'app', 'logs', 'errors.log'),
                    level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection function
def get_document_details(document_id, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT pipe_id, document_id, path
            FROM documents_table
            WHERE document_id = ?
        """, (document_id,))
        result = cursor.fetchone()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error accessing database file at path: {db_path}. Error: {e}", exc_info=True)
        raise

# Generic PDF download function
def download_pdf(url, save_path):
    save_path = os.path.abspath(save_path)
    ua = UserAgent()
    headers = {
        'User-Agent': ua.random,
        'Referer': url,
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Accept': 'application/pdf',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
    }

    try:
        with requests.Session() as session:
            session.headers.update(headers)
            time.sleep(2)  # Add a 2-second delay before making the request
            response = session.get(url)
            response.raise_for_status()  # Check if the request was successful
            with open(save_path, 'wb') as file:
                file.write(response.content)
            print(f"PDF downloaded successfully: {os.path.normpath(save_path)}")
    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code == 403:
            print("HTTP 403 Forbidden error. Retrying with different headers.")
            headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            try:
                response = session.get(url, headers=headers)
                response.raise_for_status()
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                print(f"PDF downloaded successfully: {os.path.normpath(save_path)}")
            except Exception as e:
                logger.error(f"Failed to download PDF on retry. URL: {url}, Save Path: {save_path}. Error: {e}", exc_info=True)
        else:
            logger.error(f"HTTP error occurred. URL: {url}, Save Path: {save_path}. HTTP Error: {http_err}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to download PDF. URL: {url}, Save Path: {save_path}. Error: {e}", exc_info=True)

# Function for the first pipeline type
def execute_pdf_download(document_id, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    # Fetch document details
    details = get_document_details(document_id, db_path)
    if not details:
        print(f"No document found with ID {document_id}")
        return

    pipe_id, document_id, url = details

    # Define save path and file name
    current_date = datetime.now().strftime("%Y%m%d")
    save_dir = os.path.abspath(os.path.join(project_root, 'data', 'raw', 'pdf'))
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{document_id}_{pipe_id}_{current_date}.pdf")

    # Download the PDF
    download_pdf(url, save_path)

# Function for the second pipeline type
def execute_pdf_download_with_url(document_id, url, current_release_date, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    # Fetch document details
    details = get_document_details(document_id, db_path)
    if not details:
        print(f"No document found with ID {document_id}")
        return

    pipe_id, document_id, _ = details  # We ignore the URL from the database in this case

    # Define save path and file name
    save_dir = os.path.abspath(os.path.join(project_root, 'data', 'raw', 'pdf'))
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{document_id}_{pipe_id}_{current_release_date}.pdf")

    # Download the PDF
    download_pdf(url, save_path)

```

## pdf_hash.py

```python
import hashlib
import sqlite3
import os
import sys
import shutil
import re
import json
from datetime import datetime

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from scripts.utils.completions_general import extract_release_date

def get_previous_hash(document_id, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT hash FROM documents_table WHERE document_id = ?
        """, (document_id,))
        result = cursor.fetchone()
        conn.close()
        print(f"Fetched previous hash for document_id {document_id}: {result[0] if result else 'None'}")
        return result[0] if result else None
    except sqlite3.Error as e:
        print(f"Database error occurred while fetching previous hash: {e}")
        return None

def update_hash(document_id, new_hash, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE documents_table SET hash = ? WHERE document_id = ?
        """, (new_hash, document_id))
        conn.commit()
        conn.close()
        print(f"Updated hash for document_id {document_id} to {new_hash}")
    except sqlite3.Error as e:
        print(f"Database error occurred while updating hash: {e}")

def calculate_pdf_hash(pdf_path, num_chars=300):
    pdf_path = os.path.abspath(pdf_path)
    try:
        with open(pdf_path, 'rb') as file:
            content = file.read(num_chars)
            pdf_hash = hashlib.sha256(content).hexdigest()
            print(f"Generated hash for PDF ({pdf_path}): {pdf_hash}")
            return pdf_hash
    except Exception as e:
        print(f"Failed to calculate PDF hash: {e}")
        return None

def update_current_release_date(document_id, release_date, db_path):
    db_path = os.path.abspath(db_path)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE documents_table SET current_release_date = ? WHERE document_id = ?
        """, (release_date, document_id))
        conn.commit()
        conn.close()
        print(f"Updated current release date for document_id {document_id} to {release_date}")
    except sqlite3.Error as e:
        print(f"Database error occurred while updating current release date: {e}")

def check_hash_and_extract_release_date(pdf_path, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    pdf_path = os.path.abspath(pdf_path)

    # Extract document_id from the pdf_path
    match = re.search(r'(\d+)_(\d+)_(\d{8})\.pdf$', pdf_path.replace('\\', '/'))
    if not match:
        print("Invalid PDF path format. Expected format: 'data/raw/pdf/{document_id}_{pipe_id}_{release_date}.pdf'")
        return json.dumps({"status": "error", "message": "Invalid PDF path format"})

    document_id, pipe_id, release_date = match.groups()

    previous_hash = get_previous_hash(document_id, db_path)
    new_hash = calculate_pdf_hash(pdf_path)

    if new_hash is None:
        print("Failed to calculate new hash.")
        return json.dumps({"status": "error", "message": "Failed to calculate new hash"})

    print(f"Previous hash: {previous_hash}")
    print(f"New hash: {new_hash}")

    if previous_hash is None or previous_hash != new_hash:
        update_hash(document_id, new_hash, db_path)
        release_date = extract_release_date(pdf_path)

        if release_date:
            print(f"Extracted release date: {release_date}")
            valid_release_date = release_date.strip("*")
            new_file_name = os.path.join(os.path.dirname(pdf_path), f"{document_id}_{pipe_id}_{valid_release_date}.pdf")

            if not os.path.exists(os.path.dirname(new_file_name)):
                os.makedirs(os.path.dirname(new_file_name))

            shutil.move(pdf_path, new_file_name)
            print(f"File renamed to: {new_file_name}")
            update_current_release_date(document_id, valid_release_date, db_path)
            return json.dumps({"status": "success", "release_date": valid_release_date, "updated_pdf_path": new_file_name})
        else:
            print("Failed to extract release date.")
            return json.dumps({"status": "error", "message": "Failed to extract release date"})
    else:
        print("Hash matches the previous one. No update needed.")
        return json.dumps({"status": "no_update", "message": "Hash matches the previous one. No update needed."})

```

## orchestrator.py

```python
import os
import sys
from datetime import datetime
import json
import sqlite3
import re
import io
import logging
from contextlib import redirect_stdout 

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(filename=os.path.join(project_root, 'app', 'logs', 'orchestrator_br.log'),
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Importing script functions
from scripts.html_scraping.adp_html import process_adp_html
from scripts.html_scraping.conf_html import process_conference_board_html
from scripts.html_scraping.ny_html import process_ny_html
from scripts.html_scraping.sca_html import process_sca_html
from scripts.link_scraping.bea_link import process_bea_link
from scripts.link_scraping.nar_link import process_nar_link
from scripts.pipelines.modules.fhfa import process_fhfa_logic
from scripts.html_scraping.mdic_html import process_balança_comercial_html
from scripts.link_scraping.ibge_link import process_ibge_link
from scripts.link_scraping.anfavea_link import process_anfavea_link
from scripts.api_scraping.bcb_api import process_bcb_api
from scripts.html_scraping.bcb_html import process_bcb_html
from scripts.pdf.pdf_download import execute_pdf_download, execute_pdf_download_with_url
from scripts.utils.completions_general import generate_output
from scripts.utils.parse_load import parse_and_load
from scripts.pdf.pdf_hash import check_hash_and_extract_release_date
from scripts.utils.check_date import check_and_update_release_date
from scripts.html_scraping.census_html import process_census_html
from scripts.html_scraping.atlanta_html import process_gdpnow_html
from scripts.html_scraping.ism_html import process_ism_html  # Import ISM processing function

# List of allowed document IDs
ALLOWED_DOCUMENT_IDS = list(range(1, 50))

# Mapping document IDs to processing functions
PROCESSING_FUNCTIONS = {
    1: process_conference_board_html,
    2: process_conference_board_html,
    39: process_conference_board_html,
    40: process_conference_board_html,
    3: process_sca_html,
    11: process_nar_link,
    12: process_bea_link,
    13: process_bea_link,
    14: process_bea_link,
    42: process_bea_link,
    43: process_bea_link,
    15: process_census_html,
    17: process_ny_html,
    18: process_adp_html,
    22: process_anfavea_link,
    23: process_bcb_api,
    24: process_bcb_html,
    25: process_ibge_link,
    26: process_ibge_link,
    27: process_ibge_link,
    28: process_ibge_link,
    29: process_ibge_link,
    30: process_balança_comercial_html,
    41: process_gdpnow_html,
    44: process_ism_html,  # Added the new script for PMI
    45: process_ism_html,  # Added the new script for Services
}

def get_document_details(document_id, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pipe_id, path, country FROM documents_table WHERE document_id = ?
    """, (document_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def is_already_processed(document_id, release_date, db_path=os.path.join(project_root, 'data', 'database', 'database.sqlite')):
    db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM summary_table WHERE document_id = ? AND release_date = ?
    """, (document_id, release_date))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

def process_html_content(process_func, url, document_id, pipe_id):
    with io.StringIO() as buf, redirect_stdout(buf):
        process_func(url, document_id, pipe_id)
        log_message = buf.getvalue()

    print(f"Log message for document_id {document_id}: {log_message}")  # Debugging: Print the log message

    file_path_match = re.search(r'Page content saved to (.+?\.txt)', log_message)
    base_path = os.path.abspath(os.path.join(project_root, "data", "raw", "txt"))

    if file_path_match:
        file_path = os.path.normpath(os.path.join(base_path, file_path_match.group(1).replace('\\', '/')))
        release_date_match = re.search(r'_(\d{8})\.txt$', file_path)
        if release_date_match:
            release_date = release_date_match.group(1)
            return file_path, release_date, None
        else:
            error_message = f"Failed to extract release date from {file_path}"
            print(error_message)  # Debugging: Log the error
            return None, None, error_message
    else:
        error_message = f"Failed to extract file path from log message: {log_message}"
        print(error_message)  # Debugging: Log the error
        return None, None, error_message

def process_pdf_content(document_id, url, pipe_id):
    with io.StringIO() as buf, redirect_stdout(buf):
        execute_pdf_download(document_id)
        log_message = buf.getvalue()

    print(f"Log message for document_id {document_id}: {log_message}")  # Debugging: Print the log message

    pdf_path_match = re.search(r'PDF downloaded successfully: (.+?\.pdf)', log_message.replace('\\', '/'))

    if pdf_path_match:
        pdf_path = os.path.normpath(os.path.join(project_root, pdf_path_match.group(1)))
        release_date_match = re.search(r'_(\d{8})\.pdf$', pdf_path)
        if release_date_match:
            release_date = release_date_match.group(1)
            return pdf_path, release_date, None
        else:
            error_message = f"Failed to extract release date from {pdf_path}"
            print(error_message)  # Debugging: Log the error
            return None, None, error_message
    else:
        error_message = f"Failed to extract PDF path from log message: {log_message}"
        print(error_message)  # Debugging: Log the error
        return None, None, error_message

def process_output(file_path, document_id, pipe_id, release_date):
    if file_path.endswith('.txt'):
        try:
            generate_output(file_path)
            processed_file_path = os.path.join(project_root, f"data/processed/{document_id}_{pipe_id}_{release_date}.txt")
            parse_and_load(processed_file_path)
        except Exception as e:
            logger.error(f"Error processing TXT file for document_id {document_id}: {e}", exc_info=True)
    elif file_path.endswith('.pdf'):
        try:
            result = check_hash_and_extract_release_date(file_path)
            response = json.loads(result)
            if response.get("status") == "success":
                updated_pdf_path = response.get("updated_pdf_path")
                generate_output(updated_pdf_path)
                processed_file_path = os.path.join(project_root, f"data/processed/{document_id}_{pipe_id}_{release_date}.txt")
                parse_and_load(processed_file_path)
            else:
                logger.error(f"Error processing PDF file for document_id {document_id}: {response.get('message')}")
        except Exception as e:
            logger.error(f"Error processing PDF file for document_id {document_id}: {e}", exc_info=True)

def run_pipeline_old(document_id, pipe_id, url):
    if document_id not in ALLOWED_DOCUMENT_IDS:
        return "Document ID not allowed"

    release_date = None
    txt_path = None
    pdf_path = None

    try:
        if document_id in PROCESSING_FUNCTIONS:
            txt_path, release_date, error_message = process_html_content(PROCESSING_FUNCTIONS[document_id], url, document_id, pipe_id)
        elif document_id == 5:
            # Redirect logic to the relevant script for ID 5
            txt_path, release_date, error_message = process_fhfa_logic(document_id, url, pipe_id)
        elif document_id in [4, 6, 7, 8, 9, 10, 16, 19, 20, 21, 31, 32, 33, 34, 35, 36, 37, 38]:
            pdf_path, release_date, error_message = process_pdf_content(document_id, url, pipe_id)

            if not error_message:
                # Step 2: Check hash and extract release date for PDFs
                result = check_hash_and_extract_release_date(pdf_path)

                if "Hash matches the previous one. No update needed." in result:
                    return "Hash matches the previous one. No update needed."

                try:
                    response = json.loads(result)
                except json.JSONDecodeError as e:
                    return f"Failed to parse JSON output: {e}. Output was: {result}"

                if response["status"] == "no_update":
                    return response["message"]

                if response["status"] == "error":
                    return response["message"]

                release_date = response.get("release_date")
                pdf_path = response.get("updated_pdf_path")

                if not release_date or not pdf_path:
                    return "Failed to extract release date or updated PDF path"

        if error_message:
            return f"Error occurred: {error_message}"

        # Check if the content has already been processed
        if release_date and is_already_processed(document_id, release_date):
            return "Content already up-to-date. No processing needed."

        # Check and update release date
        if release_date and not check_and_update_release_date(document_id, release_date):
            return "Execution interrupted due to release date mismatch."

        # Generate output
        if pdf_path:
            generate_output(pdf_path)
        elif txt_path:
            generate_output(txt_path)

        # Parse and load
        processed_file_path = os.path.join(project_root, f"data/processed/{document_id}_{pipe_id}_{release_date}.txt")
        parse_and_load(processed_file_path)

        return "Pipeline executed successfully"
    except Exception as e:
        logger.error(f"Error in run_pipeline for document_id {document_id}: {e}", exc_info=True)
        return f"Error occurred: {e}"

def run_pipeline_new(document_id, pipe_id, url):
    if document_id not in PROCESSING_FUNCTIONS:
        return f"Document ID {document_id} not supported in this orchestrator."

    try:
        process_func = PROCESSING_FUNCTIONS[document_id]
        if document_id in [22, 23]:
            # Specific processing for PDF generating functions
            file_path, release_date = process_func(document_id, pipe_id)
        else:
            # Generic processing for HTML scraping functions
            file_path, release_date, error_message = process_func(url, document_id, pipe_id)
        
        if file_path and release_date:
            # Ensure release_date is in YYYYMMDD format
            try:
                # Try parsing as YYYY-MM-DD first
                parsed_date = datetime.strptime(release_date, "%Y-%m-%d")
            except ValueError:
                try:
                    # If that fails, try parsing as YYYYMMDD
                    parsed_date = datetime.strptime(release_date, "%Y%m%d")
                except ValueError:
                    # If both fail, log an error and return
                    logger.error(f"Invalid date format for document_id {document_id}: {release_date}")
                    return f"Failed to process document_id {document_id}: Invalid date format"

            # Convert to YYYYMMDD format
            formatted_release_date = parsed_date.strftime("%Y%m%d")
            process_output(file_path, document_id, pipe_id, formatted_release_date)
        else:
            return f"Failed to process document_id {document_id}"
    except Exception as e:
        logger.error(f"Error in run_pipeline for document_id {document_id}: {e}", exc_info=True)
        return f"Error occurred: {e}"

    return f"Pipeline executed successfully for document_id {document_id}"

def run_pipeline(document_id):
    details = get_document_details(document_id)
    if not details:
        return f"No details found for document_id {document_id}"

    pipe_id, url, country = details

    # Specific handling for document IDs 44 and 45
    if document_id in [44, 45]:
        try:
            if document_id == 44:
                base_url = "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi"
            else:
                base_url = "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/services"

            process_ism_html(base_url, document_id, pipe_id)
            
            # Extract the output path and release date from the ISM script's log
            file_path = f"data/raw/txt/{document_id}_{pipe_id}_{datetime.now().strftime('%Y%m%d')}.txt"
            release_date = datetime.now().strftime('%Y%m%d')

            process_output(file_path, document_id, pipe_id, release_date)

            return f"Pipeline executed successfully for document_id {document_id}"
        except Exception as e:
            logger.error(f"Error running ism_html.py for document_id {document_id}: {e}", exc_info=True)
            return f"Error occurred: {e}"

    # Default handling for other document IDs
    if country == 'US':
        return run_pipeline_old(document_id, pipe_id, url)
    elif country == 'BR':
        return run_pipeline_new(document_id, pipe_id, url)
    else:
        return f"Unsupported country '{country}' for document_id {document_id}"

if __name__ == "__main__":
    document_ids = [43, 44, 45]  # Specify the document IDs to process
    statuses = []
    for document_id in document_ids:
        try:
            result = run_pipeline(document_id)
            if isinstance(result, str):
                status = "failed" if "Error" in result or "Unsupported" in result else "success"
            else:
                status = "success" if "successfully" in result or "No update needed" in result or "No processing needed" in result else "failed"
        except Exception as e:
            logger.error(f"Exception occurred while processing document_id {document_id}: {e}", exc_info=True)
            result = f"Error occurred: {e}"
            status = "failed"
        print(f"Result for document_id {document_id}: {result}")
        statuses.append({"document_id": document_id, "status": status})
    print(statuses)

```

## check_date.py

```python
import os
import sqlite3

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

def get_current_release_date(document_id, db_path=os.path.join(BASE_DIR, 'data/database/database.sqlite')):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT current_release_date FROM documents_table WHERE document_id = ?
    """, (document_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def update_release_date(document_id, new_date, db_path=os.path.join(BASE_DIR, 'data/database/database.sqlite')):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE documents_table SET current_release_date = ? WHERE document_id = ?
    """, (new_date, document_id))
    conn.commit()
    conn.close()

def check_and_update_release_date(document_id, new_date, db_path=os.path.join(BASE_DIR, 'data/database/database.sqlite')):
    current_date = get_current_release_date(document_id, db_path)
    
    if current_date is None or current_date != new_date:
        update_release_date(document_id, new_date, db_path)
        print(f"Release date updated to {new_date} for document ID {document_id}")
        return True
    else:
        print(f"Release date for document ID {document_id} is already up to date.")
        return False




############################# Test Examples #################################


# Example usage
document_id = 4  # Replace with actual document_id for testing
new_date = '20240527'  # Replace with actual new date for testing
#check_and_update_release_date(document_id, new_date)

```

## completions_general.py

```python
import os
import sqlite3
import PyPDF2
import re
import time
from datetime import datetime
import sys
# Add the project root directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import the Azure OpenAI client from the config module
from app.config import client

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Function to convert PDF to text
def convert_pdf_to_text(pdf_path):
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text += page.extract_text()
    except Exception as e:
        raise RuntimeError(f"Failed to convert PDF to text: {e}")

    return text

# Function to convert TXT to text
def read_txt_file(txt_path):
    text = ""
    try:
        with open(txt_path, 'r', encoding='utf-8') as file:
            text = file.read()
    except Exception as e:
        raise RuntimeError(f"Failed to read TXT file: {e}")

    return text

def extract_release_date(pdf_path, num_chars=1000, retries=3, timeout=20):
    def make_request(content, prompt):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": content},
                ],
                temperature=0.1,
                max_tokens=1000,
                stream=True,
            )

            full_response = ""
            for chunk in response:
                if chunk.choices and chunk.choices[0].delta.content:
                    full_response += chunk.choices[0].delta.content

            return full_response
        except Exception as e:
            print(f"Error in make_request: {e}")
            return None

    def extract_date_from_response(response):
        if response is None:
            return None
        match = re.search(r'\*\*(\d{8})\*\*', response)
        if match:
            return match.group(1)
        else:
            fallback_match = re.search(r'(\d{8})', response)
            if fallback_match:
                print("Warning: Release date extracted without asterisks.")
                return fallback_match.group(1)
        return None

    prompt = (
        "You are an assistant designed to replace a python function in an application. "
        "Your task is to analyze the provided text and identify the release date within it. "
        "The release date should be formatted as **YYYYMMDD**. "
        "Please ensure your response strictly follows this format. "
        "Examples: "
        "- Given 'The product was launched on May 30, 2024.', your response should be '**20240530**'. "
        "- Given 'Release Date: 2024-05-30', your response should still be '**20240530**'."
        "OUTPUT FORMAT: **YYYYMMDD** -> DO NOT OUTPUT THE LOGIC USED, ONLY THE DATE, REMEMBER, THIS INFERENCE HAS THE GOAL OF REPLACING A PYTHON FUNCTION"
        "ABSOLUTELY ALWAYS OUTPUT A SIX LETTER NUMBER FOR THE COMBINED DATE BETWEEN ASTERISKS"
    )

    for attempt in range(retries):
        try:
            content = convert_pdf_to_text(pdf_path)[:num_chars]
            full_response = make_request(content, prompt)
            print("Full response:", full_response)

            release_date = extract_date_from_response(full_response)
            if release_date:
                return release_date

            if attempt == retries - 1:
                num_chars = 1000
                current_year = datetime.now().year
                prompt = (
                    f"You are an assistant designed to replace a python function in an application. "
                    f"Your task is to analyze the provided text and identify the release date within it. "
                    f"The release date should be formatted as **YYYYMMDD**. "
                    f"Please ensure your response strictly follows this format. "
                    f"Examples: "
                    f"- Given 'The product was launched in May 2024.', your response should be '**20240501**'. "
                    f"- Given 'Release Date: May 2024', your response should still be '**20240501**'."
                    f"OUTPUT FORMAT: **YYYYMMDD** -> DO NOT OUTPUT THE LOGIC USED, ONLY THE DATE, REMEMBER, THIS INFERENCE HAS THE GOAL OF REPLACING A PYTHON FUNCTION"
                )

            time.sleep(timeout)
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(timeout)

    print("Failed to extract release date after multiple attempts.")
    return None

def get_prompt(document_id, db_path=os.path.join(BASE_DIR, 'data/database/database.sqlite')):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            p.persona_expertise, p.persona_tone, p.format_input, p.format_output_overview_title,
            p.format_output_overview_description, p.format_output_overview_enclosure,
            p.format_output_overview_title_enclosure, p.format_output_key_takeaways_title,
            p.format_output_key_takeaways_description, p.format_output_key_takeaways_enclosure,
            p.format_output_key_takeaways_title_enclosure, p.format_output_macro_environment_impacts_title,
            p.format_output_macro_environment_impacts_description, p.format_output_macro_environment_impacts_enclosure,
            p.tasks_1, p.tasks_2, p.tasks_3, p.tasks_4, p.tasks_5, p.audience, p.objective,
            p.constraints_language_usage, p.constraints_language_style, p.constraints_search_tool_use,
            d.country
        FROM prompts_table p
        JOIN documents_table d ON p.document_id = d.document_id
        WHERE p.document_id = ?
    """, (document_id,))
    row = cursor.fetchone()
    conn.close()

    if row is None:
        raise ValueError(f"No data found for document_id {document_id}")

    prompt_dict = {
        "PERSONA": row[0],
        "PERSONA_TONE": row[1],
        "FORMAT_INPUT": row[2],
        "OVERVIEW_TITLE": row[3],
        "OVERVIEW_DESCRIPTION": row[4],
        "OVERVIEW_ENCLOSURE": row[5],
        "OVERVIEW_TITLE_ENCLOSURE": row[6],
        "KEY_TAKEAWAYS_TITLE": row[7],
        "KEY_TAKEAWAYS_DESCRIPTION": row[8],
        "KEY_TAKEAWAYS_ENCLOSURE": row[9],
        "KEY_TAKEAWAYS_TITLE_ENCLOSURE": row[10],
        "MACRO_ENVIRONMENT_IMPACTS_TITLE": row[11],
        "MACRO_ENVIRONMENT_IMPACTS_DESCRIPTION": row[12],
        "MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE": row[13],
        "TASKS_1": row[14],
        "TASKS_2": row[15],
        "TASKS_3": row[16],
        "TASKS_4": row[17],
        "TASKS_5": row[18],
        "AUDIENCE": row[19],
        "OBJECTIVE": row[20],
        "CONSTRAINTS_LANGUAGE_USAGE": row[21],
        "CONSTRAINTS_LANGUAGE_STYLE": row[22],
        "CONSTRAINTS_SEARCH_TOOL_USE": row[23],
        "COUNTRY": row[24]
    }

    formatted_prompt = (
        f"#PERSONA:\n{prompt_dict['PERSONA']}\n\n"
        f"#PERSONA_TONE:\n{prompt_dict['PERSONA_TONE']}\n\n"
        f"#AUDIENCE:\n{prompt_dict['AUDIENCE']}\n\n"
        f"#OBJECTIVE:\n{prompt_dict['OBJECTIVE']}\n\n"
        f"#FORMAT_INPUT:\n{prompt_dict['FORMAT_INPUT']}\n\n"
        f"#OVERVIEW:\n{prompt_dict['OVERVIEW_ENCLOSURE']}{prompt_dict['OVERVIEW_TITLE_ENCLOSURE']}{prompt_dict['OVERVIEW_TITLE']}{prompt_dict['OVERVIEW_TITLE_ENCLOSURE']}{prompt_dict['OVERVIEW_DESCRIPTION']}{prompt_dict['OVERVIEW_ENCLOSURE']}\n\n"
        f"#KEY_TAKEAWAYS:\n{prompt_dict['KEY_TAKEAWAYS_ENCLOSURE']}{prompt_dict['KEY_TAKEAWAYS_TITLE_ENCLOSURE']}{prompt_dict['KEY_TAKEAWAYS_TITLE']}{prompt_dict['KEY_TAKEAWAYS_TITLE_ENCLOSURE']}{prompt_dict['KEY_TAKEAWAYS_DESCRIPTION']}{prompt_dict['KEY_TAKEAWAYS_ENCLOSURE']}\n\n"
        f"#MACRO_ENVIRONMENT_IMPACTS:\n{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE']}{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_TITLE']}{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE']}{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_DESCRIPTION']}{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE']}\n\n"
        "#TASKS:\n"
        f"1. {prompt_dict['TASKS_1']}\n"
        f"2. {prompt_dict['TASKS_2']}\n"
        f"3. {prompt_dict['TASKS_3']}\n"
        f"4. {prompt_dict['TASKS_4']}\n"
        f"5. {prompt_dict['TASKS_5']}\n\n"
        f"#CONSTRAINTS_LANGUAGE_USAGE:\n{prompt_dict['CONSTRAINTS_LANGUAGE_USAGE']}\n\n"
        f"#CONSTRAINTS_LANGUAGE_STYLE:\n{prompt_dict['CONSTRAINTS_LANGUAGE_STYLE']}\n\n"
        f"#CONSTRAINTS_SEARCH_TOOL_USE:\n{prompt_dict['CONSTRAINTS_SEARCH_TOOL_USE']}"
    )

    if prompt_dict['COUNTRY'] == 'BR':
        portuguese_system_message = "Sistema: Você é um assistente de IA especializado em análise de dados e relatórios econômicos para o mercado brasileiro. Por favor, responda em português conforme os exemplos fornecidos."
        formatted_prompt = portuguese_system_message + "\n\n" + formatted_prompt
    else:
        english_system_message = "System: You are an AI assistant specialized in data analysis and economic reporting. Please respond in English."
        formatted_prompt = english_system_message + "\n\n" + formatted_prompt

    example_file_path = os.path.join(BASE_DIR, "data/examples/processed_ex_us.txt")
    try:
        with open(example_file_path, 'r') as ex_file:
            prompt_example = ex_file.read()
            formatted_prompt += "\n\n#EXAMPLES:\n" + prompt_example
    except FileNotFoundError:
        formatted_prompt += "\n\n#EXAMPLES:\nNo example available for this prompt ID."

    print(f"Formatted prompt for document_id {document_id}:\n{formatted_prompt}")
    return formatted_prompt


def generate_output(file_path, db_path=os.path.join(BASE_DIR, 'data/database/database.sqlite'), stream_timeout=None):
    def request_inference(history, retries=3, timeout=20):
        for attempt in range(retries):
            try:
                response_stream = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=history,
                    temperature=0.1,
                    stream=True,
                    timeout=stream_timeout
                )
                return response_stream
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(timeout)
        return None

    match = re.search(r'(\d+)_(\d+)_(\d{8})\.(txt|pdf)$', file_path)
    if not match:
        print("Invalid file path format. Expected format: 'data/raw/{file_type}/{document_id}_{pipe_id}_{release_date}.txt'")
        return

    document_id, pipe_id, release_date, file_type = match.groups()

    if file_type == 'pdf':
        print("Starting PDF to text conversion...")
        content = convert_pdf_to_text(file_path)
    else:
        print("Starting TXT file reading...")
        content = read_txt_file(file_path)

    if not content:
        print("Failed to read content.")
        return

    print("Retrieving formatted prompt...")
    formatted_prompt = get_prompt(document_id, db_path)
    if not formatted_prompt:
        print(f"No prompt found for document_id {document_id}")
        return

    print("Appending content to the prompt...")
    full_prompt = f"{formatted_prompt}\n\n{content}"

    print("Initializing Azure OpenAI client...")
    history = [
        {
            "role": "system",
            "content": (
                "You are an assistant designed to generate a comprehensive analysis based on the provided document. "
                "Your task is to analyze the document content and create a structured response that adheres to the following prompt format. "
                "Please ensure your response is detailed and follows the guidelines provided."
                "When provided input that is partially in Brazilian Portuguese, answer in Brazilian Portuguese"
            )
        },
        {"role": "user", "content": full_prompt},
    ]

    print("Performing LLM inference...")
    response_stream = request_inference(history)
    if not response_stream:
        print("Failed to generate output after multiple attempts.")
        return

    output = ""

    for chunk in response_stream:
        if chunk.choices and chunk.choices[0].delta.content:
            content = chunk.choices[0].delta.content
            print(content, end="", flush=True)
            output += content

    print("\nStreaming completed.")

    print("Saving generated output...")
    save_dir = os.path.join(BASE_DIR, 'data/processed')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(output)

    print(f"Generated output saved to {save_path}")


def generate_short_summaries(file_path, prompt_path=os.path.join(BASE_DIR, "data/prompts/short_summary.txt"), stream_timeout=None):
    def request_inference(history, retries=3, timeout=20):
        for attempt in range(retries):
            try:
                response_stream = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=history,
                    temperature=0.1,
                    max_tokens=500,
                    stream=True,
                    timeout=stream_timeout
                )
                return response_stream
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(timeout)
        return None

    for attempt in range(3):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                input_text = file.read()

            with open(prompt_path, 'r', encoding='utf-8') as file:
                prompt = file.read().replace('{input}', input_text)

            print("Initializing Azure OpenAI client...")
            history = [
                {
                    "role": "system",
                    "content": "You are an assistant designed to generate concise summaries. Your task is to analyze the provided text and create a short summary. The summary should be clear and concise, capturing the key points of the input text."
                },
                {"role": "user", "content": prompt},
            ]

            print("Performing LLM inference...")
            response_stream = request_inference(history)
            if not response_stream:
                print("Failed to generate output after multiple attempts.")
                return None

            full_response = ""
            for chunk in response_stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    content = chunk.choices[0].delta.content
                    print(content, end="", flush=True)
                    full_response += content

            print("\nStreaming completed.")
            print("Full response:", full_response)

            summary = full_response.strip()
            return summary

        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(20)

    print("Failed to generate short summaries after multiple attempts.")
    return None


############################# Test Functions - #################################

def test_get_prompt():
    document_id = 6  # Example document_id for testing
    db_path = 'data/database/database.sqlite'  # Adjust the path according to your setup
    prompt = get_prompt(document_id, db_path)

def test_generate_output():
    pdf_path = "data/raw/pdf/4_2_20240607.pdf"  # Replace with your PDF path
    db_path = 'data/database/database.sqlite'  # Adjust the path according to your setup
    generate_output(pdf_path, db_path)

def test_extract_date():
    pdf_path = "data/raw/pdf/36_2_20240719.pdf"
    release_date = extract_release_date(pdf_path)
    print(f"Extracted release date: {release_date}")

def test_pdf():
    pdf_path = "data/raw/pdf/6_2_20240606.pdf"
    convert_pdf_to_text(pdf_path)

def test_generate_short_summaries():
    file_path = "data/processed/4_2_20240607.txt"  # Replace with your test file path
    summaries = generate_short_summaries(file_path)
    print("Generated Summaries:")
    print(summaries)

# Uncomment the following lines to run the tests individually
# test_get_prompt()
# test_generate_output()
# test_pdf()
test_extract_date()
# test_generate_short_summaries()

```

## parse_load.py

```python
import os
import sys
import sqlite3
import re
from scripts.utils.completions_general import generate_short_summaries

# Define project root directory
project_root  = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

def read_processed_file(file_path):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

    return content

def parse_content(content):
    data = {}
    sections = content.split("\n\n")
    print("Sections found:", [s.encode('utf-8') for s in sections])  # Debugging: Print all sections with Unicode encoding

    for section in sections:
        print("Processing section:", section[:20].encode('utf-8'))  # Debugging: Print the start of each section with Unicode encoding
        try:
            if section.startswith("**Title:**"):
                data['title'] = section.split("{")[1].split("}")[0]
            elif section.startswith("**Overview:**"):
                data['overview'] = section.split("||")[1].split("||")[0]
            elif section.startswith("**Key Takeaways:**"):
                key_takeaways = section.split("\n")
                takeaways = []
                for takeaway in key_takeaways[1:]:
                    if takeaway.strip():
                        topic = takeaway.split("{**")[1].split("**}")[0]
                        content = takeaway.split("[")[1].split("]")[0]
                        takeaways.append((topic, content))
                data['key_takeaways'] = takeaways
            elif section.startswith("**Macro Environment Impacts**"):
                try:
                    data['macro_environment_impacts'] = section.split("||")[1].split("||")[0]
                except IndexError:
                    print("Error parsing Macro Environment Impacts section without colon.")
            elif section.startswith("**Macro Environment Impacts:**"):
                try:
                    data['macro_environment_impacts'] = section.split("||")[1].split("||")[0]
                except IndexError:
                    print("Error parsing Macro Environment Impacts section with colon.")
        except Exception as e:
            print(f"Error parsing section: {e}")

    # Debugging: Print the parsed data with Unicode encoding
    print("Parsed data:", {k: v.encode('utf-8') if isinstance(v, str) else v for k, v in data.items()})

    return data

def insert_data_to_tables(document_id, release_date, data, file_path, db_path=None):
    if db_path is None:
        db_path = os.path.join(project_root, 'data/database/database.sqlite')

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return

    try:
        # Insert into SummaryTable
        cursor.execute("""
            INSERT INTO summary_table (document_id, release_date, style, content)
            VALUES (?,?,?,?)
        """, (document_id, release_date, 'Overview', data['overview']))

        # Insert into KeyTakeawaysTable
        for topic, content in data['key_takeaways']:
            cursor.execute("""
                INSERT INTO key_takeaways_table (document_id, release_date, title, content)
                VALUES (?,?,?,?)
            """, (document_id, release_date, topic, content))

        # Insert into AnalysisTable
        macro_environment_impacts = data.get('macro_environment_impacts', 'No data available')
        cursor.execute("""
            INSERT INTO analysis_table (document_id, release_date, topic, content)
            VALUES (?,?,?,?)
        """, (document_id, release_date, 'Macro Environment Impacts', macro_environment_impacts))

        # Generate short summaries
        short_summaries = generate_short_summaries(file_path)
        print("Generated Summaries:", short_summaries.encode('utf-8'))  # Debugging: Print the generated summaries with Unicode encoding

        if short_summaries:
            # Updated regex pattern to match the entire summary block for each language
            en_match = re.search(r'\[EN\]([\s\S]*?)(?=\[PT\]|\Z)', short_summaries)
            pt_match = re.search(r'\[PT\]([\s\S]*?)(?=\[EN\]|\Z)', short_summaries)

            if en_match and pt_match:
                en_summary = en_match.group(1).strip()
                pt_summary = pt_match.group(1).strip()
                cursor.execute("""
                    UPDATE summary_table
                    SET en_summary = ?, pt_summary = ?
                    WHERE document_id = ? AND release_date = ?
                """, (en_summary, pt_summary, document_id, release_date))
            else:
                print("Error: Could not find the expected summary format in the generated summaries.")
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error with database operation: {e}")
    finally:
        conn.close()
        print(f"Data inserted into tables for document_id {document_id} and release_date {release_date}.")

def parse_and_load(file_path, db_path=None):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return

    # Extract document_id, pipe_id, and release_date from the file name
    base_name = os.path.basename(file_path)
    try:
        document_id, pipe_id, release_date = os.path.splitext(base_name)[0].split('_')
    except ValueError as e:
        print(f"Error parsing filename {base_name}: {e}")
        return

    content = read_processed_file(file_path)

    if content:
        data = parse_content(content)
        insert_data_to_tables(document_id, release_date, data, file_path, db_path)
        print(f"Data parsed and loaded for document_id {document_id} and release_date {release_date}.")
    else:
        print(f"No content found in file {file_path}.")
```

