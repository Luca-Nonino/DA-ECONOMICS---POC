# Project Overview

This project provides an application for managing and viewing economic indicators. It is built using FastAPI for the backend, with SQLite for the database and various scripts for handling data processing and interactions.

## Project Structure

```
.
├── app/
│   ├── endpoints/
│   ├── templates/
│   ├── config.py
│   ├── main.py
├── data/
│   ├── database/
│   ├── migrations/
├── scripts/
│   ├── html_scraping/
│   ├── link_scraping/
│   ├── pdf/
│   ├── pipelines/
│   ├── utils/
│   ├── tests/
│   ├── examples/
├── requirements.txt
├── README.md
```

### `app/` Directory

#### `endpoints/`

- **`api.py`**: Contains the FastAPI endpoint definitions for querying document data. 
  - `query_source_api()`: Retrieves specific document data based on document ID and date.

- **`indicators_list.py`**: Endpoint for displaying a list of economic indicators.
  - `indicators_list()`: Fetches and displays a list of documents from the database.

- **`process_source.py`**: Endpoint for processing and updating source data.
  - `update_source()`: Runs a pipeline to update source data based on the provided ID.

- **`query_source.py`**: Endpoint for querying detailed source data.
  - `query_source()`: Retrieves detailed data for a specific document.

#### `templates/`
Contains HTML templates for rendering the web pages.

#### `config.py`

Holds configuration variables, such as API keys.

```python
api_key="your_api_key_here"
```

#### `main.py`

The main entry point for the FastAPI application. Sets up routes, mounts static files, and configures logging.

```python
import logging
from fastapi import FastAPI, Request, HTTPException, Depends, Query
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
import sqlite3
from typing import Optional

# Additional imports for user authentication and running pipelines
from scripts.utils.auth import get_current_user
from scripts.pipelines.orchestrator import run_pipeline
from app.endpoints.api import api_app

# Configure logging
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

app = FastAPI()

# Mount static files and API endpoints
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.mount("/DAeconomics/indicators/api", api_app)

# Configure Jinja2 templates
templates = Jinja2Templates(directory="app/templates")

@app.get("/DAeconomics", response_class=HTMLResponse)
async def redirect_to_list():
    return RedirectResponse(url="/DAeconomics/indicators/list")

@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse(url="/DAeconomics/indicators/list")

@app.get("/DAeconomics/indicators/list", response_class=HTMLResponse)
async def indicators_list(request: Request, user: dict = Depends(get_current_user)):
    try:
        db_path = 'data/database/database.sqlite'
        logger.info(f"Connecting to database at {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Fetch unique document names for the sidebar
        cursor.execute("SELECT DISTINCT document_name FROM documents_table")
        document_names = [row[0] for row in cursor.fetchall()]

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

# More endpoint definitions...

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
```

### `data/` Directory

#### `database/`

Contains the database models and initialization scripts for SQLite.

- **`analysis_table.py`**: Defines the `AnalysisTable` model.
- **`documents_table.py`**: Defines the `DocumentsTable` model and its relationships.
- **`keytakeaways_table.py`**: Defines the `KeyTakeawaysTable` model.
- **`prompts_table.py`**: Defines the `PromptsTable` model.
- **`summary_table.py`**: Defines the `SummaryTable` model.
- **`users_table.py`**: Defines the `User` model and a function to create the users table.

#### `migrations/`

Contains scripts to initialize and migrate the database.

- **`initiate_sqlite.py`**: Script to initialize the SQLite database.

### `scripts/` Directory

#### `html_scraping/`

Contains scripts for scraping HTML content from various sources.

- **`adp_html.py`**: Scrapes data from the ADP website.
- **`conf_html.py`**: Scrapes data from the Conference Board website.
- **`ny_html.py`**: Scrapes data from the New York Fed website.

#### `link_scraping/`

Contains scripts for scraping links to PDF files from various sources.

- **`bea_link.py`**: Scrapes links from the BEA website.
- **`fhfa_link.py`**: Scrapes links from the FHFA website.
- **`nar_link.py`**: Scrapes links from the NAR website.
- **`sca_link.py`**: Scrapes links from the SCA website.

#### `pdf/`

Contains scripts for handling PDF files.

- **`pdf_download.py`**: Downloads PDF files.
- **`pdf_hash.py`**: Calculates and checks hashes of PDF files.

#### `pipelines/`

Contains orchestrator scripts for running data processing pipelines.

- **`orchestrator.py`**: Orchestrates various data processing tasks.

#### `utils/`

Contains utility scripts.

- **`auth.py`**: Handles user authentication.
- **`check_date.py`**: Checks and updates release dates.
- **`completions_general.py`**: General utility functions for handling OpenAI completions.
- **`parse_load.py`**: Parses and loads data into the database.

#### `tests/`

Contains test scripts.

#### `examples/`

Contains example data and prompts.

## Setup

### Prerequisites

Ensure you have Python 3.8 or higher installed.

### Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/yourusername/yourrepository.git
   cd yourrepository
   ```

2. Install dependencies:

   ```sh
   pip install -r requirements.txt
   ```

3. Initialize the database:

   ```sh
   python data/database/migrations/initiate_sqlite.py
   ```

### Running the Application

1. Run the FastAPI application:

   ```sh
   uvicorn app.main:app --reload
   ```

2. Access the application at [http://127.0.0.1:8000/DAeconomics/indicators/list](http://127.0.0.1:8000/DAeconomics/indicators/list).

### Usage

- **List Indicators**: Navigate to `/DAeconomics/indicators/list` to view a list of economic indicators.
- **Query Source**: Use `/DAeconomics/indicators/query/{doc_name}` to query detailed data for a specific document.
- **Update Source**: Use `/DAeconomics/indicators/update` to update source data by running a pipeline.

## Contributing

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes.
4. Commit your changes (`git commit -am 'Add new feature'`).
5. Push to the branch (`git push origin feature-branch`).
6. Open a pull request.

## License

This project is licensed under the MIT License.

---
