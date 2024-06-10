# App Directory

This directory contains the main application code for the project.

## Structure

- `endpoints/`: Contains the FastAPI endpoint implementations.
- `templates/`: HTML templates for rendering web pages.
- `static/`: Static files such as CSS and JS.

## Setup

1. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

2. Run the application:
   ```sh
   uvicorn main:app --reload
   ```

## Endpoints

- `/DAeconomics/indicators/list`: Lists indicators.
- `/DAeconomics/indicators/query/{doc_name}`: Queries specific documents.
- `/DAeconomics/indicators/update/{doc_id}`: Updates documents.