import logging
import os
import sqlite3
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse
from openai import OpenAI
from datetime import datetime
import time

logger = logging.getLogger(__name__)
import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import BASE_DIR

from app.config import api_key

client = OpenAI(base_url="https://api.openai.com/v1", api_key=api_key)
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

        # Make the request to the OpenAI API
        history = [
            {"role": "system", "content": "You are an assistant specialized in translating and formatting economic summaries from English to Brazilian Portuguese. Your task is to ensure the translated summaries are clear, accurate, and well-formatted."},
            {"role": "user", "content": prompt}
        ]

        def request_inference(history, retries=3, timeout=20):
            for attempt in range(retries):
                try:
                    response_stream = client.chat.completions.create(
                        model="gpt-4o",
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
            if chunk.choices[0].delta.content:
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

