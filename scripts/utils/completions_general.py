import os
import sqlite3
import PyPDF2
import re
import time
from datetime import datetime
from openai import OpenAI

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
from app.config import api_key

client = OpenAI(base_url="https://api.openai.com/v1", api_key=api_key)

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
        history = [
            {
                "role": "system",
                "content": prompt
            },
            {"role": "user", "content": content},
        ]

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=history,
            temperature=0.1,
            stream=True,
        )

        full_response = ""
        for chunk in response:
            if chunk.choices[0].delta.content:
                full_response += chunk.choices[0].delta.content

        return full_response

    def extract_date_from_response(response):
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
            persona_expertise, persona_tone, format_input, format_output_overview_title,
            format_output_overview_description, format_output_overview_enclosure,
            format_output_overview_title_enclosure, format_output_key_takeaways_title,
            format_output_key_takeaways_description, format_output_key_takeaways_enclosure,
            format_output_key_takeaways_title_enclosure, format_output_macro_environment_impacts_title,
            format_output_macro_environment_impacts_description, format_output_macro_environment_impacts_enclosure,
            tasks_1, tasks_2, tasks_3, tasks_4, tasks_5, audience, objective,
            constraints_language_usage, constraints_language_style, constraints_search_tool_use
        FROM prompts_table
        WHERE document_id =?
    """, (document_id,))
    row = cursor.fetchone()
    conn.close()

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
        "CONSTRAINTS_SEARCH_TOOL_USE": row[23]
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

    print("Initializing OpenAI client...")
    history = [
        {
            "role": "system",
            "content": (
                "You are an assistant designed to generate a comprehensive analysis based on the provided document. "
                "Your task is to analyze the document content and create a structured response that adheres to the following prompt format. "
                "Please ensure your response is detailed and follows the guidelines provided."
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
        if chunk.choices[0].delta.content:
            print(chunk.choices[0].delta.content, end="", flush=True)
            output += chunk.choices[0].delta.content

    print("\nStreaming completed.")

    print("Saving generated output...")
    save_dir = os.path.join(BASE_DIR, 'data/processed')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{release_date}.txt"
    save_path = os.path.join(save_dir, file_name)

    with open(save_path, 'w', encoding='utf-8') as file:
        file.write(output)

    print(f"Generated output saved to {save_path}")

def generate_short_summaries(file_path, prompt_path=os.path.join(BASE_DIR, "data/prompts/short_summary.txt")):
    def make_request(input_text, prompt):
        history = [
            {
                "role": "system",
                "content": (
                    "You are an assistant designed to generate concise summaries. "
                    "Your task is to analyze the provided text and create a short summary. "
                    "The summary should be clear and concise, capturing the key points of the input text."
                )
            },
            {"role": "user", "content": prompt},
        ]

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=history,
            temperature=0.1,
            max_tokens=500,
            stream=True,
        )

        full_response = ""
        for chunk in response:
            if chunk.choices[0].delta.content:
                full_response += chunk.choices[0].delta.content

        return full_response

    for attempt in range(3):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                input_text = file.read()

            with open(prompt_path, 'r', encoding='utf-8') as file:
                prompt = file.read().replace('{input}', input_text)

            full_response = make_request(input_text, prompt)

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
    pdf_path = "data/raw/pdf/6_2_20240606.pdf"
    release_date = extract_release_date(pdf_path)
    print(f"Extracted release date: {release_date}")

def test_pdf():
    pdf_path = "data/raw/pdf/6_2_20240606.pdf"
    convert_pdf_to_text(pdf_path)

def test_generate_short_summaries():
    file_path = "data/processed/6_2_20240607.txt"  # Replace with your test file path
    summaries = generate_short_summaries(file_path)
    print("Generated Summaries:")
    print(summaries)

# Uncomment the following lines to run the tests individually -> all tests performed
# test_get_prompt()
# test_generate_output()
# test_pdf()
# test_extract_date()
# test_generate_short_summaries()
