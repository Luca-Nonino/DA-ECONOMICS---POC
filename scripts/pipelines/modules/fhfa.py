import os
import re
import io
from contextlib import redirect_stdout
from scripts.link_scraping.fhfa_link import process_fhfa_link

def process_fhfa_logic(document_id, url, pipe_id):
    print("Entering process_fhfa_logic")  # Debugging: Entry point
    print(f"Searching for document ID: {document_id}, URL: {url}, Pipe ID: {pipe_id}")  # Log what we are searching for

    with io.StringIO() as buf, redirect_stdout(buf):
        print("Calling process_fhfa_link")  # Debugging: Before calling process_fhfa_link
        log_message = process_fhfa_link(url, document_id, pipe_id)
        captured_message = buf.getvalue()
        print("Returned from process_fhfa_link")  # Debugging: After returning from process_fhfa_link

    print(f"Captured log message for FHFA link (ID {document_id}): {captured_message}")  # Debugging: Print the captured log message

    # Update the regex to match the expected log message format
    release_date_match = re.search(r'Release Date: (\d{8})', captured_message)
    save_path_match = re.search(r'Save Path: (.+?\.pdf)', captured_message)

    if release_date_match and save_path_match:
        release_date = release_date_match.group(1)
        file_path = save_path_match.group(1)
        print(f"Extracted file path: {file_path}, release date: {release_date}")  # Log extracted file path and release date
        print("Exiting process_fhfa_logic successfully")  # Debugging: Exit point success
        return file_path, release_date, None
    else:
        error_message = f"Failed to extract release date or file path from log message: {captured_message}"
        print(error_message)  # Log the error
        print("Exiting process_fhfa_logic with error")  # Debugging: Exit point error
        return None, None, error_message
