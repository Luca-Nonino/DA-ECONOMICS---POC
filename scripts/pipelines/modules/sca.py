import os
import re
import io
from contextlib import redirect_stdout
from scripts.link_scraping.sca_link import process_sca_link

def process_sca_logic(document_id, url, pipe_id):
    with io.StringIO() as buf, redirect_stdout(buf):
        process_sca_link(url, document_id, pipe_id)
        log_message = buf.getvalue()

    file_path_match = re.search(r'data/raw/pdf[\\/](\d+_\d+_\d{8}\.pdf)', log_message)
    if file_path_match:
        file_name = file_path_match.group(1)
        file_path = os.path.join("data/raw/pdf", file_name)
        release_date_match = re.search(r'_(\d{8})\.pdf$', file_name)
        if release_date_match:
            release_date = release_date_match.group(1)
            return file_path, release_date, None  # Return None for the error_message
        else:
            error_message = f"Failed to extract release date from {file_name}"
            return None, None, error_message  # Return error_message

    else:
        error_message = f"Failed to extract file path from log message: {log_message}"
        return None, None, error_message  # Return error_message