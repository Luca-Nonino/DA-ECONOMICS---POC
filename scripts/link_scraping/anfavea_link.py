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
