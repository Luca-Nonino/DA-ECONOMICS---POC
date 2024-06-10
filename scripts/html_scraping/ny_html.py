import requests
from bs4 import BeautifulSoup
import os

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
    
    save_dir = 'data/raw/txt'
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
