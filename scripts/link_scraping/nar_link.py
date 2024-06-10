import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime

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
    save_dir = 'data/raw/txt'
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
