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
            else:
                print("Failed to fetch secondary HTML content.")
        else:
            print("Failed to extract release date or secondary link.")
    else:
        print("Failed to fetch HTML content.")

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
