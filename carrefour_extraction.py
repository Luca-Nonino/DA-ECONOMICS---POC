
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime
from webdriver_manager.chrome import ChromeDriverManager
import asyncio
import aiohttp

# Define output directory at the root level with a single data folder
output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data-carrefour'))

# Setup Chrome options to avoid detection
chrome_options = Options()
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

# Initialize the Chrome driver using webdriver_manager
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

async def disable_cep_popup():
    try:
        # Wait for the popup to appear and insert the CEP
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@placeholder='CEP']"))
        ).send_keys("01001-000")  # Insert your CEP code here
        
        # Find and click the "Submit" button
        submit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[@data-testid='store-button']"))
        )
        submit_button.click()
        
        # Wait for the popup to disappear
        WebDriverWait(driver, 10).until(
            EC.invisibility_of_element((By.XPATH, "//div[@role='dialog']"))
        )
        print("CEP popup disabled successfully.")
    except Exception as e:
        print(f"Failed to insert CEP: {e}")

async def fetch_data_using_javascript():
    try:
        # Use JavaScript to fetch product names and prices directly from the DOM
        script = """
        let products = document.querySelectorAll("article.relative.flex.flex-col");
        let data = [];
        let currentDate = new Date().toISOString().split('T')[0];
        
        products.forEach((product) => {
            let name = product.querySelector("span.overflow-hidden.text-ellipsis").getAttribute('title');
            let price = product.querySelector("span.text-base.text-blue-royal.font-medium").innerText;
            data.push([currentDate, name, price]);
        });
        return data;
        """
        data = driver.execute_script(script)
        
        # Clean the prices by removing 'R$\xa0' and converting to float
        cleaned_data = []
        for entry in data:
            # Clean the price string and convert to float
            price_cleaned = entry[2].replace('R$\xa0', '').replace('.', '').replace(',', '.').strip()
            entry[2] = float(price_cleaned)  # Convert to float
            cleaned_data.append(entry)
        
        print(f"[JavaScript Method] Found {len(cleaned_data)} products on the page.")
        return cleaned_data
    except Exception as e:
        print(f"[JavaScript Method] Error: {e}")
        return []

async def fetch_product_data(term, base_url):
    search_url = base_url.format(term.replace(' ', '+'))

    # Load the page
    driver.get(search_url)
    
    # Give the page some time to load
    time.sleep(5)  # This can be adjusted based on page load speed
    
    # Disable the CEP popup
    await disable_cep_popup()
    
    # Fetch data (current_date, product_name, price)
    data_js = await fetch_data_using_javascript()
    
    return data_js

async def main():
    # List of search terms
    search_terms = [
        "Filé de Frango Peito Congelado sem Osso Sadia 1Kg",
        "Filé Sassami de Peito de Frango Congelado Sadia 1Kg",
        "Pedaços de Sobrecoxa de Frango Congelado Sadia 1Kg",
        "Pedaços de Frango Asa, Coxinha da Asa, Sobrecoxa e Peito Congelado com Osso Sadia 1Kg",
        "Picanha Bovina à Vácuo Aprox. 1,5Kg",
        "Filé Mignon à Vácuo Aprox. 2,5 Kg",
        "Cordão de Filé Mignon á Vacuo",
        "Contra Filé Peça à Vácuo Aprox. 4Kg",
        "Alcatra Miolo Peça Vácuo Aprox. 4kg",
        "Coxão Duro Carrefour Aproximadamente 600 g",
        "Patinho Bovino Carrefour Aproximadamente 1 kg",
        "Patinho Bovino Moído Congelado Swift 500 g",
        "Paleta e Músculo Reserva 600g",
        "Músculo Bovino Carrefour Aproximadamente 1 kg",
        "Costela Bovina Janela Congelada Aprox. 1,8Kg",
        "Bife de Fígado Congelado Swift Aproximadamente 700 g",
        "Cupim Bovino Carrefour Aproximadamente 2,5 Kg",
        "Fraldinha à Vácuo 1,5 Kg",
        "Maminha Swift Mais Aprox. 1,8Kg",
        "Acém Bovino Fracionado à Vácuo 600g",
        "Lagarto Peça à Vácuo Aprox. 3,5Kg",
        "Ossobuco Swift Aproximadamente 1,2 Kg",
        "Capa de Filé à Vácuo Aprox. 2 Kg",
        "Lombo Suíno Assa Fácil Swift Aprox. 1,4Kg",
        "Filé Mignon Suíno Temperado Resfriado Aprox. 900 g",
        "Pernil Suíno Resfriado sem Osso Aprox. 500g",
        "Picanha Suína Temperada Carrefour Aproximadamente 800 g",
        "Costela Suína Swift Aproximadamente 1,5 Kg",
        "Linguiça Calabresa Perdigão 400 g",
        "Bacon Defumado Sadia Aprox. 360g",
        "Lombo Suíno Pedaço Aprox.800 g",
        "Linguiça Toscana Swift 700 g",
        "Leite Integral Piracanjuba 1 Litro",
        "Leite Desnatado Piracanjuba 1 Litro",
        "Leite Semidesnatado Piracanjuba 1 Litro",
        "Leite Semidesnatado Piracanjuba Zero Lactose 1 Litro",
        "Leite Em Pó Ninho Integral Lata 380g",
        "Manteiga com Sal Président Tablete 200g",
        "Queijo Parmesao President 180g",
        "Queijo Mussarela Lac Lelo 400 g",
        "Queijo Prato Fatiado President 150g",
        "Queijo Minas Padrão 500g",
        "Requeijão Cremoso Catupiry 200g",
        "Iogurte Integral Nestlé Tradicional 170g",
        "Iogurte Natural Integral Batavo 170g",
        "Leite Condensado MOÇA Lata 395g",
        "Leite Condensado Semidesnatado Piracanjuba Caixa 395g",
        "Creme de Leite NESTLÉ 200g",
            # Frango
        "Filé de Frango Sadia",
        "Peito de Frango Sadia",
        "Frango Sadia",
        "Sassami Sadia",
        "Sobrecoxa de Frango Sadia",
        "Frango Congelado Sadia",
        "Asa de Frango Sadia",
        "Coxinha da Asa Sadia",
        "Pedaços de Frango Sadia",
        
        # Carne Bovina
        "Picanha Bovina",
        "Picanha",
        "Filé Mignon",
        "Cordão de Filé Mignon",
        "Contra Filé",
        "Alcatra",
        "Coxão Duro",
        "Patinho Bovino",
        "Patinho Moído",
        "Paleta Bovina",
        "Músculo Bovina",
        "Costela Bovina",
        "Bife de Fígado",
        "Cupim Bovino",
        "Fraldinha",
        "Maminha",
        "Acém Bovino",
        "Lagarto Bovino",
        "Ossobuco",
        "Capa de Filé",
        
        # Carne Suína
        "Lombo Suíno",
        "Lombo Assa Fácil",
        "Filé Mignon Suíno",
        "Pernil Suíno",
        "Picanha Suína",
        "Costela Suína",
        
        # Linguiça e Bacon
        "Linguiça Calabresa",
        "Linguiça Perdigão",
        "Linguiça Toscana",
        "Bacon Sadia",
        "Linguiça Suína",
        
        # Leites
        "Leite Integral Piracanjuba",
        "Leite Piracanjuba",
        "Leite Desnatado Piracanjuba",
        "Leite Semidesnatado Piracanjuba",
        "Leite Zero Lactose Piracanjuba",
        "Leite em Pó Ninho",
        "Leite Ninho",
        
        # Queijos
        "Manteiga Président",
        "Queijo Parmesão Président",
        "Queijo Mussarela Lac Lelo",
        "Queijo Prato Président",
        "Queijo Minas",
        "Requeijão Catupiry",
        
        # Iogurtes
        "Iogurte Nestlé",
        "Iogurte Integral Nestlé",
        "Iogurte Batavo",
        "Iogurte Integral Batavo",
        
        # Leite Condensado e Creme de Leite
        "Leite Condensado MOÇA",
        "Leite Condensado Piracanjuba",
        "Creme de Leite Nestlé",
        "Creme NESTLÉ",
        "Leite Nestlé",
        
        # Additional Terms
        "Picanha Suína",
        "Filé Mignon Suíno",
        "Costela Bovina Congelada",
        "Coxão Mole",
        "Picanha Assada",
        "Alcatra Assada",
        "Contra Filé Assado",
        "Carne Moída Bovina",
        "Carne Bovina Swift",
        "Carne Suína Swift",
        "Músculo Bovino Carrefour",
        "Patinho Carrefour",
        "Linguiça Suína Swift",
        "Queijo Mussarela Swift",
        "Leite Integral Swift",
        "Manteiga com Sal",
        "Queijo Prato",
        "Queijo Minas Frescal",
        "Requeijão Cremoso",
        "Bacon Defumado",
        "Linguiça Toscana Swift",
        "Creme de Leite",
        "Leite MOÇA",
        "Iogurte Natural",
        "Linguiça Swift",
        "Frango Swift",
        "Picanha Carrefour",
        "Costela Swift",
        "Costela Suína Swift",
        "Linguiça Suína Sadia",
        "Linguiça Perdigão",
        "Leite Piracanjuba Zero Lactose",
        "Queijo Parmesão",
        "Filé de Frango Swift",
        "Carne Bovina Carrefour",
        "Carne Suína Carrefour",
        "Filé Mignon Carrefour",
        "Fraldinha Carrefour",
        "Picanha Carrefour",
        "Linguiça Carrefour",
        "Bife de Fígado Carrefour",
        "Carne Moída Carrefour",
        "Costela Carrefour",
        "Leite Desnatado Carrefour",
        "Manteiga Carrefour",
        "Queijo Mussarela Carrefour",
        "Requeijão Carrefour",
        "Linguiça Toscana Carrefour",
        "Cupim Carrefour",
        "Lagarto Carrefour",
        "Maminha Carrefour",
        "Acém Carrefour",
        "Picanha Bovina Swift",
        "Contra Filé Swift",
        "Patinho Swift",
        "Coxão Mole Swift",
        "Paleta Swift",
        "Músculo Swift",
        "Costela Bovina Swift",
        "Bife de Fígado Swift",
        "Ossobuco Swift",
        "Capa de Filé Swift",
        "Lombo Suíno Swift",
        "Filé Mignon Suíno Swift",
        "Pernil Suíno Swift",
        "Picanha Suína Swift",
        "Costela Suína Swift",
        "Linguiça Calabresa Swift",
        "Bacon Swift",
        "Linguiça Toscana Swift",
        "Leite Integral Swift",
        "Leite Desnatado Swift",
        "Leite Semidesnatado Swift",
        "Leite Zero Lactose Swift",
        "Leite Ninho Swift",
        "Manteiga Swift",
        "Queijo Parmesão Swift",
        "Queijo Mussarela Swift",
        "Queijo Prato Swift",
        "Queijo Minas Swift",
        "Requeijão Cremoso Swift",
        "Iogurte Integral Swift",
        "Iogurte Natural Swift",
        "Leite Condensado Swift",
        "Creme de Leite Swift"
    ]
    
    # Base URL
    base_url = "https://mercado.carrefour.com.br/s?q={}&sort=score_desc&page=0"
    
    # Create an empty DataFrame to hold the results
    all_data = pd.DataFrame(columns=["date", "product_name", "price"])
    
    # Run 3 requests at a time
    for i in range(0, len(search_terms), 3):
        tasks = []
        for term in search_terms[i:i+3]:  # Grouping 3 terms at a time
            tasks.append(fetch_product_data(term, base_url))
        
        # Run the 3 requests concurrently
        results = await asyncio.gather(*tasks)
        
        # Process the results
        for result in results:
            if result:
                df = pd.DataFrame(result, columns=["date", "product_name", "price"])
                all_data = pd.concat([all_data, df], ignore_index=True)
    
    # Save the DataFrame to a CSV file, after removing duplicates
    current_date = datetime.now().strftime("%Y%m%d")
    os.makedirs(output_dir, exist_ok=True)  # Create the data directory if it doesn't exist
    output_file = os.path.join(output_dir, f"prices_carrefour_{current_date}.csv")

    # Remove duplicates before saving the file
    all_data = all_data.drop_duplicates()

    # Save the cleaned data to CSV
    all_data.to_csv(output_file, index=False, encoding="utf-8")
    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    asyncio.run(main())
    driver.quit()
