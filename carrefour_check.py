import pandas as pd
import os
from datetime import datetime

def main():
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
        "Creme de Leite NESTLÉ 200g"
    ]
    
    # Define the input file path (use the latest file created by the first script)
    current_date = datetime.now().strftime("%Y%m%d")
    input_file = os.path.join('data-carrefour', f"prices_carrefour_{current_date}.csv")
    
    # Load the data from the CSV file
    df = pd.read_csv(input_file)
    
    # Filter the DataFrame to include only rows where 'product_name' is in the search terms
    filtered_df = df[df['product_name'].isin(search_terms)]
    
    # Identify the items that are in the search terms but missing from the filtered DataFrame
    missing_items = [item for item in search_terms if item not in filtered_df['product_name'].values]
    
    # Save the filtered DataFrame to a new CSV file
    output_file = os.path.join('data-carrefour', f"filtered_prices_carrefour_{current_date}.csv")
    filtered_df.to_csv(output_file, index=False, encoding="utf-8")
    
    print(f"Filtered data saved to {output_file}")
    print("\nMissing items:")
    for item in missing_items:
        print(f"- {item}")

if __name__ == "__main__":
    main()
