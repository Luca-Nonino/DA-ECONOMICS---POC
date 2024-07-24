import pandas as pd
from bcb import Expectativas

def save_endpoint_data(endpoint_name):
    em = Expectativas()
    try:
        # Retrieve the endpoint data
        endpoint = em.get_endpoint(endpoint_name)
        data = endpoint.query().collect()

        # Save the full dataset to a CSV file
        filename = f'{endpoint_name}_data.csv'
        data.to_csv(filename, index=False)
        print(f"Full dataset for {endpoint_name} saved to {filename}")
        
    except Exception as e:
        print(f"Failed to retrieve data for {endpoint_name}: {e}")

def main():
    # List of endpoints to explore
    endpoints = [
        'ExpectativasMercadoTop5Anuais',
        'ExpectativaMercadoMensais',
        'ExpectativasMercadoInflacao24Meses',
        'ExpectativasMercadoInflacao12Meses',
        'ExpectativasMercadoSelic',
        'ExpectativasMercadoTop5Selic',
        'ExpectativasMercadoTop5Mensais',
        'ExpectativasMercadoTrimestrais',
        'ExpectativasMercadoAnuais'
    ]

    for endpoint in endpoints:
        save_endpoint_data(endpoint)

if __name__ == "__main__":
    main()
