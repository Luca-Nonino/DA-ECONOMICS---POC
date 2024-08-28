import os
import requests
import pandas as pd
from datetime import timedelta, datetime

# Hardcoded credentials (to be moved to config.py later)
NOME = "luca.nonino@datagro.com"
SENHA = "Lunia@443251"

# Endpoint template
ENDPOINT = "https://precos.api.datagro.com/dados/?a={ticker}&i={start_date}&f={end_date}&x=j&nome={nome}&senha={senha}"

# Ticker list
tickers = ["EURUSD", "USDJPY", "AUDUSD", "USDINR_BCBR", "USDTWD_BCBR", "GBPUSD", "USDSGD_BCBR", "USDCHF_BCBR",
           "USDCNH_BCBR", "USDCAD_BCBR", "USDKRW_BCBR", "USDMXN_BCBR", "CHFUSD", "USDBRL", "USDZAR_BCBR",
           "USDPEN_BCBR", "USDCOP_BCBR", "USDTRY_BCBR", "USDARS_BCBR", "DXY"]

# Standardize tickers (removing "_BCBR" suffix)
tickers = [ticker.replace("_BCBR", "") for ticker in tickers]

def fetch_coins_data(current_date):
    # Define date range
    end_date = current_date.strftime('%Y%m%d')
    start_date = (current_date - timedelta(days=365 + 5)).strftime('%Y%m%d')  # Added 5 days margin

    # Ensure the 'data/' directory exists
    output_dir = "scripts/fx/data/coins/raw"
    os.makedirs(output_dir, exist_ok=True)

    # Initialize DataFrames for separate outputs
    non_dxy_data = pd.DataFrame()
    dxy_data = pd.DataFrame()

    # Loop through all tickers and append data to the respective DataFrame
    for ticker in tickers:
        # Format the endpoint with parameters
        url = ENDPOINT.format(ticker=ticker, start_date=start_date, end_date=end_date, nome=NOME, senha=SENHA)

        # Fetch data from the API
        response = requests.get(url)
        data = response.json()

        # Convert JSON data to a pandas DataFrame
        df = pd.DataFrame(data)

        # Add ticker as a column for identification
        df['ticker'] = ticker

        # Separate DXY data from other tickers
        if ticker == "DXY":
            dxy_data = pd.concat([dxy_data, df], ignore_index=True)
        else:
            non_dxy_data = pd.concat([non_dxy_data, df], ignore_index=True)

    # Check if the most recent date of the DXY data is not the current date
    if not dxy_data.empty:
        most_recent_date = pd.to_datetime(dxy_data['dia']).max()
        if most_recent_date != pd.to_datetime(current_date):
            # Raise an exception indicating the data is not up-to-date and stop further execution
            raise ValueError(f"Data not updated for today. Most recent data is for {most_recent_date.strftime('%Y-%m-%d')}")

    # Generate filenames using the correct format
    non_dxy_output_file = os.path.join(output_dir, f"{end_date}_2.csv")
    dxy_output_file = os.path.join(output_dir, f"{end_date}_1.csv")

    # Save the combined DataFrame for non-DXY tickers to a CSV file
    non_dxy_data.to_csv(non_dxy_output_file, index=False)

    # Save the combined DataFrame for DXY to a separate CSV file
    dxy_data.to_csv(dxy_output_file, index=False)

    print(f"Non-DXY ticker data saved to {non_dxy_output_file}")
    print(f"DXY ticker data saved to {dxy_output_file}")

if __name__ == "__main__":
    # This will be replaced by the orchestrator passing the date
    current_date = datetime.today().date()  # Example usage, replace with orchestrator input
    try:
        fetch_coins_data(current_date)
    except ValueError as e:
        print(e)
