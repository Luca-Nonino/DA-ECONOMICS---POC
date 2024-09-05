import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# Hardcoded credentials (to be moved to config.py later)
NOME = "luca.nonino@datagro.com"
SENHA = "Lunia@443251"

# Endpoint template
ENDPOINT = "https://precos.api.datagro.com/dados/?a={ticker}&i={start_date}&f={end_date}&x=j&nome={nome}&senha={senha}"

# Ticker and Date Range
ticker = "USDBRL"  # We will later loop through all tickers
today = datetime.today()
start_date = (today - timedelta(days=35)).strftime('%Y%m%d')
end_date = today.strftime('%Y%m%d')

# Format the endpoint with parameters
url = ENDPOINT.format(ticker=ticker, start_date=start_date, end_date=end_date, nome=NOME, senha=SENHA)

# Fetch data from the API
response = requests.get(url)
data = response.json()

# Convert JSON data to a pandas DataFrame
df = pd.DataFrame(data)

# Ensure the 'data/' directory exists
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

# Save the DataFrame to a CSV file
output_file = os.path.join(output_dir, f"{ticker}_{start_date}_to_{end_date}.csv")
df.to_csv(output_file, index=False)

print(f"Data for {ticker} saved to {output_file}")
