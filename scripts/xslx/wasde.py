import pandas as pd
import requests
from io import BytesIO
from datetime import datetime

def download_excel(url):
    response = requests.get(url)
    response.raise_for_status()
    return BytesIO(response.content)

def extract_table_to_csv(file_path, sheet_name, output_csv):
    # Read the specified sheet from the Excel file using xlrd engine
    df = pd.read_excel(file_path, sheet_name=sheet_name, engine='xlrd')

    # Define the header information
    title = "World and U.S. Supply and Use for Oilseeds"
    unit = "Million Metric Tons"
    source = "WASDE - 649 - 10"

    # Extract relevant data
    table_data = []
    current_category = None

    for index, row in df.iterrows():
        if pd.notna(row[0]):
            current_category = row[0]
        if pd.notna(row[1]) and isinstance(row[1], str) and row[1] not in ["May", "Jun"]:
            year = row[1]
            output = row[2]
            total_supply = row[3]
            trade = row[4]
            total_use = row[5]
            ending_stocks = row[6]
            table_data.append([current_category, year, output, total_supply, trade, total_use, ending_stocks])

    # Create DataFrame for the table data
    table_df = pd.DataFrame(table_data, columns=["Category", "Year", "Output", "Total Supply", "Trade", "Total Use", "Ending Stocks"])

    # Write to CSV
    with open(output_csv, 'w') as file:
        file.write(f'Title, "{title}"\n')
        file.write(f'Unit, "{unit}"\n')
        file.write(f'Source, "{source}"\n\n')
        table_df.to_csv(file, index=False)

# Get the current year and month
current_year = datetime.now().year
current_month = datetime.now().strftime('%m')

# URL of the Excel file
url = f'https://www.usda.gov/oce/commodity/wasde/wasde{current_month}{current_year % 100}.xls'

# Download and read the Excel file
file_path = download_excel(url)
sheet_name = 'Page 10'
output_csv = 'output_page_10.csv'

try:
    extract_table_to_csv(file_path, sheet_name, output_csv)
except Exception as e:
    print(f"An error occurred: {e}")
    # Fallback to previous month in case of error
    previous_month = (datetime.now().replace(day=1) - pd.DateOffset(months=1)).strftime('%m')
    url = f'https://www.usda.gov/oce/commodity/wasde/wasde{previous_month}{current_year % 100}.xls'
    file_path = download_excel(url)
    extract_table_to_csv(file_path, sheet_name, output_csv)

print(f"CSV file '{output_csv}' has been created successfully.")
