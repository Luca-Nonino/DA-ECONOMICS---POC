import pandas as pd
import datetime

# Load the dataset
file_path = 'scripts/api_scraping/ExpectativaMercadoMensais_data.csv'
print(f"Loading dataset from {file_path}...")
data = pd.read_csv(file_path)

# Print columns for debugging
print("Columns in the dataset:", data.columns)

# Convert the 'Data' column to datetime format
data['Data'] = pd.to_datetime(data['Data'], errors='coerce')

# Define date range based on the last Friday
today = datetime.date.today()
last_friday = today - datetime.timedelta(days=(today.weekday() + 2) % 7 + 1)
one_month_ago = last_friday - pd.DateOffset(days=30)
previous_week_date = last_friday - pd.DateOffset(weeks=1)
previous_month_date = last_friday - pd.DateOffset(months=1)

# Convert dates to datetime64 for comparison
one_month_ago = pd.to_datetime(one_month_ago)
last_friday = pd.to_datetime(last_friday)
previous_week_date = pd.to_datetime(previous_week_date)
previous_month_date = pd.to_datetime(previous_month_date)

print(f"Filtering data from {one_month_ago} to {last_friday}...")
# Filter data for the past month
recent_data = data[(data['Data'] >= one_month_ago) & (data['Data'] <= last_friday)]
print(f"Filtered recent data has {len(recent_data)} rows.")

# Extract data for current month and next five months
current_month = last_friday.to_period('M').to_timestamp()
next_five_months = [current_month + pd.DateOffset(months=i) for i in range(6)]

print(f"Filtering data for months: {[date.strftime('%b/%Y') for date in next_five_months]}...")
# Filter data for the selected months
monthly_data = data[data['DataReferencia'].isin(next_five_months)]
print(f"Filtered monthly data has {len(monthly_data)} rows.")

# Function to aggregate statistics
def aggregate_statistics(df):
    return df.groupby(['Indicador', 'DataReferencia']).agg({
        'Media': 'mean',
        'Mediana': 'mean',
        'DesvioPadrao': 'mean',
        'Minimo': 'mean',
        'Maximo': 'mean',
        'numeroRespondentes': 'sum',
        'baseCalculo': 'sum'
    }).reset_index()

# Aggregate statistics for recent and monthly data
print("Aggregating statistics for recent data...")
recent_stats = aggregate_statistics(recent_data)
print(f"Recent statistics have {len(recent_stats)} rows.")

print("Aggregating statistics for monthly data...")
monthly_stats = aggregate_statistics(monthly_data)
print(f"Monthly statistics have {len(monthly_stats)} rows.")

# Calculate the last 5 days average
# Function to calculate the last 5 days average
def calculate_last_five_days_average(df):
    result = []
    indicators = df['Indicador'].unique()
    for indicator in indicators:
        indicator_data = df[df['Indicador'] == indicator].sort_values(by='Data', ascending=False).head(5)
        if not indicator_data.empty:
            avg_stats = indicator_data.agg({
                'Media': 'mean',
                'Mediana': 'mean',
                'DesvioPadrao': 'mean',
                'Minimo': 'mean',
                'Maximo': 'mean',
                'numeroRespondentes': 'sum',
                'baseCalculo': 'sum'
            }).to_dict()
            avg_stats['Indicador'] = indicator
            avg_stats['DataReferencia'] = indicator_data.iloc[0]['DataReferencia']
            result.append(avg_stats)
    return pd.DataFrame(result)

print("Calculating last 5 days average for monthly data...")
last_five_days_stats = calculate_last_five_days_average(data)
print(f"Last 5 days statistics have {len(last_five_days_stats)} rows.")

# Filter data for the previous week and previous month
previous_week_data = data[(data['Data'] == previous_week_date)]
previous_month_data = data[(data['Data'] == previous_month_date)]

print(f"Aggregating statistics for previous week data...")
previous_week_stats = aggregate_statistics(previous_week_data)
print(f"Previous week statistics have {len(previous_week_stats)} rows.")

print(f"Aggregating statistics for previous month data...")
previous_month_stats = aggregate_statistics(previous_month_data)
print(f"Previous month statistics have {len(previous_month_stats)} rows.")

# Function to filter data for a six-month period from the current date
def filter_six_months_data(df, current_month):
    six_months = [current_month + pd.DateOffset(months=i) for i in range(6)]
    return df[df['DataReferencia'].isin(six_months)]

# Function to generate text output
def generate_text_output(recent, monthly, last_five_days, previous_week, previous_month):
    output = []
    valid_combinations = recent['Indicador'].unique()

    for combination in valid_combinations:
        indicator_data = recent[recent['Indicador'] == combination]
        output.append(f"Indicator: {combination}\n")

        for idx, row in indicator_data.iterrows():
            detail_line = f"(Reference Date: {row['DataReferencia']}):\n"
            detail_line += f"  - Current: Média = {row['Media']:.2f}, Mediana = {row['Mediana']:.2f}, Desvio Padrão = {row['DesvioPadrao']:.2f}, Mínimo = {row['Minimo']:.2f}, Máximo = {row['Maximo']:.2f}, Respondentes = {row['numeroRespondentes']}\n"

            for month in next_five_months:
                month_data = monthly[(monthly['Indicador'] == combination) & (monthly['DataReferencia'] == month)]
                if not month_data.empty:
                    month_row = month_data.iloc[0]
                    detail_line += f"  - {month.strftime('%b/%Y')}: Média = {month_row['Media']:.2f}, Mediana = {month_row['Mediana']:.2f}, Desvio Padrão = {month_row['DesvioPadrao']:.2f}, Mínimo = {month_row['Minimo']:.2f}, Máximo = {month_row['Maximo']:.2f}, Respondentes = {month_row['numeroRespondentes']}\n"

            last_five_data = last_five_days[last_five_days['Indicador'] == combination]
            if not last_five_data.empty:
                last_five_row = last_five_data.iloc[0]
                detail_line += f"  - Last 5 Days Average: Média = {last_five_row['Media']:.2f}, Mediana = {last_five_row['Mediana']:.2f}, Desvio Padrão = {last_five_row['DesvioPadrao']:.2f}, Mínimo = {last_five_row['Minimo']:.2f}, Máximo = {last_five_row['Maximo']:.2f}, Respondentes = {last_five_row['numeroRespondentes']}\n"

            prev_week_data = previous_week[(previous_week['Indicador'] == combination)]
            if not prev_week_data.empty:
                prev_week_row = prev_week_data.iloc[0]
                detail_line += f"  - Previous Week: Média = {prev_week_row['Media']:.2f}, Mediana = {prev_week_row['Mediana']:.2f}, Desvio Padrão = {prev_week_row['DesvioPadrao']:.2f}, Mínimo = {prev_week_row['Minimo']:.2f}, Máximo = {prev_week_row['Maximo']:.2f}, Respondentes = {prev_week_row['numeroRespondentes']}\n"

            prev_month_data = previous_month[(previous_month['Indicador'] == combination)]
            if not prev_month_data.empty:
                prev_month_row = prev_month_data.iloc[0]
                detail_line += f"  - Previous Month: Média = {prev_month_row['Media']:.2f}, Mediana = {prev_month_row['Mediana']:.2f}, Desvio Padrão = {prev_month_row['DesvioPadrao']:.2f}, Mínimo = {prev_month_row['Minimo']:.2f}, Máximo = {prev_month_row['Maximo']:.2f}, Respondentes = {prev_month_row['numeroRespondentes']}\n"

            output.append(detail_line)
        output.append("\n")
    return "\n".join(output)

print("Generating text output...")
# Generate text output
text_output = generate_text_output(recent_stats, monthly_stats, last_five_days_stats, previous_week_stats, previous_month_stats)

# Save the output to a text file
output_file_path = 'scripts/api_scraping/ExpectativaMercadoMensais_summary_output.txt'
print(f"Saving summary output to {output_file_path}...")
with open(output_file_path, 'w', encoding='utf-8') as file:
    file.write(text_output)

print(f"Summary output has been saved to {output_file_path}")
