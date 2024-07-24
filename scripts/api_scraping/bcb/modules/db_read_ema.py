import pandas as pd

# Load the dataset
file_path = 'scripts/api_scraping/ExpectativasMercadoAnuais_data.csv'
data = pd.read_csv(file_path)

# Convert the 'Data' column to datetime format
data['Data'] = pd.to_datetime(data['Data'], errors='coerce')

# Create a new column concatenating 'Indicador' and 'IndicadorDetalhe'
data['Indicador_Concat'] = data['Indicador'] + ' - ' + data['IndicadorDetalhe'].fillna('')

# Sort the dataset by date
data = data.sort_values(by='Data')

# Filter the most recent date
most_recent_date = data['Data'].max()
recent_data = data[data['Data'] == most_recent_date]

# Get the previous week and previous month dates
previous_week_date = most_recent_date - pd.DateOffset(weeks=1)
previous_month_date = most_recent_date - pd.DateOffset(months=1)

# Filter data for the previous week and month
previous_week_data = data[data['Data'] == previous_week_date]
previous_month_data = data[data['Data'] == previous_month_date]

# Extract the last five periods including the most recent one
last_five_periods = data[data['Data'] >= (most_recent_date - pd.DateOffset(weeks=5))]

# Function to aggregate statistics with 'DataReferencia'
def aggregate_statistics_with_ref(df):
    return df.groupby(['Indicador_Concat', 'DataReferencia']).agg({
        'Media': 'mean',
        'Mediana': 'mean',
        'DesvioPadrao': 'mean',
        'Minimo': 'mean',
        'Maximo': 'mean',
        'numeroRespondentes': 'sum',
        'baseCalculo': 'sum'
    }).reset_index()

# Aggregate statistics with 'DataReferencia'
recent_stats = aggregate_statistics_with_ref(recent_data)
previous_week_stats = aggregate_statistics_with_ref(previous_week_data)
previous_month_stats = aggregate_statistics_with_ref(previous_month_data)
last_five_stats = aggregate_statistics_with_ref(last_five_periods)

# Filter only combinations that have data for 2024
valid_combinations = recent_stats[recent_stats['DataReferencia'] == 2024]['Indicador_Concat'].unique()

# Function to generate text output
def generate_text_output(recent, previous_week, previous_month, last_five):
    output = []
    for combination in valid_combinations:
        indicator_data = recent[recent['Indicador_Concat'] == combination]
        prev_week_data = previous_week[previous_week['Indicador_Concat'] == combination]
        prev_month_data = previous_month[previous_month['Indicador_Concat'] == combination]
        last_five_data = last_five[last_five['Indicador_Concat'] == combination]

        output.append(f"Indicator: {combination}\n")
        for idx, row in indicator_data.iterrows():
            detail_line = f"(Reference Year: {row['DataReferencia']}):\n"
            detail_line += f"  - Current: Média = {row['Media']:.2f}, Mediana = {row['Mediana']:.2f}, Desvio Padrão = {row['DesvioPadrao']:.2f}, Mínimo = {row['Minimo']:.2f}, Máximo = {row['Maximo']:.2f}, Respondentes = {row['numeroRespondentes']}\n"

            if not prev_week_data.empty:
                prev_week_row = prev_week_data.iloc[0]
                detail_line += f"  - Previous Week: Média = {prev_week_row['Media']:.2f}, Mediana = {prev_week_row['Mediana']:.2f}, Desvio Padrão = {prev_week_row['DesvioPadrao']:.2f}, Mínimo = {prev_week_row['Minimo']:.2f}, Máximo = {prev_week_row['Maximo']:.2f}, Respondentes = {prev_week_row['numeroRespondentes']}\n"

            if not prev_month_data.empty:
                prev_month_row = prev_month_data.iloc[0]
                detail_line += f"  - Previous Month: Média = {prev_month_row['Media']:.2f}, Mediana = {prev_month_row['Mediana']:.2f}, Desvio Padrão = {prev_month_row['DesvioPadrao']:.2f}, Mínimo = {prev_month_row['Minimo']:.2f}, Máximo = {prev_month_row['Maximo']:.2f}, Respondentes = {prev_month_row['numeroRespondentes']}\n"

            if not last_five_data.empty:
                last_five_row = last_five_data.iloc[0]
                detail_line += f"  - Last Five Periods Average: Média = {last_five_row['Media']:.2f}, Mediana = {last_five_row['Mediana']:.2f}, Desvio Padrão = {last_five_row['DesvioPadrao']:.2f}, Mínimo = {last_five_row['Minimo']:.2f}, Máximo = {last_five_row['Maximo']:.2f}, Respondentes = {last_five_row['numeroRespondentes']}\n"

            output.append(detail_line)
        output.append("\n")
    return "\n".join(output)

# Generate text output
text_output = generate_text_output(recent_stats, previous_week_stats, previous_month_stats, last_five_stats)

# Save the output to a text file
with open('scripts/api_scraping/ExpectativasMercadoAnuais_summary_output.txt', 'w', encoding='utf-8') as file:
    file.write(text_output)

print("Summary output has been saved to 'scripts/api_scraping/summary_output.txt'")
