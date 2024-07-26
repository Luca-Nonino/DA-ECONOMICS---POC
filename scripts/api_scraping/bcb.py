from bcb import Expectativas
import pandas as pd

# Instancia a classe
em = Expectativas()

# Define the relevant indicators
indicators = [
    'IPCA', 'PIB Total', 'Câmbio', 'Selic', 'IGP-M', 'IPCA Administrados',
    'Conta corrente', 'Balança comercial', 'Investimento direto no país',
    'Dívida líquida do setor público', 'Resultado primário', 'Resultado nominal'
]

# Define the relevant years
years = [2024, 2025, 2026, 2027]

# Initialize an empty dictionary to store data
data_dict = {year: {} for year in years}

# Fetch and store data for each indicator and year
for indicator in indicators:
    for year in years:
        query = (
            em.get_endpoint('ExpectativasMercadoAnuais')
            .query()
            .filter(em.Indicador == indicator, em.DataReferencia == str(year))
            .select(em.Indicador, em.Data, em.Media, em.Mediana, em.DataReferencia)
            .collect()
        )
        data_dict[year][indicator] = query['Mediana'].values[0] if not query.empty else None

# Convert the data dictionary to a DataFrame for easier manipulation
df = pd.DataFrame(data_dict)

# Print the DataFrame as a structured textual output
df_output = df.to_string()
print(df_output)
