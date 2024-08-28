import pandas as pd
import os

def analyse_coins_2(current_date):
    # Determine the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))

    # Define date string for the output filenames
    date_str = current_date.strftime('%Y%m%d')

    # Define the output directory relative to the project root
    output_dir = os.path.join(project_root, "data", "coins", "analysis")
    os.makedirs(output_dir, exist_ok=True)

    # Load the saved data
    non_dxy_data_path = os.path.join(project_root, "data", "coins", "raw", f"{date_str}_2.csv")
    dxy_data_path = os.path.join(project_root, "data", "coins", "raw", f"{date_str}_1.csv")
    non_dxy_df = pd.read_csv(non_dxy_data_path)
    dxy_df = pd.read_csv(dxy_data_path)

    # Convert 'dia' column to datetime
    non_dxy_df['dia'] = pd.to_datetime(non_dxy_df['dia'])
    dxy_df['dia'] = pd.to_datetime(dxy_df['dia'])

    # Filter data for the most recent date
    most_recent_date = non_dxy_df['dia'].max()
    one_year_ago = most_recent_date - pd.DateOffset(years=1)

    # Filter the data to include only the last year of data up to the most recent date
    filtered_non_dxy_df = non_dxy_df[(non_dxy_df['dia'] >= one_year_ago) & (non_dxy_df['dia'] <= most_recent_date)]
    filtered_dxy_df = dxy_df[(dxy_df['dia'] >= one_year_ago) & (dxy_df['dia'] <= most_recent_date)]

    # DXY weights for currency pairs
    dxy_weights = {
        'EURUSD': 0.2928,  # Euro
        'USDJPY': 0.1261,  # Yen
        'AUDUSD': 0.0439,  # Australian Dollar
        'USDINR': 0.0281,  # Indian Rupee
        'USDTWD': 0.0228,  # Taiwan Dollar
        'GBPUSD': 0.1019,  # British Pound
        'USDSGD': 0.0252,  # Singapore Dollar
        'USDCHF': 0.0472,  # Swiss Franc
        'USDCNH': 0.0700,  # Chinese Yuan
        'USDCAD': 0.1154,  # Canadian Dollar
        'USDKRW': 0.0326,  # Korean Won
        'USDMXN': 0.0941   # Mexican Peso
    }

    # Define a function to calculate percentage changes and weights
    def calculate_pct_changes_for_current_date(most_recent_date, df, price_col='ult'):
        df = df.sort_values(by='dia')
        last_price = df[df['dia'] == most_recent_date][price_col].values[0]

        # Calculate percentage changes based on the most recent date
        one_day_change = (last_price / df[price_col].shift(1).iloc[-1] - 1) * 100
        five_day_change = (last_price / df[price_col].shift(5).iloc[-1] - 1) * 100
        month_to_date_change = (last_price / df[df['dia'].dt.month == most_recent_date.month][price_col].iloc[0] - 1) * 100
        year_to_date_change = (last_price / df[df['dia'].dt.year == most_recent_date.year][price_col].iloc[0] - 1) * 100
        three_month_change = (last_price / df[price_col].shift(66).iloc[-1] - 1) * 100  # Approx 66 trading days in 3 months
        six_month_change = (last_price / df[price_col].shift(132).iloc[-1] - 1) * 100  # Approx 132 trading days in 6 months

        return {
            'ticker': df['ticker'].iloc[0],
            'date': most_recent_date,
            'CHG_PCT_1D': one_day_change,
            'CHG_PCT_5D': five_day_change,
            'CHG_PCT_MTD': month_to_date_change,
            'CHG_PCT_YTD': year_to_date_change,
            'CHG_PCT_3M': three_month_change,
            'CHG_PCT_6M': six_month_change
        }

    # Apply the function to all tickers in the non-DXY dataset
    def calculate_pct_changes_for_all_tickers(most_recent_date, df, price_col='ult'):
        results = []
        grouped_df = df.groupby('ticker')

        for ticker, group in grouped_df:
            pct_changes = calculate_pct_changes_for_current_date(most_recent_date, group, price_col)
            results.append(pct_changes)

        return pd.DataFrame(results)

    # Apply the logic to the entire non-DXY dataset
    non_dxy_pct_changes = calculate_pct_changes_for_all_tickers(most_recent_date, filtered_non_dxy_df)

    # Add the weight column to the non-DXY DataFrame
    non_dxy_pct_changes['weight'] = non_dxy_pct_changes['ticker'].map(dxy_weights)

    # Calculate and apply weights to calculate contributions to the DXY index
    def calculate_dxy_contributions(pct_changes_df, weights):
        results = []

        # Loop through each currency pair and apply the corresponding weight
        for _, row in pct_changes_df.iterrows():
            ticker = row['ticker']
            if ticker in weights:
                weight = weights[ticker]
                weighted_contributions = {
                    'ticker': ticker,
                    'date': row['date'],
                    'CHG_PCT_1D_CONTRIBUTION': row['CHG_PCT_1D'] * weight,
                    'CHG_PCT_5D_CONTRIBUTION': row['CHG_PCT_5D'] * weight,
                    'CHG_PCT_MTD_CONTRIBUTION': row['CHG_PCT_MTD'] * weight,
                    'CHG_PCT_YTD_CONTRIBUTION': row['CHG_PCT_YTD'] * weight,
                    'CHG_PCT_3M_CONTRIBUTION': row['CHG_PCT_3M'] * weight,
                    'CHG_PCT_6M_CONTRIBUTION': row['CHG_PCT_6M'] * weight,
                    'weight': weight  # Include the weight in the output
                }
                results.append(weighted_contributions)

        return pd.DataFrame(results)

    # Calculate DXY contributions based on the percentage changes and weights
    dxy_contributions = calculate_dxy_contributions(non_dxy_pct_changes, dxy_weights)

    # Save the DXY contribution analysis to a CSV file with the correct filename format
    dxy_contribution_output_file = os.path.join(output_dir, f"{date_str}_4.csv")
    dxy_contributions.to_csv(dxy_contribution_output_file, index=False)

    print(f"DXY contribution analysis saved to {dxy_contribution_output_file}")

    # Display the first few rows of the report as a sample
    print(dxy_contributions.head())

if __name__ == "__main__":
    # This will be replaced by the orchestrator passing the date
    from datetime import datetime
    current_date = datetime.today().date()  # Example usage, replace with orchestrator input
    analyse_coins_2(current_date)
