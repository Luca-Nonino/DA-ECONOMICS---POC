import pandas as pd
import os

def analyse_coins_1(current_date):
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
    non_dxy_df = non_dxy_df[(non_dxy_df['dia'] >= one_year_ago) & (non_dxy_df['dia'] <= most_recent_date)]
    dxy_df = dxy_df[(dxy_df['dia'] >= one_year_ago) & (dxy_df['dia'] <= most_recent_date)]

    # Currency classification (Emerging vs. Developed)
    currency_classification = {
        'EURUSD': 'Developed',
        'USDJPY': 'Developed',
        'GBPUSD': 'Developed',
        'USDCAD': 'Developed',
        'USDCHF': 'Developed',
        'AUDUSD': 'Developed',
        'USDSEK': 'Developed',
        'USDMXN': 'Emerging',
        'USDBRL': 'Emerging',
        'USDZAR': 'Emerging',
        'USDINR': 'Emerging',
        'USDKRW': 'Emerging',
        'USDTWD': 'Emerging',
        'USDCNH': 'Emerging'
    }

    # Define a function to calculate percentage changes and include the last 3 price points
    def calculate_pct_changes(df, price_col='ult'):
        df = df.sort_values(by='dia')
        last_price = df[price_col].iloc[-1]

        # Handle missing data points by taking the last available data point within the desired range
        one_day_change = (last_price / df[price_col].iloc[-2] - 1) * 100
        five_day_change = (last_price / df[price_col].iloc[-6] - 1) * 100
        month_to_date_change = (last_price / df[price_col].iloc[-22] - 1) * 100  # Assuming approx 22 trading days in a month
        year_to_date_change = (last_price / df[price_col][df['dia'].dt.year == most_recent_date.year].iloc[0] - 1) * 100

        # Additional timeframes for more comprehensive analysis
        three_month_change = (last_price / df[price_col].iloc[-66] - 1) * 100  # Approx 66 trading days in 3 months
        six_month_change = (last_price / df[price_col].iloc[-132] - 1) * 100  # Approx 132 trading days in 6 months

        # Get the last three price points
        px_close_1d = df[price_col].iloc[-2]  # Price of the previous day
        px_close_2d = df[price_col].iloc[-3]  # Price of two days ago

        # Re-add the ticker column
        df_result = pd.DataFrame({
            'ticker': [df['ticker'].iloc[0]],
            'date': [most_recent_date],
            'Last_Price': [last_price],
            'PX_CLOSE_1D': [px_close_1d],
            'PX_CLOSE_2D': [px_close_2d],
            'CHG_PCT_1D': [one_day_change],
            'CHG_PCT_5D': [five_day_change],
            'CHG_PCT_MTD': [month_to_date_change],
            'CHG_PCT_YTD': [year_to_date_change],
            'CHG_PCT_3M': [three_month_change],
            'CHG_PCT_6M': [six_month_change]
        })
        return df_result

    # Apply the function to non-DXY data
    non_dxy_df = non_dxy_df.groupby('ticker', group_keys=False).apply(calculate_pct_changes)

    # Add the classification column AFTER applying the percentage changes
    non_dxy_df['Market_Type'] = non_dxy_df['ticker'].map(currency_classification)

    # Save the calculated data for non-DXY currencies
    non_dxy_output_file = os.path.join(output_dir, f"{date_str}_1.csv")
    non_dxy_df.to_csv(non_dxy_output_file, index=False)

    # Apply the percentage change calculation function to the DXY dataset
    dxy_pct_changes = dxy_df.groupby('ticker', group_keys=False).apply(calculate_pct_changes)

    # Save the DXY percentage changes
    dxy_output_file = os.path.join(output_dir, f"{date_str}_2.csv")
    dxy_pct_changes.to_csv(dxy_output_file, index=False)

    # Function to get top and worst performers per timeframe and per type
    def get_top_and_worst_performers(df, timeframe):
        # Ensure USD/BRL is always included
        if 'USDBRL' not in df['ticker'].values:
            raise ValueError("USD/BRL must be included in the dataset.")

        # Split into Emerging and Developed
        developed_df = df[df['Market_Type'] == 'Developed']
        emerging_df = df[df['Market_Type'] == 'Emerging']

        # Get top 2 and worst 2 performers for Developed
        top_developed = developed_df.nlargest(2, timeframe)[['ticker', timeframe]]
        worst_developed = developed_df.nsmallest(2, timeframe)[['ticker', timeframe]]

        # Get top 2 and worst 2 performers for Emerging
        top_emerging = emerging_df.nlargest(2, timeframe)[['ticker', timeframe]]
        worst_emerging = emerging_df.nsmallest(2, timeframe)[['ticker', timeframe]]

        # Ensure USD/BRL is included in the Emerging Markets analysis
        usdbrl_row = df[df['ticker'] == 'USDBRL'][['ticker', timeframe]]
        if not top_emerging['ticker'].str.contains('USDBRL').any():
            worst_emerging = pd.concat([worst_emerging, usdbrl_row]).nlargest(2, timeframe)

        return top_developed, worst_developed, top_emerging, worst_emerging

    # Generate the text output for top and worst performers
    timeframes = ['CHG_PCT_1D', 'CHG_PCT_5D', 'CHG_PCT_MTD', 'CHG_PCT_YTD', 'CHG_PCT_3M', 'CHG_PCT_6M']
    output_lines = []

    for timeframe in timeframes:
        top_dev, worst_dev, top_em, worst_em = get_top_and_worst_performers(non_dxy_df, timeframe)

        output_lines.append(f"Top and Worst Performers for {timeframe}:")
        output_lines.append("Developed Markets:")
        for _, row in top_dev.iterrows():
            output_lines.append(f"Top Performer: {row['ticker']} with {row[timeframe]:.2f}%")
        for _, row in worst_dev.iterrows():
            output_lines.append(f"Worst Performer: {row['ticker']} with {row[timeframe]:.2f}%")

        output_lines.append("Emerging Markets:")
        for _, row in top_em.iterrows():
            output_lines.append(f"Top Performer: {row['ticker']} with {row[timeframe]:.2f}%")
        for _, row in worst_em.iterrows():
            output_lines.append(f"Worst Performer: {row['ticker']} with {row[timeframe]:.2f}%")

        output_lines.append("\n")

    # Write the text output to a file
    top_and_worst_performers_file = os.path.join(output_dir, f"{date_str}_3.txt")
    with open(top_and_worst_performers_file, "w") as f:
        f.writelines("\n".join(output_lines))

    print(f"Non-DXY data saved to {non_dxy_output_file}")
    print(f"DXY data saved to {dxy_output_file}")
    print(f"Top and worst performers saved to {top_and_worst_performers_file}")

if __name__ == "__main__":
    # This will be replaced by the orchestrator passing the date
    from datetime import datetime
    current_date = datetime.today().date()  # Example usage, replace with orchestrator input
    analyse_coins_1(current_date)
