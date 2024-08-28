import os
from datetime import datetime
import holidays

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))

# Import the functions from the respective modules using relative imports
from scripts.fx.scripts.coins.fetch_coins import fetch_coins_data
from scripts.fx.scripts.coins.analyse_coins_1 import analyse_coins_1
from scripts.fx.scripts.coins.analyse_coins_2 import analyse_coins_2
from scripts.fx.scripts.coins.craft_prompt import craft_prompt
from scripts.fx.scripts.coins.completions_coins import process_prompt_and_save_report


# Define directories and paths
processed_dir = "scripts/fx/data/coins/processed"

# Function to check if the exchange is open
# Function to check if the exchange is open
def is_exchange_open(current_date):
    # Check if today is a weekend (Saturday or Sunday)
    if current_date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False

    # Check if today is a holiday (you'll need to specify the country for holidays)
    # For example, using US holidays:
    us_holidays = holidays.US(years=current_date.year)
    if current_date in us_holidays:
        return False

    return True

# Orchestrator function
def orchestrator(current_date):
    # Step 1: Check if data is already processed for the current date
    processed_file_path = os.path.join(processed_dir, f"{current_date.strftime('%Y%m%d')}.txt")
    if os.path.exists(processed_file_path):
        return f"Data for {current_date.strftime('%Y-%m-%d')} is already processed."
    
    # Step 2: Check if the exchange is open
    if not is_exchange_open(current_date):
        return f"Exchange is closed on {current_date.strftime('%Y-%m-%d')} (weekend or holiday)."
    
    # Step 3: Call the fetch_coins_data function
    try:
        fetch_coins_data(current_date)
    except Exception as e:
        return f"Failed to fetch coins data: {e}"
    
    # Step 4: Call the analyse_coins_1 function
    try:
        analyse_coins_1(current_date)
    except Exception as e:
        return f"Failed to run analyse_coins_1.py: {e}"
    
    # Step 5: Call the analyse_coins_2 function
    try:
        analyse_coins_2(current_date)
    except Exception as e:
        return f"Failed to run analyse_coins_2.py: {e}"
    
    # Step 6: Call the craft_prompt function
    try:
        craft_prompt(current_date)
    except Exception as e:
        return f"Failed to run craft_prompt.py: {e}"
    
    # Step 7: Call the process_prompt_and_save_report function
    try:
        process_prompt_and_save_report(current_date)
    except Exception as e:
        return f"Failed to run completions_coins.py: {e}"
    
    # If all functions executed successfully, return a success message
    return f"Report for {current_date.strftime('%Y-%m-%d')} successfully created and processed."

# Main execution
if __name__ == "__main__":
    # Example 1: Run the orchestrator for each date in the provided table
    dates = [
        "2024-08-28"
    ]
    
    for date_str in dates:
        current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        result_message = orchestrator(current_date)
        print(result_message)

    # Example 2 (commented out): Run the orchestrator with the current date
    # current_date = datetime.today().date()
    # result_message = orchestrator(current_date)
    # print(result_message)