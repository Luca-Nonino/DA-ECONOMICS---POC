import os
import sys
from datetime import datetime, timedelta
import subprocess

# Define project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
print(f"PROJECT_ROOT: {PROJECT_ROOT}")

# Calculate the last Friday or return the current date if it's Friday
def get_last_friday(date):
    weekday = date.weekday()
    if weekday == 4:  # Friday
        return date
    days_to_last_friday = (weekday - 4) % 7
    return date - timedelta(days_to_last_friday)

# Function to execute a script
def execute_script(script_path):
    try:
        result = subprocess.run(['python', script_path], check=True, capture_output=True, text=True)
        print(f"Executed {script_path} successfully.")
        print(result.stdout)  # Print the standard output from the script
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_path}: {e}")
        print(e.stdout)  # Print the standard output if there is an error
        print(e.stderr)  # Print the standard error if there is an error

# Main function to process the BCB API data
def process_bcb_api(document_id, pipe_id):
    # Define paths to scripts
    script_ema = os.path.join(PROJECT_ROOT, 'scripts', 'api_scraping', 'bcb', 'modules', 'db_read_ema.py')
    script_emm = os.path.join(PROJECT_ROOT, 'scripts', 'api_scraping', 'bcb', 'modules', 'db_read_emm.py')
    
    # Execute the scripts
    execute_script(script_ema)
    execute_script(script_emm)
    
    # Define paths to summary files
    mensais_summary_path = os.path.join(PROJECT_ROOT, 'scripts', 'api_scraping', 'bcb', 'modules', 'ExpectativaMercadoMensais_summary_output.txt')
    anuais_summary_path = os.path.join(PROJECT_ROOT, 'scripts', 'api_scraping', 'bcb', 'modules', 'ExpectativasMercadoAnuais_summary_output.txt')
    
    # Print paths for debugging
    print(f"mensais_summary_path: {mensais_summary_path}")
    print(f"anuais_summary_path: {anuais_summary_path}")
    
    # Check if the files exist
    if not os.path.exists(mensais_summary_path):
        print(f"File not found: {mensais_summary_path}")
        return
    
    if not os.path.exists(anuais_summary_path):
        print(f"File not found: {anuais_summary_path}")
        return
    
    # Read the content from the updated summary files
    with open(mensais_summary_path, 'r', encoding='utf-8') as mensais_file:
        mensais_content = mensais_file.read()
    
    with open(anuais_summary_path, 'r', encoding='utf-8') as anuais_file:
        anuais_content = anuais_file.read()
    
    # Get the last Friday or current date if it's Friday
    current_date = datetime.now()
    last_friday = get_last_friday(current_date)
    formatted_date = last_friday.strftime('%Y-%m-%d')
    
    # Define titles
    mensais_title = f"Relatório Focus - Dados de expectativas Mensais por indicador - {formatted_date}"
    anuais_title = f"Relatório Focus - Dados de expectativas Anuais por indicador - {formatted_date}"
    
    # Consolidate the content with titles
    consolidated_content = f"{mensais_title}\n\n{mensais_content}\n\n{anuais_title}\n\n{anuais_content}"
    
    # Define the save path and file name
    save_dir = os.path.join(PROJECT_ROOT, 'data', 'raw','txt')
    os.makedirs(save_dir, exist_ok=True)
    file_name = f"{document_id}_{pipe_id}_{last_friday.strftime('%Y%m%d')}.txt"
    save_path = os.path.join(save_dir, file_name)
    
    # Save the consolidated content to the file
    with open(save_path, 'w', encoding='utf-8') as output_file:
        output_file.write(consolidated_content)
    
    print(f"Consolidated content saved to {save_path}")

# Example usage
if __name__ == "__main__":
    document_id = 23
    pipe_id = 4
    process_bcb_api(document_id, pipe_id)
