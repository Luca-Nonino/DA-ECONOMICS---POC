import os
import time
import sys

# Determine the project root directory
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..', '..', '..', '..'))

# Add the project root directory to the system path
sys.path.append(project_root)
# Print the sys.path to debug
print("Current sys.path:", sys.path)
# Directories relative to the project root
prompt_dir = os.path.join(project_root, "scripts", "fx", "data", "coins", "prompt")
processed_dir = os.path.join(project_root, "scripts", "fx", "data", "coins", "processed")

# Import the Azure OpenAI client configuration from the config module
from app.config import client
# Function to make requests to Azure OpenAI API
def make_request(content, prompt):
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": content},
            ],
            temperature=0.1,
            max_tokens=2000,
            stream=True,
        )

        full_response = ""
        for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content:
                full_response += chunk.choices[0].delta.content

        # Ensure the full response is UTF-8 encoded when writing
        return full_response.encode('utf-8').decode('utf-8')

    except Exception as e:
        print(f"Error in make_request: {e}")
        return None

# Function to generate the report from the prompt
def generate_report_from_prompt(prompt_file_path, retries=3, timeout=20):
    with open(prompt_file_path, "r", encoding="utf-8") as f:
        prompt_content = f.read()

    system_prompt = (
        "You are an assistant designed to generate a detailed financial analysis based on the prompt provided. "
        "Please follow the given format and generate a comprehensive report. "
        "Answer in Portuguese if the input is partially in Portuguese."
    )

    for attempt in range(retries):
        try:
            # Generate the report
            full_response = make_request(prompt_content, system_prompt)
            print("Full response:", full_response)

            if full_response:
                return full_response

            time.sleep(timeout)
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(timeout)

    print("Failed to generate the report after multiple attempts.")
    return None

# Main function to read the prompt and save the generated report
def process_prompt_and_save_report(current_date):
    # Define date string
    date_str = current_date.strftime('%Y%m%d')

    # Define file paths based on the current date
    prompt_file_path = os.path.join(prompt_dir, f"{date_str}.md")
    processed_file_path = os.path.join(processed_dir, f"{date_str}.txt")

    # Check if the prompt file exists
    if not os.path.exists(prompt_file_path):
        print(f"Prompt file for {date_str} not found at {prompt_file_path}.")
        return

    # Generate the report using the prompt
    report_content = generate_report_from_prompt(prompt_file_path)

    # Check if the report was generated successfully
    if report_content is None:
        print(f"Failed to generate the report for {date_str}.")
        return

    # Save the generated report to the processed directory
    with open(processed_file_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print(f"Report for {date_str} successfully created and saved to {processed_file_path}.")

# Main execution
if __name__ == "__main__":
    # This will be replaced by the orchestrator passing the date
    from datetime import datetime
    current_date = datetime.today().date()  # Example usage, replace with orchestrator input
    process_prompt_and_save_report(current_date)
