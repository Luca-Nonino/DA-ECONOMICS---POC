import os
import sys
import csv

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from data.database.models import Base, PromptsTable

# Define the path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
print(f"Database path: {db_path}")

# Create the SQLAlchemy engine
engine = create_engine(f'sqlite:///{db_path}', echo=True)

# Create a session factory
Session = sessionmaker(bind=engine)

def insert_prompts_from_csv(csv_file_path):
    session = Session()

    with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter=';')

        for row in csvreader:
            try:
                # Prepare the data for insertion
                prompt_data = {
                    "prompt_id": int(row["PROMPT_ID"]),
                    "document_id": int(row["DOCUMENT_ID"]),
                    "persona_expertise": row["PERSONA_EXPERTISE"],
                    "persona_tone": row["PERSONA_TONE"],
                    "format_input": row["FORMAT_INPUT"],
                    "format_output_overview_title": row["FORMAT_OUTPUT_OVERVIEW_TITLE"],
                    "format_output_overview_description": row["FORMAT_OUTPUT_OVERVIEW_DESCRIPTION"],
                    "format_output_overview_enclosure": row["FORMAT_OUTPUT_OVERVIEW_ENCLOSURE"],
                    "format_output_overview_title_enclosure": row["FORMAT_OUTPUT_OVERVIEW_TITLE_ENCLOSURE"],
                    "format_output_key_takeaways_title": row["FORMAT_OUTPUT_KEY_TAKEAWAYS_TITLE"],
                    "format_output_key_takeaways_description": row["FORMAT_OUTPUT_KEY_TAKEAWAYS_DESCRIPTION"],
                    "format_output_key_takeaways_enclosure": row["FORMAT_OUTPUT_KEY_TAKEAWAYS_ENCLOSURE"],
                    "format_output_key_takeaways_title_enclosure": row["FORMAT_OUTPUT_KEY_TAKEAWAYS_TITLE_ENCLOSURE"],
                    "format_output_macro_environment_impacts_title": row["FORMAT_OUTPUT_MACRO_ENVIRONMENT_IMPACTS_TITLE"],
                    "format_output_macro_environment_impacts_description": row["FORMAT_OUTPUT_MACRO_ENVIRONMENT_IMPACTS_DESCRIPTION"],
                    "format_output_macro_environment_impacts_enclosure": row["FORMAT_OUTPUT_MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE"],
                    "tasks_1": row["TASKS_1"],
                    "tasks_2": row.get("TASKS_2", None),  # Use .get to handle nullable fields
                    "tasks_3": row.get("TASKS_3", None),
                    "tasks_4": row.get("TASKS_4", None),
                    "tasks_5": row.get("TASKS_5", None),
                    "audience": row["AUDIENCE"],
                    "objective": row["OBJECTIVE"],
                    "constraints_language_usage": row["CONSTRAINTS_LANGUAGE_USAGE"],
                    "constraints_language_style": row["CONSTRAINTS_LANGUAGE_STYLE"],
                    "constraints_search_tool_use": row["CONSTRAINTS_SEARCH_TOOL_USE"]
                }

                stmt = insert(PromptsTable).values(**prompt_data)
                result = session.execute(stmt)
                session.commit()
                print(f"Successfully inserted prompt with id {prompt_data['prompt_id']}")
            except IntegrityError:
                print(f"Prompt with id {prompt_data['prompt_id']} already exists")
                session.rollback()
            except Exception as e:
                print(f"An error occurred while inserting prompt {prompt_data['prompt_id']}: {e}")
                session.rollback()

    session.close()

if __name__ == "__main__":
    # Specify the relative path to your CSV file
    csv_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'prompts_table_bls.csv'))
    insert_prompts_from_csv(csv_file_path)
