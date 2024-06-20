# data/database/migrations/initiate_and_load.py
import csv
import sys
import os
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure the root of the project is in the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from data.database.documents_table import DocumentsTable, Base as DocumentsBase
from data.database.prompts_table import PromptsTable
from data.database.summary_table import SummaryTable
from data.database.keytakeaways_table import KeyTakeawaysTable
from data.database.analysis_table import AnalysisTable
from users_table import User, Base as UsersBase

def parse_date(date_str):
    if date_str:
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            pass
    return None

def initialize_db():
    # Define the explicit path for the database file
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../database.sqlite'))
    engine = create_engine(f'sqlite:///{db_path}')
    DocumentsBase.metadata.create_all(engine)
    UsersBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Load data from CSV files with semicolon as the delimiter
    with open(os.path.join(os.path.dirname(__file__), 'documents_table.csv'), newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            document = session.query(DocumentsTable).filter_by(document_id=row['DOCUMENT_ID']).first()
            if document:
                print(f"Skipping duplicate document_id: {row['DOCUMENT_ID']}")
                continue
            document = DocumentsTable(
                document_id=row['DOCUMENT_ID'],
                path=row['PATH'],
                reference_month=row['REFERENCE_MONTH'],
                pipe_name=row['PIPE_NAME'],
                pipe_id=row['PIPE_ID'],
                freq=row['FREQ'],
                document_name=row['DOCUMENT_NAME'],
                source_name=row['SOURCE_NAME'],
                escope=row['ESCOPE'],
                current_release_date=parse_date(row['CURRENT_RELEASE_DATE']),
                hash=row['HASH'],
                next_release_date=parse_date(row['NEXT_RELEASE_DATE']),
                next_release_time=row['NEXT_RELEASE_TIME']
            )
            session.add(document)

    with open(os.path.join(os.path.dirname(__file__), 'prompts_table.csv'), newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            prompt = PromptsTable(
                prompt_id=row['PROMPT_ID'],
                document_id=row['DOCUMENT_ID'],
                persona_expertise=row['PERSONA_EXPERTISE'],
                persona_tone=row['PERSONA_TONE'],
                format_input=row['FORMAT_INPUT'],
                format_output_overview_title=row['FORMAT_OUTPUT_OVERVIEW_TITLE'],
                format_output_overview_description=row['FORMAT_OUTPUT_OVERVIEW_DESCRIPTION'],
                format_output_overview_enclosure=row['FORMAT_OUTPUT_OVERVIEW_ENCLOSURE'],
                format_output_overview_title_enclosure=row['FORMAT_OUTPUT_OVERVIEW_TITLE_ENCLOSURE'],
                format_output_key_takeaways_title=row['FORMAT_OUTPUT_KEY_TAKEAWAYS_TITLE'],
                format_output_key_takeaways_description=row['FORMAT_OUTPUT_KEY_TAKEAWAYS_DESCRIPTION'],
                format_output_key_takeaways_enclosure=row['FORMAT_OUTPUT_KEY_TAKEAWAYS_ENCLOSURE'],
                format_output_key_takeaways_title_enclosure=row['FORMAT_OUTPUT_KEY_TAKEAWAYS_TITLE_ENCLOSURE'],
                format_output_macro_environment_impacts_title=row['FORMAT_OUTPUT_MACRO_ENVIRONMENT_IMPACTS_TITLE'],
                format_output_macro_environment_impacts_description=row['FORMAT_OUTPUT_MACRO_ENVIRONMENT_IMPACTS_DESCRIPTION'],
                format_output_macro_environment_impacts_enclosure=row['FORMAT_OUTPUT_MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE'],
                tasks_1=row['TASKS_1'],
                tasks_2=row.get('TASKS_2'),
                tasks_3=row.get('TASKS_3'),
                tasks_4=row.get('TASKS_4'),
                tasks_5=row.get('TASKS_5'),
                audience=row['AUDIENCE'],
                objective=row['OBJECTIVE'],
                constraints_language_usage=row['CONSTRAINTS_LANGUAGE_USAGE'],
                constraints_language_style=row['CONSTRAINTS_LANGUAGE_STYLE'],
                constraints_search_tool_use=row['CONSTRAINTS_SEARCH_TOOL_USE']
            )
            session.add(prompt)

    session.commit()

    # Print the first 3 rows of each table with headers
    print("DocumentsTable: First 3 Rows")
    for document in session.query(DocumentsTable).limit(3).all():
        print({k: v for k, v in document.__dict__.items() if k != '_sa_instance_state'})

    print("\nPromptsTable: First 3 Rows")
    for prompt in session.query(PromptsTable).limit(3).all():
        print({k: v for k, v in prompt.__dict__.items() if k != '_sa_instance_state'})

    session.close()
    print(f"Database initialized with documents and prompts data at {db_path}.")


def load_users():
    engine = create_engine('sqlite:///data/database/database.sqlite')
    UsersBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    users = [
        User(username='NataliaTonin', password='NT@2024'),
        User(username='AlexLima', password='AL@2024'),
        User(username='AnersonAlvarenga', password='AA@2024'),
        User(username='ThalesCaramella', password='TC@2024'),
        User(username='VitorOliveira', password='VO@2024'),
        User(username='LucaNonino', password='LN@2024'),
        User(username='PedroJuliato', password='PJ@2024'),
        User(username='Admin', password='Admin@443251'),
        User(username='Tester', password='Tester')
    ]

    session.bulk_save_objects(users)
    session.commit()
    session.close()

if __name__ == "__main__":
    initialize_db()
    load_users()
