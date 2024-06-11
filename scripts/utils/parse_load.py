import os
import sys
import sqlite3
import re
from scripts.utils.completions_general import generate_short_summaries

# Define project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

def read_processed_file(file_path):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

    return content

def parse_content(content):
    data = {}
    sections = content.split("\n\n")
    print("Sections found:", sections)  # Debugging: Print all sections to check if they are correctly split

    for section in sections:
        print("Processing section:", section[:20])  # Debugging: Print the start of each section for inspection
        try:
            if section.startswith("**Title:**"):
                data['title'] = section.split("{")[1].split("}")[0]
            elif section.startswith("**Overview:**"):
                data['overview'] = section.split("||")[1].split("||")[0]
            elif section.startswith("**Key Takeaways:**"):
                key_takeaways = section.split("\n")
                takeaways = []
                for takeaway in key_takeaways[1:]:
                    if takeaway.strip():
                        topic = takeaway.split("{**")[1].split("**}")[0]
                        content = takeaway.split("[")[1].split("]")[0]
                        takeaways.append((topic, content))
                data['key_takeaways'] = takeaways
            elif section.startswith("**Macro Environment Impacts**"):
                try:
                    data['macro_environment_impacts'] = section.split("||")[1].split("||")[0]
                except IndexError:
                    print("Error parsing Macro Environment Impacts section without colon.")
            elif section.startswith("**Macro Environment Impacts:**"):
                try:
                    data['macro_environment_impacts'] = section.split("||")[1].split("||")[0]
                except IndexError:
                    print("Error parsing Macro Environment Impacts section with colon.")
        except Exception as e:
            print(f"Error parsing section: {e}")

    # Debugging: Print the parsed data to check if all sections are correctly parsed
    print("Parsed data:", data)

    return data

def insert_data_to_tables(document_id, release_date, data, file_path, db_path=None):
    if db_path is None:
        db_path = os.path.join(project_root, 'data/database/database.sqlite')

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return

    try:
        # Insert into SummaryTable
        cursor.execute("""
            INSERT INTO summary_table (document_id, release_date, style, content)
            VALUES (?,?,?,?)
        """, (document_id, release_date, 'Overview', data['overview']))

        # Insert into KeyTakeawaysTable
        for topic, content in data['key_takeaways']:
            cursor.execute("""
                INSERT INTO key_takeaways_table (document_id, release_date, title, content)
                VALUES (?,?,?,?)
            """, (document_id, release_date, topic, content))

        # Insert into AnalysisTable
        macro_environment_impacts = data.get('macro_environment_impacts', 'No data available')
        cursor.execute("""
            INSERT INTO analysis_table (document_id, release_date, topic, content)
            VALUES (?,?,?,?)
        """, (document_id, release_date, 'Macro Environment Impacts', macro_environment_impacts))

        # Generate short summaries
        short_summaries = generate_short_summaries(file_path)
        print("Generated Summaries:", short_summaries)  # Debugging: Print the generated summaries

        if short_summaries:
            en_match = re.search(r'\[EN\]\n\{(.+?)\}', short_summaries)
            pt_match = re.search(r'\[PT\]\n\{(.+?)\}', short_summaries)

            if en_match and pt_match:
                en_summary = en_match.group(1)
                pt_summary = pt_match.group(1)
                cursor.execute("""
                    UPDATE summary_table
                    SET en_summary = ?, pt_summary = ?
                    WHERE document_id = ? AND release_date = ?
                """, (en_summary, pt_summary, document_id, release_date))
            else:
                print("Error: Could not find the expected summary format in the generated summaries.")
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error with database operation: {e}")
    finally:
        conn.close()
        print(f"Data inserted into tables for document_id {document_id} and release_date {release_date}.")

def parse_and_load(file_path, db_path=None):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return

    # Extract document_id, pipe_id, and release_date from the file name
    base_name = os.path.basename(file_path)
    try:
        document_id, pipe_id, release_date = os.path.splitext(base_name)[0].split('_')
    except ValueError as e:
        print(f"Error parsing filename {base_name}: {e}")
        return

    content = read_processed_file(file_path)

    if content:
        data = parse_content(content)
        insert_data_to_tables(document_id, release_date, data, file_path, db_path)
        print(f"Data parsed and loaded for document_id {document_id} and release_date {release_date}.")
    else:
        print(f"No content found in file {file_path}.")