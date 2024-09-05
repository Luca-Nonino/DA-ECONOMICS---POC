import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '..', '..', 'data', 'database', 'database.sqlite')

def get_prompt(document_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                persona_expertise, persona_tone, format_input, format_output_overview_title,
                format_output_overview_description, format_output_overview_enclosure,
                format_output_overview_title_enclosure, format_output_key_takeaways_title,
                format_output_key_takeaways_description, format_output_key_takeaways_enclosure,
                format_output_key_takeaways_title_enclosure, format_output_macro_environment_impacts_title,
                format_output_macro_environment_impacts_description, format_output_macro_environment_impacts_enclosure,
                tasks_1, tasks_2, tasks_3, tasks_4, tasks_5, audience, objective,
                constraints_language_usage, constraints_language_style, constraints_search_tool_use
            FROM prompts_table
            WHERE document_id =?
        """, (document_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            prompt_dict = {
                "PERSONA": row[0],
                "PERSONA_TONE": row[1],
                "FORMAT_INPUT": row[2],
                "OVERVIEW_TITLE": row[3],
                "OVERVIEW_DESCRIPTION": row[4],
                "OVERVIEW_ENCLOSURE": row[5],
                "OVERVIEW_TITLE_ENCLOSURE": row[6],
                "KEY_TAKEAWAYS_TITLE": row[7],
                "KEY_TAKEAWAYS_DESCRIPTION": row[8],
                "KEY_TAKEAWAYS_ENCLOSURE": row[9],
                "KEY_TAKEAWAYS_TITLE_ENCLOSURE": row[10],
                "MACRO_ENVIRONMENT_IMPACTS_TITLE": row[11],
                "MACRO_ENVIRONMENT_IMPACTS_DESCRIPTION": row[12],
                "MACRO_ENVIRONMENT_IMPACTS_ENCLOSURE": row[13],
                "TASKS_1": row[14],
                "TASKS_2": row[15],
                "TASKS_3": row[16],
                "TASKS_4": row[17],
                "TASKS_5": row[18],
                "AUDIENCE": row[19],
                "OBJECTIVE": row[20],
                "CONSTRAINTS_LANGUAGE_USAGE": row[21],
                "CONSTRAINTS_LANGUAGE_STYLE": row[22],
                "CONSTRAINTS_SEARCH_TOOL_USE": row[23]
            }

            formatted_prompt = (
                f"#PERSONA:\n{prompt_dict['PERSONA']}\n\n"
                f"#PERSONA_TONE:\n{prompt_dict['PERSONA_TONE']}\n\n"
                f"#AUDIENCE:\n{prompt_dict['AUDIENCE']}\n\n"
                f"#OBJECTIVE:\n{prompt_dict['OBJECTIVE']}\n\n"
                f"#FORMAT_INPUT:\n{prompt_dict['FORMAT_INPUT']}\n\n"
                f"#OVERVIEW:\n{prompt_dict['OVERVIEW_TITLE']} {prompt_dict['OVERVIEW_DESCRIPTION']}\n\n"
                f"#KEY_TAKEAWAYS:\n{prompt_dict['KEY_TAKEAWAYS_TITLE']} {prompt_dict['KEY_TAKEAWAYS_DESCRIPTION']}\n\n"
                f"#MACRO_ENVIRONMENT_IMPACTS:\n{prompt_dict['MACRO_ENVIRONMENT_IMPACTS_TITLE']} {prompt_dict['MACRO_ENVIRONMENT_IMPACTS_DESCRIPTION']}\n\n"
                "#TASKS:\n"
                f"1. {prompt_dict['TASKS_1']}\n"
                f"2. {prompt_dict['TASKS_2']}\n"
                f"3. {prompt_dict['TASKS_3']}\n"
                f"4. {prompt_dict['TASKS_4']}\n"
                f"5. {prompt_dict['TASKS_5']}"
            )

            print(formatted_prompt)  # Added print statement to display the output
        else:
            print("Document ID not found")
    except sqlite3.OperationalError as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    document_id = input("Please enter the document ID: ")
    get_prompt(document_id)