import os

files_to_consolidate = [
    #'app\\config.py',
    #'app\\main.py',
    #'app\\endpoints\\api.py',
    #'app\\endpoints\\indicators_list.py',
    #'app\\endpoints\\process_source.py',
    #'app\\endpoints\\query_source.py',
    #'app\\templates\\base.html',
    #'data\\database\\migrations\\clear_michigan.py',
    #'data\\database\\analysis_table.py',
    #'data\\database\\documents_table.py',
    #'data\\database\\keytakeaways_table.py',
    #'data\\database\\prompts_table.py',
    #'data\\database\\summary_table.py',
    #'data\\database\\users_table.py',
    'scripts\\html_scraping\\mdic_html.py',
    'scripts\\link_scraping\\ibge_link.py',
    'scripts\\link_scraping\\anfavea_link.py',
    'scripts\\api_scraping\\bcb_api.py',
    'scripts\\html_scraping\\adp_html.py',
    'scripts\\html_scraping\\conf_html.py',
    'scripts\\html_scraping\\ny_html.py',
    'scripts\\link_scraping\\bea_link.py',
    'scripts\\link_scraping\\fhfa_link.py',
    'scripts\\link_scraping\\nar_link.py',
    'scripts\\pdf\\pdf_download.py',
    'scripts\\pdf\\pdf_hash.py',
    'scripts\\pipelines\\orchestrator.py',
    'scripts\\pipelines\\ocherstrator_br.py',
    #"scripts\\pipelines\\modules\\fhfa.py",
    #'scripts\\tests\\consolidated_code.md',
    #'scripts\\tests\\generate_context.py',
    #'scripts\\utils\\auth.py',
    'scripts\\utils\\check_date.py',
    'scripts\\utils\\completions_general.py',
    'scripts\\utils\\parse_load.py',
    #'scripts\\utils\\token_count.py',
]


import os

output_path = 'scripts\\tests\\consolidated_code.md'

with open(output_path, 'w') as outfile:
    for path in files_to_consolidate:
        with open(path, 'r') as infile:
            outfile.write(f"## {os.path.basename(path)}\n\n```python\n")
            outfile.write(infile.read())
            outfile.write("\n```\n\n")

output_path = 'scripts\\tests\\consolidated_code.md'

with open(output_path, 'w') as outfile:
    for path in files_to_consolidate:
        with open(path, 'r') as infile:
            outfile.write(f"## {os.path.basename(path)}\n\n```python\n")
            outfile.write(infile.read())
            outfile.write("\n```\n\n")
import os

output_path = 'scripts\\tests\\consolidated_code.md'

with open(output_path, 'w') as outfile:
    for path in files_to_consolidate:
        with open(path, 'r') as infile:
            outfile.write(f"## {os.path.basename(path)}\n\n```python\n")
            outfile.write(infile.read())
            outfile.write("\n```\n\n")

output_path = 'scripts\\tests\\consolidated_code.md'

with open(output_path, 'w') as outfile:
    for path in files_to_consolidate:
        with open(path, 'r') as infile:
            outfile.write(f"## {os.path.basename(path)}\n\n```python\n")
            outfile.write(infile.read())
            outfile.write("\n```\n\n")