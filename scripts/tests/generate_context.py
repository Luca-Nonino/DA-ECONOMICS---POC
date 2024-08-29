import os

files_to_consolidate = [
    'app\\config.py',
    'app\\main.py',
    'app\\endpoints\\api.py',
    'app\\endpoints\\indicators_list.py',
    'app\\endpoints\\process_source.py',
    'app\\endpoints\\query_source.py',
    'app\\templates\\indicators_html',
    'app\\templates\\base.html',
    'app\\static\\js\\report.js',
    'app\\static\\js\\prompts.js',
    'app\\static\\js\\menu.js',
    'app\\static\\js\\main.js',
    'app\\static\\css\\report.css',
    'app\\static\\css\\prompts.css',
    'app\\static\\css\\menu.css',
    'app\\static\\css\\main.css',
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
    'scripts\\utils\\check_date.py',
    'scripts\\utils\\completions_general.py',
    'scripts\\utils\\parse_load.py',
]


import os

output_path = 'scripts\\tests\\consolidated_code.md'

with open(output_path, 'w', encoding='utf-8') as outfile:
    for path in files_to_consolidate:
        with open(path, 'r', encoding='utf-8') as infile:
            outfile.write(f"## {os.path.basename(path)}\n\n```python\n")
            outfile.write(infile.read())
            outfile.write("\n```\n\n")


