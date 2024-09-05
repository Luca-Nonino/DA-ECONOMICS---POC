
import os
import time
from crawl4ai.web_crawler import WebCrawler
from crawl4ai.chunking_strategy import *
from crawl4ai.extraction_strategy import *
from crawl4ai.crawler_strategy import *
import json

crawler = WebCrawler()
crawler.warmup()

result = crawler.run(
        url="https://www.nbcnews.com/business",
        extraction_strategy=LLMExtractionStrategy(
            api_base = "https://datagromarkets.openai.azure.com/openai/deployments/gpt-4o-mini/chat/completions?api-version=2023-03-15-preview",
            provider = "azure/gpt-4o-mini",
            exception_provider ="https://datagromarkets.openai.azure.com/openai/deployments/gpt-4o-mini/chat/completions?api-version=2023-03-15-preview",
            api_token="9a6ebe3b4a664dbb90a5cab565a90785",
            api_version="2024-07-18",
            instruction="Extract only content related to technology"
        ),
    bypass_cache=True,
    )

model_fees = json.loads(result.extracted_content)

print(len(model_fees))

with open(os.path.join(os.path.dirname(__file__), "data.json"), "w", encoding="utf-8") as f:
    f.write(result.extracted_content)