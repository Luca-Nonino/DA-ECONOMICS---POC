
import os
from openai import AzureOpenAI

# Set up Azure OpenAI client
client = AzureOpenAI(
    api_key = "9a6ebe3b4a664dbb90a5cab565a90785",
    api_version="2024-02-01",
    azure_endpoint="https://datagromarkets.openai.azure.com/"
)

def get_chat_completion(prompt, deployment_name="gpt-4o", max_tokens=150):
    try:
        response = client.chat.completions.create(
            model=deployment_name,  # This should be the deployment name you chose in Azure
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=max_tokens
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Example usage
if __name__ == "__main__":
    prompt = "What is the capital of France?"
    completion = get_chat_completion(prompt)
    
    if completion:
        print("Completion:", completion)
    else:
        print("Failed to get a completion.")
