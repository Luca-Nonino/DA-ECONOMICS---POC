import os
from langchain_openai import AzureChatOpenAI
from langchain.chains import ConversationChain
from langchain.memory import ConversationBufferMemory

# Set up Azure OpenAI environment variables
os.environ["AZURE_OPENAI_API_KEY"] = "9a6ebe3b4a664dbb90a5cab565a90785"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://datagromarkets.openai.azure.com/openai/deployments/gpt-4o/chat/completions?api-version=2023-03-15-previewpi"

# Create AzureChatOpenAI instance
llm = AzureChatOpenAI(
    azure_deployment="gpt-4o",
    api_version="2023-03-15-preview",
    temperature=0.7
)

# Set up conversation chain
conversation = ConversationChain(
    llm=llm,
    memory=ConversationBufferMemory()
)

print("Chatbot: Hello! I'm your AI assistant. How can I help you today?")

# Main chat loop
while True:
    user_input = input("You: ")
    
    if user_input.lower() in ['exit', 'quit', 'bye']:
        print("Chatbot: Goodbye! Have a great day!")
        break
    
    response = conversation.predict(input=user_input)
    print(f"Chatbot: {response}")
