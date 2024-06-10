import openai
import cohere
from app.config import OPENAI_API_KEY, COHERE_API_KEY

# Function to tokenize and count using OpenAI
def tokenize_and_count_openai(text, model="text-davinci-003"):
    openai.api_key = OPENAI_API_KEY
    try:
        # Tokenize input
        response = openai.Completion.create(
            engine=model,
            prompt=f"Tokenize the following text:\n\n{text}",
            max_tokens=1  # We just need the prompt to initiate tokenization
        )
        input_tokens = len(response.usage['prompt_tokens'])

        # Generate output tokens
        response = openai.Completion.create(
            engine=model,
            prompt=f"Tokenize the following text:\n\n{text}",
            max_tokens=100
        )
        output_tokens = response.usage['completion_tokens']

        return input_tokens, output_tokens
    except Exception as e:
        print(f"Failed to tokenize using OpenAI: {e}")
        return None, None

# Function to tokenize and count using Cohere+
def tokenize_and_count_cohere(text):
    cohere_client = cohere.Client(COHERE_API_KEY)
    try:
        response = cohere_client.tokenize(texts=[text])
        input_tokens = len(response.tokens[0])

        # Cohere's tokenize API doesn't directly provide output tokens, so we simulate it
        # by generating a similar tokenization for outputs
        # Here we assume output is similar length to input for demonstration purposes
        output_text = "This is a sample output text to be tokenized."
        response = cohere_client.tokenize(texts=[output_text])
        output_tokens = len(response.tokens[0])

        return input_tokens, output_tokens
    except Exception as e:
        print(f"Failed to tokenize using Cohere+: {e}")
        return None, None

############################# Test Examples #################################


# Example usage
if __name__ == "__main__":
    text = "This is a sample text to be tokenized."

    openai_input_tokens, openai_output_tokens = tokenize_and_count_openai(text)
    print(f"OpenAI Input Tokens: {openai_input_tokens}")
    print(f"OpenAI Output Tokens: {openai_output_tokens}")

    cohere_input_tokens, cohere_output_tokens = tokenize_and_count_cohere(text)
    print(f"Cohere+ Input Tokens: {cohere_input_tokens}")
    print(f"Cohere+ Output Tokens: {cohere_output_tokens}")