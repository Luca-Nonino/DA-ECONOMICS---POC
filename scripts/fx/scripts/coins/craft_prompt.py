import os
import sys
import pandas as pd

# Add the project root directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))

def craft_prompt(current_date):
    # Get the directory of the current script (craft_prompt.py)
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Navigate to the root directory by moving up the directory structure
    root_dir = os.path.abspath(os.path.join(script_dir, '..', '..'))

    # Define the directories where files are stored relative to the root directory
    analysis_dir = os.path.join(root_dir, "data", "coins", "analysis")
    prompt_dir = os.path.join(root_dir, "data", "coins", "prompt")
    example_path = os.path.join(root_dir, "data", "output_example.txt")

    # Print the computed paths for debugging
    print(f"Root Directory: {root_dir}")
    print(f"Analysis Directory: {analysis_dir}")
    print(f"Prompt Directory: {prompt_dir}")
    print(f"Example Path: {example_path}")

    # Create the prompt directory if it doesn't exist
    os.makedirs(prompt_dir, exist_ok=True)

    # Define file paths dynamically based on the current date
    date_str = current_date.strftime('%Y%m%d')
    dxy_pct_changes_path = os.path.join(analysis_dir, f"{date_str}_1.csv")
    non_dxy_pct_changes_path = os.path.join(analysis_dir, f"{date_str}_2.csv")
    top_and_worst_performers_path = os.path.join(analysis_dir, f"{date_str}_3.txt")
    dxy_contribution_analysis_path = os.path.join(analysis_dir, f"{date_str}_4.csv")

    final_markdown_path = os.path.join(prompt_dir, f"{date_str}.md")

    # Load data (make sure files exist and handle exceptions accordingly)
    try:
        dxy_pct_changes_df = pd.read_csv(dxy_pct_changes_path, encoding="utf-8")
        non_dxy_pct_changes_df = pd.read_csv(non_dxy_pct_changes_path, encoding="utf-8")
        dxy_contribution_analysis_df = pd.read_csv(dxy_contribution_analysis_path, encoding="utf-8")
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # Load the top and worst performers as text
    try:
        with open(top_and_worst_performers_path, "r", encoding="utf-8") as f:
            top_and_worst_performers_content = f.read()
    except FileNotFoundError as e:
        print(f"Error loading top and worst performers file: {e}")
        return

    # Load the output example format
    try:
        with open(example_path, "r", encoding="utf-8") as f:
            output_example = f.read()
    except FileNotFoundError as e:
        print(f"Error loading output example file: {e}")
        return

    # Convert dataframes to CSV format strings
    dxy_pct_changes_content = dxy_pct_changes_df.to_csv(index=False)
    non_dxy_pct_changes_content = non_dxy_pct_changes_df.to_csv(index=False)
    dxy_contribution_analysis_content = dxy_contribution_analysis_df.to_csv(index=False)

    # -----> Add the initial section with clear emphasis on the rules
    markdown_content = """
    # 🔥 Instruções Cruciais para a Geração do Relatório 🔥

    ## **Regras Importantes:**

    1. **Sempre incluir as duas moedas no par de comparação**:
       - É **crucial** que ambas as moedas sejam mencionadas explicitamente em todas as comparações. 
       - **Nunca omita** a segunda moeda, pois a clareza do relatório depende de se saber **o que se valorizou ou desvalorizou em relação a quê**.
       - Por exemplo, sempre escreva _"O Real (BRL) se desvalorizou frente ao Dólar (USD)"_ ou _"O Peso Mexicano (MXN) se valorizou contra o Dólar (USD)"_, **nunca omita** as duas moedas.

    2. **Cuidado com a direção da movimentação**:
       - Para que a análise seja precisa, preste **muita atenção** ao **sentido** da valorização ou desvalorização.
       - A moeda local deve ser apresentada sempre em comparação ao Dólar Americano, conforme a categoria (Mercado Emergente ou Desenvolvido).

    3. **Consistência em todas as seções**:
       - Em **todas as seções** do relatório, seja nos 5 dias, 1 mês, 3 meses, 6 meses ou YTD, **ambas as moedas** devem estar sempre presentes e **em cada análise**.

    4. **Formatação Padronizada**:
       - Siga o exemplo fornecido no template e mantenha sempre a consistência de formatação, mencionando a cotação atual e a variação percentual para **ambas as moedas** em todos os momentos.

    **Essas instruções são fundamentais para garantir a precisão e clareza do relatório gerado.** Não seguir essas regras pode gerar confusão e análises incorretas.
    ---

    # Análise Diária de Fechamento do Dólar e Moedas Globais

    ## Instruções e Dados das Tabelas
    """

    # Section 1: Instrução para analisar a Tabela 1 (DXY Percentual Changes)
    markdown_content += """
    ### Instrução para Analisar Tabela 1:
    - Utilize os dados da tabela abaixo para avaliar as mudanças percentuais no índice DXY para diferentes períodos (1 dia, 5 dias, MTD, YTD, etc.).
    - Considere como essas mudanças influenciam o Dólar globalmente e identifique as tendências relevantes.

    #### Tabela 1: DXY Percentual Changes
    """
    markdown_content += f"```\n{dxy_pct_changes_content}\n```\n"

    # Section 2: Instrução para analisar a Tabela 2 (Non-DXY Percentual Changes)
    markdown_content += """
    ### Instrução para Analisar Tabela 2:
    - Avalie o desempenho das principais moedas não-DXY, tanto de mercados desenvolvidos quanto emergentes.
    - Compare as variações percentuais entre diferentes períodos e identifique as moedas com melhor e pior desempenho.

    #### Tabela 2: Non-DXY Percentual Changes
    """
    markdown_content += f"```\n{non_dxy_pct_changes_content}\n```\n"

    # Section 3: Instrução para analisar a Tabela 3 (Top and Worst Performers)
    markdown_content += """
    ### Instrução para Analisar Tabela 3:
    - Utilize os dados para identificar as moedas com melhor e pior desempenho tanto em mercados desenvolvidos quanto emergentes.
    - Compare esses desempenhos em diferentes períodos de tempo.

    #### Tabela 3: Top and Worst Performers
    """
    markdown_content += f"```\n{top_and_worst_performers_content}\n```\n"

    # Section 4: Instrução para analisar a Tabela 4 (DXY Contribution Analysis)
    markdown_content += """
    ### Instrução para Analisar Tabela 4:
    - Analise as contribuições de cada moeda para o índice DXY.
    - Identifique as maiores contribuições positivas e negativas e como elas impactam o desempenho geral do índice.

    #### Tabela 4: DXY Contribution Analysis
    """
    markdown_content += f"```\n{dxy_contribution_analysis_content}\n```\n"

    # General daily closing analysis instructions and example formatting
    markdown_content += """
    ## Instruções Gerais para a Análise Diária de Fechamento
    - Realize uma análise completa considerando as tendências de curto e longo prazo para o Dólar e outras moedas.
    - Identifique os principais eventos que podem ter impactado as movimentações no mercado de câmbio.
    - Compare o desempenho do Dólar em relação a outras moedas globais, tanto de mercados desenvolvidos quanto emergentes.
    - Utilize os exemplos de formatação abaixo para garantir consistência nas análises.

    ### Exemplo de Formatação a Ser Utilizada:
    """
    markdown_content += f"```\n{output_example}\n```\n"

    # -----> Add the explicit date reference here
    markdown_content += f"\n\n## Data do Relatório\nEste relatório deve ser gerado para a data: **{current_date.strftime('%d/%m/%Y')}**\n\n"

    # Save the markdown content to a file
    with open(final_markdown_path, "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print(f"Final analysis markdown file created at {final_markdown_path}.")

if __name__ == "__main__":
    # This will be replaced by the orchestrator passing the date
    from datetime import datetime
    current_date = datetime.today().date()  # Example usage, replace with orchestrator input
    craft_prompt(current_date)
