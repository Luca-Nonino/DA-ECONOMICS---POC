### Documentação Compreensiva do Projeto: Processamento de Dados COMEX

Este projeto tem como objetivo o processamento de grandes volumes de dados do comércio exterior brasileiro (COMEX) utilizando Python, abordando desde a extração dos dados, filtragem e enriquecimento com dados auxiliares, até a disponibilização final desses dados em um formato CSV enriquecido. O pipeline foi implementado de forma assíncrona, garantindo eficiência no processamento de grandes conjuntos de dados.

---

### 1. **Formato dos Dados de Entrada**

Os dados de entrada para este projeto são arquivos CSV contendo informações sobre o comércio exterior do Brasil. Esses dados podem ser divididos em dois conjuntos principais:

1. **COMEX-IMP**: Dados de importação do Brasil.
2. **COMEX-EXP**: Dados de exportação do Brasil.

Cada arquivo CSV segue uma estrutura tabular com várias colunas relacionadas a informações sobre o produto, país de origem ou destino, via de transporte, entre outros detalhes.

#### Estrutura Geral dos Arquivos CSV:
- **COMEX-IMP** (Exemplo de colunas):
  - `CO_ANO`: Ano da transação
  - `CO_MES`: Mês da transação
  - `CO_NCM`: Código NCM do produto
  - `QT_ESTAT`: Quantidade estatística do produto
  - `KG_LIQUIDO`: Peso líquido do produto em quilogramas
  - `VL_FOB`: Valor FOB da transação
  - `VL_FRETE`: Valor do frete
  - `VL_SEGURO`: Valor do seguro
  - `CO_PAIS`: Código do país de origem
  - `SG_UF_NCM`: Unidade Federativa do Brasil associada ao produto
  - `CO_VIA`: Código da via de transporte
  - `CO_URF`: Código da Unidade de Recepção Fiscal

- **COMEX-EXP** (Exemplo de colunas):
  - Colunas similares às do COMEX-IMP, mas sem as colunas de `VL_FRETE` e `VL_SEGURO`, pois essas são específicas de importação.

### 2. **Operações de Junção e Enriquecimento de Dados**

O objetivo principal do projeto é enriquecer os dados brutos do COMEX com informações auxiliares, como a classificação dos produtos em grupos específicos (`GRUPO_DATAGRO`) e a adição de dados descritivos a partir de tabelas auxiliares.

#### Etapas de Enriquecimento:
1. **Filtragem por Códigos NCM**:
   - O projeto filtra os dados do COMEX com base em uma lista de códigos NCM fornecidos pelo usuário.
   - A filtragem é feita em chunks (pedaços) de dados, permitindo o processamento de grandes volumes de dados sem sobrecarregar a memória.

2. **Adição da Coluna `GRUPO_DATAGRO`**:
   - Uma nova coluna chamada `GRUPO_DATAGRO` é adicionada ao dataset com base no mapeamento entre o código NCM e o grupo de produtos (`GRUPO_DATAGRO`) fornecido pelo usuário.
   - Essa coluna é inserida antes das colunas `CO_ANO` e `CO_MES`.

3. **Junção com Tabelas Auxiliares**:
   - Os dados do COMEX são enriquecidos com informações adicionais a partir de várias tabelas auxiliares armazenadas em um arquivo Excel.
   - As junções são feitas com base em colunas como `CO_NCM`, `CO_PAIS`, `CO_VIA`, e `CO_UNID`, garantindo que o dataset final contenha descrições detalhadas dos produtos, países, vias de transporte, etc.
   - A lógica de junção também lida com colunas duplicadas (_x e _y), preferindo os dados mais completos e eliminando redundâncias.

### 3. **Lógica Assíncrona Utilizada**

Devido ao grande volume de dados a ser processado e à necessidade de fazer múltiplas operações de I/O (Input/Output), como leitura e escrita de arquivos e requisições web, a lógica do projeto foi desenvolvida de forma assíncrona usando o módulo `asyncio` do Python. A programação assíncrona permite que operações de I/O sejam realizadas de forma não bloqueante, aumentando a eficiência e velocidade do pipeline.

#### Componentes Principais da Lógica Assíncrona:

1. **Extração de Dados (Fetch CSV)**:
   - A função `fetch_all_csvs` faz o download dos arquivos CSV de um servidor remoto de forma assíncrona.
   - É utilizado o módulo `aiohttp` para gerenciar as conexões HTTP e o módulo `aiofiles` para operações assíncronas de leitura e escrita em arquivos.
   - A função limita o número de requisições concorrentes através do uso de semáforos (`asyncio.Semaphore`), evitando sobrecarregar o servidor ou a rede.

2. **Filtragem de Grandes Arquivos CSV**:
   - A filtragem dos arquivos CSV é feita em chunks (blocos de dados) para que arquivos grandes possam ser processados sem carregar todo o arquivo na memória.
   - A função `filter_all_csvs_in_directory` gerencia a execução assíncrona da filtragem em todos os arquivos de um diretório, processando cada arquivo em paralelo quando possível.

3. **Enriquecimento Assíncrono em Chunks**:
   - O enriquecimento dos dados também é feito em chunks, utilizando a função `enrich_data_chunked`. Esta função processa o arquivo em pedaços e faz junções incrementais com as tabelas auxiliares.

4. **Manejo de Erros e Retentativas**:
   - O código utiliza mecanismos de retentativa para garantir que falhas temporárias de conexão ou leitura/escrita de arquivos sejam tratadas de forma adequada. Se uma operação falhar, o código espera um período de tempo antes de tentar novamente.

### 4. **Pipeline Completo**

O pipeline completo do projeto segue estas etapas:

1. **Download dos Dados Brutos**: Os arquivos CSV são baixados a partir do servidor remoto e armazenados no diretório local `data/`.
2. **Filtragem de Dados**: Os arquivos CSV são filtrados de acordo com os códigos NCM fornecidos.
3. **Adição do Grupo DATAGRO**: A coluna `GRUPO_DATAGRO` é adicionada aos dados filtrados.
4. **Enriquecimento com Tabelas Auxiliares**: Os dados são enriquecidos com descrições adicionais a partir das tabelas auxiliares.
5. **Consulta à Data de Publicação**: A data de publicação mais recente dos dados é consultada em um site externo.
6. **Limpeza de Arquivos Temporários**: Arquivos temporários são removidos, mantendo apenas os arquivos finais enriquecidos.
7. **Geração de Link para Download**: O arquivo final enriquecido é disponibilizado para download.

### 5. **Tecnologias Utilizadas**

- **Python 3.10+**
- **FastAPI**: Framework utilizado para criar a API que gerencia o pipeline.
- **Pandas**: Biblioteca para manipulação de dados, leitura e escrita de arquivos CSV.
- **Asyncio**: Framework de programação assíncrona do Python.
- **Aiohttp**: Cliente HTTP assíncrono usado para realizar requisições web.
- **Aiofiles**: Biblioteca assíncrona para operações de arquivo.
- **Logging**: Utilizado para rastrear o fluxo de execução e registrar erros ou sucessos.

### 6. **Conclusão**

Este projeto foi desenhado para lidar com grandes volumes de dados de forma eficiente e escalável, utilizando a programação assíncrona para otimizar operações de I/O. A estrutura modular do código permite a fácil manutenção e possível expansão para outras funcionalidades no futuro.
