# FusionPipeline

FusionPipeline é uma pipeline para extração, transformação e fusão de dados provenientes de arquivos JSON e CSV. O objetivo é consolidar dados de diferentes fontes em um único arquivo CSV para análise e processamento adicional.

## Funcionalidades

- **Extração de Dados:** Importa dados de arquivos JSON e CSV.
- **Transformação de Dados:** Renomeia colunas conforme um mapeamento pré-definido para padronizar os dados.
- **Fusão de Dados:** Combina dados extraídos de diferentes fontes em uma única estrutura.
- **Exportação de Dados:** Salva os dados combinados em um novo arquivo CSV com codificação UTF-8.

## Estrutura do Projeto

- **`processamento_dados.py`**: Contém a classe `Dados` e funções para manipulação e processamento dos dados.
- **`fusao_index.py`**: Script principal que coordena a ETL e salvamento dos dados.
