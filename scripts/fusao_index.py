from processamento_dados import Dados


def extrair_dados(caminho_json, caminho_csv):
    """Extrai os dados dos arquivos JSON e CSV."""
    dados_empresaA = Dados(caminho_json, "json")
    dados_empresaB = Dados(caminho_csv, "csv")

    return dados_empresaA, dados_empresaB


def exibir_informacoes(dados, empresa):
    """Exibe informações sobre os dados."""
    print(f"EMPRESA: {empresa}")
    print("COLUNAS: ", dados.nome_colunas)
    print("LINHAS: ", dados.qtd_linhas)
    print('-'*20 + "\n")


def transformar_dados(dados_empresaB, mapeamento_chaves):
    """Renomeia colunas de dados_empresaB conforme o mapeamento fornecido."""
    dados_empresaB.rename_columns(mapeamento_chaves)
    return dados_empresaB


def fundir_dados(dados_empresaA, dados_empresaB):
    """Funde os dados das empresas A e B."""
    return Dados.join(dados_empresaA, dados_empresaB)


def salvar_dados(dados, caminho_saida):
    """Salva os dados combinados no caminho especificado."""
    dados.salvando_dados(caminho_saida)


path_json = './pipeline_dados/data_raw/dados_empresaA.json'
path_csv = './pipeline_dados/data_raw/dados_empresaB.csv'
path_dados_combinados = './pipeline_dados/data_processed/dados_combinados.csv'

try:
    # Extract
    dados_empresaA, dados_empresaB = extrair_dados(path_json, path_csv)
    print("Extract")
    exibir_informacoes(dados_empresaA, "Empresa A")
    exibir_informacoes(dados_empresaB, "Empresa B")

    # Transform
    key_mapping = {
        'Nome do Item': 'Nome do Produto',
        'Classificação do Produto': 'Categoria do Produto',
        'Valor em Reais (R$)': 'Preço do Produto (R$)',
        'Quantidade em Estoque': 'Quantidade em Estoque',
        'Nome da Loja': 'Filial',
        'Data da Venda': 'Data da Venda'
    }

    dados_empresaB = transformar_dados(dados_empresaB, key_mapping)
    print("Transform")
    exibir_informacoes(dados_empresaB, "Empresa B")

    dados_fusao = fundir_dados(dados_empresaA, dados_empresaB)
    exibir_informacoes(dados_fusao, "Empresa A + Empresa B")

    # Load
    salvar_dados(dados_fusao, path_dados_combinados)
    print(f"Dados combinados salvos em: {path_dados_combinados}")

except Exception as e:
    print(f"Erro ao processar os dados: {e}")
