import os
import urllib.request
from pyspark.sql.functions import lit

# ==============================================================================
# 1. PARÂMETROS E CONFIGURAÇÕES
# ==============================================================================
# Define o diretório atual de trabalho do notebook para salvar os arquivos
CURRENT_WORKSPACE_DIR = os.getcwd() 
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Meses requeridos pelo case de negócio (Jan a Mai)
MONTHS_TO_INGEST = ["01", "02", "03", "04", "05"]

# Tipos de táxis mapeados na base de Nova York
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# ==============================================================================
# 2. FUNÇÃO DE EXTRAÇÃO 
# ==============================================================================
def download_and_read_native(url, filename, taxi):
    """
    Função responsável por fazer o download do arquivo Parquet da web 
    e carregar no formato DataFrame do Spark.
    """
    local_path = os.path.join(CURRENT_WORKSPACE_DIR, filename)
    print(f" -> Lendo: {filename}")
    
    try:
        # Baixa o arquivo simulando um navegador para evitar bloqueio do servidor
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response, open(local_path, 'wb') as out_file:
            while True:
                chunk = response.read(8192 * 1024) # Baixa em blocos de 8MB
                if not chunk:
                    break
                out_file.write(chunk)
                
        # Faz a leitura do arquivo local usando PySpark
        df_spark = spark.read.parquet(f"file:{local_path}")
        return df_spark
        
    except Exception as e:
        print(f"    [AVISO] Erro ao baixar ou ler o arquivo: {e}")
        return None

# ==============================================================================
# 3. CRIAÇÃO DA CAMADA BRONZE (DADOS BRUTOS)
# ==============================================================================
print("INICIANDO INGESTÃO DA CAMADA BRONZE\n" + "="*50)

# Estrutura de repetição para percorrer cada frota de táxi
for taxi in TAXI_TYPES:
    dfs_parciais = []
    
    for month in MONTHS_TO_INGEST:
        # Regra de negócio: A análise 1 pede o Yellow de Jan a Mai.
        # A análise 2 pede "Todos os Táxis" apenas no mês de Maio.
        # Esse filtro evita baixar dados que não serão usados no case.
        if taxi != "yellow" and month != "05":
            continue
            
        filename = f"{taxi}_tripdata_2023-{month}.parquet"
        url = f"{BASE_URL}/{filename}"
        
        # Chama a função para baixar e criar o dataframe
        df_temp = download_and_read_native(url, filename, taxi)
        
        if df_temp is not None:
            dfs_parciais.append(df_temp)
            
    # Se houver dados baixados para esta frota, unifica tudo em uma tabela só
    if dfs_parciais:
        df_final_taxi = dfs_parciais[0]
        
        for df_part in dfs_parciais[1:]:
            # Usando allowMissingColumns porque algumas tabelas brutas podem ter colunas diferentes
            df_final_taxi = df_final_taxi.unionByName(df_part, allowMissingColumns=True)
        
        # Cria a view temporária com os dados puros (Raw)
        view_name = f"vw_bronze_{taxi}_trips"
        df_final_taxi.createOrReplaceTempView(view_name)
        
        print(f" ✅ Camada Bronze criada: {view_name}")

print("="*50 + "\n PROCESSO DE INGESTÃO FINALIZADO!")
