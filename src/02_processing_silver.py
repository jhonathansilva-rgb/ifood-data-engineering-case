from pyspark.sql.functions import col, lit, year, month, to_timestamp

print("INICIANDO CAMADA SILVER (PROCESSAMENTO E LIMPEZA)\n" + "="*50)

# ==============================================================================
# 1. FUNÇÕES DE DATA QUALITY E AUDITORIA
# ==============================================================================
def apply_quality_rules(df, taxi_type):
    """
    Função principal de processamento da Camada Silver.
    Ela realiza 3 grandes ações:
    1. Padroniza as colunas de data (o case exige 'tpep_pickup_datetime').
    2. Garante a estrutura correta (preenche colunas faltantes com Nulo).
    3. Remove sujeiras dos dados com auditoria (printando o que foi removido).
    """
    print(f"\n -> Auditando frota: {taxi_type.upper()}")
    
    # --------------------------------------------------------------------------
    # PASSO A: PADRONIZAÇÃO DE COLUNAS
    # --------------------------------------------------------------------------
    if "lpep_pickup_datetime" in df.columns:
        df = df.withColumnRenamed("lpep_pickup_datetime", "tpep_pickup_datetime") \
               .withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")
    elif "pickup_datetime" in df.columns:
        df = df.withColumnRenamed("pickup_datetime", "tpep_pickup_datetime") \
               .withColumnRenamed("dropoff_datetime", "tpep_dropoff_datetime")

    # Garante que as colunas exigidas nas análises existam em todas as frotas
    expected_cols = ["VendorID", "passenger_count", "total_amount"]
    for c in expected_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))
            
    # Cache inicial para servir de base matemática na auditoria
    df.cache()
    linhas_brutas = df.count()
    
    # --------------------------------------------------------------------------
    # PASSO B: FILTROS E AUDITORIA DE REJEIÇÃO (DATA QUALITY)
    # --------------------------------------------------------------------------
    # A utilização do .cache() trava a tabela na memória RAM, permitindo 
    # rodar o .count() super rápido sem forçar o Spark a ler da internet novamente.

    # 1. Filtro de Data Nula
    df_step1 = df.filter(col("tpep_pickup_datetime").isNotNull())
    df_step1.cache()
    qtd_step1 = df_step1.count()
    removidos_nulos = linhas_brutas - qtd_step1
    
    # 2. Filtro do Ano Correto (Remove lixos do taxímetro de anos passados/futuros)
    df_step2 = df_step1.filter(year("tpep_pickup_datetime") == 2023)
    df_step2.cache()
    qtd_step2 = df_step2.count()
    removidos_ano = qtd_step1 - qtd_step2
    
    # 3. Filtro do Mês (Garante que só fiquem os meses que pedimos - Jan a Mai)
    df_step3 = df_step2.filter(month("tpep_pickup_datetime").between(1, 5))
    df_step3.cache()
    qtd_step3 = df_step3.count()
    removidos_mes = qtd_step2 - qtd_step3
    
    # 4. Filtro Lógico (Remove viagens instantâneas ou negativas)
    df_step4 = df_step3.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
    df_step4.cache()
    qtd_step4 = df_step4.count()
    removidos_tempo = qtd_step3 - qtd_step4
    
    # --------------------------------------------------------------------------
    # PASSO C: PROJEÇÃO FINAL
    # --------------------------------------------------------------------------
    # Força a tipagem exigida nos dados (Cast) e remove redundâncias
    df_final = df_step4.select(
        col("VendorID").cast("integer"),
        to_timestamp("tpep_pickup_datetime").alias("tpep_pickup_datetime"),
        to_timestamp("tpep_dropoff_datetime").alias("tpep_dropoff_datetime"),
        col("passenger_count").cast("integer"),
        col("total_amount").cast("double"),
        lit(taxi_type).alias("taxi_type")
    ).dropDuplicates()
    
    df_final.cache()
    linhas_finais = df_final.count()
    removidos_duplicados = qtd_step4 - linhas_finais
    
    # Impressão do Relatório de Auditoria
    print(f"    Total Bruto: {linhas_brutas:,}")
    print(f"    [-] Removidos por Data Nula: {removidos_nulos:,}")
    print(f"    [-] Removidos por Ano Inválido: {removidos_ano:,}")
    print(f"    [-] Removidos por Mês Inválido: {removidos_mes:,}")
    print(f"    [-] Removidos por Duração Inválida: {removidos_tempo:,}")
    print(f"    [-] Linhas 100% Duplicadas: {removidos_duplicados:,}")
    print(f"    [=] Total Saudável (Silver): {linhas_finais:,}\n")
    
    # Limpa a memória RAM das tabelas intermediárias (Boas Práticas de DE)
    df.unpersist()
    df_step1.unpersist()
    df_step2.unpersist()
    df_step3.unpersist()
    df_step4.unpersist()
    
    return df_final

# ==============================================================================
# 2. LEITURA E PROCESSAMENTO
# ==============================================================================
# Aplica a função para cada uma das frotas da camada Raw
yellow_clean = apply_quality_rules(spark.table("vw_bronze_yellow_trips"), "yellow")
green_clean = apply_quality_rules(spark.table("vw_bronze_green_trips"), "green")
fhv_clean = apply_quality_rules(spark.table("vw_bronze_fhv_trips"), "fhv")
fhvhv_clean = apply_quality_rules(spark.table("vw_bronze_fhvhv_trips"), "fhvhv")

# ==============================================================================
# 3. UNIFICAÇÃO DA BASE DE CONSUMO (A TABELA FINAL DA SILVER)
# ==============================================================================
print(" -> Empilhando frotas...")

# Faz a união de todas as frotas em uma tabela só
consumption_df = yellow_clean.unionByName(green_clean) \
                             .unionByName(fhv_clean) \
                             .unionByName(fhvhv_clean)

# Adiciona partição lógica por mês para acelerar as consultas na Camada Analítica
consumption_df = consumption_df.withColumn("pickup_month", month("tpep_pickup_datetime"))

# Registra a tabela unificada
view_name = "vw_silver_nyc_taxi_consumption"
consumption_df.createOrReplaceTempView(view_name)
print("="*50 + f"\n ✅ CAMADA SILVER FINALIZADA: {view_name}")
