from pyspark.sql.functions import col, round as spark_round, avg, count, hour

print("INICIANDO CAMADA GOLD (DATA WAREHOUSE E DATAMARTS)\n" + "="*50)

# ==============================================================================
# 1. BASE UNIVERSAL GOLD (ONE BIG TABLE)
# ==============================================================================
# O objetivo aqui é criar uma base única, limpa e com regras de negócio 
# já aplicadas. Isso garante que qualquer analista (ou modelo de ML) que 
# consumir a Gold chegue no mesmo resultado, evitando divergência de métricas.

print(" -> Construindo Tabela Universal (Regras de Negócio)...")

df_silver = spark.table("vw_silver_nyc_taxi_consumption")

df_gold_universal = (
    df_silver
    # Remoção de corridas financeiramente ou operacionalmente nulas/negativas (Ex: estornos)
    .filter(
        (col("total_amount") > 0) & 
        (col("passenger_count") > 0)
    )
    # Extração de tempo para facilitar as análises futuras de BI (comportamento por hora)
    .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
)

# Registra a tabela unificada para quem precisar explorar a granularidade por corrida
df_gold_universal.createOrReplaceTempView("vw_gold_universal_trips")


# ==============================================================================
# 2. DATAMARTS: VISÕES DE NEGÓCIO PRONTAS PARA O NOTEBOOK
# ==============================================================================
# Para não onerar o cluster processando funções matemáticas pesadas 
# direto no Notebook de visualização, deixamos as agregações já calculadas.

print(" -> Gerando Datamart: Receita Mensal (Yellow)...")

# Datamart focado na Pergunta 1
df_mart_revenue = (
    spark.table("vw_gold_universal_trips")
    .filter(col("taxi_type") == "yellow")
    .groupBy("pickup_month")
    .agg(
        spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
        count("*").alias("total_trips")
    )
    .orderBy("pickup_month")
)
df_mart_revenue.createOrReplaceTempView("vw_gold_monthly_revenue")


print(" -> Gerando Datamart: Volume de Passageiros por Hora (All Taxis, Mês 05)...")

# Datamart focado na Pergunta 2
df_mart_passengers = (
    spark.table("vw_gold_universal_trips")
    .filter(col("pickup_month") == 5)
    .groupBy("pickup_hour")
    .agg(
        spark_round(avg("passenger_count"), 2).alias("avg_passenger_count"),
        count("*").alias("total_trips")
    )
    .orderBy("pickup_hour")
)
df_mart_passengers.createOrReplaceTempView("vw_gold_hourly_passengers")

print("="*50 + "\n ✅ CAMADA GOLD E DATAMARTS FINALIZADOS COM SUCESSO!")
