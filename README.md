# 🚕 Case Engenharia de Dados: NYC Taxi Data Pipeline (iFood)

Este repositório contém a solução do desafio técnico de Engenharia de Dados, focado na ingestão, processamento, qualidade de dados e modelagem analítica da base de viagens de táxi de Nova York (NYC TLC Trip Record Data).

O objetivo principal deste projeto é demonstrar a construção de um pipeline **End-to-End** em **PySpark**, unindo boas práticas de Engenharia de Dados com a visão crítica e interpretativa de Business Intelligence.

---

## 🏗️ Arquitetura de Dados (Medallion Architecture)

O pipeline foi construído adotando a **Arquitetura Medalhão** (padrão de mercado estabelecido pela Databricks), garantindo rastreabilidade, governança e progressão lógica da qualidade do dado:

<p align="center">
  <img src="img/arquitetura.png" alt="Arquitetura Medalhão Databricks" width="800">
</p>

*   **🥉 Camada Bronze (Ingestion / Raw Integration):** Dados puros (Parquet) extraídos da nuvem pública e carregados em memória (Views) no motor distribuído do Spark, sem perda de informações.
*   **🥈 Camada Silver (Filtered, Cleaned, Augmented):** Etapa rigorosa de *Data Quality* e *Schema Enforcement*. Forçamos um schema unificado entre as 4 frotas de táxi (Yellow, Green, FHV, FHVHV) e aplicamos filtros físicos para remover anomalias do sistema (ex: viagens com duração negativa ou anos incorretos).
*   **🥇 Camada Gold (Business-Level Aggregates):** Criação de uma *One Big Table (OBT)* enriquecida e geração de *Datamarts* específicos e pré-calculados. Essa etapa isola a lógica matemática complexa do usuário final.
*   **📊 Consumo (BI / Analytics):** Notebook Jupyter focado estritamente na visualização de dados e *Storytelling*, consumindo os Datamarts da Camada Gold.

---

## 💡 Destaques de Solução e Troubleshooting

O desenvolvimento deste case foi homologado no **Databricks Community Edition**. Devido às restrições severas do ambiente gratuito (bloqueios de escrita persistente no DBFS e falta de acessos IAM via Unity Catalog), as seguintes adaptações arquiteturais foram implementadas para provar o domínio sobre o *framework*:

1.  **Transient Ingestion Bypass:** Ao invés de tentar baixar os arquivos para o DBFS bloqueado, utilizamos bibliotecas nativas do Python (`urllib`) para pousar arquivos binários localmente no nó de *workspace* temporário, permitindo a leitura nativa otimizada do PySpark.
2.  **In-Memory Views & Lazy Evaluation:** As camadas não foram gravadas fisicamente no Hive Metastore (restrito). Todo o pipeline trafega através de *Global Temporary Views*. O uso estratégico do método `.cache()` foi implementado para quebrar o *Lazy Evaluation* do Spark temporariamente, permitindo a impressão de **Logs de Auditoria de Data Quality** sem onerar o I/O do cluster.
3.  **Separation of Concerns (SoC):** A regra de negócio matemática foi blindada dentro de scripts de Engenharia (`src/`), deixando o Notebook (`analysis/`) leve e focado inteiramente na extração de *insights* visuais.

---

## 📁 Estrutura do Repositório

```text
📦 ifood-data-engineering-case
 ┣ 📂 img/
 ┃ ┗ 🖼️ arquitetura.png            # Diagrama explicativo da Arquitetura Medalhão
 ┣ 📂 src/
 ┃ ┣ 📜 01_ingestion_bronze.py     # Script PySpark: Extração e criação das Raw Views
 ┃ ┣ 📜 02_processing_silver.py    # Script PySpark: Data Quality, Schema Enforcement e UNION
 ┃ ┗ 📜 03_aggregation_gold.py     # Script PySpark: Geração de Datamarts com lógicas de negócio
 ┣ 📂 analysis/
 ┃ ┗ 📓 04_business_answer.ipynb   # Notebook de visualização e Storytelling consumindo a Gold
 ┣ 📜 requirements.txt             # Dependências necessárias do ambiente Python
 ┗ 📜 README.md                    # Documentação do Projeto
🚀 Como Executar o Pipeline
Para reproduzir os resultados, siga os passos sequenciais em um ambiente configurado com Spark 3.0+:

Instale as dependências executando: pip install -r requirements.txt
Camada Bronze: Execute o script src/01_ingestion_bronze.py para realizar o download dos arquivos Parquet e instanciar as Views brutas.
Camada Silver: Execute o script src/02_processing_silver.py para acionar a malha de Data Quality, gerando os logs de rejeição de "Dirty Data" e a unificação das frotas.
Camada Gold: Execute o script src/03_aggregation_gold.py para processar a matemática de negócio e criar os Datamarts agregados.
Insights: Abra o notebook analysis/04_business_answer.ipynb e rode as células para visualizar a plotagem gráfica e a interpretação dos resultados do negócio.