**Case Engenharia de Dados: NYC Taxi Data Pipeline**

Este repositório contém a solução do desafio técnico de Engenharia de Dados focado na ingestão, processamento e modelagem dos dados de viagens de táxi de Nova York (TLC Trip Record Data).



O objetivo principal deste projeto é construir um pipeline End-to-End capaz de extrair dados públicos massivos, aplicar regras rigorosas de Qualidade de Dados (Data Quality) e fornecer uma camada analítica pronta para consumo pelo time de negócios.



**Arquitetura e Decisões Técnicas**

O pipeline foi construído seguindo a Arquitetura Medalhão, garantindo a rastreabilidade e a progressão lógica da qualidade do dado:



🥉 Camada Bronze (Ingestion / Raw): Dados puros extraídos da nuvem pública e convertidos para o motor distribuído do Spark sem perda de informações.

🥈 Camada Silver (Processing / Curated): Etapa de Schema Enforcement e auditoria. Aqui garantimos que 4 frotas distintas (Yellow, Green, FHV, FHVHV) tenham as mesmas colunas. Aplicamos filtros temporais e físicos para remoção de lixo (sujeiras de taxímetro) e duplicidades.

🥇 Camada Gold (Analysis / Business): Camada de negócio focada em responder as métricas de forma performática e analiticamente segura.

💡 Destaques de Solução (Troubleshooting)

O desenvolvimento deste case foi testado no Databricks Community Edition. Devido às restrições severas do ambiente gratuito (bloqueios de escrita no DBFS e acesso negado a storages via Unity Catalog), as seguintes adaptações arquiteturais de contorno foram feitas para provar a viabilidade técnica:



Transient Ingestion: Uso de bibliotecas nativas do Python (urllib) fazendo bypass via HTTPS para pousar arquivos binários localmente no Workspace.

Global Temporary Views: Em vez de salvar as tabelas no Hive Metastore (bloqueado na conta grátis), as camadas foram processadas in-memory através de Views, utilizando .cache() estrategicamente para fins de auditoria de dados.

Lazy Evaluation vs Auditoria: Criação de logs de Data Quality intermediários na Silver Layer para garantir aos analistas visibilidade total sobre o percentual de dados rejeitados e aceitos.

📁 Estrutura do Repositório

bash



📦 case-ifood-de

&#x20;┣ 📂 src/

&#x20;┃ ┣ 📜 01\_ingestion\_bronze.py     # Script PySpark responsável pela extração

&#x20;┃ ┗ 📜 02\_processing\_silver.py    # Script PySpark com lógicas de DQ e UNION All

&#x20;┣ 📂 analysis/

&#x20;┃ ┗ 📓 03\_business\_answers.ipynb  # Notebook respondendo às duas questões do case

&#x20;┣ 📜 requirements.txt             # Dependências necessárias do Python

&#x20;┗ 📜 README.md                    # Documentação do Projeto


📊 Regras de Negócio e Data Quality (Silver Layer)

Durante a exploração dos dados da NYC TLC, foram encontradas e tratadas as seguintes anomalias sistêmicas:



Renomeação Dinâmica: Algumas frotas adotam nomenclaturas de data distintas (lpep\_pickup vs pickup\_datetime). Forçamos a conversão estrutural para o padrão tpep\_pickup\_datetime.

Preenchimento Estrutural: As frotas For-Hire Vehicle (FHV) não contêm identificadores VendorID em seus Parquets mensais originais. Para possibilitar o empilhamento das tabelas de consumo, o schema foi forçado, preenchendo as inconsistências com Null.

Limpeza Temporal (Dirty Data): Foram identificadas e removidas viagens contendo datas pertencentes aos anos de 2008 ou 2024, além de meses fora do escopo analítico (Jan-Mai).

Viagens "Instantâneas": Viagens onde a data de desembarque (dropoff) era anterior ou igual a data de embarque foram removidas.

🚀 Como Executar

Para reproduzir os resultados, siga os passos abaixo em um ambiente configurado com Spark 3.0+:



Clone este repositório ou faça o upload direto dos arquivos para o seu Workspace Databricks.

Instale as dependências: pip install -r requirements.txt

Execute o script src/01\_ingestion\_bronze.py para criar a camada Raw.

Execute o script src/02\_processing\_silver.py para aplicar as regras e criar a base unificada.

Abra o notebook analysis/03\_business\_answers.ipynb e rode as células para visualizar as análises de negócio respondidas pela Camada Gold.

