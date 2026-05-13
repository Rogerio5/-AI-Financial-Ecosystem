
# Projeto de Pipelines de Dados com Spark e Airflow

## Visão Geral

Este projeto demonstra a construção de pipelines ETL/ELT utilizando **Apache Spark** e **Airflow**, aplicando conceitos da **Medallion Architecture (Bronze, Silver, Gold)** para organização e evolução de um Data Lake.

---

## Estratégia de Ambiente

### Local (Desenvolvimento e Prototipagem)

- Utilizamos **Spark Standalone** em containers Docker (Master + Worker).
- Motivos:
  - Configuração simples e leve.
  - Ideal para validar DAGs e scripts ETL.
  - Permite simular a arquitetura Medallion em pastas locais (`/bronze`, `/silver`, `/gold`).

**Exemplo de DAG com SparkSubmitOperator (local):**

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="etl_local_spark",
    start_date=datetime(2026, 3, 13),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    spark_job_transacoes = SparkSubmitOperator(
        task_id="spark_job_transacoes",
        application="/opt/airflow/dags/spark_jobs/transacoes_flat.py",
        conn_id="spark_default",
        verbose=True,
        name="arrow-spark",
    )
```

## Produção (Nuvem)

```text
Em produção, a escolha é diferente:

Databricks é a opção preferencial:

Spark gerenciado (sem necessidade de configurar cluster manualmente).

Integração nativa com cloud (Azure, AWS, GCP).

Suporte direto à Medallion Architecture com Delta Lake.

Ferramentas adicionais para DataOps, observabilidade e MLOps (MLflow).

Spark on Kubernetes também é uma alternativa válida:

Indicado para empresas que já possuem infraestrutura Kubernetes consolidada.

Oferece flexibilidade e controle total sobre configuração e escalabilidade.

Exige maior esforço de engenharia e manutenção.
```

## Exemplo de DAG com DatabricksSubmitRunOperator (produção)

```python
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="etl_producao_databricks",
    start_date=datetime(2026, 3, 13),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    spark_job_transacoes = DatabricksSubmitRunOperator(
        task_id="spark_job_transacoes",
        databricks_conn_id="databricks_default",
        existing_cluster_id="cluster-id",
        notebook_task={"notebook_path": "/Repos/projeto/transacoes_flat"},
    )
```

## Como subir o ambiente

1. Certifique-se de ter **Docker** e **Docker Compose** instalados.
2. Crie um arquivo `.env` com as variáveis necessárias (usuário e senha do Postgres, credenciais do Airflow etc.).
3. Suba os containers:

   ```bash
    docker-compose up -
   ```

Verifique se todos os serviços estão rodando:

docker ps

## Acessos principais

Airflow Webserver  
URL: <http://localhost:8080> (localhost in Bing)  
Usuário e senha definidos no .env.

Spark Master UI  
URL: <http://localhost:8080> (localhost in Bing)  
(porta mapeada para o Master).

Spark Worker UI  
URL: <http://localhost:8081> (localhost in Bing)

pgAdmin  
URL: <http://localhost:5050> (localhost in Bing)  
Usuário e senha definidos no .env.

API Python  
URL: <http://localhost:8000> (localhost in Bing)

MongoDB  
Porta: 27017

Redis  
Porta: 6379

## Tecnologias

Tecnologias Utilizadas
Airflow

Apache Spark

Postgres + pgAdmin

MongoDB

Redis

Docker Compose

Python + SQL

Git + CI/CD

Airflow para orquestração de pipelines.

Apache Spark para processamento distribuído.

Docker Compose para ambiente local.

Databricks (produção) ou Spark on Kubernetes (alternativa).

Python e SQL para transformação de dados.

Git + CI/CD para versionamento e automação.

## Arquitetura Medallion

Bronze: dados brutos, ingestão inicial.

Silver: dados limpos e transformados.

Gold: dados prontos para consumo analítico e relatórios.

## tabela aggregates

(.venv) PS C:\Users\roger\bankpy> docker cp "C:\Users\roger\bankpy\data\agregados_spark\part-00000-1fbd50b1-c151-4aa5-9919-30b3fee3f48c-c000.csv" bankpy_db:/tmp/agregados.csv
>>
Successfully copied 2.05kB to bankpy_db:/tmp/agregados.csv
(.venv) PS C:\Users\roger\bankpy> docker exec -it bankpy_db psql -U postgres -d airflow29
>>
psql (15.17 (Debian 15.17-1.pgdg13+1))
Type "help" for help.

airflow29=# \COPY aggregates FROM '/tmp/agregados.csv' DELIMITER ',' CSV;
COPY 2
airflow29=# SELECT * FROM aggregates;
  owner_cpf  | total_valor | media_valor | qtd_transacoes
-------------+-------------+-------------+----------------
 98765432100 |      1100.0 |       220.0 |              5
 12345678901 |      2151.5 |       430.3 |              5
(2 rows)

airflow29=#  = o fluxo manual (Spark → CSV → Postgres)

## Pipeline

esses dois pipelines têm papéis diferentes dentro do seu fluxo de dados:

🔹 etl_clientes_pipeline
Esse é o pipeline mais completo que você montou:

Extract: lê dados de clientes no Postgres.

Transform: converte para JSON.

Load: grava no MongoDB e no Redis.

Spark job: roda o script transacoes_flat.py no cluster Spark para gerar agregados.

Load agregados: carrega os resultados do Spark (CSV) de volta no MongoDB e no Redis.

👉 Em resumo: é um ETL completo de clientes, que integra Postgres → Spark → MongoDB/Redis.

🔹 spark_to_postgres
Esse segundo pipeline é mais simples e focado:

Ele provavelmente roda um job Spark que gera dados transformados e grava diretamente no Postgres.

Serve para cenários em que você quer que o Spark faça o processamento pesado e depois salve o resultado em tabelas relacionais (Postgres), em vez de mandar para Mongo/Redis.

É útil para relatórios, dashboards ou quando você precisa que os dados fiquem disponíveis em SQL.

👉 Em resumo: é um pipeline de integração Spark → Postgres, mais direto e voltado para persistir resultados processados no banco relacional.

✅ Então:

etl_clientes_pipeline = fluxo completo de ETL de clientes, com múltiplos destinos (Mongo + Redis).

spark_to_postgres = fluxo específico para gravar resultados do Spark direto no Postgres.

## Objetivo do spark_to_postgres

Esse pipeline é focado em usar o Spark para processar dados e gravar diretamente no Postgres.
Enquanto o etl_clientes_pipeline distribui os resultados para MongoDB e Redis, o spark_to_postgres mantém tudo dentro do banco relacional.

🔹 Estrutura típica da DAG
O código dessa DAG geralmente tem:

Um SparkSubmitOperator que dispara um job Spark (application=/opt/airflow/dags/spark_jobs/...).

Esse job Spark lê dados brutos (CSV, JSON ou tabelas do Postgres), faz transformações (agregações, joins, limpeza) e gera resultados.

Ao final, o Spark grava os resultados diretamente em uma tabela do Postgres usando o JDBC connector (spark.write.format("jdbc")...).

🔹 Para que serve
Centralizar resultados no Postgres: ideal para relatórios, dashboards e consultas SQL.

Evitar múltiplos destinos: diferente do etl_clientes_pipeline, que espalha dados em Mongo e Redis, aqui o foco é só Postgres.

Simplificar integração: muitos sistemas corporativos já consomem dados direto de Postgres, então esse pipeline facilita.

🔹 Exemplo de fluxo
Spark lê transações de clientes (CSV ou Postgres).

Spark calcula agregados (ex: total gasto por cliente, número de compras).

Spark grava os resultados em uma tabela clientes_agregados no Postgres.

Airflow marca a DAG como concluída.

✅ Em resumo:

etl_clientes_pipeline → ETL completo, integrando Postgres → Spark → MongoDB/Redis.

spark_to_postgres → pipeline direto, Spark processa e grava no Postgres para uso em relatórios/SQL.

## pitch

Localmente eu uso Spark Standalone para prototipar.
Em produção, eu levaria para nuvem usando Databricks, porque ele oferece Spark gerenciado, integração com cloud e suporte nativo à Medallion Architecture.
Se a empresa já tiver Kubernetes consolidado, Spark on K8s também é uma opção, mas Databricks acelera muito a entrega de valor

## Docker-compose.yml

👉 Com esse ajuste, você tem um ambiente completo:

Banco relacional (Postgres + pgAdmin).

NoSQL (MongoDB, Redis).

Orquestração (Airflow).

Processamento distribuído (Spark Master + Worker).

API Python integrada

## 🔹 Infraestrutura com Docker

Criou um docker-compose.yml robusto com múltiplos serviços:

Postgres como banco relacional principal.

PgAdmin para gerenciar o Postgres via interface web.

MongoDB como banco NoSQL.

Redis como cache e armazenamento rápido.

Airflow (init, webserver, scheduler, triggerer) para orquestração de pipelines.

Spark Master e Worker para processamento distribuído.

Configurou volumes e redes para que todos os serviços compartilhem dados e consigam se comunicar.

Ajustou os volumes para que Airflow e Spark enxerguem os mesmos diretórios (/opt/airflow/data e /opt/airflow/dags).

🔹 Configuração do Airflow
Criou conexões automáticas no airflow-init para Postgres, MongoDB e Redis.

Configurou o scheduler e o webserver para rodar corretamente.

Criou e logou com usuário admin no Airflow Webserver.

🔹 Pipelines (DAGs)
etl_clientes_pipeline

Extrai clientes do Postgres.

Transforma em JSON.

Carrega em MongoDB e Redis.

Dispara um job Spark (transacoes_flat.py) para gerar agregados.

Carrega os agregados de volta em MongoDB e Redis.
➝ É o seu ETL completo de clientes.

spark_to_postgres

Pipeline focado em rodar Spark e gravar resultados diretamente no Postgres.
➝ Útil para relatórios e consultas SQL.

🔹 Testes e Validações
Subiu os containers e verificou que o Spark Master e Worker estão vivos.

Ajustou o deploy-mode do SparkSubmitOperator para cluster, garantindo que o driver rode dentro do cluster Spark.

Validou que o Airflow Scheduler voltou a rodar e está processando DAGs.

Já consegue ver suas DAGs listadas na UI do Airflow (etl_clientes_pipeline e spark_to_postgres).

✅ Em resumo: você já montou toda a infraestrutura de dados (Postgres, Mongo, Redis, Spark, Airflow), configurou conexões, criou usuários, e desenvolveu dois pipelines ETL que integram esses sistemas

##

 O erro nos logs (Invalid arguments: {'master': 'spark://spark-master:7077'}) indica que o SparkSubmitOperator está recebendo um kwarg master que a versão do provider não aceita. A solução mais direta e segura agora é remover conn_id="spark_default" (ou qualquer master= explícito) e passar o endereço do master apenas via conf={"spark.master": "..."} no operador. Vou te dar o patch exato e os comandos para aplicar, recarregar o scheduler e testar o job.
Resumo curto
Sim — o Spark gerou os artefatos: _SUCCESS e part-*.csv em /opt/airflow/data/agregados_spark. Isso significa que o job Spark foi executado com sucesso fora (ou independentemente) do registro de sucesso do Airflow. A task do Airflow ficou failed, então o próximo passo é carregar os CSVs no Postgres e, se quiser, investigar por que o Airflow marcou a task como falha.

Situação atual — conclusão curta
Você confirmou que o Spark executou e completou (logs mostram jobs finalizados e a tabela agregada), mas não há arquivos part-*.csv em /opt/airflow/data/agregados_spark. O mount está correto (scheduler e worker veem ./data), então o problema não é volume — é onde o job gravou ou se o commit final não moveu os arquivos para o diretório final.

O que os resultados que você mostrou significam
Mounts OK — C:\Users\roger\bankpy\data -> /opt/airflow/data aparece em ambos os containers.

Touch OK — test_from_scheduler criado no scheduler aparece também no worker.

Spark job rodou — logs mostram FileOutputCommitter e Job finished e a tabela de agregados impressa.

Saída ausente — /opt/airflow/data/agregados_spark existe, mas está vazio (nenhum part-*.csv), e não há_temporary visível. Isso indica que o job não deixou arquivos finais no diretório esperado.

Possíveis causas mais prováveis
O job gravou em outro caminho (ex.: spark-warehouse, outro parâmetro --output, ou caminho relativo).

Os arquivos ficaram em _temporary e não foram renomeados/committed (commit falhou silenciosamente).

O job escreveu localmente no executor e depois não moveu para o volume (menos provável aqui, já que o worker tem o mount).

O código Spark usa um caminho dinâmico diferente do que você acha (parâmetro --output não foi passado ou ignorado).
=======
# 🏦 AI Financial Ecosystem: Arquitetura Lakehouse Multimodal com Agentes Autônomos e Inteligência de Crédito

## End-toEnd Banking Solution | OCI Certified | Databricks, Spark, RAG & Vision AI

Projeto em Construção !!

![Capa do Projeto - ml](ml.png)

---

## 🏅 Badges

- 📦 Tamanho do repositório:  
  ![GitHub repo size](https://img.shields.io/repo-size/Rogerio5/bank-system-python)

- 📄 Licença do projeto:  
  ![GitHub license](https://img.shields.io/github/license/Rogerio5/bank-system-python)

---

## 📋 Índice

- [📖 Descrição](#-descrição)
- [🧩 Funcionalidades](#-funcionalidades)
- [🚀 Execução](#-execução)
- [🧰 Tecnologias](#-tecnologias)
- [📊 Arquitetura](#-arquitetura)
- [🔄 Pipelines ETL](#-pipelines-etl)
- [📂 Dados de Exemplo](#-dados-de-exemplo)
- [⚙️ Automação](#-automação)
- [📈 Observabilidade](#-observabilidade)
- [🧪 Testes](#-testes)
- [🔒 Configuração](#-configuração)
- [🤖 Modelos de Machine Learning](#-Modelos-de-Machine-Learning)
- [🌐 Integração com Engenharia de Dados, Ciência de Dados e Machine Learning](#-Integração-com-Engenharia-de-Dados,-Ciência-de-Dados-e-Machine-Learning)
- [👨‍💻 Desenvolvedor](#-desenvolvedor)
- [📜 Licença](#-licença)
- [🏁 Conclusão](#-conclusão)

---

## 📖 Descrição

## 📖 Descrição

O **BankPy** é um projeto de engenharia de dados aplicado a um sistema bancário em Python. Ele cobre todo o ciclo de ingestão, transformação e armazenamento de transações financeiras, integrando múltiplas tecnologias modernas: **FastAPI, PostgreSQL, MongoDB, Redis, Apache Spark, Apache Airflow, Superset, Grafana e Streamlit**.

Além da camada de engenharia de dados, o projeto evolui para incluir **modelos de Machine Learning**, capazes de prever saldo, detectar fraudes, segmentar clientes e recomendar produtos financeiros. Os resultados desses modelos podem ser acompanhados em tempo real por meio de dashboards no Superset e Grafana, além de uma interface interativa construída em Streamlit.

---

## 🧩 Funcionalidades

- CRUD de clientes, contas e transações.
- API REST com FastAPI.
- ETL com Airflow + Spark.
- Exportação de transações para CSV.
- Persistência em Postgres, MongoDB e Redis.
- Dashboards com Superset e Grafana.
- Automação com scripts PowerShell e CLI Python.
- Testes automatizados com Pytest.

---

## 🚀 Execução

1. Clone o repositório e copie `.env.example` para `.env`.
2. Ajuste variáveis de ambiente (Postgres, MongoDB, Redis, Superset).
3. Suba os serviços:
   ```bash
   docker-compose up -d --build
4. Acesse:

   API → http://localhost:8000

   Airflow → http://localhost:8088

   Superset → http://localhost:8089
    
   Grafana → http://localhost:3001
    
   PgAdmin → http://localhost:5050
    
   Spark Master UI → http://localhost:8080

---

## 🧰 Tecnologias / Technologies

<p>
  <img align="left" alt="Python" title="Python 3.11" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/python/python-original.svg"/>
  <img align="left" alt="FastAPI" title="FastAPI" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/fastapi/fastapi-original.svg"/>
  <img align="left" alt="SQLAlchemy" title="SQLAlchemy" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/sqlalchemy/sqlalchemy-original.svg"/>
  <img align="left" alt="Alembic" title="Alembic" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/alembic/alembic-original.svg"/>
  <img align="left" alt="PostgreSQL" title="PostgreSQL + PgAdmin" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/postgresql/postgresql-original.svg"/>
  <img align="left" alt="MongoDB" title="MongoDB" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/mongodb/mongodb-original.svg"/>
  <img align="left" alt="Redis" title="Redis" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/redis/redis-original.svg"/>
  <img align="left" alt="Apache Spark" title="Apache Spark" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/apachespark/apachespark-original.svg"/>
  <img align="left" alt="Grafana" title="Grafana" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/grafana/grafana-original.svg"/>
  <img align="left" alt="Docker" title="Docker Compose" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/docker/docker-original.svg"/>
  <img align="left" alt="GitHub Actions" title="GitHub Actions" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/github/github-original.svg"/>
  <img align="left" alt="Pytest" title="Pytest" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/pytest/pytest-original.svg"/>
  <img align="left" alt="Apache Airflow" title="Apache Airflow" width="40px" src="Apache Airflow.png"/>


  <img align="left" alt="Apache Superset" title="Apache Superset" width="40px" src="https://superset.apache.org/img/superset-logo-horiz.svg"/>
</p>

<br clear="all"/>

---

## 📊 Arquitetura

```
+-------------------+        +-------------------+        +-------------------+
|   API Python      | -----> |   Airflow DAGs    | -----> |   Spark Cluster   |
|   (FastAPI)       |        |   (ETL/ELT)       |        |   (Master/Worker) |
+-------------------+        +-------------------+        +-------------------+
        |                                                        |
        v                                                        v
+-------------------+                                    +-------------------+
|   Postgres        |                                    |   MongoDB / Redis |
|   (Relacional)    |                                    |   (NoSQL + Cache) |
+-------------------+                                    +-------------------+
```
---

## 🔄 Pipelines ETL

etl_clientes_pipeline → Postgres → Spark → MongoDB + Redis.

spark_to_postgres → Spark → Postgres (para relatórios SQL).

Jobs Spark:

clientes_flat.py → agregados por estado.

contas_flat.py → agregados por cliente.

transacoes_flat.py → agregados por CPF.

validate_data.py → validação dos CSVs.

---

## 📂 Dados de Exemplo

clientes_flat.csv → dados cadastrais (nome, email, renda estimada).

contas_flat.csv → contas bancárias (account_id, cliente_id, saldo).

transactions_flat.csv → histórico de transações (deposit, withdraw, transfer).

---

## ⚙️ Automação

CLI (cli.py) → comandos para criar clientes, abrir contas, depositar, sacar, transferir e rodar ETL.

Scripts PowerShell:

BankPy.ps1 → subir/derrubar ambiente, seed, ETL, testes.

airflow_dag_debug.ps1 → debug de DAGs no Airflow.

automacao-superset.ps1 → setup automatizado do Superset.

setup-bankpy.ps1 → inicialização rápida do banco.

---

## 📈 Observabilidade

Superset → dashboards SQL configurados via superset_config.py.

Grafana → monitoramento e relatórios.

PgAdmin → administração do Postgres.

---

## 🧪 Testes

Unitários → CRUD de clientes, contas e transferências.

Integração → seed + ETL + geração de CSV.

End-to-End → fluxo completo da API (clientes, contas, depósitos, saques, transferências).

Rodar testes:
```
docker-compose -f docker-compose.tests.yml up --build --abort-on-container-exit
```

---

## 🔒 Configuração

.env.example → modelo de variáveis de ambiente.

.dockerignore / .gitignore → ignoram logs, caches e dados sensíveis.

alembic.ini → configuração do Alembic.

pyproject.toml → metadados do projeto.

requirements.txt → dependências (Airflow providers, psycopg2, pandas, pyspark).

---

## Modelos de Machine Learning

Os modelos estão em `dags/ml/` e podem ser executados de duas formas:

1. **Via Airflow DAG**  
   - O DAG `bankpy_pipeline` dispara os scripts após a validação dos dados.
   - Os resultados são gravados automaticamente na tabela `ml_results` do Postgres.

2. **Manual (para testes)**  
   - Entre no container do Airflow:
     ```bash
     docker exec -it airflow_scheduler bash
     ```
   - Rode o script desejado:
     ```bash
     python dags/ml/fraude_detect.py
     ```
   - O resultado será gravado em `ml_results`.

3. **Exploração em Jupyter**  
   - Use os notebooks em `notebooks/` para prototipar e visualizar os modelos.

---

## 🤖 Integração com Engenharia de Dados, Ciência de Dados e Machine Learning

O **BankPy** evolui para um ecossistema completo de dados, cobrindo três pilares:

### 🔹 Engenharia de Dados
- Pipelines ETL com Airflow e Spark.  
- Armazenamento híbrido (Postgres, MongoDB, Redis).  
- Orquestração com Docker Compose.  
- Scripts SQL para inicialização e simulação de dados.  

### 🔹 Ciência de Dados
- Agregações estatísticas com Spark (médias, totais, contagens).  
- Validação e profiling dos dados (`validate_data.py`).  
- Dashboards interativos com Superset e Grafana.  

### 🔹 Machine Learning
Expansão do projeto para incluir modelos preditivos e prescritivos:
- **Previsão de saldo** → regressão ou séries temporais para estimar evolução do saldo.  
- **Detecção de fraude** → classificação supervisionada para identificar transações suspeitas.  
- **Segmentação de clientes** → clustering (K-Means, DBSCAN) para agrupar perfis de clientes.  
- **Recomendação de produtos financeiros** → sistemas de recomendação baseados em histórico de transações.  

### 🔹 Fluxo Completo

```
ETL (Airflow + Spark) → Data Lake (Postgres/MongoDB)
→ Ciência de Dados (exploração + dashboards)
→ Machine Learning (modelos preditivos/detecção)
→ API/Serviços
```
---

## 👨‍💻 Desenvolvedor / Developer

- [Rogerio](https://github.com/Rogerio5)
- [Ronaldo](https://github.com/Ronaldo94-GITHUB)

---

## 📜 Licença / License

Este projeto está sob licença MIT. Para mais detalhes, veja o arquivo LICENSE.

This project is under the MIT license. For more details, see the LICENSE file.

---

## 🏁 Conclusão

O BankPy demonstra como aplicar conceitos de engenharia de dados em um sistema bancário realista, integrando múltiplas tecnologias para ingestão, transformação, análise e visualização de transações financeiras. É um projeto completo, com infraestrutura Docker, pipelines ETL, API REST, automação, observabilidade e testes automatizados.

Com a adição dos modelos de Machine Learning, o BankPy evolui para um ecossistema de dados ainda mais robusto, capaz de gerar previsões de saldo, detectar fraudes, segmentar clientes e recomendar produtos financeiros. Os resultados desses modelos já podem ser acompanhados em tempo real por meio de dashboards interativos no Superset, Grafana e Streamlit.

Nos próximos passos planejados, o projeto poderá incluir modelos de séries temporais para previsão de saldo, algoritmos avançados de detecção de fraude e técnicas de clustering mais sofisticadas (como DBSCAN ou Gaussian Mixture Models), ampliando ainda mais a capacidade analítica e preditiva do sistema
