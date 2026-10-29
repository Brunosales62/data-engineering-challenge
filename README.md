# WeCogno Data Engineering Challenge

## Objetivo

Este projeto tem como objetivo implementar um pipeline ETL (Extract, Transform, Load) utilizando Apache Airflow, PostgreSQL e Docker para processamento de dados de corridas de táxi da cidade de Nova York.

A solução foi desenvolvida utilizando uma arquitetura em camadas, permitindo a separação entre dados brutos, tratados e refinados para consumo analítico.

---

# Arquitetura da Solução

```text
NYC Taxi Dataset (GitHub)
            │
            ▼
          Source
            │
            ▼
         Extract
            │
            ▼
           Raw
            │
            ▼
        Transform
            │
            ▼
         Trusted
            │
            ▼
         Refined
            │
            ▼
        PostgreSQL
```

---

# Tecnologias Utilizadas

- Python
- Apache Airflow
- Docker
- PostgreSQL
- Pandas
- SQLAlchemy
- Parquet

---

# Estrutura do Projeto

```text
challenge/
│
├── dags/
│   ├── extract_data.py
│   └── nyc_taxi_etl.py
│
├── scripts/
│   ├── source.py
│   ├── extract.py
│   ├── transform.py
│   ├── refined.py
│   └── load.py
│
├── data/
│   ├── source/
│   ├── raw/
│   ├── trusted/
│   └── refined/
│
├── docker-compose.yaml
│
└── README.md
```

---

# Processo ETL

## Source

Os arquivos foram obtidos a partir do repositório disponibilizado no desafio:

https://github.com/datarisk-io/data-engineering-challenge/tree/master/nyc-tlc-data

Nesta etapa os arquivos foram baixados e armazenados em:

```text
data/source
```

preservando os dados originais disponibilizados na origem.

---

## Extract

Após o download, foi criada uma etapa de extração responsável pela movimentação dos arquivos da área de origem para a camada Raw.

Fluxo:

```text
source → raw
```

A separação desta etapa permite representar explicitamente o processo ETL e facilita futuras implementações de validação ou controle de qualidade.

Os arquivos foram copiados preservando a estrutura e conteúdo originais.

---

## Transform (Trusted)

Nesta etapa os arquivos da camada Raw foram tratados e enriquecidos para geração da camada Trusted.

### Tratamentos realizados

- Remoção de registros duplicados;
- Conversão de colunas de data para o tipo datetime;
- Remoção de registros inválidos:
  - trip_distance <= 0
  - fare_amount < 0
  - trip_duration_minutes <= 0

### Colunas adicionadas

| Coluna | Descrição |
|----------|----------|
| trip_duration_minutes | Duração da corrida em minutos |
| pickup_date | Data da corrida |
| pickup_hour | Hora da corrida |
| pickup_month | Mês da corrida |
| pickup_day_of_week | Dia da semana |
| trip_revenue | Receita da corrida |
| average_speed_mph | Velocidade média da corrida |

Os dados transformados foram armazenados em:

```text
data/trusted
```

---

## Refined

A camada Refined foi criada para materializar os resultados analíticos solicitados pelo desafio.

Foram gerados os seguintes datasets:

### Total de Registros

```text
question_1_total_records.parquet
```

Contém o total de registros processados.

---

### Viagens em 17/06/2022

```text
question_2_trips_17_june.parquet
```

Contém o total de viagens iniciadas e finalizadas em 17 de junho de 2022.

---

### Viagem Mais Longa

```text
question_3_longest_trip.parquet
```

Contém os dados da viagem com maior distância registrada.

---

### Estatísticas de Distância

```text
question_4_trip_distance_stats.parquet
```

Contém métricas estatísticas da distribuição de distâncias percorridas.

---

## Load

Os dados gerados na camada Refined foram carregados para o PostgreSQL.

Configuração utilizada:

```text
Host: localhost
Port: 5432
Database: airflow
User: airflow
```

O PostgreSQL foi executado em container Docker juntamente com o ambiente Apache Airflow.

---

# Decisões Técnicas

## Utilização de Pandas

Inicialmente foi avaliada a utilização de PySpark para processamento dos dados.

Entretanto, considerando o contexto do desafio e a execução local, foi adotado Pandas para simplificar a implementação e reduzir a complexidade operacional.

---

## Arquitetura em Camadas

Foi adotada a arquitetura:

```text
Source
 ↓
Raw
 ↓
Trusted
 ↓
Refined
 ↓
PostgreSQL
```

Onde:

- Source: área de aterrissagem dos arquivos obtidos na origem;
- Raw: cópia dos dados originais;
- Trusted: dados tratados e enriquecidos;
- Refined: dados agregados para consumo analítico;
- PostgreSQL: persistência final dos resultados.

---

## Processamento Incremental

Durante os testes foi identificado que o carregamento simultâneo de todos os datasets resultava em consumo elevado de memória.

Para contornar essa limitação, os arquivos foram processados individualmente, reduzindo significativamente o consumo de recursos e aproximando a solução de um cenário real de produção.

---

# Orquestração

O pipeline foi preparado para execução através do Apache Airflow utilizando Docker Compose.

A DAG realiza a orquestração das etapas do processo:

```text
Source
 ↓
Extract
 ↓
Transform
 ↓
Refined
 ↓
Load
```

Essa abordagem permite rastreabilidade, monitoramento e reprocessamento das execuções de forma centralizada.

---

# Respostas do Desafio

## 1. Qual o total de registros na tabela final?

### Resultado

```text
38.849.120 registros
```

### SQL

```sql
SELECT COUNT(*)
FROM taxi_trips;
```

---

## 2. Qual o total de viagens iniciadas e finalizadas no dia 17 de junho?

### Resultado

```text
121.119 viagens
```

### SQL

```sql
SELECT COUNT(*)
FROM taxi_trips
WHERE DATE(tpep_pickup_datetime) = '2022-06-17'
  AND DATE(tpep_dropoff_datetime) = '2022-06-17';
```

---

## 3. Qual foi o dia da viagem mais longa percorrida?

### Resultado

```text
Data: 2022-10-28

Distância: 389.678,46 milhas
```

### SQL

```sql
SELECT
    DATE(tpep_pickup_datetime) AS trip_day,
    trip_distance
FROM taxi_trips
ORDER BY trip_distance DESC
LIMIT 1;
```

---

## 4. Qual a média, o desvio padrão, o mínimo, o máximo e os quartis da distribuição de distância percorrida?

### Resultado

| Métrica | Valor |
|----------|----------:|
| Média | 6,043089 |
| Desvio Padrão | 602,913835 |
| Mínimo | 0,01 |
| Q1 (25%) | 1,14 |
| Mediana (50%) | 1,91 |
| Q3 (75%) | 3,60 |
| Máximo | 389.678,46 |

### SQL

```sql
SELECT
    AVG(trip_distance) AS media,
    STDDEV(trip_distance) AS desvio_padrao,
    MIN(trip_distance) AS minimo,
    PERCENTILE_CONT(0.25)
        WITHIN GROUP (ORDER BY trip_distance) AS q1,
    PERCENTILE_CONT(0.50)
        WITHIN GROUP (ORDER BY trip_distance) AS mediana,
    PERCENTILE_CONT(0.75)
        WITHIN GROUP (ORDER BY trip_distance) AS q3,
    MAX(trip_distance) AS maximo
FROM taxi_trips;
```

---

# Análise de Qualidade dos Dados

Durante a exploração dos dados foram identificados registros atípicos na base de origem.

## Distribuição da Distância

| Percentil | Distância (milhas) |
|-----------|-------------------:|
| 90% | 9,04 |
| 95% | 15,31 |
| 99% | 20,22 |
| 99,9% | 29,33 |
| 99,99% | 58,32 |
| Máximo | 389.678,46 |

---

## Distribuição da Velocidade Média

| Percentil | Velocidade Média (mph) |
|-----------|----------------------:|
| 25% | 7,80 |
| 50% | 10,33 |
| 75% | 14,18 |
| 90% | 20,59 |
| 95% | 25,93 |
| 99% | 37,10 |
| Máximo | 8.936.506,00 |

---

## Observações

Foram identificados registros contendo:

- Distâncias superiores a 300 mil milhas;
- Velocidades superiores a 8 milhões de mph;
- Corridas com duração aproximada de 1 segundo;
- Corridas com duração superior a 10 milhões de minutos.

Esses valores indicam possíveis inconsistências presentes na base de origem.

Entretanto, como o desafio não especificou regras adicionais para tratamento de outliers, os resultados foram calculados considerando integralmente os dados disponibilizados, preservando a rastreabilidade e a fidelidade dos dados carregados.

---

# Como Executar

## Subir os containers

```bash
docker compose up -d
```

---

## Download dos dados

```bash
python scripts/source.py
```

---

## Executar extração para camada Raw

```bash
python scripts/extract.py
```

---

## Executar transformação para camada Trusted

```bash
python scripts/transform.py
```

---

## Gerar camada Refined

```bash
python scripts/refined.py
```

---

## Realizar carga para PostgreSQL

```bash
python scripts/load.py
```

---

## Acessar PostgreSQL

```bash
docker exec -it challenge-postgres-1 psql -U airflow -d airflow
```

---

# Melhorias Futuras

- Implementação de regras automatizadas de Data Quality;
- Tratamento automatizado de outliers;
- Migração da camada de transformação para PySpark;
- Particionamento dos dados;
- Implementação de testes automatizados;
- Observabilidade e monitoramento do pipeline;
- Utilização do comando COPY do PostgreSQL para aumento de performance;
- Criação de dashboards analíticos para consumo dos resultados.

---

# Conclusão

A solução implementa um pipeline ETL completo utilizando Apache Airflow, Docker, PostgreSQL e Python.

Foram desenvolvidas as etapas de ingestão, extração, transformação, refinamento e persistência dos dados, além da geração dos resultados analíticos solicitados no desafio.

A arquitetura em camadas adotada permite rastreabilidade, organização e evolução futura da solução, mantendo separadas as responsabilidades de cada etapa do processamento.