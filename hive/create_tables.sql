-- Substitua 'seu_banco_de_dados' pelo nome desejado para o banco de dados
CREATE DATABASE IF NOT EXISTS db_prescribe_research;

-- Use o banco de dados recém-criado
USE db_prescribe_research;

CREATE EXTERNAL TABLE top_prescribes (
  presc_id STRING,
  presc_fullname STRING,
  presc_state STRING,
  country_name STRING,
  years_of_exp INT,
  trx_cnt STRING,
  total_day_supply STRING,
  total_drug_cost STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/application/gold/top_prescribes';

CREATE EXTERNAL TABLE city_report (
  city STRING,
  state_name STRING,
  county_name STRING,
  population INT,
  zip_counts INT,
  trx_counts DOUBLE,
  presc_counts BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/application/gold/city_report';