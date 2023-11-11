from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

####### COMMANDS SPARK AND HDFS ########
DIR_HDFS_FACT          = "hdfs dfs -mkdir -p /application/fact/"
DIR_HDFS_DIMENSION     = "hdfs dfs -mkdir -p /application/dimension_city/"
COPY_DIMENSION_TO_HDFS = "hdfs dfs -put -f /home/phillipefs/spark-applications/pyspark-end-to-end-application/staging/dimension_city/us_cities_dimension.parquet /application/dimension_city/"
COPY_FACT_TO_HDFS      = "hdfs dfs -put -f /home/phillipefs/spark-applications/pyspark-end-to-end-application/staging/fact/USA_Presc_Medicare_Data.csv /application/fact/"
SPARK_SUBMIT_PIPELINE  = "cd /home/phillipefs/spark-applications/pyspark-end-to-end-application/ && spark-submit --master yarn /home/phillipefs/spark-applications/pyspark-end-to-end-application/app_pipeline_prescriber_research.py"
HIVE_COMMAND           = "cd /home/phillipefs/spark-applications/pyspark-end-to-end-application/hive/ && hive -f create_tables.sql"
########################################

default_args = {
    'owner': 'phillipefs',
    'start_date': datetime(2020, 11, 18),
}

with DAG('pipeline_prescriber_research',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,  
         tags=['Hadoop', 'Spark', 'ELT'], 
) as dag:
    
    start_process = EmptyOperator(task_id="start_process")
    end_process   = EmptyOperator(task_id="end_process")
    load_files_hdfs   = EmptyOperator(task_id="load_files_hdfs")

    create_dir_dimension_hdfs = BashOperator(
        task_id= 'create_dir_dimension_hdfs',
        bash_command= DIR_HDFS_DIMENSION,
        dag=dag,
    )

    create_dir_fact_hdfs = BashOperator(
        task_id= 'create_dir_fact_hdfs',
        bash_command= DIR_HDFS_FACT,
        dag=dag,
    )

    copy_dimension_to_hdfs = BashOperator(
        task_id = "copy_dimension_to_hdfs",
        bash_command= COPY_DIMENSION_TO_HDFS
    )

    copy_fact_to_hdfs = BashOperator(
        task_id = "copy_fact_to_hdfs",
        bash_command= COPY_FACT_TO_HDFS
    )

    spark_submit_pipeline = BashOperator(
        task_id = "spark_submit_pipeline",
        bash_command= SPARK_SUBMIT_PIPELINE
    )

    create_tables_hive = BashOperator(
        task_id = "create_tables_hive",
        bash_command= HIVE_COMMAND
    )

    # Configura as dependências da tarefa
    start_process >> [create_dir_dimension_hdfs, create_dir_fact_hdfs]
    [create_dir_dimension_hdfs, create_dir_fact_hdfs] >> load_files_hdfs
    load_files_hdfs >> [copy_dimension_to_hdfs, copy_fact_to_hdfs] >> spark_submit_pipeline
    spark_submit_pipeline >> create_tables_hive >> end_process