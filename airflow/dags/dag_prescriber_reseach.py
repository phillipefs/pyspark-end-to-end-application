from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor

from datetime import datetime, timedelta


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
    
    list_hdfs_task = BashOperator(
        task_id='list_hdfs_directory',
        bash_command='hdfs dfs -ls /',
        dag=dag,
    )

    # pwd_task = BashOperator(
    #     task_id='pwd_task',
    #     bash_command='ls',
    #     dag=dag,
    # )

    spark_job_bash = BashOperator(
    task_id='spark_job_bash',
    bash_command='spark-submit --master local[*] --name arrow-spark /home/phillipefs/spark-applications/pyspark-end-to-end-application/app_pipeline_prescriber_research.py',
    dag=dag,
    )

    spark_task = SparkSubmitOperator(
        task_id="spark_job_task",
        conn_id= 'spark_cnn',
        application= "/home/phillipefs/spark-applications/pyspark-end-to-end-application/app_pipeline_prescriber_research.py",
        dag=dag,
    )

    # Configura as dependências da tarefa
    start_process >> list_hdfs_task >> spark_job_bash>> spark_task >> end_process
