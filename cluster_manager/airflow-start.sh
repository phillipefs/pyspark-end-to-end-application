# ps aux | grep airflow

echo "Start Webserver and Scheduler"
export AIRFLOW_HOME=/home/phillipefs/spark-applications/pyspark-end-to-end-application/airflow
airflow webserver -D
airflow scheduler -D