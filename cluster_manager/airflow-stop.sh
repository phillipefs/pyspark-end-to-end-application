echo "Kill Webserver and Scheduler..."
pkill -f "airflow webserver"
pkill -f "airflow scheduler"