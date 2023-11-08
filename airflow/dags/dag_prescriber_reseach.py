from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Define os argumentos padrão da DAG
default_args = {
    'owner': 'phillipefs',
    'start_date': days_ago(1)
}

# Crie uma instância da DAG
dag = DAG(
    'list_hdfs_directory',  # Nome da DAG
    default_args=default_args,
    schedule_interval=None,  # Defina o agendamento conforme necessário
    catchup=False,  # Impede a execução de tarefas em atraso
    tags=['PresciberResearch'],  # Tags para identificar a DAG
)

# Defina a tarefa que executa o comando hdfs dfs -ls /
list_hdfs_task = BashOperator(
    task_id='list_hdfs_directory',
    bash_command='hdfs dfs -ls /',  # Comando que será executado
    dag=dag,
)

# Configura as dependências da tarefa
list_hdfs_task

if __name__ == "__main__":
    dag.cli()
