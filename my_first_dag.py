# import the libraries
from datetime import timedelta

#DAG Object, we will need this to instatiate DAG
from airflow import DAG
#Operators, We need this to write tasks
from airflow.operators.bash_operator import BashOperator
#This makes scheduling easily
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Allen Mwandunga',
    'start_date': days_ago(0),
    'email':['allenmcndounger@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG Definition
dag = DAG(
    dag_id='my-first-dag',
    default_args=default_args,
    description='My First DAG',
    schedule_interval=timedelta(days=1)
)

# Task Definition
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d":" -f1,3,6 /etc/passwd > /home/project/airflow/dags/extracted-data.txt',
    dag=dag
)

transform_and_load = BashOperator(
    task_id='transform',
    bash_command='tr ":" "," < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv',
    dag=dag
)

# Task Pipeline
extract >> transform_and_load