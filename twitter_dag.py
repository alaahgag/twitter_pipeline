from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from twitter_etl import run_twitter_etl

default_args = {
    'owner' : 'alaa',
    'start_date' : datetime(2023,6,7),
    'email' : 'alaahgag34@gmail.com',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1) ,
    'depends_on_past' : False,
    'email_on_failure' : False,
    'email_on_retry' : False 

}


dag = DAG('twitter_etl',
          default_args=default_args,
          description='my first etl code')


run_etl= PythonOperator(task_id='twitter_pipeline',
                        python_callable = run_twitter_etl,
                        dag=dag)

run_etl



