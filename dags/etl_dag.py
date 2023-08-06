from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import pandas as pd

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'Samir',
    'poke_interval': 600
}

with DAG(
    dag_id='etl_dag',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-aliev']
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command='curl {url} > /tmp/yellow_taxi_data.parquet'.format(
            url='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet'),
        dag=dag
        )


    def transform_func():
        df = pd.read_parquet('/tmp/yellow_taxi_data.parquet')
        df = df.query('passenger_count > 1 and trip_distance >= 5 and payment_type == 1')
        df = df[['VendorID', 'passenger_count', 'trip_distance', 'RatecodeID', 'payment_type', 'total_amount']]
        data_tuple = [tuple(row) for row in df.itertuples(index=False, name=None)]
        return data_tuple


    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_func,
        dag=dag
    )

    def load_func(data_tuple):

        mysql_hook = MySqlHook(mysql_conn_id='mysql_local', schema='db')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        sql = f"INSERT INTO taxi_data VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.executemany(sql, data_tuple)
        connection.commit()
        cursor.close()
        connection.close()


    load = PythonOperator(
        task_id='load',
        python_callable=load_func,
        op_args=[transform.output],
        dag=dag
    )

    extract >> transform >> load
