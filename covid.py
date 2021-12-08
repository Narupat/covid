import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests


def get_covid():
    url = 'https://covid19.ddc.moph.go.th/api/Cases/today-cases-by-provinces'
    response = requests.get(url)
    dataset = response.json()
    #with open('data2.json', 'w') as f:
        #json.dump(data2, f)
    return dataset


def save_data_into_db():
    #mysql_hook = MySqlHook(mysql_conn_id='app_db')
    dataset = get_covid()
    for data in dataset:
        import mysql.connector
        db = mysql.connector.connect(host='3.220.8.99',user='root',passwd='password',db='covid')
        
        cursor = db.cursor()
        new_case = data['new_case']
        new_death = data['new_death']
        province = data['province']
        total_case = data['total_case']
        total_death = data['total_death']
        update_date	 = data['update_date']

        cursor.execute('INSERT INTO covid_tb (new_case,new_death,province,total_case,total_death,update_date)'
                  'VALUES("%s","%s","%s","%s","%s","%s")',
                   (new_case,new_death,province,total_case,total_death,update_date))
        
        db.commit()
        print("Record inserted successfully into  table")
        cursor.close()
                                       
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    
}
with DAG('covid_data_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for travel report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_covid',
        python_callable= get_covid
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )

    t1 >> t2 

