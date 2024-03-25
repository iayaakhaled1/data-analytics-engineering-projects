#---------------------------------------------------#
import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
#---------------------------------------------------#
from airflow.operators import python
from airflow.operators.python import PythonOperator
from airflow.models import Variable
#---------------------------------------------------#
from airflow.decorators import dag, task, task_group
from airflow.operators.sqlite_operator import SqliteOperator
import sqlite3

#---------------------------------------------------#
import datetime
from datetime import datetime, timedelta
import pandas as pd
from statistics import mean

#---------------------------------------------------#


args = {"owner": "airflow", "start_date": airflow.utils.dates.days_ago(1)}
#---------------------------------------------------#

## Read the dataset 

def read_dataset():
    df_path = Variable.get("insurance_dataset_path", default_var=None)

    df = pd.read_csv(df_path)
    return df.to_json() 

def drop_nulls(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='read_dataset')
    df = pd.read_json(json_data)
    df = df.dropna()
    clean_df = df.to_json()
    print(df.isnull().sum())
    return clean_df

def create_age_groups(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='drop_nulls')
    df = pd.read_json(json_data)
    # Define age group logic as a lambda function
    age_group_lambda = lambda age: '0-19' if age < 20 else (
        '20-39' if age < 40 else ('40-59' if age < 60 else '60+'))

    # Create the age group column using the lambda
    df['age_group'] = df['age'].apply(age_group_lambda)
    clean_df = df.to_json()
    return clean_df


def create_age_groups_measures(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='create_age_groups')
    df = pd.read_json(json_data)
    # Define age group logic as a lambda function
    df_grouped = df.groupby('age_group').agg({'age':mean, 'charges':mean}).reset_index()
    df_grouped_json = df_grouped.to_json()
    return df_grouped_json

def create_gender_groups_measures(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='create_age_groups')
    df = pd.read_json(json_data)
    # Define age group logic as a lambda function
    df_grouped = df.groupby('gender').agg({'age':mean, 'charges':mean}).reset_index()
    df_grouped_json = df_grouped.to_json()
    return df_grouped_json

def insert_into_databse(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='create_age_groups')
    df = pd.read_json(json_data)
    # Connect to SQLite database
    database_path = Variable.get("sqlite_conn_path", default_var=None)
    conn = sqlite3.connect(database_path)
    
    # Insert DataFrame into SQLite table
    df.to_sql('insurance', conn, if_exists='replace', index=False)
    
    # Close the connection
    conn.close()
    print("insurance table has been created")



with DAG (
    dag_id="insurance_data_pipeline",
    description    = "insurance_data_pipeline for Airflow data pipeline project." ,
    default_args=args, 
    schedule_interval=None,

) as dag:
        read_dataset = PythonOperator(
            task_id = "read_dataset",
            python_callable = read_dataset
        )

        drop_nulls = PythonOperator(
            task_id = "drop_nulls",
            python_callable = drop_nulls
        )

        create_age_groups = PythonOperator(
            task_id = "create_age_groups",
            python_callable = create_age_groups
        )

        create_age_groups_measures = PythonOperator(
            task_id = "create_age_groups_measures",
            python_callable = create_age_groups_measures
        )

        create_gender_groups_measures = PythonOperator(
            task_id = "create_gender_groups_measures",
            python_callable = create_gender_groups_measures
        )
        
        insert_into_databse = PythonOperator(
            task_id = "insert_into_databse",
            python_callable = insert_into_databse,
            do_xcom_push = True 
        )


read_dataset >> drop_nulls >> create_age_groups >> [insert_into_databse,create_age_groups_measures,create_gender_groups_measures]
