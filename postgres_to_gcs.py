from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',   
    'depends_on_past': False,    
    'email': ['bimatrimukti@gmail.com'],   
    'email_on_failure': True,  
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# DAG definition
dag = DAG(
    'postgres_to_bigquery',
    default_args=default_args,
    description='ETL from PostgreSQL to BigQuery using Spark',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2024, 6, 1),
    catchup=False,
)

def extract_data_from_postgres(table_name, local_file_path):
    pg_hook = PostgresHook(postgres_conn_id='postgresbimzconn')
    sql = f"SELECT * FROM {table_name}"
    df = pg_hook.get_pandas_df(sql)
    df.to_csv(local_file_path, index=False)

def check_file_exists(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

def load_to_bigquery(dataset_name, table_name, local_file_path, schema, write_disposition='WRITE_TRUNCATE'):
    check_file_exists(local_file_path)
    client = bigquery.Client()
    table_id = f'{dataset_name}.{table_name}'
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        allow_jagged_rows=True,
        field_delimiter=",",
        write_disposition=write_disposition,
        schema=schema
    ) 
    with open(local_file_path, 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)  
    job.result()
    os.remove(local_file_path)

def cleanup_files(file_paths):
    for file_path in file_paths:
        if os.path.exists(file_path):
            os.remove(file_path)

# Define the tasks for each table
tables = [
    'billing', 
    'churn_details', 
    'contracts', 
    'customers', 
    'internet_services', 
    'orders', 
    'payment_history', 
    'services'
]

extract_tasks = []
for table in tables:
    local_file_path = f'/home/bimz/dummy_data/final_project/{table}.csv'
    extract_task = PythonOperator(
        task_id=f'extract_{table}',
        python_callable=extract_data_from_postgres,
        op_kwargs={'table_name': table, 'local_file_path': local_file_path},
        dag=dag,
    )
    extract_tasks.append(extract_task)

# Define Spark submit task for transformation and load
spark_transform_load_task = SparkSubmitOperator(
    task_id='spark_etl_task',
    application='/home/bimz/airflow/dags/finalProject/spark_transformation.py',
    application_args=[],
    conn_id='spark_default',
    conf={
        'spark.app.name': 'spark_etl_job',
        'spark.executor.memory': '4g',
        'spark.driver.memory': '4g',
        'spark.executor.cores': '2',
    },
    verbose=True,
    dag=dag,
)

# Task to load the transformed data to BigQuery
load_transaksi_task = PythonOperator(
    task_id='load_transaksi',
    python_callable=load_to_bigquery,
    op_kwargs={
        'dataset_name': 'data_final',
        'table_name': 'data_transaksi',
        'local_file_path': '/home/bimz/dummy_data/final_project/transaksi.csv',
        'schema': [
            bigquery.SchemaField("customerID", "STRING"),
            bigquery.SchemaField("OrderID", "STRING"),
            bigquery.SchemaField("OrderDate", "STRING"),
            bigquery.SchemaField("AverageMonthlyCharges", "FLOAT"),
            bigquery.SchemaField("TotalCharges", "FLOAT")
        ],
        'write_disposition': 'WRITE_TRUNCATE'
    },
    dag=dag,
)

load_referensi_task = PythonOperator(
    task_id='load_referensi',
    python_callable=load_to_bigquery,
    op_kwargs={
        'dataset_name': 'data_final',
        'table_name': 'data_referensi',
        'local_file_path': '/home/bimz/dummy_data/final_project/referensi.csv',
        'schema': [
            bigquery.SchemaField("customerID", "STRING"),
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("SeniorCitizen", "INTEGER"),
            bigquery.SchemaField("Partner", "STRING"),
            bigquery.SchemaField("Dependents", "STRING"),
            bigquery.SchemaField("InternetService", "STRING"),
            bigquery.SchemaField("OnlineSecurity", "STRING"),
            bigquery.SchemaField("OnlineBackup", "STRING"),
            bigquery.SchemaField("DeviceProtection", "STRING"),
            bigquery.SchemaField("TechSupport", "STRING"),
            bigquery.SchemaField("StreamingTV", "STRING"),
            bigquery.SchemaField("StreamingMovies", "STRING"),
            bigquery.SchemaField("Contract", "STRING"),
            bigquery.SchemaField("PaperlessBilling", "STRING"),
            bigquery.SchemaField("PaymentMethod", "STRING"),
            bigquery.SchemaField("tenure", "INTEGER"),
            bigquery.SchemaField("PhoneService", "STRING"),
            bigquery.SchemaField("MultipleLines", "STRING"),
            bigquery.SchemaField("OrderDate", "DATE")
        ],
        'write_disposition': 'WRITE_TRUNCATE'
    },
    dag=dag,
)

load_churn_task = PythonOperator(
    task_id='load_churn',
    python_callable=load_to_bigquery,
    op_kwargs={
        'dataset_name': 'data_final',
        'table_name': 'data_churn',
        'local_file_path': '/home/bimz/dummy_data/final_project/churn.csv',
        'schema': [
            bigquery.SchemaField("customerID", "STRING"),
            bigquery.SchemaField("ChurnReason", "STRING"),
            bigquery.SchemaField("ChurnDate", "DATE"),
            bigquery.SchemaField("LastInteractionDate", "DATE"),
            bigquery.SchemaField("SupportContactCount", "INTEGER"),
            bigquery.SchemaField("RetentionOffers", "STRING"),
            bigquery.SchemaField("FeedbackScore", "FLOAT"),
            bigquery.SchemaField("tenure", "INTEGER"),
            bigquery.SchemaField("PhoneService", "STRING"),
            bigquery.SchemaField("MultipleLines", "STRING")
        ],
        'write_disposition': 'WRITE_TRUNCATE'
    },
    dag=dag,
)


# Task to clean up the temporary files after all processes are successful
cleanup_task = PythonOperator(
    task_id='cleanup_files',
    python_callable=cleanup_files,
    op_kwargs={'file_paths': [
        '/home/bimz/dummy_data/final_project/transaksi.csv',
        '/home/bimz/dummy_data/final_project/referensi.csv',
        '/home/bimz/dummy_data/final_project/churn.csv',
        *[f'/home/bimz/dummy_data/final_project/{table}.csv' for table in tables]
    ]},
    dag=dag,
)

# Define task dependencies
extract_tasks >> spark_transform_load_task >> [load_transaksi_task, load_referensi_task, load_churn_task] >> cleanup_task
