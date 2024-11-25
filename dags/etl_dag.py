import os
import sys 
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from minio import Minio
from minio.error import S3Error
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl.extract import extract
from etl.transform_load import transform_load
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

# ACCESS_KEY='N79HX3u7UH09JkOOwJiO'
# SECRET_KEY='HMNV0GKm60wEFs7SEAsWHsX4J9oA7qiTCUUs5Nmk'
        
url = 'https://github.com/erkansirin78/datasets/raw/master/sensors_instrumented_in_an_office_building_dataset.zip'

start_date = datetime(2024, 11, 17, 12, 20)


default_args = {
    'owner': 'etl_dag',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def load_to_minio(access_key, secret_key, file_path):
    bucket_name = "mybucket"

    print(f"Connecting to MinIO at 'localhost:9000' with access key '{access_key}' and secret key '{secret_key}'")

    minio_client = Minio(
        "localhost:9008",
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

    try:
        if not minio_client.bucket_exists(bucket_name):
            print(f"Bucket '{bucket_name}' does not exist. Creating it...")
            minio_client.make_bucket(bucket_name)
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        print(f"Error occurred while checking/creating the bucket: {e}")
        return

    try:
        minio_client.fput_object(bucket_name, "sensors.csv", file_path)
        print(f"'{file_path}' uploaded successfully to '{bucket_name}'")
    except S3Error as e:
        print(f"Error occurred during file upload: {e}")


def hello():
    print("start")

with DAG('etl_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    start = PythonOperator(task_id = "start",python_callable=hello)

    etl_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'etl'))
    utils_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','utils'))

    extract_data = PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_kwargs={'url':url, 'extracted_dir':'data'},
    )

    transform_data = PythonOperator(
        task_id='transform',
        python_callable=transform_load,
        op_kwargs={'directory': 'data/KETI','data_generator_path': 'data-generator'},
    )

    load = PythonOperator(
        task_id = 'load',
        python_callable=load_to_minio,
        op_kwargs={'access_key' : ACCESS_KEY, 
                   'secret_key': SECRET_KEY, 
                   'file_path': 'data-generator/sensors.csv'}
    )
     
    end = PythonOperator(task_id = 'end', python_callable=hello)

    start >> extract_data >> transform_data >> load >> end