from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import the necessary modules from the project's src directory
from src.data_ingestion import download_dataset
from src.data_transformation import transform_data, create_spark_session, save_to_gcs
from src.data_load_to_gcs import upload_to_gcs

# Set the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG with its properties
dag = DAG(
    'kaggle_to_gcs_elt',
    default_args=default_args,
    description='A DAG to download from Kaggle, upload to GCS, and transform data.',
    schedule='@daily',
    catchup=False,
)

# Define the PythonOperator for downloading the dataset
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

# Define the PythonOperator for uploading the dataset to Google Cloud Storage
upload_task = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={'bucket_name': 'your-bucket-name', 'local_folder': './Datasets/'},
    dag=dag,
)

# Define a Python function to handle the data transformation pipeline
def transformation_pipeline():
    spark_session = create_spark_session()  # Create a Spark session
    gcs_read_path = 'gs://your-bucket-name/geolocation.csv'  # Define the GCS read path
    local_write_path = './transformed_geolocation'  # Local path for the transformed data
    gcs_write_bucket = 'your-bucket-name'  # GCS bucket name for writing the transformed data
    gcs_write_path = 'transformed/geolocation_accurated.csv'  # GCS path for the final output
    
    # Process and transform the data
    transformed_df = transform_data(spark_session, gcs_read_path)
    save_to_gcs(transformed_df, local_write_path, gcs_write_bucket, gcs_write_path)

# Define the PythonOperator for data transformation
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transformation_pipeline,
    dag=dag,
)

# Set the task dependencies to define the DAG's execution flow
download_task >> upload_task >> transform_task
