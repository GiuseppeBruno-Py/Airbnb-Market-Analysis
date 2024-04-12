import os
import glob
import googlemaps
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, regexp_replace, udf
from pyspark.sql.types import StringType
from google.cloud import storage
from dotenv import load_dotenv

def create_spark_session():
    """
    Creates and returns a Spark session configured for reading and writing to Google Cloud Storage.
    """
    load_dotenv()  # Load environment variables from .env file
    spark = SparkSession.builder \
        .appName("GCSFilesRead") \
        .config("spark.jars", "/home/giuseppe/airbnb-project/secrets/gcs-connector-hadoop3-latest.jar") \
        .config('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
        .config('fs.gs.auth.service.account.enable', 'true') \
        .config('fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
        .config('google.cloud.auth.service.account.json.keyfile', '/home/giuseppe/airbnb-project/secrets/my-creds.json') \
        .getOrCreate()
    return spark  # Return the configured Spark session

def transform_data(spark, gcs_path):
    """
    Reads and transforms data from GCS, returning a transformed DataFrame.
    """
    df = spark.read.csv(gcs_path, inferSchema=True, header=True, sep=';').limit(1).repartition(1)
    gmaps = googlemaps.Client(key=os.getenv('GOOGLE_MAPS_API_KEY'))  # Initialize Google Maps client

    def get_street_name(latitude, longitude):
        # Perform reverse geocoding to get the street name from latitude and longitude
        reverse_geocode_result = gmaps.reverse_geocode((latitude, longitude))
        return reverse_geocode_result[0]['formatted_address'] if reverse_geocode_result else None

    # Register UDF to convert latitude and longitude to street names
    get_street_name_udf = udf(get_street_name, StringType())
    df_geolocation = df.withColumn("latitude", regexp_replace(col("latitude"), ",", ".")) \
                       .withColumn("longitude", regexp_replace(col("longitude"), ",", ".")) \
                       .withColumn("street", get_street_name_udf(col("latitude"), col("longitude"))) \
                       .withColumn('street', regexp_replace('street', r'^\d+\s+', '')) \
                       .drop("street_name")  # Drop old street name column
    
    return df_geolocation  # Return the transformed DataFrame

def save_to_gcs(df_transformed, local_dir_path, bucket_name, destination_blob_name):
    """
    Saves the transformed DataFrame to Google Cloud Storage.
    """
    # Write the DataFrame as a single CSV file to a local directory
    df_transformed.coalesce(1).write.mode('overwrite').option('header', 'true').csv(local_dir_path)
    part_files = glob.glob(f"{local_dir_path}/part-*.csv")  # Find the CSV file
    if part_files:
        os.rename(part_files[0], os.path.join(os.path.dirname(part_files[0]), "transformed_geolocation.csv"))

    storage_client = storage.Client()  # Initialize the Google Cloud Storage client
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(os.path.join(os.path.dirname(part_files[0]), "transformed_geolocation.csv"))
    print(f"File uploaded to {destination_blob_name}.")

if __name__ == "__main__":
    # Initialize Spark session
    spark_session = create_spark_session()
    gcs_read_path = os.getenv('GCS_READ_PATH')  # Get the GCS path from environment variables
    local_write_path = 'teste_geolocation'  # Local directory for temporary storage
    gcs_write_bucket = os.getenv('GCS_WRITE_BUCKET')  # GCS bucket for uploading the data
    gcs_write_path = 'teste/geolocation_accurated.csv'  # GCS path for the final CSV file
    
    transformed_df = transform_data(spark_session, gcs_read_path)  # Transform data
    save_to_gcs(transformed_df, local_write_path, gcs_write_bucket, gcs_write_path)  # Save data to GCS
