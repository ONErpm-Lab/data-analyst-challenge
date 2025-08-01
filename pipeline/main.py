import os
import requests
import gzip
import shutil
from pyspark.sql import SparkSession
from tqdm import tqdm
from pyspark.sql.types import *


jdbc_url = f'jdbc:postgresql://{os.getenv("POSTGRES_HOST", "localhost")}:{os.getenv("PGPORT", "5432")}/{os.getenv("POSTGRES_DB", "mydatabase")}'
jdbc_properties = {
    "user": os.getenv("POSTGRES_USER", "myuser"),
    "password": os.getenv("POSTGRES_PASSWORD", "mypassword"),
    "driver": "org.postgresql.Driver"
}


spark = SparkSession.builder \
    .appName("ONERPM to Postgres") \
    .config("spark.jars", "/opt/spark/jars/postgresql-jdbc.jar") \
    .getOrCreate()

files = {
    "onerpm_data_analyst_test_data_2022_csv000.gz": "https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2022_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202554Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=9b6b2f24273b62bdf3e52cbd11baaf4133c4786f7af33884a66272ec967fd3e4",
    "onerpm_data_analyst_test_data_2022_csv001.gz": "https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2022_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202611Z&X-Amz-Expires=604797&X-Amz-SignedHeaders=host&X-Amz-Signature=f9ce822b3e3d712d8028faa23f9a9d1e38bb45d3a43978df0ae2df1f098624f1",
    "onerpm_data_analyst_test_data_2023_csv000.gz": "https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2023_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202624Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=db9482f1d06e5344c5ba5f64345eb8527f94a72494afb3a2416d581f8249cd96",
    "onerpm_data_analyst_test_data_2023_csv001.gz": "https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2023_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202641Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=e04c276a359c4633892bfdc64f1f7d7c78acdd1c86cb2e45fe2e8acac8e233b6",
    "onerpm_data_analyst_test_data_2024_csv000.gz": "https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202703Z&X-Amz-Expires=604799&X-Amz-SignedHeaders=host&X-Amz-Signature=d64ffd4d22e0fa8160105936b08222209ca8328721d78b41f1ec6285e5369cd7",
    "onerpm_data_analyst_test_data_2024_csv001.gz": "https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202713Z&X-Amz-Expires=604797&X-Amz-SignedHeaders=host&X-Amz-Signature=773fcf27ead30802b96ccf4011651afa6915c10ee4f2eb2d37a4c3f43fd1eaa4",
    "onerpm_data_analyst_test_data_2024_csv002.gz": "https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv002.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202720Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=9389e6879e3c350f175793ca67f02050cacd3a4d7b0ccc9c25b3c7ecd60eed76"
}

schema = StructType([
    StructField("store", StringType(), True),
    StructField("date", DateType(), True),
    StructField("product", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("is_stream", IntegerType(), True),
    StructField("is_download", IntegerType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("genre_id", IntegerType(), True),
    StructField("genre_name", StringType(), True)
])

download_folder = "onerpm_data"
os.makedirs(download_folder, exist_ok=True)

def download_and_extract(filename, url):
    gz_path = os.path.join(download_folder, filename)
    csv_path = gz_path.replace(".gz", "")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(gz_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    with gzip.open(gz_path, 'rb') as f_in:
        with open(csv_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return csv_path

for filename, url in tqdm(files.items(), desc="Processing files"):
    print(f"Processing {filename}")
    
    csv_path = download_and_extract(filename, url)
    
    df = spark.read.csv(csv_path, schema=schema, header=False).sample(fraction=0.1, seed=42)

    table_name = filename.replace(".gz", "").replace("-", "_")

    print(f"Writing to PostgreSQL table: {table_name}")
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .option("batchsize", "10000") \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

print("✅ All data successfully written to PostgreSQL.")