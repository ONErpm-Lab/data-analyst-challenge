

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("LeerParquet").getOrCreate()

# Path to the Parquet file (can be the directory of the parquet file)
ruta_parquet = "datos_agrupados_ano_mes_20250717_085026.parquet"

# Read the Parquet file
df = spark.read.parquet(ruta_parquet)

# Show the schema
print("Schema:")
df.printSchema()

# Count the number of rows
conteo = df.count()
print(f"Number of rows: {conteo}")

# Show the first 10 rows
print("First 10 rows:")
df.show(10, truncate=False)

df.coalesce(1).write.csv("datos_agrupados_ano_mes_20250717_085026_final.csv", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()