from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, date_format, sum as spark_sum, count, col, when
import os
import time
from datetime import datetime

# Define column names
columns = [
    "store",
    "date", 
    "product",
    "quantity",
    "is_stream",
    "is_download",
    "revenue",
    "currency",
    "country_code",
    "genre_id",
    "genre_name"
]

def create_spark_session():
    """
    Function to create an optimized Spark session
    """
    return SparkSession.builder \
        .appName("OneRPM Data Processing") \
        .config("spark.driver.memory", "32g") \
        .config("spark.executor.memory", "32g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1m") \
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200") \
        .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false") \
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "10m") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m") \
        .config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "0") \
        .config("spark.sql.adaptive.forceOptimizeSkewedJoin", "true") \
        .getOrCreate()

def load_parquet_data():
    """
    Function to load all parquet files with optimizations
    """
    start_time = time.time()
    
    # Create optimized Spark session
    spark = create_spark_session()
    
    # Set logging level to reduce warnings
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # List of all parquet directories to process
        parquet_files = [
            "onerpm_data_analyst_test_data_2022_csv000.parquet",
            "onerpm_data_analyst_test_data_2022_csv001.parquet",
            "onerpm_data_analyst_test_data_2023_csv000.parquet",
            "onerpm_data_analyst_test_data_2023_csv001.parquet",
            "onerpm_data_analyst_test_data_2024_csv000.parquet",
            "onerpm_data_analyst_test_data_2024_csv001.parquet",
            "onerpm_data_analyst_test_data_2024_csv002.parquet"
        ]
        
        # Check that all files exist
        existing_files = []
        for file in parquet_files:
            if os.path.exists(file):
                existing_files.append(file)
                print(f"✓ File found: {file}")
            else:
                print(f"✗ File not found: {file}")
        
        if not existing_files:
            print("❌ No parquet files found to process")
            return None, None
        
        print(f"\n🔄 Processing {len(existing_files)} parquet files...")
        
        # Read all files at once using list comprehension
        dataframes = []
        for i, file in enumerate(existing_files, 1):
            print(f" Loading file {i}/{len(existing_files)}: {file}")
            df_temp = spark.read.parquet(file)
            temp_rows = df_temp.count()
            print(f"   ✓ Rows loaded: {temp_rows:,}")
            dataframes.append(df_temp)
        
        # Union all dataframes at once
        print(f"\n🔗 Unioning {len(dataframes)} dataframes...")
        df_union = dataframes[0]
        for i, df_temp in enumerate(dataframes[1:], 2):
            df_union = df_union.union(df_temp)
            # Unpersist temporary dataframe to free memory
            df_temp.unpersist()
        
        # Show info about the unioned dataset
        total_rows = df_union.count()
        load_time = time.time() - start_time
        
        print(f"\n📊 === LOAD SUMMARY ===")
        print(f"📁 Total files processed: {len(existing_files)}")
        print(f"📈 Total rows in unioned dataset: {total_rows:,}")
        print(f"📋 Columns: {len(columns)}")
        print(f"⏱️  Load time: {load_time:.2f} seconds")
        print(f"🚀 Speed: {total_rows/load_time:,.0f} rows/second")
        
        return df_union, spark
        
    except Exception as e:
        print(f"❌ Error during loading: {str(e)}")
        return None, None

def clean_data(df_union):
    """
    Function to clean and validate the data
    """
    print("\n🧹 === DATA CLEANING ===")
    
    # Count rows before cleaning
    rows_before = df_union.count()
    print(f"📊 Rows before cleaning: {rows_before:,}")
    
    # Filter valid data
    df_clean = df_union.filter(
        col("date").isNotNull() &
        col("country_code").isNotNull() &
        col("genre_name").isNotNull() &
        col("revenue").isNotNull() &
        (col("revenue") >= 0) &
        (col("quantity") >= 0)
    )
    
    # Count rows after cleaning
    rows_after = df_clean.count()
    rows_removed = rows_before - rows_after
    
    print(f"📊 Rows after cleaning: {rows_after:,}")
    print(f"️  Rows removed: {rows_removed:,} ({rows_removed/rows_before*100:.2f}%)")
    
    return df_clean

def group_by_year_month(df_union, spark):
    """
    Function to group data by year-month (YYYY-MM) and save as parquet and CSV
    """
    start_time = time.time()
    
    try:
        print("\n📊 === GROUPING BY YEAR-MONTH (YYYY-MM) ===")
        
        # Clean data before grouping
        df_clean = clean_data(df_union)
        
        # Group by year-month with requested columns
        df_grouped = df_clean.groupBy(
            date_format("date", "yyyy-MM").alias("year_month"),
            "store",
            "country_code",
            "genre_name"
        ).agg(
            spark_sum("quantity").alias("total_quantity"),
            spark_sum("is_stream").alias("total_streams"),
            spark_sum("is_download").alias("total_downloads"),
            spark_sum("revenue").alias("total_revenue"),
            count("*").alias("total_transactions")
        ).orderBy("year_month", "store", "country_code", "genre_name")
        
        # Show info about the grouping
        total_grouped_rows = df_grouped.count()
        grouping_time = time.time() - start_time
        
        print(f"📊 Total grouped rows: {total_grouped_rows:,}")
        print(f"⏱️  Grouping time: {grouping_time:.2f} seconds")
        
        # Show schema
        print("\n📋 Grouped dataset schema:")
        df_grouped.printSchema()
        
        # Show some sample rows
        print("\n👀 First 10 rows of grouped dataset:")
        df_grouped.show(10, truncate=False)
        
        # Show stats by year-month
        print("\n === YEAR-MONTH SUMMARY ===")
        year_month_summary = df_grouped.groupBy("year_month").agg(
            spark_sum("total_quantity").alias("quantity_total"),
            spark_sum("total_streams").alias("streams_total"),
            spark_sum("total_downloads").alias("downloads_total"),
            spark_sum("total_revenue").alias("revenue_total"),
            spark_sum("total_transactions").alias("transactions_total")
        ).orderBy("year_month")
        
        year_month_summary.show(truncate=False)
        
        # Save as Parquet with optimizations
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_parquet = f"grouped_data_year_month_{timestamp}.parquet"
        
        print(f"\n💾 Saving as Parquet: {output_parquet}")
        df_grouped.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", "1000000") \
            .parquet(output_parquet)
        
        # Verify saved parquet file
        df_verify_parquet = spark.read.parquet(output_parquet)
        parquet_rows = df_verify_parquet.count()
        print(f"✓ Parquet saved: {parquet_rows:,} rows in {output_parquet}")
        
        # Save as CSV with optimizations
        output_csv = f"grouped_data_year_month_{timestamp}.csv"
        print(f"\n💾 Saving as CSV: {output_csv}")
        df_grouped.write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("compression", "gzip") \
            .option("maxRecordsPerFile", "1000000") \
            .csv(output_csv)
        
        # Verify saved CSV file
        csv_dir = output_csv
        if os.path.exists(csv_dir):
            for file in os.listdir(csv_dir):
                if file.endswith('.csv'):
                    csv_file_path = os.path.join(csv_dir, file)
                    print(f"✓ CSV saved: {csv_file_path}")
                    break
        
        # Show final stats
        total_time = time.time() - start_time
        print(f"\n🎯 === FINAL STATISTICS ===")
        print(f" Parquet file: {output_parquet}")
        print(f"📁 CSV file: {output_csv}")
        print(f" Total grouped records: {total_grouped_rows:,}")
        print(f"⏱️  Total processing time: {total_time:.2f} seconds")
        
        # Show date range
        dates = df_grouped.select("year_month").distinct().orderBy("year_month").collect()
        if dates:
            print(f" Date range: {dates[0]['year_month']} to {dates[-1]['year_month']}")
        
        # Show top 5 countries by revenue
        print(f"\n🏆 === TOP 5 COUNTRIES BY REVENUE ===")
        top_countries = df_grouped.groupBy("country_code").agg(
            spark_sum("total_revenue").alias("revenue_total")
        ).orderBy("revenue_total", ascending=False).limit(5)
        top_countries.show(truncate=False)
        
        # Show top 5 stores by revenue
        print(f"\n🏬 === TOP 5 STORES BY REVENUE ===")
        top_stores = df_grouped.groupBy("store").agg(
            spark_sum("total_revenue").alias("revenue_total")
        ).orderBy("revenue_total", ascending=False).limit(5)
        top_stores.show(truncate=False)
        
        # Show top 5 genres by revenue
        print(f"\n🎵 === TOP 5 GENRES BY REVENUE ===")
        top_genres = df_grouped.groupBy("genre_name").agg(
            spark_sum("total_revenue").alias("revenue_total")
        ).orderBy("revenue_total", ascending=False).limit(5)
        top_genres.show(truncate=False)
        
        return {
            'parquet_path': output_parquet,
            'csv_path': output_csv,
            'total_rows': total_grouped_rows,
            'dataframe': df_grouped,
            'processing_time': total_time
        }
        
    except Exception as e:
        print(f"❌ Error during grouping: {str(e)}")
        return None

# Run the processing
if __name__ == "__main__":
    print("🚀 Starting OneRPM data processing...")
    print("=" * 60)
    
    df_union, spark_session = load_parquet_data()
    
    if df_union is not None and spark_session is not None:
        print(f"\n✅ Load completed successfully!")
        
        try:
            # Run grouping by year-month
            print("\n🔄 Running grouping by year-month...")
            grouped_result = group_by_year_month(df_union, spark_session)
            
            if grouped_result:
                print(f"\n🎉 Grouping by year-month completed successfully!")
                print(f" Generated files:")
                print(f"   📄 Parquet: {grouped_result['parquet_path']}")
                print(f"    CSV: {grouped_result['csv_path']}")
                print(f"⏱️  Total time: {grouped_result['processing_time']:.2f} seconds")
                
        finally:
            # Close Spark session at the end
            spark_session.stop()
            print("\n🔚 Spark session closed.")
            print("\n✅ Process completed successfully!")
            print("=" * 60)
    else:
        print("\n❌ Error loading files") 