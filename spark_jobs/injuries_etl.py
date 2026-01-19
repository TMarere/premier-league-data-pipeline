## this Script is used to extract, transform, and load injury data using PySpark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff, year, month




spark = (
    SparkSession.builder
    .appName("Premier League Injuries ETL")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)


df = spark.read.option("header", True).option("inferSchema", True).csv("s3a://pl-injuries-data-tanatswa/raw/player_injuries_impact_2024_25.csv")


print("=== Input schema ===") 
df.printSchema() 
print("=== Input sample ===") 
df.show(5, truncate=False)

# Data Cleaning and Transformation

df_cleaned = (
    df
    .withColumn("injury_start", to_date(col("Injury Start"), "yyyy-MM-dd"))
    .withColumn("expected_return", to_date(col("Expected Return"), "yyyy-MM-dd"))
    .withColumn("days_out", datediff(col("expected_return"), col("injury_start")))
)

             
             #Overwrite
df_cleaned.write.mode("overwrite").partitionBy("injury_year", "injury_month").parquet("s3a://pl-injuries-data-tanatswa/processed/player_injuries_impact_2024_25.parquet")

print(type(df_cleaned))
df_cleaned.printSchema()
df_cleaned.show(5, truncate=False)
