from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, datediff, year, month
import sys


## main method
def main(input_path, output_path):
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Injuries ETL") \
        .getOrCreate()

    # Read the input CSV file
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Transformations
    df_transformed = df.withColumn("injury_date", to_date(col("injury_date"), "yyyy-MM-dd")) \
                       .withColumn("report_date", to_date(col("report_date"), "yyyy-MM-dd")) \
                       .withColumn("days_to_report", datediff(col("report_date"), col("injury_date"))) \
                       .withColumn("injury_year", year(col("injury_date"))) \
                       .withColumn("injury_month", month(col("injury_date")))

    # Write the transformed data to output path in Parquet format
    df_transformed.write.parquet(output_path, mode="overwrite")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: injuries_etl.py <input_path> <output_path>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    main(input_path, output_path)