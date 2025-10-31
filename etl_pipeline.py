from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, upper
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ETL Pipeline Example") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Define schema for source data
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("purchase_amount", DoubleType(), True),
    StructField("region", StringType(), True)
])

# Extract: Read data from source (CSV, Parquet, JSON, etc.)
df_source = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("/path/to/source/data.csv")

print("Source data loaded:")
df_source.show(5)

# Transform: Data cleaning and transformation
df_transformed = df_source \
    .filter(col("purchase_amount").isNotNull()) \
    .filter(col("purchase_amount") > 0) \
    .withColumn("customer_name", trim(col("customer_name"))) \
    .withColumn("region", upper(col("region"))) \
    .withColumn("customer_tier", 
                when(col("purchase_amount") > 1000, "Premium")
                .when(col("purchase_amount") > 500, "Gold")
                .otherwise("Standard"))

print("Transformed data:")
df_transformed.show(5)

# Perform aggregations
df_aggregated = df_transformed \
    .groupBy("region", "customer_tier") \
    .agg(
        {"purchase_amount": "sum", "customer_id": "count"}
    ) \
    .withColumnRenamed("sum(purchase_amount)", "total_revenue") \
    .withColumnRenamed("count(customer_id)", "customer_count")

print("Aggregated data by region and tier:")
df_aggregated.show()

# Load: Write data to target destination
df_transformed.write \
    .mode("overwrite") \
    .partitionBy("region") \
    .parquet("/path/to/target/transformed_data")

df_aggregated.write \
    .mode("overwrite") \
    .parquet("/path/to/target/aggregated_data")

print("ETL Pipeline completed successfully!")

# Stop Spark session
spark.stop()
