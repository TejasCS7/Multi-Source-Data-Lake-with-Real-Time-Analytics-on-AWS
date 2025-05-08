import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark and Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define source and target
database = "multi_source_data_lake"
ecommerce_table = "ecommerce_consumer_behavior_analysis_data_csv"
target_path = "s3://multi-source-data-lake/analytics/ecommerce/"

# Load data from catalog
ecommerce_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name=ecommerce_table
)

# Convert to DataFrame
ecommerce_df = ecommerce_dyf.toDF()

# Add purchase_efficiency column
ecommerce_df = ecommerce_df.withColumn(
    "purchase_efficiency",
    F.when(
        F.col("Time_to_Decision") > 0,
        F.col("Purchase_Amount") / F.col("Time_to_Decision")
    ).otherwise(0)
)

# Customer-level aggregation
customer_agg = ecommerce_df.groupBy("Customer_ID").agg(
    F.sum("Purchase_Amount").alias("total_purchase_amount"),
    F.avg("Purchase_Amount").alias("avg_purchase_amount"),
    F.count("Customer_ID").alias("purchase_count"),
    F.avg("Customer_Satisfaction").alias("avg_satisfaction"),
    F.avg("Product_Rating").alias("avg_product_rating"),
    F.max("Customer_Satisfaction").alias("customer_value_score")
)

# Add customer segments
customer_agg = customer_agg.withColumn(
    "customer_segment",
    F.when(F.col("customer_value_score") > 0.8, "HIGH_VALUE")
     .when(F.col("customer_value_score") > 0.5, "MEDIUM_VALUE")
     .otherwise("STANDARD")
)

# Convert back to DynamicFrame
customer_agg_dyf = DynamicFrame.fromDF(customer_agg, glueContext, "customer_aggregations")

# Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=customer_agg_dyf,
    connection_type="s3",
    connection_options={"path": target_path + "customer_aggregations/"},
    format="parquet"
)

# Save processed raw data
processed_dyf = DynamicFrame.fromDF(ecommerce_df, glueContext, "processed_ecommerce")
glueContext.write_dynamic_frame.from_options(
    frame=processed_dyf,
    connection_type="s3",
    connection_options={"path": target_path + "processed/"},
    format="parquet"
)

# Commit the job
job.commit()
