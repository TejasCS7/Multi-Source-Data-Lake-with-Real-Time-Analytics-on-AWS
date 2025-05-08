import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context and Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define database and S3 path
database = "multi_source_data_lake"
manufacturing_table = "smart_manufacturing_data_csv"
target_path = "s3://multi-source-data-lake/analytics/manufacturing/"

# Read data from AWS Glue catalog
manufacturing_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name=manufacturing_table
)

# Convert to DataFrame
manufacturing_df = manufacturing_dyf.toDF()

# Add machine_efficiency column
manufacturing_df = manufacturing_df.withColumn(
    "machine_efficiency",
    F.when(
        F.col("energy_consumption") > 0,
        F.col("predicted_remaining_life") / F.col("energy_consumption")
    ).otherwise(0)
)

# Group by machine_id and compute metrics
machine_agg = manufacturing_df.groupBy("machine_id").agg(
    F.avg("temperature").alias("avg_temperature"),
    F.avg("vibration").alias("avg_vibration"),
    F.avg("humidity").alias("avg_humidity"),
    F.avg("pressure").alias("avg_pressure"),
    F.avg("energy_consumption").alias("avg_energy_consumption"),
    F.avg("downtime_risk").alias("avg_downtime_risk"),
    F.sum(F.when(F.col("anomaly_flag") > 0, 1).otherwise(0)).alias("total_anomalies"),
    F.avg("maintenance_required").alias("avg_maintenance_priority")
)

# Convert back to DynamicFrame and write to S3
machine_agg_dyf = DynamicFrame.fromDF(machine_agg, glueContext, "machine_aggregations")
glueContext.write_dynamic_frame.from_options(
    frame=machine_agg_dyf,
    connection_type="s3",
    connection_options={"path": target_path + "machine_aggregations/"},
    format="parquet"
)

# Save processed raw data
processed_dyf = DynamicFrame.fromDF(manufacturing_df, glueContext, "processed_manufacturing")
glueContext.write_dynamic_frame.from_options(
    frame=processed_dyf,
    connection_type="s3",
    connection_options={"path": target_path + "processed/"},
    format="parquet"
)

# Commit job
job.commit()
