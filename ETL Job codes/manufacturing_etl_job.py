import sys
from multiprocessing.pool import ThreadPool
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context and Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

# Define database and S3 path
database = "multi_source_data_sea"
target_path = "s3://multi-source-data-sea/analytics/manufacturing/"

# --- OPTIMIZATION CONFIGURATIONS ---
# (Only setting allowed configurations)
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Allowed
spark.conf.set("spark.sql.adaptive.enabled", "true")  # Allowed

try:
    # --- TABLE PROCESSING ---
    # Get tables using Spark SQL (faster than Glue API)
    tables_df = spark.sql(f"SHOW TABLES IN {database}")
    tables = [row['tableName'] for row in tables_df.collect()]
    
    if not tables:
        raise Exception(f"No tables found in database {database}")
    
    logger.info(f"Found {len(tables)} tables to process")

    # --- PARALLEL TABLE READING ---
    def process_table(table_name):
        try:
            # Pushdown predicate if possible
            dyf = glueContext.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table_name
            )
            df = dyf.toDF().withColumn("source_table", F.lit(table_name))
            return df
        except Exception as e:
            logger.error(f"Error processing {table_name}: {str(e)}")
            return None

    # Process 4 tables at a time (adjust based on DPUs)
    with ThreadPool(4) as pool:
        dfs = pool.map(process_table, tables)
    
    # Combine results
    dfs = [df for df in dfs if df is not None]
    if not dfs:
        raise Exception("No tables processed successfully")
    
    all_manufacturing_df = dfs[0]
    for df in dfs[1:]:
        all_manufacturing_df = all_manufacturing_df.unionByName(df, allowMissingColumns=True)

    # --- DATA OPTIMIZATIONS ---
    # Cache only if dataset fits in memory
    if all_manufacturing_df.count() < 1000000:  # Adjust threshold
        all_manufacturing_df.cache()
    
    # Early filtering
    all_manufacturing_df = all_manufacturing_df.filter(
        F.col("energy_consumption").isNotNull() & 
        F.col("predicted_remaining_life").isNotNull()
    )

    # Efficient column operations
    all_manufacturing_df = all_manufacturing_df.withColumn(
        "machine_efficiency",
        F.when(F.col("energy_consumption") > 0, 
               F.col("predicted_remaining_life") / F.col("energy_consumption"))
        .otherwise(0)
    )

    # --- AGGREGATIONS ---
    machine_agg = all_manufacturing_df.groupBy("machine_id").agg(
        F.avg("temperature").alias("avg_temperature"),
        F.avg("vibration").alias("avg_vibration"),
        F.avg("humidity").alias("avg_humidity"),
        F.avg("pressure").alias("avg_pressure"),
        F.avg("energy_consumption").alias("avg_energy_consumption"),
        F.avg("downtime_risk").alias("avg_downtime_risk"),
        F.sum(F.when(F.col("anomaly_flag") > 0, 1).otherwise(0)).alias("total_anomalies"),
        F.avg("maintenance_required").alias("avg_maintenance_priority"),
        F.count("*").alias("total_records"),
        F.countDistinct("source_table").alias("source_tables_count")
    )

    # --- OPTIMIZED WRITES ---
    # Write aggregations (use direct Spark for better performance)
    machine_agg.write.mode("overwrite").parquet(
        path=target_path + "machine_aggregations/",
        compression="snappy"
    )

    # Write processed data (partition if large dataset)
    if all_manufacturing_df.count() > 100000:  # Adjust threshold
        all_manufacturing_df.write.mode("overwrite").partitionBy("machine_id").parquet(
            path=target_path + "processed/",
            compression="snappy"
        )
    else:
        all_manufacturing_df.write.mode("overwrite").parquet(
            path=target_path + "processed/",
            compression="snappy"
        )

    job.commit()
    logger.info("Job completed successfully")

except Exception as e:
    logger.error(f"Job failed: {str(e)}")
    job.commit()
    raise
