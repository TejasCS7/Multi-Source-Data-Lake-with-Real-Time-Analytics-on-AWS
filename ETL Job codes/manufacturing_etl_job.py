import sys
from multiprocessing.pool import ThreadPool
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, DoubleType
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
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")

try:
    # --- TABLE PROCESSING ---
    # Get tables using Spark SQL
    tables_df = spark.sql(f"SHOW TABLES IN {database}")
    tables = [row['tableName'] for row in tables_df.collect()]
    
    if not tables:
        raise Exception(f"No tables found in database {database}")
    
    logger.info(f"Found {len(tables)} tables to process")

    # --- PARALLEL TABLE READING ---
    def process_table(table_name):
        try:
            dyf = glueContext.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table_name
            )
            df = dyf.toDF()
            
            # Identify columns that actually contain data
            non_null_cols = [col for col in df.columns 
                           if df.filter(F.col(col).isNotNull()).count() > 0]
            
            # Always include these essential columns
            essential_cols = {'machine_id', 'energy_consumption', 'predicted_remaining_life'}
            cols_to_keep = list(set(non_null_cols) | essential_cols)
            
            # Select only columns with data
            df = df.select(cols_to_keep).withColumn("source_table", F.lit(table_name))
            
            return df
        except Exception as e:
            logger.error(f"Error processing {table_name}: {str(e)}")
            return None

    # Process 4 tables at a time
    with ThreadPool(4) as pool:
        dfs = pool.map(process_table, tables)
    
    # Combine results
    dfs = [df for df in dfs if df is not None]
    if not dfs:
        raise Exception("No tables processed successfully")
    
    all_manufacturing_df = dfs[0]
    for df in dfs[1:]:
        all_manufacturing_df = all_manufacturing_df.unionByName(df, allowMissingColumns=True)

    # --- DATA VALIDATION AND TRANSFORMATIONS ---
    # Validate vibration readings (log warning if negative)
    if "vibration" in all_manufacturing_df.columns:
        try:
            negative_vibration_count = all_manufacturing_df.filter(F.col("vibration") < 0).count()
            if negative_vibration_count > 0:
                logger.info(f"Found {negative_vibration_count} records with negative vibration values")
        except Exception as e:
            logger.error(f"Error checking vibration values: {str(e)}")

    # Calculate machine efficiency with bounds/clamping
    all_manufacturing_df = all_manufacturing_df.withColumn(
        "machine_efficiency",
        F.when(F.col("energy_consumption") <= 0, 0)
         .otherwise(
             F.least(F.lit(500), 
             F.greatest(F.lit(0),
             F.col("predicted_remaining_life") / F.col("energy_consumption")))
         )
    )

    # Filter out null values in key columns
    all_manufacturing_df = all_manufacturing_df.filter(
        F.col("energy_consumption").isNotNull() & 
        F.col("predicted_remaining_life").isNotNull() &
        F.col("machine_id").isNotNull()
    )

    # --- AGGREGATIONS ---
    # Get numeric columns for aggregation
    numeric_cols = [col for col, dtype in all_manufacturing_df.dtypes 
                   if dtype in ('int', 'bigint', 'float', 'double', 'decimal') 
                   and col not in {'machine_id', 'source_table'}]
    
    agg_exprs = [F.avg(col).alias(f"avg_{col}") for col in numeric_cols]
    agg_exprs.extend([
        F.sum(F.when(F.col("anomaly_flag") > 0, 1).otherwise(0)).alias("total_anomalies"),
        F.count("*").alias("total_records"),
        F.countDistinct("source_table").alias("source_tables_count")
    ])
    
    machine_agg = all_manufacturing_df.groupBy("machine_id").agg(*agg_exprs)

    # --- OPTIMIZED WRITES ---
    # Write aggregations
    machine_agg.write.mode("overwrite").parquet(
        path=target_path + "machine_aggregations/",
        compression="snappy"
    )

    # Write processed data (partition if large dataset)
    if all_manufacturing_df.count() > 100000:
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
