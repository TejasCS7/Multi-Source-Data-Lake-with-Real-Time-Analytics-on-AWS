import sys
from multiprocessing.pool import ThreadPool
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize with optimized Spark config
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

# Configure Spark for better performance
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Optimal for medium datasets
spark.conf.set("spark.sql.adaptive.enabled", "true")   # Enable adaptive query execution

# Define source and target
database = "multi_source_data_sea"
target_path = "s3://multi-source-data-sea/analytics/ecommerce/"
ecommerce_table = "ecommerce"  # Make sure to define this

try:
    # --- PARALLEL DATA LOADING ---
    # Get all tables if multiple (adapt if needed)
    tables_df = spark.sql(f"SHOW TABLES IN {database} LIKE '{ecommerce_table}*'")
    tables = [row['tableName'] for row in tables_df.collect()] or [ecommerce_table]
    
    logger.info(f"Processing tables: {tables}")

    def load_table(table_name):
        try:
            dyf = glueContext.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table_name,
                push_down_predicate="1=1"  # Enables predicate pushdown
            )
            return dyf.toDF().withColumn("source_table", F.lit(table_name))
        except Exception as e:
            logger.error(f"Error loading {table_name}: {str(e)}")
            return None

    # Process tables in parallel (4 threads optimal for most Glue jobs)
    with ThreadPool(4) as pool:
        dfs = pool.map(load_table, tables)
    
    # Combine DataFrames
    ecommerce_df = None
    for df in [df for df in dfs if df is not None]:
        if ecommerce_df is None:
            ecommerce_df = df
        else:
            ecommerce_df = ecommerce_df.unionByName(df, allowMissingColumns=True)

    if ecommerce_df is None:
        raise Exception("No data was successfully loaded")

    # --- DATA OPTIMIZATIONS ---
    # Cache only if dataset isn't too large
    if ecommerce_df.count() < 500000:  # Adjust based on your cluster size
        ecommerce_df.cache()
        logger.info("DataFrame cached for faster processing")

    # Efficient column operations
    ecommerce_df = ecommerce_df.withColumn(
        "purchase_efficiency",
        F.when(F.col("Time_to_Decision") > 0,
              F.col("Purchase_Amount") / F.col("Time_to_Decision"))
        .otherwise(0)
    )

    # --- OPTIMIZED AGGREGATIONS ---
    customer_agg = ecommerce_df.groupBy("Customer_ID").agg(
        F.sum("Purchase_Amount").alias("total_purchase_amount"),
        F.avg("Purchase_Amount").alias("avg_purchase_amount"),
        F.count("*").alias("purchase_count"),  # More efficient than counting a column
        F.avg("Customer_Satisfaction").alias("avg_satisfaction"),
        F.avg("Product_Rating").alias("avg_product_rating"),
        F.max("Customer_Satisfaction").alias("customer_value_score")
    ).withColumn(
        "customer_segment",
        F.when(F.col("customer_value_score") > 0.8, "HIGH_VALUE")
         .when(F.col("customer_value_score") > 0.5, "MEDIUM_VALUE")
         .otherwise("STANDARD")
    )

    # --- OPTIMIZED WRITES ---
    # Write aggregations (direct Spark write is faster)
    customer_agg.write.mode("overwrite").parquet(
        path=target_path + "customer_aggregations/",
        compression="snappy"
    )

    # Write processed data (partition if large)
    if ecommerce_df.count() > 100000:
        ecommerce_df.write.mode("overwrite").partitionBy("Customer_ID").parquet(
            path=target_path + "processed/",
            compression="snappy"
        )
    else:
        ecommerce_df.write.mode("overwrite").parquet(
            path=target_path + "processed/",
            compression="snappy"
        )

    job.commit()
    logger.info("Job completed successfully")

except Exception as e:
    logger.error(f"Job failed: {str(e)}")
    job.commit()
    raise
