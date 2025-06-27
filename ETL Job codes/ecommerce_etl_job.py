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
ecommerce_table = "ecommerce"

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

    # Log schema for debugging
    logger.info("Original Schema:")
    ecommerce_df.printSchema()

    # --- DATA CLEANSING ---
    # Clean purchase amount (remove $ and convert to numeric)
    ecommerce_df = ecommerce_df.withColumn(
        "cleaned_purchase_amount",
        F.regexp_replace(F.col("purchase_amount"), "[$,]", "").cast("double")
    )

    # Ensure time_to_decision is numeric and handle NULLs
    ecommerce_df = ecommerce_df.withColumn(
        "cleaned_time_to_decision",
        F.coalesce(F.col("time_to_decision").cast("double"), F.lit(0))
    )

    # --- DATA TRANSFORMATIONS ---
    # Calculate purchase efficiency with proper handling
    ecommerce_df = ecommerce_df.withColumn(
        "purchase_efficiency",
        F.when(
            (F.col("cleaned_time_to_decision") > 0) &
            (F.col("cleaned_purchase_amount").isNotNull()),
            F.col("cleaned_purchase_amount") / F.col("cleaned_time_to_decision")
        ).otherwise(0)
    )

    # Cache only if dataset isn't too large
    if ecommerce_df.count() < 500000:  # Adjust based on your cluster size
        ecommerce_df.cache()
        logger.info("DataFrame cached for faster processing")

    # --- OPTIMIZED AGGREGATIONS ---
    customer_agg = ecommerce_df.groupBy("customer_id").agg(
        F.sum("cleaned_purchase_amount").alias("total_purchase_amount"),
        F.avg("cleaned_purchase_amount").alias("avg_purchase_amount"),
        F.count("*").alias("purchase_count"),
        F.avg("customer_satisfaction").alias("avg_satisfaction"),
        F.avg("product_rating").alias("avg_product_rating"),
        F.max("customer_satisfaction").alias("customer_value_score")
    ).withColumn(
        "customer_segment",
        F.when(F.col("customer_value_score") > 0.8, "HIGH_VALUE")
         .when(F.col("customer_value_score") > 0.5, "MEDIUM_VALUE")
         .otherwise("STANDARD")
    )

    # Log sample data for verification
    logger.info("Sample purchase efficiency values:")
    ecommerce_df.select("customer_id", "purchase_amount", "time_to_decision", "purchase_efficiency").show(5)

    # --- OPTIMIZED WRITES ---
    # Write aggregations (direct Spark write is faster)
    customer_agg.write.mode("overwrite").parquet(
        path=target_path + "customer_aggregations/",
        compression="snappy"
    )

    # Write processed data (partition if large)
    if ecommerce_df.count() > 100000:
        ecommerce_df.write.mode("overwrite").partitionBy("customer_id").parquet(
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
