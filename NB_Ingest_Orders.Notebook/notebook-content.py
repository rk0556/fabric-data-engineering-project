# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "367f9bf9-4db7-4c93-97bc-a8987b05d97b",
# META       "default_lakehouse_name": "LH_Sales_DEV",
# META       "default_lakehouse_workspace_id": "7127ff6a-2d66-474f-abb2-65330730ea9e",
# META       "known_lakehouses": [
# META         {
# META           "id": "367f9bf9-4db7-4c93-97bc-a8987b05d97b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Retail Data Engineering Project
# 
# ## Overview
# This notebook implements an end-to-end Microsoft Fabric Medallion Architecture pipeline:
# 
# **Source CSV → Bronze → Silver → Gold**
# 
# The objective is to ingest retail transaction data, apply data quality checks, transform the data using PySpark, and create analytics-ready Delta tables.
# 
# ## Technologies
# - Microsoft Fabric Lakehouse
# - PySpark
# - Delta Lake
# - Fabric Data Pipeline
# - Git-integrated development workflow

# MARKDOWN ********************

# ##### **CONFIGURATION & SCHEMA DEFINITION**


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType,
    StringType, TimestampType
)
from datetime import datetime
from uuid import uuid4

schema = StructType([
    StructField("Transaction_ID", IntegerType(), True),
    StructField("Customer_ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Zipcode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Income", StringType(), True),
    StructField("Customer_Segment", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Month", StringType(), True),
    StructField("Time", TimestampType(), True),
    StructField("Total_Purchases", IntegerType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Total_Amount", DoubleType(), True),
    StructField("Product_Category", StringType(), True),
    StructField("Product_Brand", StringType(), True),
    StructField("Product_Type", StringType(), True),
    StructField("Feedback", StringType(), True),
    StructField("Shipping_Method", StringType(), True),
    StructField("Payment_Method", StringType(), True),
    StructField("Order_Status", StringType(), True),
    StructField("Ratings", IntegerType(), True),
    StructField("products", StringType(), True)
])

# State tracking variables
run_id = str(uuid4())
start_time = datetime.now()
status = "Failed"
error_message = None
current_stage = "Initialization"

bronze_count = 0
silver_count = 0
gold_count = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================================
# 1. LOGGING HELPER FUNCTION
# ==============================================================================

def write_audit_log(run_id, pipeline_name, start_time, status, source_file, 
                    bronze_count, silver_count, gold_count, error_message):
    """Writes or appends execution run status into Delta audit log table."""
    try:
        end_time = datetime.now()
        
        log_schema = StructType([
            StructField("run_id", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("bronze_count", IntegerType(), True),
            StructField("silver_count", IntegerType(), True),
            StructField("gold_count", IntegerType(), True),
            StructField("error_message", StringType(), True)
        ])
        
        log_data = [(
            run_id,
            pipeline_name,
            start_time,
            end_time,
            status,
            source_file,
            bronze_count,
            silver_count,
            gold_count,
            error_message
        )]
        
        log_df = spark.createDataFrame(log_data, schema=log_schema)
        
        log_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("pipeline_execution_log")
            
        print(f"[AUDIT] Pipeline status '{status}' logged successfully.")
    except Exception as log_err:
        print(f"[AUDIT ERROR] Failed to write pipeline execution log: {str(log_err)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **PIPELINE PARAMETERS**

# PARAMETERS CELL ********************

source_file = "retail1_data.csv"
source_path = f"Files/source/{source_file}"
pipeline_name = "PL_Retail_Data_Engineering"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================================
# 2. MAIN MEDALLION PIPELINE EXECUTION
# ==============================================================================

try:
    # --------------------------------------------------------------------------
    # STAGE 1: BRONZE LAYER (Ingestion & Metadata Attachment)
    # --------------------------------------------------------------------------
    current_stage = "Bronze Ingestion"
    print(f"Starting stage: {current_stage}")

    raw_df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(source_path)
    )

    # Attach ingestion audit metadata
    bronze_df = (
        raw_df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
    )

    bronze_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("bronze_retail_transactions")

    bronze_count = bronze_df.count()
    print(f"Bronze Stage Completed. Records Ingested: {bronze_count:,}")


    # --------------------------------------------------------------------------
    # STAGE 2: SILVER LAYER (Cleansing, Date Normalization & DQ Flagging)
    # --------------------------------------------------------------------------


    current_stage = "Silver Processing"
    print(f"Starting stage: {current_stage}")

    # Date normalization
    silver_base_df = bronze_df.withColumn(
        "Transaction_Date",
        F.coalesce(
            F.to_date("Date", "M/d/yyyy"),
            F.to_date("Date", "M-d-yy"),
            F.to_date("Date", "M-d-yyyy")
        )
    )

    # Data quality evaluation flags
    silver_flagged_df = (
        silver_base_df
        .withColumn("dq_missing_transaction_id", F.col("Transaction_ID").isNull())
        .withColumn("dq_missing_customer_id", F.col("Customer_ID").isNull())
        .withColumn("dq_missing_date", F.col("Date").isNull())
        .withColumn("dq_invalid_date", F.col("Date").isNotNull() & F.col("Transaction_Date").isNull())
        .withColumn("dq_negative_amount", F.col("Amount").isNotNull() & (F.col("Amount") < 0))
        .withColumn("dq_negative_total_amount", F.col("Total_Amount").isNotNull() & (F.col("Total_Amount") < 0))
        .withColumn("dq_invalid_rating", F.col("Ratings").isNotNull() & ~F.col("Ratings").between(1, 5))
    )

    silver_flagged_df = silver_flagged_df.withColumn(
        "Transaction_Timestamp",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.date_format(F.col("Transaction_Date"), "yyyy-MM-dd"),
                F.date_format(F.col("Time"), "HH:mm:ss")
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    # Split Valid vs. Rejected records
    silver_df = silver_flagged_df.withColumn(
        "dq_status",
        F.when(
            F.col("dq_missing_transaction_id")
            | F.col("dq_missing_customer_id")
            | F.col("dq_missing_date")
            | F.col("dq_invalid_date")
            | F.col("dq_negative_amount")
            | F.col("dq_negative_total_amount")
            | F.col("dq_invalid_rating"),
            "REJECT"
        ).otherwise("VALID")
    )

    silver_valid_df = silver_df.filter(F.col("dq_status") == "VALID")
    silver_rejected_df = silver_df.filter(F.col("dq_status") == "REJECT")

    silver_valid_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_retail_transactions")

    silver_rejected_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_retail_rejected")

    silver_count = silver_valid_df.count()
    print(f"Silver Stage Completed. Valid: {silver_count:,} | Rejected: {silver_rejected_df.count():,}")

    # --------------------------------------------------------------------------
    # STAGE 3: GOLD LAYER (Star Schema Modeling) and AUDIT LOG EXECUTION
    # --------------------------------------------------------------------------
    current_stage = "Gold Dimensional Modeling"
    print(f"Starting stage: {current_stage}")

    # Dimension: Customer
    dim_customer = (
        silver_valid_df
        .select(
            "Customer_ID", "Name", "Email", "Phone", "Address", "City",
            "State", "Zipcode", "Country", "Age", "Gender", "Income", "Customer_Segment"
        )
        .dropDuplicates(["Customer_ID"])
    )

    # Dimension: Product
    dim_product = (
        silver_valid_df
        .select("products", "Product_Category", "Product_Brand", "Product_Type")
        .distinct()
    )

    # Dimension: Date
    dim_date = (
        silver_valid_df
        .select("Transaction_Date")
        .filter(F.col("Transaction_Date").isNotNull())
        .distinct()
        .withColumn("Year", F.year("Transaction_Date"))
        .withColumn("Month", F.month("Transaction_Date"))
        .withColumn("Month_Name", F.date_format("Transaction_Date", "MMMM"))
        .withColumn("Quarter", F.quarter("Transaction_Date"))
        .withColumn("Day", F.dayofmonth("Transaction_Date"))
        .withColumn("Day_Name", F.date_format("Transaction_Date", "EEEE"))
    )

    # Fact: Sales
    fact_sales = (
        silver_valid_df
        .select(
            "Transaction_ID", "Customer_ID", "products", "Transaction_Date",
            "Time", "Amount", "Total_Amount", "Total_Purchases", "Product_Category",
            "Product_Brand", "Product_Type", "Shipping_Method", "Payment_Method",
            "Order_Status", "Ratings", "Feedback", "Customer_Segment",
            "Country", "State", "City"
        )
    )

    dim_customer.write.format("delta").mode("overwrite").saveAsTable("gold_dim_customer")
    dim_product.write.format("delta").mode("overwrite").saveAsTable("gold_dim_product")
    dim_date.write.format("delta").mode("overwrite").saveAsTable("gold_dim_date")
    fact_sales.write.format("delta").mode("overwrite").saveAsTable("gold_fact_sales")

    gold_count = fact_sales.count()
    print(f"Gold Stage Completed. Fact Rows: {gold_count:,}")

    # Pipeline completed all stages successfully
    status = "Success"
    error_message = None

except Exception as ex:
    status = "Failed"
    error_message = f"[{current_stage}] {type(ex).__name__}: {str(ex)}"
    print(f"\n[PIPELINE ABORTED] Error during '{current_stage}': {error_message}\n")

finally:
    write_audit_log(
        run_id=run_id,
        pipeline_name=pipeline_name,
        start_time=start_time,
        status=status,
        source_file=source_file,
        bronze_count=bronze_count,
        silver_count=silver_count,
        gold_count=gold_count,
        error_message=error_message
    )

# View latest run in the log table
display(spark.table("pipeline_execution_log").orderBy(F.col("start_time").desc()).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
