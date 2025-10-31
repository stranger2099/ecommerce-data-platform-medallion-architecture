from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import dlt 

#schema definition 
schema = StructType([
    StructField("type", StringType(), True),
    StructField("days_for_shipping_real", IntegerType(), True),
    StructField("days_for_shipment_scheduled", IntegerType(), True),
    StructField("benefit_per_order",DoubleType(), True),
    StructField("sales_per_customer",DoubleType(), True),
    StructField("delivery_status",StringType(),True),
    StructField("late_delivery_risk",IntegerType(),True),
    StructField("category_id", IntegerType(), True),
    StructField("category_name",StringType(),True),
    StructField("customer_city",StringType(),True),
    StructField("customer_country",StringType(),True),
    StructField("customer_email",StringType(),True),
    StructField("customer_fname",StringType(),True),
    StructField("customer_id",IntegerType(),True),
    StructField("customer_lname",StringType(),True),
    StructField("customer_password",StringType(),True),
    StructField("customer_segment",StringType(),True),
    StructField("customer_state",StringType(),True),
    StructField("customer_street",StringType(),True),
    StructField("customer_zipcode",IntegerType(),True),
    StructField("department_id",IntegerType(),True),
    StructField("department_name",StringType(),True),
    StructField("latitude",DoubleType(),True),
    StructField("longitude",DoubleType(),True),
    StructField("market",StringType(),True),
    StructField("order_city",StringType(),True),
    StructField("order_country",StringType(),True),
    StructField("order_customer_id",IntegerType(),True),
    StructField("order_date_dateorders",StringType(),True),
    StructField("order_id",IntegerType(),True),
    StructField("order_item_cardprod_id",IntegerType(),True),
    StructField("order_item_discount",FloatType(),True),
    StructField("order_item_discount_rate",FloatType(),True),
    StructField("order_item_id",IntegerType(),True),
    StructField("order_item_product_price",FloatType(),True),
    StructField("order_item_profit_ratio",FloatType(),True),
    StructField("order_item_quantity",IntegerType(),True),
    StructField("sales",FloatType(),True),
    StructField("order_item_total",FloatType(),True),
    StructField("order_profit_per_order",FloatType(),True),
    StructField("order_region",StringType(),True),
    StructField("order_state",StringType(),True),
    StructField("order_status",StringType(),True),
    StructField("order_zipcode",IntegerType(),True),
    StructField("product_card_id",IntegerType(),True),
    StructField("product_category_id",IntegerType(),True),
    StructField("product_description",StringType(),True),
    StructField("product_image",StringType(),True),
    StructField("product_name",StringType(),True),
    StructField("product_price",FloatType(),True),
    StructField("product_status",IntegerType(),True),
    StructField("shipping_date_dateorders",StringType(),True),
    StructField("shipping_mode",StringType(),True),
    ])

from pyspark.sql.functions import col, to_timestamp, coalesce, when
import re

def cleanDF(
    df,
    date_cols=['order_date_dateorders', 'shipping_date_dateorders'],
    boolean_cols=['late_delivery_risk', 'product_status']
):
    """
    Clean and standardize a raw Spark DataFrame:
      - Normalize column names
      - Parse date formats into timestamps
      - Convert binary (0/1) columns into booleans
    
    Args:
        df (DataFrame): Input Spark DataFrame
        date_cols (list): List of date column names to parse
        boolean_cols (list): List of binary columns to normalize
    
    Returns:
        DataFrame: Cleaned Spark DataFrame
    """
    
    #Clean column names
    def normalize_column_name(name):
        name = re.sub(r'[\(\)\[\]\{\}]', '', name)  # remove brackets
        name = name.strip().lower().replace(' ', '_')
        return name

    df = df.toDF(*[normalize_column_name(c) for c in df.columns])

    #Clean date columns
    for col_name in date_cols:
        df = df.withColumn(
            f"cleaned_{col_name}",
            coalesce(
                to_timestamp(col(col_name), "M/d/yyyy H:mm"),
                to_timestamp(col(col_name), "M/d/yyyy HH:mm"),
                to_timestamp(col(col_name), "dd-MM-yyyy HH:mm")
            )
        )

    #Normalize boolean columns (0/1 → True/False)
    for col_name in boolean_cols:
        df = df.withColumn(
            f"cleaned_{col_name}",
            when(col(col_name) == 1, True).when(col(col_name) == 0, False).otherwise(None)
        )

    #Remove the orginal columns and replace them cleaned columns
    for col_name in date_cols + boolean_cols:
        df = df.drop(col_name).withColumnRenamed(f"cleaned_{col_name}", col_name)
    
    return df

@dlt.table(
    name="bronze_complete",
    comment="This is the complete bronze table"
    )
@dlt.expect_or_drop("valid_type", "type IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_item_id", "order_item_cardprod_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_date", "order_date_dateorders IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_card_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_status", "order_status IS NOT NULL")
@dlt.expect_or_drop("valid_shipping_mode", "shipping_mode IS NOT NULL")
@dlt.expect_or_drop("valid_delivery_status", "delivery_status IS NOT NULL")
@dlt.expect_or_drop("valid_market", "market IS NOT NULL")
@dlt.expect_or_drop("valid_region", "order_region IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "order_item_quantity IS NOT NULL AND order_item_quantity > 0")
@dlt.expect_or_drop("valid_price", "order_item_product_price IS NOT NULL AND order_item_product_price > 0")
def bronze_ingestion():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .schema(schema)\
        .option("header","true")\
        .option("schemaEvolutionMode", "rescue")\
        .load("/Volumes/ecomm_e2e/ecomm_raw/raw_volume/raw_data/")

    return cleanDF(df)
        
 




