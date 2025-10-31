from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import dlt 


@dlt.table(
    comment = 'this is transformed customers',
    name = 'silver_customers'
)
def silver_customers():
    df = spark.readStream.table('bronze_complete')
    customers = df.select(col("customer_id"),
                          col("customer_fname").alias('customer_firstname'),
                          col("customer_lname").alias('customer_lastname'),
                          col("customer_segment"),
                          col("customer_email").alias('email_masked'),
                          col("customer_password").alias("password_masked"),
                          col("customer_zipcode")
                          ).withColumn('transformed_at', current_timestamp())\

    return customers

@dlt.table(
    comment = 'this is transformed products',
    name = 'silver_products'
)
def silver_products():
    df = spark.readStream.table('bronze_complete')
    products = df.select(col("product_card_id").alias("product_id"),
                          col("category_id"),
                          col("category_name"),
                          col("product_name"),
                          col('product_price'),
                          col('product_status').alias('is_notavailable'),
                          ).withColumn('transformed_at', current_timestamp())\

    return products

@dlt.table(
    comment = 'this is transformed orders',
    name = 'silver_orders'
)
def silver_orders():
    df = spark.readStream.table('bronze_complete')
    orders = df.select(col("order_id"),
                          col("order_customer_id").alias('customer_id'),
                          col("order_item_cardprod_id").alias('product_id'),
                          col("order_item_id").alias("item_id"),
                          col("order_date_dateorders").alias('order_date'),
                          col("order_item_product_price").alias("product_price"),
                          col("order_item_quantity").alias("quantity"),
                          col("order_item_total").alias("line_total"),
                          col("sales"),
                          col("sales_per_customer"),
                          col('order_item_discount').alias("discount"),
                          col("order_item_discount_rate").alias("discount_rate"),
                          col("order_item_profit_ratio").alias('profit_ratio'),
                          col('order_profit_per_order').alias("profit_per_order"),
                          col('type').alias('transaction_method'),
                          col('order_status'),
                          #store_columns
                          col('department_id').alias('store_dept_id'),
                          col('department_name').alias('store_dept_name'),
                          col('customer_city').alias('store_city'),
                          col('customer_state').alias('store_state'),
                          col('customer_country').alias('store_country'),
                          col('customer_street').alias('store_street'),
                          col('latitude').alias('store_latitude'),
                          col('longitude').alias('store_longitude'),
                          #delivery_columns
                          col('market').alias('delivery_market'),
                          col("order_city").alias("delivery_city"),
                          col("order_state").alias("delivery_state"),
                          col("order_country").alias("delivery_country"),
                          col("order_region").alias("delivery_region"),
                          col('days_for_shipping_real').alias('actual_shipping_days'),
                          col('days_for_shipment_scheduled').alias('scheduled_shipping_days'),
                          col('delivery_status'),
                          col('late_delivery_risk'),
                          col('shipping_mode'),
                          col('shipping_date_dateorders').alias('shipping_date'),
                          ).withColumn('processed_at', current_timestamp())
    return orders   

#Implementation of SCD Type 2
dlt.create_streaming_table(
    comment="Silver table for customers",
    name="dimSilverCustomers"
)
dlt.create_auto_cdc_flow(
    target="dimSilverCustomers",
    source="silver_customers",
    keys=["customer_id"],
    sequence_by=col('transformed_at'),
    stored_as_scd_type=2,
    track_history_column_list=["customer_segment","customer_zipcode"]
)

dlt.create_streaming_table(
    comment="Silver table for products",
    name="dimSilverProducts"
)
dlt.create_auto_cdc_flow(
    target="dimSilverProducts",
    source="silver_products",
    keys=["product_id"],
    sequence_by=col('transformed_at'),
    stored_as_scd_type=2,
    track_history_column_list=["is_notavailable"]
)




