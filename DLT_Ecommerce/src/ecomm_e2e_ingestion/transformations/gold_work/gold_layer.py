import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dlt.table(
    name="orders_enriched",
    comment="Complete enriched orders with customer and product dimensions for reusable analytics.This table is the foundation for ALL gold layer analytics."
)
def orders_enriched():    
    # Read silver orders (full table)
    orders = spark.read.table("silver_orders")
    
    # Read current customers (SCD Type 2 - current records only)
    current_customers = (
        spark.read.table("dimSilverCustomers")
        .filter(col("__END_AT").isNull())
        .withColumnRenamed("__START_AT", "customer_valid_from")
        .select(
            col("customer_id"),
            col("customer_firstname"),
            col("customer_lastname"),
            col("customer_segment"),
            col("customer_zipcode"),
        )
    )
    
    # Read current products (SCD Type 2 - current records only)
    current_products = spark.read.table("dimSilverProducts").filter(
        col("__END_AT").isNull()
    ).select(
        col("product_id"),
        col("product_name"),
        col("category_id"),
        col("category_name"),
        col("is_notavailable")
    )
    
    # Join orders with customers
    orders_with_customers = orders.alias("o").join(
        current_customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"  # Keep orders even if customer not found
    ).select(
        col("o.*"),  # All order fields
        col("c.customer_firstname"),
        col("c.customer_lastname"),
        col("c.customer_segment"),
        col("c.customer_zipcode")
    )
    
    # Join with products
    orders_fully_enriched = orders_with_customers.alias("o").join(
        current_products.alias("p"),
        col("o.product_id") == col("p.product_id"),
        "left"  # Keep orders even if product not found
    ).select(
        col("o.*"),  # All order + customer fields
        col("p.product_name"),
        col("p.category_id"),
        col("p.category_name"),
        col("p.is_notavailable")
    ).withColumn(
        "enriched_at",
        current_timestamp()
    )
    return orders_fully_enriched


#Fundamental Analysis of Customers

@dlt.table(
    name="customers360",
    comment="Complete customer 360 with RFM"
)
def customers360():
    """
    Perform Customer Segmentation and RFM Analysis
    
    """

   
    
    orders = dlt.read("orders_enriched")

    latest_order_date = orders.agg(max("order_date").alias("max_order_date")).collect()[0]["max_order_date"]
    REFERENCE_DATE = add_months(lit(latest_order_date), 1)
    
    # Step 1: Aggregate customer metrics
    customer_agg = orders.groupBy(
        "customer_id", "customer_firstname", "customer_lastname",
        "customer_segment", "customer_zipcode"
    ).agg(
        count("order_id").alias("total_orders"),
        sum("line_total").alias("total_revenue"),
        sum("profit_per_order").alias("total_profit"),
        max('order_date').alias("latest_order_date"),
        min('order_date').alias("first_order_date")
    )
    
    # Get all customers (including those without orders)
    all_customers = orders.select(
        "customer_id", "customer_firstname", "customer_lastname",
        "customer_segment", "customer_zipcode"
    ).distinct()
    
    # Join to include customers with 0 orders
    customer_with_metrics = all_customers.alias("c").join(
        customer_agg.alias("m"),
        "customer_id",
        "left"
    ).select(
        col("c.*"),
        coalesce(col("m.total_orders"), lit(0)).alias("total_orders"),
        coalesce(col("m.total_revenue"), lit(0.0)).alias("total_revenue"),
        coalesce(col("m.total_profit"), lit(0.0)).alias("total_profit"),
        col("m.latest_order_date"),
        col("m.first_order_date")
    ).withColumn(
        "days_since_last_order",
        when(col("latest_order_date").isNotNull(),
             datediff(REFERENCE_DATE, col("latest_order_date"))
        ).otherwise(lit(999999))  # High value for customers without orders
    ).withColumn(
        "avg_order_value",
        when(col("total_orders") > 0, col("total_revenue") / col("total_orders"))
        .otherwise(lit(0.0))
    )
    
    recency_window = Window.orderBy(col("days_since_last_order").asc())  # Lower is better
    frequency_window = Window.orderBy(col("total_orders").desc())        # Higher is better
    monetary_window = Window.orderBy(col("total_revenue").desc())        # Higher is better
    
    customer_with_quintiles = customer_with_metrics.withColumn(
        "recency_quintile_raw",
        ntile(5).over(recency_window)
    ).withColumn(
        "frequency_quintile",
        ntile(5).over(frequency_window)
    ).withColumn(
        "monetary_quintile",
        ntile(5).over(monetary_window)
    )
    customer_rfm = customer_with_quintiles.withColumn(
        "recency_score",
        when(col("total_orders") == 0, lit(1))  # Prospects get score 1
        .otherwise(lit(6) - col("recency_quintile_raw"))  # Invert: quintile 1 = score 5
    ).withColumn(
        "frequency_score",
        when(col("total_orders") == 0, lit(1))
        .otherwise(lit(6)-col("frequency_quintile"))
    ).withColumn(
        "monetary_score",
        when(col("total_revenue") == 0, lit(1))
        .otherwise(lit(6)-col("monetary_quintile"))
    ).withColumn(
        "rfm_score",
        concat(
            col('recency_score').cast("string"),
            col('frequency_score').cast("string"),
            col('monetary_score').cast("string")
        )
    )

    customer_360_final = customer_rfm.withColumn(
        "rfm_segment",
        when(
            (col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4),
            "Champions"
        )
        .when(
            (col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3),
            "Loyal Customers"
        )
        .when(
            (col("recency_score") >= 4) & (col("frequency_score").between(2, 3)),
            "Potential Loyalists"
        )
        .when(
            (col("recency_score") >= 4) & (col("frequency_score") <= 1),
            "New Customers"
        )
        .when(
            (col("recency_score") <= 2) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3),
            "At Risk"
        )
        .when(
            (col("recency_score") <= 2) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4),
            "Cannot Lose Them"
        )
        .when(
            (col("recency_score") <= 2) & (col("frequency_score") >= 2),
            "Hibernating"
        )
        .when(
            (col("recency_score") <= 1) & (col("total_orders") > 0),
            "Lost Customers"
        )
        .when(
            col("total_orders") == 0,
            "Prospects"
        )
        .otherwise("Other")
    ).withColumn("processed_at", current_timestamp())
    
    return customer_360_final.select(
        "customer_id",
        "customer_firstname",
        "customer_lastname",
        "customer_segment",
        "customer_zipcode",
        "total_orders",
        "total_revenue",
        "total_profit",
        "avg_order_value",
        "latest_order_date",
        "first_order_date",
        "days_since_last_order",
        "recency_score",
        "frequency_score",
        "monetary_score",
        "rfm_score",
        "rfm_segment",
        "processed_at"
    )

#Fundamental Analysis of Products 
@dlt.table(
    comment="Product-level performance metrics with segmentation",
    name="products360"
)
def products_360():
   
    orders = dlt.read("orders_enriched")
    latest_order_date = orders.agg(max("order_date").alias("max_order_date")).collect()[0]["max_order_date"]
    REFERENCE_DATE = add_months(lit(latest_order_date), 1)
    product_agg = orders.groupBy(
        "product_id",
        "product_name",
        "category_name",
        "product_price",
        "is_notavailable"
    ).agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("units_sold"),
        sum("line_total").alias("total_revenue"),
        sum("profit_per_order").alias("total_profit"),
        avg("line_total").alias("avg_order_value"),
        avg("profit_per_order").alias("avg_profit_per_order"),
        avg("discount_rate").alias("avg_discount_rate"),
        avg("profit_ratio").alias("avg_profit_ratio"),
        avg("product_price").alias("avg_price_charged"),
        countDistinct("customer_id").alias("unique_customers"),
        min("order_date").alias("first_order_date"),
        max("order_date").alias("last_order_date"),
        count(when(col("customer_segment") == "Consumer", 1)).alias("consumer_orders"),
        count(when(col("customer_segment") == "Corporate", 1)).alias("corporate_orders"),
        count(when(col("customer_segment") == "Home Office", 1)).alias("home_office_orders"),
        count(when(col("order_status") == "COMPLETE", 1)).alias("completed_orders"),
        count(when(col("order_status").isin("CANCELED", "CLOSED"), 1)).alias("canceled_orders"),
        count(when(col("order_status") == "SUSPECTED_FRAUD", 1)).alias("fraud_orders"),
        count(when(col("order_status").isin("PENDING", "PROCESSING", "ON_HOLD", "PAYMENT_REVIEW"), 1)).alias("pending_orders"),
        avg(datediff(col("shipping_date"), col("order_date"))).alias("avg_order_cycle_days"),
        avg("actual_shipping_days").alias("avg_shipping_days"),
        sum(when(col('late_delivery_risk') == True, 1).otherwise(0)).alias('late_deliveries')
    )

    product_metrics = product_agg.withColumn(
        'order_completion_rate',
        when(col('total_orders') > 0, col('completed_orders') / col('total_orders')).otherwise(0)
    ).withColumn(
        'order_cancellation_rate',
        when(col('total_orders') > 0, col('canceled_orders') / col('total_orders')).otherwise(0)
    ).withColumn(
        'order_fraud_rate',
        when(col('total_orders') > 0, col('fraud_orders') / col('total_orders')).otherwise(0)
    ).withColumn(
        'order_pending_rate',
        when(col('total_orders') > 0, col('pending_orders') / col('total_orders')).otherwise(0)
    ).withColumn(
        'days_in_catalog',
        datediff(col('last_order_date'), col('first_order_date'))
    )

    revenue_window = Window.orderBy(col("total_revenue").desc())
    margin_window = Window.orderBy(col("avg_profit_per_order").desc())

    product_360_percentiles = product_metrics.withColumn(
        'revenue_quintile', ntile(5).over(revenue_window)
    ).withColumn(
        'margin_quartile', ntile(4).over(margin_window)
    )

    product_360_metrics = product_360_percentiles.withColumn(
        'revenue_score',
        when(col('total_orders') == 0, lit(1)).otherwise(lit(6) - col('revenue_quintile'))
    ).withColumn(
        'margin_score',
        when(col('total_orders') == 0, lit(1)).otherwise(lit(5) - col('margin_quartile'))
    )

    product_360_final = product_360_metrics.withColumn(
        "performance_tier",
        when(col("revenue_score") >= 4, lit("Star Products"))
        .when(col("revenue_score") >= 3, lit("Strong Performers"))
        .when(col("revenue_score") >= 2, lit("Steady Performers"))
        .when(col("revenue_score") >= 1, lit("Weak Performers"))
        .otherwise(lit("Slow Movers"))
    ).withColumn(
        "margin_category",
        when(col("margin_score") >= 3, lit("High Margin"))
        .when(col("margin_score") >= 2, lit("Medium Margin"))
        .when(col("margin_score") >= 1, lit("Low Margin"))
        .otherwise(lit("Very Low Margin"))
    ).withColumn(
        "product_health",
        when(
            (col("performance_tier").isin("Star Products", "Strong Performers")) &
            (col("order_completion_rate") >= 0.9) &
            (col("order_cancellation_rate") < 0.05),
            "Healthy"
        ).when(
            (col("order_cancellation_rate") > 0.10) | (col("order_fraud_rate") > 0.05) | (col("order_completion_rate") < 0.8),
            "At Risk"
        ).otherwise("Monitor")
    ).withColumn(
        "strategic_priority",
        when(
            (col("performance_tier") == "Star Products") & (col("margin_category") == "High Margin"),
            "Invest & Promote"
        ).when(
            (col("order_cancellation_rate") > 0.15) | (col("order_fraud_rate") > 0.10),
            "Investigate Quality Issues"
        ).when(
            (col("performance_tier") == "Slow Movers") & (col("order_cancellation_rate") > 0.10),
            "Consider Discontinuing"
        ).otherwise("Maintain")
    ).withColumn(
        "processed_at", current_timestamp()
    )

    return product_360_final    


                                                            
                         






