# Databricks notebook source
# Databricks notebook source
# Gold layer: dim_customers (grain: ONE ROW PER CUSTOMER, delivered orders only)
# Definitions:
#   - "returning" = purchased on a LATER day than the first purchase day
#   - returned_within_180d is defined only for customers observable for 180 days (else NULL)
#   - the data end date comes from the data, not from a hard-coded value

from pyspark.sql import functions as F
from pyspark.sql.window import Window

RETURN_WINDOW_DAYS = 180

orders = (spark.read.table("olist_project.gold.fact_orders")
          .filter(F.col("is_delivered") == 1))

end_date = orders.agg(F.max("purchase_date")).first()[0]
print("Last delivered purchase date in the data:", end_date)

# First order of every customer (earliest purchase; order_id breaks ties)
w_first = Window.partitionBy("customer_unique_id").orderBy("purchase_ts", "order_id")
first_order = (orders
    .withColumn("rn", F.row_number().over(w_first))
    .filter(F.col("rn") == 1)
    .select("customer_unique_id",
            F.col("order_id").alias("first_order_id"),
            F.col("purchase_date").alias("first_purchase_date"),
            F.col("is_late").alias("first_order_is_late"),
            F.col("delay_days").alias("first_order_delay_days"),
            F.col("review_score").alias("first_order_review_score")))

# First purchase on a LATER day than the first purchase day
later = (orders
    .join(first_order.select("customer_unique_id", "first_purchase_date"), "customer_unique_id")
    .filter(F.col("purchase_date") > F.col("first_purchase_date"))
    .groupBy("customer_unique_id")
    .agg(F.min("purchase_date").alias("second_purchase_date")))

totals = (orders.groupBy("customer_unique_id")
    .agg(F.countDistinct("order_id").alias("total_orders"),
         F.countDistinct("purchase_date").alias("purchase_days"),
         F.max("purchase_date").alias("last_purchase_date"),
         F.sum("items_value").alias("total_spent")))

dim_customers = (totals
    .join(first_order, "customer_unique_id")
    .join(later, "customer_unique_id", "left")
    .withColumn("customer_segment",
        F.when(F.col("purchase_days") > 1, "Returning Customer").otherwise("One-Time Buyer"))
    .withColumn("days_to_second_purchase",
        F.datediff("second_purchase_date", "first_purchase_date"))
    .withColumn("is_observable_180d",
        F.when(F.col("first_purchase_date") <= F.date_sub(F.lit(end_date), RETURN_WINDOW_DAYS), 1)
         .otherwise(0))
    .withColumn("returned_within_180d",
        F.when(F.col("is_observable_180d") == 0, F.lit(None).cast("int"))
         .when(F.col("days_to_second_purchase") <= RETURN_WINDOW_DAYS, 1)
         .otherwise(0)))

(dim_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_project.gold.dim_customers"))

print("gold.dim_customers written, customers:", dim_customers.count())