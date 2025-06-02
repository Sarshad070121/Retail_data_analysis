# Importing Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import types
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import when, col
from pyspark.sql.functions import from_json, window, sum, avg, count
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, ArrayType


# Starting Spark Session
spark = SparkSession \
    .builder \
    .appName('RetailBuyingStudy') \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# checking orders
is_a_order_col = functions.when(functions.col("type") == "ORDER", 1).otherwise(0)

# Checking return
is_a_return_col = functions.when(functions.col("type") == "RETURN", 1).otherwise(0)

#Total items
def total_no_items(items):
    if items is None:
        return 0
    
    item_cnt = 0
    for item in items:
        item_cnt += item['quantity']
    return item_cnt

#Total cost
def total_cost(items, order_type):
    if items is None:
        return 0

    total_cost = 0
    for item in items:
        item_price = item['quantity'] * item['unit_price']
        total_cost += item_price

    if order_type == 'RETURN':
        return -total_cost
    else:
        return total_cost

#UDF's

add_total_item_count = udf(total_no_items, IntegerType())
add_total_cost = udf(total_cost, FloatType())

# Data reading from kafka
data_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","18.211.252.152:9092") \
    .option("subscribe","real-time-project") \
    .option("startingOffsets", "latest")  \
    .load()

#Schema
file_schema = StructType() \
    .add("invoice_no", LongType()) \
    .add("country",StringType()) \
    .add("timestamp", TimestampType()) \
    .add("type", StringType()) \
    .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType()) 
        ])))

final_stream = data_stream.select(from_json(col("value").cast("string"), file_schema).alias("data")).select("data.*")

order_stream = final_stream \
   .withColumn("total_cost", add_total_cost(final_stream.items,final_stream.type)) \
   .withColumn("total_items", add_total_item_count(final_stream.items)) \
   .withColumn("is_order", when(col("type") == "ORDER", 1).otherwise(0)) \
   .withColumn( "is_return", when(col("type") == "RETURN", 1).otherwise(0))

# Raw transaction output stream (console sink)
raw_transactions_output = order_stream \
    .select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("path", "/Console_output") \
    .option("checkpointLocation", "/Console_output_checkpoints") \
    .trigger(processingTime="1 minute") \
    .start()

    # KPI Aggregation by time window
kpi_by_time_window = order_stream \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute")) \
    .agg(
        sum("total_cost").alias("total_volume_of_sales"),
        avg("total_cost").alias("average_transaction_size"),
        count("invoice_no").alias("OPM"),
        avg("is_return").alias("rate_of_return")
    ) \
    .select("window.start", "window.end", "OPM", "total_volume_of_sales", "average_transaction_size", "rate_of_return")

# KPI Aggregation by time window and country
kpi_by_time_and_country = order_stream \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute"), "country") \
    .agg(
        sum("total_cost").alias("total_volume_of_sales"),
        count("invoice_no").alias("OPM"),
        avg("is_return").alias("rate_of_return")
    ) \
    .select("window.start", "window.end", "country", "OPM", "total_volume_of_sales", "rate_of_return")

# Writing KPIs by time window to JSON
kpi_time_output = kpi_by_time_window.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "timeKPIvalue") \
    .option("checkpointLocation", "timeKPIvalue_checkpoints") \
    .trigger(processingTime="1 minute") \
    .start()

# Writing KPIs by time and country to JSON
kpi_time_country_output = kpi_by_time_and_country.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time_countryKPIvalue") \
    .option("checkpointLocation", "time_countryKPIvalue_checkpoints") \
    .trigger(processingTime="1 minute") \
    .start()

# Termination
raw_transactions_output.awaitTermination()
kpi_time_output.awaitTermination()
kpi_time_country_output.awaitTermination()


# Run using: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 streaming.py