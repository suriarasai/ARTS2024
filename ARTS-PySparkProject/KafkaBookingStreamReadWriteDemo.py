from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()
kafka_df = spark \
         .readStream \
         .format("kafka") \
         .option("kafka.bootstrap.servers", "localhost:9092") \
         .option("subscribe", "booking") \
         .option("startingOffsets", "earliest") \
         .load()
schema = StructType([
        StructField("BookingID", IntegerType()),
        StructField("MessageSubmittedTime", LongType()),
        StructField("MessageReceivedTime", LongType()),
        StructField("CustomerID", IntegerType()),
        StructField("CustomerName", StringType()),
        StructField("PhoneNumber", IntegerType()),
        StructField("PickUpLocation", StringType()),
        StructField("PickupPostcode", IntegerType()),
        StructField("PickUpTime", StringType()),
        StructField("DropLocation", StringType()),
        StructField("DropPostcode", IntegerType()),
        StructField("TaxiType", StringType()),
        StructField("FareType", StringType()),
        StructField("Fare", StringType())
    ])
value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
explode_df = value_df.selectExpr("value." + "BookingID", \
                    "value." + "MessageSubmittedTime", \
                    "value." + "MessageReceivedTime", \
                    "value." + "CustomerID", \
                    "value." + "CustomerName", \
                    "value." + "PhoneNumber", \
                    "value." + "PickUpLocation", \
                    "value." + "PickupPostcode", \
                    "value." + "PickUpTime", \
                    "value." + "DropLocation", \
                    "value." + "DropPostcode", \
                    "value." + "TaxiType", \
                    "value." + "FareType", \
                    "value." + "Fare")
writer_query = explode_df.writeStream \
        .format("json") \
        .queryName("Flattened Booking Writer") \
        .outputMode("append") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()
writer_query.awaitTermination()
