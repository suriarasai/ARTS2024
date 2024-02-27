from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

raw_df = spark.readStream \
        .format("csv") \
        .option("path", "booking") \
        .option("maxFilesPerTrigger", 1) \
        .option("inferSchema", "True") \
        .load()
#raw_df.printSchema()
bookingWriterQuery = raw_df.writeStream \
        .format("csv") \
        .queryName("Flattened Driver Writer") \
        .outputMode("append") \
        .option("path", "booking-output") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="10 seconds") \
        .start()
bookingWriterQuery.awaitTermination()



