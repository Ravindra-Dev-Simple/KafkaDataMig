# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("KafkaReadExample") \
#     .getOrCreate()

# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092") \
#     .option("subscribe", "finacle-  ") \
#     .option("startingOffsets", "latest") \
#     .load()

# # Convert binary to string
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# query = df.writeStream \
#     .format("console") \
#     .start()

# query.awaitTermination()



# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("test").getOrCreate()

# df = spark.read.csv("D:/Datamig/raw-zone", header=True)
# df.show()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
print("Spark started")
spark.stop()