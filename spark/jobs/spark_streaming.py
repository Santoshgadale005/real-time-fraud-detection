from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FraudDetectionStreaming")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")   # <-- changed
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .load()
)

transactions = df.selectExpr("CAST(value AS STRING) AS value")

query = (
    transactions.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

print("=" * 60)
print("Spark Streaming Started Successfully")
print("Listening to Kafka Topic: transactions")
print("=" * 60)

query.awaitTermination()