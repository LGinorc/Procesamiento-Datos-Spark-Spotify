from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

spark = SparkSession.builder.appName("SpotifyRealTime").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("genre", StringType()),
    StructField("listen_duration", FloatType()),
    StructField("timestamp", IntegerType())
])

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "spotify_streams").load()

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

windowed_stats = parsed_df.groupBy(window(col("timestamp"), "1 minute"), "genre").agg({"listen_duration": "avg", "user_id": "count"})

query = windowed_stats.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
