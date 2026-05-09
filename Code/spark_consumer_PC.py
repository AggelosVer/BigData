from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, max, min
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# 1. Ορισμός του Schema
schema = StructType() \
    .add("name", StringType()) \
    .add("dn", IntegerType()) \
    .add("orig", StringType()) \
    .add("dest", StringType()) \
    .add("t", DoubleType()) \
    .add("link", StringType()) \
    .add("x", DoubleType()) \
    .add("s", DoubleType()) \
    .add("v", DoubleType())


# 2. Δημιουργία Spark Session
spark = (
    SparkSession.builder
    .appName("UXSIM-Consumer")
    .master("spark://spark-master:7077")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# 3. Σύνδεση στον Redpanda (Kafka-compatible)
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "vehicle_positions")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parsing του JSON και Μετασχηματισμός
parsed = (
    df
    .select(col("value").cast("string").alias("json_str"))
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

# 5. Υπολογισμός Στατιστικών ανά Ακμή (link) και Χρόνο (t)
# BONUS: Προσθήκη max/min speed ανά link
stats = (
    parsed
    .groupBy(
        col("t").alias("time"),
        col("link")
    )
    .agg(
        count("*").alias("vcount"),
        avg("v").alias("vspeed"),
        max("v").alias("vmax"),    # BONUS: μέγιστη ταχύτητα
        min("v").alias("vmin")     # BONUS: ελάχιστη ταχύτητα
    )
)

# BONUS: Windowed aggregations - μέσες τιμές ανά 30s παράθυρο
# Δημιουργούμε bucket χρόνου: κάθε 30 δευτερόλεπτα εξομοίωσης = 1 παράθυρο
# π.χ. t=5,10,15,20,25 → window_start=0 | t=30,35,...,55 → window_start=30
windowed_stats = (
    parsed
    .withColumn("window_start", ((col("t") / 30).cast("integer") * 30).cast("double"))
    .groupBy("window_start", "link")
    .agg(
        count("*").alias("vcount"),
        avg("v").alias("avg_speed"),
        max("v").alias("max_speed"),
        min("v").alias("min_speed")
    )
)


# ── Helper: MongoDB write function ──────────────────────────────────────────
def make_mongo_writer(database, collection):
    """Επιστρέφει foreachBatch function που γράφει στη MongoDB."""
    def write_to_mongo(batch_df, batch_id):
        try:
            (batch_df.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
                .option("spark.mongodb.write.database", database)
                .option("spark.mongodb.write.collection", collection)
                .mode("append")
                .save())
            print(f"[{collection}] Batch {batch_id} -> MongoDB OK")
        except Exception as e:
            print(f"[{collection}] Batch {batch_id}: ERROR -> {e}")
    return write_to_mongo


# 6α. Αποθήκευση raw δεδομένων → traffic.raw_data
query_raw = (
    parsed.writeStream
    .outputMode("append")
    .foreachBatch(make_mongo_writer("traffic", "raw_data"))
    .option("checkpointLocation", "/tmp/checkpoints/raw")
    .trigger(processingTime="5 seconds")
    .start()
)

# 6β. Αποθήκευση επεξεργασμένων δεδομένων → traffic.stats
query_mongo = (
    stats.writeStream
    .outputMode("update")
    .foreachBatch(make_mongo_writer("traffic", "stats"))
    .option("checkpointLocation", "/tmp/checkpoints/stats")
    .trigger(processingTime="5 seconds")
    .start()
)

# BONUS: Αποθήκευση windowed aggregations → traffic.windowed_stats
query_windowed = (
    windowed_stats.writeStream
    .outputMode("update")
    .foreachBatch(make_mongo_writer("traffic", "windowed_stats"))
    .option("checkpointLocation", "/tmp/checkpoints/windowed")
    .trigger(processingTime="10 seconds")
    .start()
)

# 7. Προβολή stats στην κονσόλα για debugging
query_console = (
    stats.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("numRows", 20)
    .trigger(processingTime="5 seconds")
    .start()
)


# Αναμονή για τον τερματισμό όλων των queries
spark.streams.awaitAnyTermination()
