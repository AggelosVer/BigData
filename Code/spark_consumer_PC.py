from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Μπορειτε να χρησιμοποιειτε παρενθέσεις ( ... ) γύρω από μπλοκ κώδικα για να επιτρέψετε αλλαγή γραμμής (multi-line statement) χωρίς να χρειάζεται να χρησιμοποιείτε τον χαρακτήρα backslash \


# 1. Ορισμός του Schema
## vehicles_to_pandas returns a pd.DataFrame:
#             A DataFrame containing the travel logs of vehicles, with the columns:
#
#             - 'name': the name of the vehicle (platoon).
#             - 'dn': the platoon size.
#             - 'orig': the origin node of the vehicle's trip.
#             - 'dest': the destination node of the vehicle's trip.
#             - 't': the timestep.
#             - 'link': the link the vehicle is on (or relevant status).
#             - 'x': the position of the vehicle on the link.
#             - 's': the spacing of the vehicle.
#             - 'v': the speed of the vehicle.
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
# Χρησιμοποιούμε "cast" για να μετατρέψουμε τις raw binary τιμές σε αναγνώσιμο JSON string.
# Εφαρμόζουμε το schema στο string, δημιουργώντας ένα struct με την ονομασία data.
# Κάθε key του JSON (name, orig, dest, etc.) μετατρέπεται σε ξεχωριστή στήλη.
parsed = (
    df
    .select(col("value").cast("string").alias("json_str"))
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

# 5. Υπολογισμός Στατιστικών ανά Ακμή (link) και Χρόνο (t)
# t = time από την εξομοίωση, v = ταχύτητα οχήματος
# vcount: πλήθος οχημάτων, vspeed: μέση ταχύτητα
stats = (
    parsed
    .groupBy(
        col("t").alias("time"),
        col("link")
    )
    .agg(
        count("*").alias("vcount"),
        avg("v").alias("vspeed")
    )
)


# 6α. Αποθήκευση στη MongoDB των αρχικών δεδομενων
# outputMode("append"): κάθε νέο record γράφεται αμέσως στη MongoDB
query_raw = (
    parsed.writeStream
    .outputMode("append")
    .format("mongodb")
    .option("checkpointLocation", "/tmp/checkpoints/raw")
    .option("spark.mongodb.write.database", "uxsim_db")
    .option("spark.mongodb.write.collection", "raw_data")
    .start()
)

# 6β. Αποθήκευση στη MongoDB των επεξεργασμενων δεδομενων
# Το outputMode("complete") γράφει ολόκληρο το αποτέλεσμα κάθε φορά.
# Ο MongoDB Spark Connector δεν υποστηρίζει "update" mode για aggregations.
query_mongo = (
    stats.writeStream
    .outputMode("complete")
    .format("mongodb")
    .option("checkpointLocation", "/tmp/checkpoints/stats")
    .option("spark.mongodb.write.database", "uxsim_db")
    .option("spark.mongodb.write.collection", "processed_data")
    .start()
)

# 7. Προβολή στην κονσόλα για debugging (προαιρετικά)
query_console = (
    stats.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("numRows", 30)
    .trigger(processingTime="5 seconds")
    .start()
)


# Αναμονή για τον τερματισμό όλων των queries
spark.streams.awaitAnyTermination()
