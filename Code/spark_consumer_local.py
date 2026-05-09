from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# 1. Ορισμός Schema (ίδιο με τα πεδία που στέλνει ο UXSIM producer)
schema = (
    StructType()
    .add("name", StringType())
    .add("dn", IntegerType())
    .add("orig", StringType())
    .add("dest", StringType())
    .add("t", DoubleType())        # χρονική στιγμή εξομοίωσης
    .add("link", StringType())     # ακμή του δικτύου
    .add("x", DoubleType())
    .add("s", DoubleType())
    .add("v", DoubleType())        # ταχύτητα οχήματος
)

# 2. Δημιουργία Spark Session (local mode - τρέχει τοπικά χωρίς cluster)
spark = (
    SparkSession.builder
    .appName("UXSIM-Spark-Consumer")
    .master("local[*]")   # χρησιμοποιεί όλους τους διαθέσιμους πυρήνες τοπικά
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # Μειώνει το "θόρυβο" στα logs
print("Spark Session έτοιμη!")

# 3. Σύνδεση στον Redpanda ως Kafka Source (readStream)
# Χρησιμοποιούμε το port 19092 που είναι ο external listener του Redpanda
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("subscribe", "vehicle_positions")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Μετατροπή binary value -> string -> JSON με το schema
parsed = (
    raw_df
    .select(col("value").cast("string").alias("json_str"))  # binary -> string
    .select(from_json(col("json_str"), schema).alias("data"))  # string -> struct
    .select("data.*")  # ανάπτυγμα όλων των πεδίων σε ξεχωριστές στήλες
)

# 5. Υπολογισμός Στατιστικών ανά Ακμή (link) και Χρόνο (t)
# groupBy: για κάθε συνδυασμό χρόνου (t) και ακμής (link)
# count("*")  -> vcount: αριθμός οχημάτων
# avg("v")    -> vspeed: μέση ταχύτητα
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

# 6. Εγγραφή αποτελεσμάτων στην κονσόλα (console sink) για debugging
# outputMode("update") -> εκτυπώνει μόνο τις γραμμές που άλλαξαν σε κάθε micro-batch
query = (
    stats.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)    # να φαίνονται ολόκληρες οι τιμές
    .option("numRows", 30)        # εμφάνιση έως 30 γραμμών ανά batch
    .trigger(processingTime="5 seconds")  # επεξεργασία κάθε 5 δευτερόλεπτα
    .start()
)

print("Spark Streaming Consumer ξεκίνησε. Αναμονή μηνυμάτων...")
print("Πατήστε Ctrl+C για τερματισμό.\n")
query.awaitTermination()
