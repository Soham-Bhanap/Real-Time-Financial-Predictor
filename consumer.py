from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, when
from pyspark.ml import PipelineModel

MASTER_IP = "100.110.200.107"

spark = SparkSession.builder \
    .appName("RealTimeFinancialPredictor") \
    .master(f"spark://{MASTER_IP}:7077") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("[CONSUMER] Loading Random Forest Model...")
# Load the brain you created in Phase 0
model = PipelineModel.load("rf_financial_model")

print("[CONSUMER] Connecting to Live Stream...")
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", MASTER_IP) \
    .option("port", 9999) \
    .load()

# Parse the incoming string based on our new Data Contract
# Note: We alias the price to "Close" to match the training data exactly
parsed_stream = raw_stream.select(
    split(col("value"), ",").getItem(0).alias("Timestamp"),
    split(col("value"), ",").getItem(1).alias("Ticker"),
    split(col("value"), ",").getItem(2).cast("float").alias("Close"),
    split(col("value"), ",").getItem(3).cast("float").alias("Volume"),
    split(col("value"), ",").getItem(4).cast("float").alias("MA_5"),
    split(col("value"), ",").getItem(5).cast("float").alias("Volatility_5")
)

# Apply the Machine Learning Model
predictions = model.transform(parsed_stream)

# Format the output for the console. 
# The model outputs a column called "prediction" (1.0 = BUY, 0.0 = HOLD/SELL)
final_output = predictions.withColumn(
    "Action", 
    when(col("prediction") == 1.0, "BUY").otherwise("HOLD/SELL")
).select("Timestamp", "Ticker", "Close", "MA_5", "Action")

# Push to Terminal
query = final_output.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()