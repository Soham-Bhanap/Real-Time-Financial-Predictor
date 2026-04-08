import yfinance as yf
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, lead, when
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

print("[PHASE 0] Initializing Spark for Offline Training...")
# Initialize a local Spark session (using all available CPU cores)
spark = SparkSession.builder \
    .appName("OfflineModelTraining") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Step 1: Ingest Historical Data (2 Years of TSLA)
print("[PHASE 0] Downloading historical TSLA data...")
df_pandas = yf.download("TSLA", period="2y", interval="1d")
df_pandas.reset_index(inplace=True)
# Keep only necessary columns and rename for PySpark compatibility
df_pandas = df_pandas[['Date', 'Close', 'Volume']].dropna()
df_pandas.columns = ['Date', 'Close', 'Volume']

# Convert Pandas DataFrame to PySpark DataFrame
df = spark.createDataFrame(df_pandas)

# Step 2: Feature Engineering (Quantitative Indicators)
print("[PHASE 0] Engineering financial features...")
# Create a window to calculate rolling metrics over the past 5 days
windowSpec5 = Window.orderBy("Date").rowsBetween(-4, 0)

# Calculate 5-Day Moving Average and 5-Day Volatility (Standard Deviation)
df_features = df.withColumn("MA_5", avg(col("Close")).over(windowSpec5)) \
                .withColumn("Volatility_5", stddev(col("Close")).over(windowSpec5))

# Create the Target Variable: Will tomorrow's price be higher than today's?
# 1.0 = BUY (Price goes up), 0.0 = SELL/HOLD (Price goes down)
windowSpecLead = Window.orderBy("Date")
df_features = df_features.withColumn("Next_Day_Close", lead(col("Close"), 1).over(windowSpecLead))
df_features = df_features.withColumn("Target_Action", 
                                     when(col("Next_Day_Close") > col("Close"), 1.0).otherwise(0.0))

# Drop nulls created by rolling windows and lead functions
df_clean = df_features.dropna()

# Step 3: Machine Learning Pipeline
print("[PHASE 0] Assembling Vector and Training Random Forest Model...")
# Combine our engineered features into a single vector column
assembler = VectorAssembler(
    inputCols=["Close", "Volume", "MA_5", "Volatility_5"],
    outputCol="features"
)

# Initialize the Random Forest Classifier
rf = RandomForestClassifier(featuresCol="features", labelCol="Target_Action", numTrees=50)

# Create and run the pipeline
pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(df_clean)

# Step 4: Serialization (Save the Brain)
model_path = "rf_financial_model"
print(f"[PHASE 0] Training Complete. Saving model to local disk: {model_path}")
model.write().overwrite().save(model_path)

print("[PHASE 0] Execution Terminated Successfully.")