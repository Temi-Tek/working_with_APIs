from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

# Spark on Windows Palava!!!!
# os.environ['HADOOP_HOME'] = "C:/hadoop/"
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
hadoop_home = "C:/hadoop"
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['PATH'] = os.path.join(hadoop_home, 'bin') + os.pathsep + os.environ['PATH']
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable # faahhh!!

spark = SparkSession.builder \
    .appName("XTD_Labs_Historical_Data_Processing") \
    .master("local") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Read All Bronce data (All JSON files at once)
raw_df = spark.read.json("./data/bronze/*.json", multiLine=True)
# Count the total number of 30-minute intervals (rows) in the raw data for auditing purposes 
data_count = raw_df.count()
print(f"Audit: Total 30-minute intervals found in Bronze: {data_count}")

# 2. SILVER LAYER i: Flattening the data points
# First, explode the 'data' array to get individual records
exploded_data = raw_df.select(F.explode(F.col("data")).alias("record"))

exploded_df = exploded_data.select(
    F.col("record.`from`").alias("timestamp"),
    F.explode(F.col("record.regions")).alias("region")
)

silver_df = exploded_df.select(
    "timestamp",
    F.col("region.regionid").alias("regionid"),
    F.col("region.shortname").alias("shortname"),
    F.col("region.dnoregion").alias("dno"),
    F.col("region.intensity.forecast").alias("intensity"),
    F.col("region.intensity.index").alias("index"),
    F.explode(F.col("region.generationmix")).alias("mix")
)

# 3. SILVER LAYER ii: 
silver_pivoted = silver_df.groupBy("regionid", "shortname", "dno", "timestamp", "intensity", "index") \
    .pivot("mix.fuel") \
    .agg(F.first("mix.perc"))

# 4. GOLD LAYER: Aggregate to Daily Averages
print('Starting Gold Layer Aggregation')
gold_df = silver_pivoted.withColumn("date_recorded", F.to_date("timestamp")) \
    .groupBy("regionid", "date_recorded") \
    .agg(
        F.first("shortname").alias("shortname"),
        F.first("dno").alias("dno"),
        F.round(F.mean("intensity"), 2).alias("intensity_avg"),
        F.mode("index").alias("index_mode"),
        F.round(F.mean("biomass"), 2).alias("fuel_biomass"),
        F.round(F.mean("coal"), 2).alias("fuel_coal"),
        F.round(F.mean("gas"), 2).alias("fuel_gas"),
        F.round(F.mean("hydro"), 2).alias("fuel_hydro"),
        F.round(F.mean("imports"), 2).alias("fuel_imports"),
        F.round(F.mean("nuclear"), 2).alias("fuel_nuclear"),
        F.round(F.mean("other"), 2).alias("fuel_other"),
        F.round(F.mean("solar"), 2).alias("fuel_solar"),
        F.round(F.mean("wind"), 2).alias("fuel_wind")
    ).orderBy("date_recorded", "regionid")

# Final audit count of records in Gold layer (should be 14 regions * number of days)
final_count = gold_df.count()
print(f"Audit: Final processed research records in Gold: {final_count}")

# Save
gold_df.write.csv("data/gold_carbon_historical.csv", header=True, mode="overwrite")

# See what happened
gold_df.show(10)