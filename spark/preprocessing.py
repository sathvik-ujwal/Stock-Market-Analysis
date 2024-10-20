from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, last
from pyspark.sql import Window

spark = SparkSession.builder \
    .appName("Stock Data Preprocessing") \
    .getOrCreate()

df = spark.read.csv("hdfs:///stock_project/data/raw/historical_stock_data.csv", header=True, inferSchema=True)

print("Initial Data:")
df.show(5)

window_spec = Window.orderBy("Date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = df.withColumn("Open", last("Open", ignorenulls=True).over(window_spec)) \
       .withColumn("High", last("High", ignorenulls=True).over(window_spec)) \
       .withColumn("Low", last("Low", ignorenulls=True).over(window_spec)) \
       .withColumn("Close", last("Close", ignorenulls=True).over(window_spec)) \
       .withColumn("Adj Close", last("Adj Close", ignorenulls=True).over(window_spec)) \
       .withColumn("Volume", last("Volume", ignorenulls=True).over(window_spec))

df = df.dropDuplicates()

df = df.filter(col("Close") >= 0)

print("Cleaned Data:")
df.show(5)

df.write.csv("hdfs:///stock_project/data/processed/cleaned_stock_data.csv", header=True)

spark.stop()
