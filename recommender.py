from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, avg
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("Top Rated Products") \
    .getOrCreate()

file_path = "D:\\VJ\\UM\\WQD7007\\'Final Project'\\product_reviews.csv" # Replace with your HDFS path
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Convert Unix Timestamp to regular date
df = df.withColumn("Date", from_unixtime(col("Timestamp")))

# Ask the user for a date input
user_input_date = input("Enter the reference date in YYYY-MM-DD format: ")

# Convert the input string to a datetime object
try:
    reference_date = datetime.strptime(user_input_date, "%Y-%m-%d")
except ValueError:
    print("Invalid date format. Please enter the date in YYYY-MM-DD format.")
    spark.stop()
    exit()

# Calculate the date one month before the user-provided date
one_month_ago = (reference_date - timedelta(days=30)).strftime("%Y-%m-%d")

filtered_df = df.filter(col("Date") >= one_month_ago)

# Group by ProductId and calculate average rating
result_df = filtered_df.groupBy("ProductId").agg(avg("Rating").alias("AverageRating"))

# Sort by AverageRating and get top 10
top_products_df = result_df.orderBy(col("AverageRating").desc()).limit(10)

top_products_df.show()
