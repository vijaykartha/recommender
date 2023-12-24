from pyspark.sql.functions import to_date, from_unixtime, avg, col, lower
from pyspark.sql.functions import to_date, from_unixtime, avg, col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import rank
from pyspark.sql.functions import to_date, min, max
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, avg
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("Top Rated Products") \
    .getOrCreate()

file_path = "hdfs:///user/hive/warehouse/vijay.db/product_reviews/product_reviews.csv"  # Replace with your HDFS path
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Convert Unix Timestamp to regular date
df = df.withColumn("Date", from_unixtime(col("Timestamp")))

# Ask the user for a date input
search_term = input("Enter a search term for the product type: ").lower()
#input_date = input("Enter the reference date in YYYY-MM-DD format: ")

user_input_date = input("Enter the reference date in YYYY-MM-DD format: ")

# Convert to lowercase for case-insensitive comparison
df = df.withColumn("ProductTypeLower", lower(col("ProductType")))

# Filter DataFrame based on search term
matching_types = df.filter(col("ProductTypeLower").contains(search_term)).select("ProductType").distinct()

# Assuming you take the first match for simplicity
closest_product_type = matching_types.first()["ProductType"] if matching_types.count() > 0 else None

if closest_product_type is None:
    print("No matching product type found.")
    spark.stop()
    exit()


# Convert the input string to a datetime object
try:
    reference_date = datetime.strptime(user_input_date, "%Y-%m-%d")
except ValueError:
    print("Invalid date format. Please enter the date in YYYY-MM-DD format.")
    spark.stop()
    exit()
# Calculate the date one month before the user-provided date
one_month_ago = (reference_date - timedelta(days=30)).strftime("%Y-%m-%d")

# Assuming 'Timestamp' is the column with Unix timestamp values
df_with_dates = df.withColumn("ConvertedDate", to_date(from_unixtime("Timestamp")))


# Filter based on the user-provided date
filtered_df = df_with_dates.filter((col("ProductType") == closest_product_type) & (col("ConvertedDate") >= one_month_ago))

result_df = filtered_df.groupBy("ProductType", "ProductId", "ConvertedDate")\
    .agg(avg("Rating").alias("AverageRating"))

# Define a window spec partitioned by ProductType and ordered by AverageRating
windowSpec = Window.partitionBy("ProductType").orderBy(col("AverageRating").desc())

# Apply the window spec to assign a row number
result_df_with_row_number = result_df.withColumn("row_number", row_number().over(windowSpec))

top_10_products_per_type_df = result_df_with_row_number.filter(col("row_number") <= 10)
top_10_products_per_type_df.select("ProductType", "ProductId", "ConvertedDate", "AverageRating").show()

