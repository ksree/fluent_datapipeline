# Databricks notebook source
# DBTITLE 1,Create  customer dataset using Faker library  and write it as JSON files in Unity Catalog Volumes
# MAGIC %pip install faker
# MAGIC from faker import Faker
# MAGIC import pandas as pd
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# MAGIC
# MAGIC # Initialize Faker and Spark
# MAGIC fake = Faker()
# MAGIC spark = SparkSession.builder.appName("FakeDataGeneration").getOrCreate()
# MAGIC
# MAGIC # Generate fake data
# MAGIC data = {
# MAGIC     "customer_id": [i for i in range(1, 101)],  # 100 unique customer IDs
# MAGIC     "name": [fake.name() for _ in range(100)],
# MAGIC     "email": [fake.email() for _ in range(100)],
# MAGIC     "age": [fake.random_int(min=18, max=80) for _ in range(100)],
# MAGIC     "location": [f"{fake.city()}, {fake.state()}" for _ in range(100)]
# MAGIC }
# MAGIC
# MAGIC # Create a Pandas DataFrame
# MAGIC pdf = pd.DataFrame(data)
# MAGIC
# MAGIC # Define the schema for Spark DataFrame
# MAGIC schema = StructType([
# MAGIC     StructField("customer_id", IntegerType(), False),
# MAGIC     StructField("name", StringType(), False),
# MAGIC     StructField("email", StringType(), False),
# MAGIC     StructField("age", IntegerType(), False),
# MAGIC     StructField("location", StringType(), False)
# MAGIC ])
# MAGIC
# MAGIC # Convert Pandas DataFrame to Spark DataFrame
# MAGIC df = spark.createDataFrame(pdf, schema=schema)
# MAGIC
# MAGIC # Show sample data
# MAGIC df.write.mode("overwrite").json("/Volumes/my_workspace/default/customer_data_feeds")
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Sales Transactions dataset
from faker import Faker
import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# Initialize Faker
fake = Faker()

# Generate fake data
data = {
    "transaction_id": [i for i in range(1, 101)],  # 100 unique transaction IDs
    "customer_id": [fake.random_int(min=1, max=100) for _ in range(100)],
    "item_id": [fake.random_int(min=1, max=50) for _ in range(100)],
    "quantity": [fake.random_int(min=1, max=10) for _ in range(100)],
    "price": [round(fake.random_number(digits=2, fix_len=True) * 0.01, 2) for _ in range(100)]
}

# Create a Pandas DataFrame
pdf = pd.DataFrame(data)

# Define the schema for Spark DataFrame
schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("item_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", FloatType(), False)
])

# Convert Pandas DataFrame to Spark DataFrame
df = spark.createDataFrame(pdf, schema=schema)

df.write.mode("overwrite").json("/Volumes/my_workspace/default/sales_transactions")

