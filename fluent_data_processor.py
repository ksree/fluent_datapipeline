# Databricks notebook source
confluentClusterName = "fluent_inc_demo"
confluentBootstrapServers = "pkc-12576z.us-west2.gcp.confluent.cloud:9092"
confluentApiKey =  "GFJZXKL632TBU3QL"
confluentSecret =  "BqCDZiojRE+orfG1xYWT8BDtHrDPJV0VEQsDjnTA+lnydwq5u8YDNunXIUcEtzog"
schema_registry_address = "https://psrc-12dd33.us-west2.gcp.confluent.cloud"
schema_registry_options = {
    "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
    "confluent.schema.registry.basic.auth.user.info": f"PCK3OYKB2Z534O7Z:WDipWAc9ogZehPIQDpv8/62vylKPfHrYZq2E7pUyyr8fYAw9IE+NNWE2X8W00tfu",
    "avroSchemaEvolutionMode": "restart",
    "mode": "FAILFAST",
}


# COMMAND ----------

import dlt
from pyspark.sql.avro.functions import *
from pyspark.sql.functions import col
@dlt.table
def sensor_bronze():
  return (
    spark.readStream
      .format("kafka")
      .option("subscribe", "sensor_topic")
      .option("startingOffsets", "latest")
      .option("kafka.bootstrap.servers", confluentBootstrapServers)
      .option("kafka.security.protocol", "SASL_SSL")
      .option(
        "kafka.sasl.jaas.config",
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
            confluentApiKey, confluentSecret
        ),
     )
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
        .select(
        "topic",
        "partition",
        "offset",
        "timestamp",
        from_avro(
            data=col("value"),
            options=schema_registry_options,
            subject="sensor_topic-value",
            schemaRegistryAddress=schema_registry_address,
        ).alias("value"),
    )
    .select("topic", "partition", "offset", "timestamp", "value.*")
  )

# COMMAND ----------

@dlt.table
def clickstream_bronze():
  return (
    spark.readStream
      .format("kafka")
      .option("subscribe", "clickstreams_topic")
      .option("startingOffsets", "latest")
      .option("kafka.bootstrap.servers", confluentBootstrapServers)
      .option("kafka.security.protocol", "SASL_SSL")
      .option(
        "kafka.sasl.jaas.config",
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
            confluentApiKey, confluentSecret
        ),
     )
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()       
    .select(
        "topic",
        "partition",
        "offset",
        "timestamp",
        from_avro(
            data=col("value"),
            options=schema_registry_options,
            subject="clickstreams_topic-value",
            schemaRegistryAddress=schema_registry_address,
        ).alias("value"),
    )
    .select("topic", "partition", "offset", "timestamp", "value.*")
  )

# COMMAND ----------

# DBTITLE 1,Ingest customer data and create a streaming table
@dlt.table
def customer_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load("/Volumes/my_workspace/default/customer_data_feeds")
  )


# COMMAND ----------

# DBTITLE 1,Ingest sales transaction data and create a streaming table
@dlt.table
def sales_txn_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load("/Volumes/my_workspace/default/sales_transactions")
  )


# COMMAND ----------

# DBTITLE 1,Clean sensor table and create a new sensor silver table
from pyspark.sql.functions import *

@dlt.table(
  comment="The cleaned sensor table",
  table_properties={
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("device_id", "device_id IS NOT NULL")
@dlt.expect_or_drop("customer_id", "customer_id IS NOT NULL")
def sensor_clean():
  df = dlt.read_stream("sensor_bronze")
  return df

# COMMAND ----------

# DBTITLE 1,Clean the clickstream table and create a new silver table
from pyspark.sql.functions import *

@dlt.table(
  comment="The cleaned clickstream table",
  table_properties={
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("customer_id", "customer_id IS NOT NULL")
def clickstream_silver():
  df = dlt.read_stream("clickstream_bronze")
  return df

# COMMAND ----------

# DBTITLE 1,Clean the customer table and create a new silver table
from pyspark.sql.functions import *

@dlt.table(
  comment="The cleaned customer table",
)
@dlt.expect_or_drop("customer_id", "customer_id IS NOT NULL")
def customer_silver():
  df = dlt.read_stream("customer_bronze")
  return df

# COMMAND ----------

# DBTITLE 1,Clean sales txn table and create a new sales txn silver layer
from pyspark.sql.functions import *

@dlt.table(
  comment="The cleaned customer table",
)
@dlt.expect_or_drop("customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("transaction_id", "customer_id IS NOT NULL")
def sales_txn_silver():
  df = dlt.read_stream("sales_txn_bronze")
  return df

# COMMAND ----------

# DBTITLE 1,sensor hourly aggregate table for DS
from pyspark.sql.functions import *

@dlt.table(
  comment="The cleaned sensor hourly table",
  table_properties={
    "pipelines.autoOptimize.managed": "true"
  }
)
def sensor_hourly():
  df = dlt.read("sensor_clean")
  df = df.withColumn("hour", hour("timestamp"))
  df = df.groupBy("hour").agg(stddev_pop("value").alias("stddev_pop_value"))
  return df


# COMMAND ----------

# DBTITLE 1,Refer aggregation table for dashboards
from pyspark.sql.functions import *

@dlt.table(
  comment="Landing page aggregator: refer aggregator ",
  table_properties={
    "pipelines.autoOptimize.managed": "true"
  }
)
def referer_agg():
  df = dlt.read("clickstream_silver")
  df = df.groupBy("referrer").agg(count("event_id").alias("event_count"))
  return df

# COMMAND ----------

from pyspark.sql.functions import *

@dlt.table(
  comment="Joined customer and sales transactions table",
  table_properties={
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_sales():
  customer_df = dlt.read("customer_silver").drop("_rescued_data")
  sales_df = dlt.read("sales_txn_silver").drop("_rescued_data")
  joined_df = customer_df.join(sales_df, "customer_id", "inner")
  return joined_df
