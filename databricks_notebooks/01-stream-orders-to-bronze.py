from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

schema = StructType([
  StructField("order_id", StringType(), True),
  StructField("timestamp", StringType(), True),
  StructField("customer_id", StringType(), True),
  StructField("product_id", StringType(), True),
  StructField("category", StringType(), True),
  StructField("price", DoubleType(), True),
  StructField("quantity", IntegerType(), True),
  StructField("total_amount", DoubleType(), True),
  StructField("city", StringType(), True),
  StructField("state", StringType(), True),
  StructField("country", StringType(), True),
  StructField("latitude", StringType(), True),
  StructField("longitude", StringType(), True),
  StructField("delivery_status", StringType(), True)
]) 


# Azure Event Hub Configuration
event_hub_namespace = "<<host name>>"
event_hub_name = "<<event name >>"
event_hub_conn_str = "<<connection string >>"

eh_conf = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

df = spark.readStream.format("kafka").options(**eh_conf).load()


df_orders = (
    df.selectExpr("Cast(value AS STRING) as json")
    .select(from_json("json",schema).alias("data"))
    .select("data.*")
)


spark.conf.set(
  "fs.azure.account.key.<<storage account >>.dfs.core.windows.net",
  "<<key >>"
)

bronze_path = "abfss://<<container>>@<<storage account >>.dfs.core.windows.net/bronze"


df_orders.writeStream \
    .format('delta') \
    .outputMode("append") \
    .option("checkpointLocation",bronze_path+"/_checkpoint") \
    .start(bronze_path)