from pyspark.sql.types import *
from pyspark.sql.functions import *





spark.conf.set(
  "fs.azure.account.key.<<storage account>>.dfs.core.windows.net",
  "<<key >>"
)

gold_path = "abfss://<<container>>@<<storage account>>.dfs.core.windows.net/gold"
silver_path = "abfss://<<container>>@<<storage account>>.dfs.core.windows.net/silver"




silver_df = (
                spark.readStream
                .format('delta')
                .load(silver_path)
                .select('order_id', 'timestamp', 'customer_id', 'product_id', 'category', 'price', 'quantity', 'total_amount', 'city', 'state', 'country', 'latitude', 'longitude', 'delivery_status')
              
)


#Aggregation: Total Sales and total items sold per state per min
# total_sales =(
#                 silver_df
#                 .withWatermark("timestamp", "1 minutes")
#                 .groupBy(
#                     window("timestamp", "1 minutes"),
#                     "state"
#                     )
#                 .agg(
#                     sum("total_amount").alias("total_sales"),
#                     sum("quantity").alias("total_items_sold")
#                     )
#                 .select(
#                     col("window.start").alias("start"),
#                     col("window.end").alias("end"),
#                     col("state"),
#                     col("total_sales"),
#                     col("total_items_sold")
#                 )

# )


silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", gold_path+"/_checkpoint") \
    .start(gold_path)