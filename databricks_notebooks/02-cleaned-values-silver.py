from pyspark.sql.types import *
from pyspark.sql.functions import *

spark.conf.set(
  "fs.azure.account.key.<<storage account>>.dfs.core.windows.net",
  "<<key >>"
)

bronze_path = "abfss://<<container>>@<<storage account>>.dfs.core.windows.net/bronze"
silver_path = "abfss://<<container>>@<<storage account>>.dfs.core.windows.net/silver"


bronze_df = (
                spark.readStream
                .format('delta')
                .load(bronze_path)
)

from pyspark.sql.functions import col, when, to_timestamp

df_clean = (
    bronze_df
    .dropDuplicates(["order_id"])
    .filter(col("state") != "null")
)

df_clean.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", silver_path+"/_checkpoint") \
    .start(silver_path)  # Ensure 'silver_path' points to a Delta table or directory with a transaction log
    