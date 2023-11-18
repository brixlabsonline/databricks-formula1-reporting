# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest pit_stops file (Multiline Json)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructType,StructField


                                      
pitstops_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                       StructField("duration", StringType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("milliseconds", IntegerType(), True),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("stop", StringType(), True),
                                       StructField("time", StringType(), True)
                                       ])

# COMMAND ----------

pit_stops_df = spark.read \
             .schema(pitstops_schema) \
             .option("multiline", True) \
             .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

pitstops_final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#pitstops_final_df.write \
#                 .mode("overwrite") \
#                 .parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

#pitstops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Incremental Load

# COMMAND ----------

#overwrite_partition(pitstops_final_df,'f1_processed', 'pit_stops', 'race_id')

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id" # all 3 makes primary key 
merge_delta_data(pitstops_final_df,'f1_processed', 'pit_stops', processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("sucess")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops; 
