# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest lap_times file (Multiple CSV files)

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


                                      
lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", StringType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("milliseconds", IntegerType(), True),
                                       StructField("time", StringType(), True)
                                       ])

# COMMAND ----------

lap_times_df = spark.read \
            .schema(lap_times_schema) \
            .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

lap_times_final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#lap_times_final_df.write \
#                 .mode("overwrite") \
#                 .parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

#lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Incremental Load

# COMMAND ----------

#overwrite_partition(lap_times_final_df,'f1_processed', 'lap_times', 'race_id')

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id" # all 3 makes primary key 
merge_delta_data(lap_times_final_df,'f1_processed', 'lap_times', processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("sucess")
