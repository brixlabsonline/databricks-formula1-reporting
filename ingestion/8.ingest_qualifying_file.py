# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest qualifying file (Multiple Multiline Json files)

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


                                      
qualifying_schema = StructType(fields = [StructField("constructorId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True),
                                       StructField("qualifyId", IntegerType(), True),
                                       StructField("raceId", IntegerType(), True)
                                       ])

# COMMAND ----------

qualifying_df = spark.read \
            .option("multiline", True) \
            .schema(qualifying_schema) \
            .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

qualifying_final_df = qualifying_df.withColumnRenamed("constructorId", "constructor_id") \
                                .withColumnRenamed("driverID", "driver_id") \
                                .withColumnRenamed("qualifyid", "qualify_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#qualifying_final_df.write \
#                 .mode("overwrite") \
#                 .parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

#qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

#overwrite_partition(qualifying_final_df,'f1_processed', 'qualifying', 'race_id')

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df,'f1_processed', 'qualifying', processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("sucess")
