# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest results file (Single line JSON)
# MAGIC ###Incremental load

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

#resultId":1,"raceId":18,"driverId":1,"constructorId":1,"number":22,"grid":1,"position":1,"positionText":1,"positionOrder":1,"points":10,"laps":58,"time":"1:34:50.616","milliseconds":5690616,"fastestLap":39,"rank":2,"fastestLapTime":"1:27.452","fastestLapSpeed":218.3,"statusId":1}

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,FloatType

results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
                                      StructField("driverId", IntegerType(),True),
                                      StructField("constructorId", IntegerType(),True),
                                      StructField("fastestLap", IntegerType(),True),
                                      StructField("fastestLapSpeed", FloatType(),True),
                                      StructField("fastestLapTime", StringType(),True),
                                      StructField("grid", IntegerType(),True),
                                      StructField("laps", IntegerType(),True),
                                      StructField("milliseconds", IntegerType(),True),
                                      StructField("number", IntegerType(),True),
                                      StructField("points", IntegerType(),True),
                                      StructField("position", IntegerType(),True),
                                      StructField("positionOrder", IntegerType(),True),
                                      StructField("positionText", StringType(),True),
                                      StructField("raceId", IntegerType(),True),
                                      StructField("rank", IntegerType(),True),
                                      StructField("statusId", IntegerType(),True),
                                      StructField("time", StringType(),True)
])

# COMMAND ----------

results_df = spark.read \
             .schema(results_schema) \
             .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rename column and add ingestion_date field 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("constructorId", "constructor_id") \
                               .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                               .withColumnRenamed("fastestLap", "fastest_lap") \
                               .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("positionOrder", "position_order") \
                               .withColumnRenamed("positionText", "position_text") \
                               .withColumn("ingestion_date", current_timestamp()) \
                               .withColumn("data_source", lit(v_data_source)) \
                               .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop unwanted column
# MAGIC

# COMMAND ----------

final_result_df = results_renamed_df.drop("statusId")

# COMMAND ----------

# MAGIC %md 
# MAGIC ####De-duplicate the dataframe  

# COMMAND ----------

final_duduped_df = final_result_df.dropDuplicates(['driver_id', 'race_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write result file to sink (Parquet and partitioned by race_id) 

# COMMAND ----------

#final_result_df.write \
#                  .mode("overwrite") \
#                  .partitionBy("race_id") \
#                  .parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Incremental Load - Method 1

# COMMAND ----------

# for race_id_list in final_result_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# final_result_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Incremental Load - Method 2 

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS f1_processed.results

# COMMAND ----------

##This conf need to be set inorder to enable over-write a partition rather than full over-write

#spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

##Spark expecting partitioned column as last column when 'insertInto' used so it requires a re-arrange 

# final_result_df = final_result_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text",
#                                          "position_order","points","laps","time","milliseconds", "fastest_lap","rank","fastest_lap_time",
#                                          "fastest_lap_speed", "data_source","file_date","ingestion_date","race_id")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     final_results_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     final_result_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Turn Method 2 as a function instead

# COMMAND ----------

#overwrite_partition(final_result_df,'f1_processed', 'results',  'race_id')  

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(final_duduped_df,'f1_processed', 'results', processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("sucess")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC --There are some duplicates which are handled by de-duplicate cell
# MAGIC
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC 
