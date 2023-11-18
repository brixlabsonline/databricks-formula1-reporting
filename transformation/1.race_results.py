# Databricks notebook source
# MAGIC %md 
# MAGIC ## Produce Race results dataset

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Race Results

# COMMAND ----------

#races.race_year, races.race_name, races.race_date

#circuites.circuit_location

#drivers.driver_name
#drivers.driver_number
#drivers.driver_nationality 

#constructors.team

#results.grid
#results.fastest_lap
#results.race_time
#results.points

#current_timestamp --> created_date 


# COMMAND ----------

from pyspark.sql.functions import col

#Read from a file 
#circuits_df = spark.read \
#                   .parquet(f"{processed_folder_path}/circuits/") \
#                   .select (col("circuit_id").alias("circuit_circuit_id"), col("location").alias("circuit_location"))

#Read from delta file 
circuits_df = spark.read \
                    .format("delta") \
                    .load(f"{processed_folder_path}/circuits/") \
                    .select (col("circuit_id").alias("circuit_circuit_id"), col("location").alias("circuit_location"))

# COMMAND ----------

races_df = spark.read \
                    .format("delta") \
                    .load(f"{processed_folder_path}/races") \
                    .select(col("circuit_id").alias("races_circuit_id"),col("race_id").alias("races_race_id"),col("name").alias("race_name"),"race_year",col("race_timestamp").alias("race_date"))

# COMMAND ----------

drivers_df = spark.read \
                    .format("delta") \
                    .load(f"{processed_folder_path}/drivers") \
                    .select(col("driver_id").alias("drivers_driver_id"),col("name").alias("driver_name"),col("number").alias("driver_number"),col("nationality").alias("driver_nationality"))

# COMMAND ----------

constructors_df = spark.read \
                    .format("delta") \
                    .load(f"{processed_folder_path}/constructors") \
                    .select(col("name").alias("team"), col("constructor_id").alias("constructors_constructor_id"))

                    #team? 

# COMMAND ----------

results_df = spark.read \
                    .format("delta") \
                    .load(f"{processed_folder_path}/results") \
                    .select(col("result_id").alias("results_result_id"),col("driver_id").alias("results_driver_id"),col("constructor_id").alias("results_constructor_id"),"grid","fastest_lap","points",col("time").alias("race_time"),col("race_id").alias("results_race_id"), col("position"), col("file_date")) \
                    .filter(f"file_date = '{v_file_date}'")
                    

# COMMAND ----------

join_out_1 = results_df.join(races_df, results_df.results_race_id == races_df.races_race_id, "inner")

# COMMAND ----------

join_out_2 = join_out_1.join(drivers_df, join_out_1.results_driver_id == drivers_df.drivers_driver_id, "inner")

# COMMAND ----------

join_out_3 = join_out_2.join(constructors_df, join_out_2.results_constructor_id == constructors_df.constructors_constructor_id, "inner")

# COMMAND ----------

join_out_final = join_out_3.join(circuits_df, join_out_3.races_circuit_id == circuits_df.circuit_circuit_id, "inner")

# COMMAND ----------

date_added_df = add_ingestion_date(join_out_final)

# COMMAND ----------

create_date_added = date_added_df.withColumnRenamed("ingestion_date", "created_date")

# COMMAND ----------

final_df = create_date_added.select(col("race_year"), col("race_name"), col("race_date"), \
           col("circuit_location"), col("driver_name"), col("driver_number"), col("team"), col("driver_nationality"), \
           col("grid"), col("fastest_lap"), col("race_time"), col("points"), col("position"), col("created_date"), col("results_race_id").alias("race_id"), col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write Output to Presentation container in parquet format

# COMMAND ----------

#final_df.write \
#        .mode("overwrite") \
#        .parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# overwrite_partition(final_df,'f1_presentation', 'race_results', 'race_id')

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id" # first one can be ideal for primary key
merge_delta_data(final_df,'f1_presentation', 'race_results', presentation_folder_path, merge_condition,'race_id')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC; 
