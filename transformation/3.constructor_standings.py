# Databricks notebook source
# MAGIC %md
# MAGIC ##Produce constructor standings 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read.format("delta") \
                   .load(f"{presentation_folder_path}/race_results") \
                    .filter(f"file_date = '{v_file_date}'") \
                    .select("race_year") \
                    .distinct() \
                    .collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)
        

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read \
                .format("delta") \
                .load(f"{presentation_folder_path}/race_results") \
                .filter(col("race_year").isin(race_year_list)) 

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col 

constructor_standing_df = race_results_df \
        .groupBy("race_year", "team") \
        .agg(sum("points").alias("total_points"), \
             count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window 
from pyspark.sql.functions import col, rank, desc, asc 

constructor_standing_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

final_df = constructor_standing_df.withColumn("rank", rank().over(constructor_standing_spec))

# COMMAND ----------

#final_df.write \
#        .mode("overwrite") \
#        .parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_presentation.constructor_standings;

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

#overwrite_partition(final_df,'f1_presentation', 'constructor_standings', 'race_year')

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year" 
merge_delta_data(final_df,'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition,'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings;
