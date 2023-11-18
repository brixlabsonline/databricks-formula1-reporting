# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest races (CSV) file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read races files

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType, DateType

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                             StructField("year", IntegerType(), True),
                             StructField("round", IntegerType(), True),
                             StructField("circuitId", IntegerType(), True),
                             StructField("name", StringType(), True),
                             StructField("date", DateType(), True),
                             StructField("time", StringType(), True),
                             StructField("url", StringType(), True)])

# COMMAND ----------

races_df = spark.read \
            .option("header", "true") \
            .schema(races_schema) \
            .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import col

selected_races = races_df.select (
                col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time")
                )

# COMMAND ----------

renamed_races = selected_races.withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("year", "race_year") \
        .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

column_added_races = renamed_races.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                  .withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("data_source", lit(v_data_source)) \
                                  .withColumn("file_date", lit(v_file_date))
                                

# COMMAND ----------

final_races_df = column_added_races.drop("date") \
                                   .drop("time")

                        

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write race file to sink (As Parquet)

# COMMAND ----------

#final_races_df.write.mode("overwrite").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

#final_races_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

final_races_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")


# COMMAND ----------

dbutils.notebook.exit("sucess")
