# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest constructors (JSON) file
# MAGIC

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
# MAGIC ###DDL way of defining the schema

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
        .schema(constructors_schema) \
        .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructors_dropped = constructors_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

constructors_final_df = constructors_dropped.withColumnRenamed("constructorId", "constructor_id") \
                                       .withColumnRenamed("constructorRef", "constructor_ref") \
                                       .withColumn("ingestion_date" , current_timestamp()) \
                                       .withColumn("data_source", lit(v_data_source)) \
                                       .withColumn("file_date", lit(v_file_date))
                                    

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to sink (As Parquet)

# COMMAND ----------

#constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

#constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("sucess")
