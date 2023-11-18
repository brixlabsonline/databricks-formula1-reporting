# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Drivers (nested JASON) file using reader API 

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
# MAGIC ##Rename and add new columns
# MAGIC ###1. Rename driverId, driverRef 
# MAGIC ###2. Add new ingest date column
# MAGIC ###3. Name added concatinate forname and surname 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                   StructField("surname", StringType(), True)
                                  ])

drivers_schema = StructType(fields = [StructField("code", StringType(), True),
                                      StructField("dob", DateType(), True),
                                      StructField("driverId", IntegerType(),True),
                                      StructField("driverRef", StringType(),True),
                                      StructField("name", name_schema),
                                      StructField("nationality", StringType(), True),
                                      StructField("number", IntegerType(),True),
                                      StructField("url", StringType(),True)
                                    ])


# COMMAND ----------

drivers_df = spark.read \
                  .schema(drivers_schema) \
                  .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp

drivers_column_changed = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("driverRef", "driver_ref") \
                                   .withColumn("ingestion_date", current_timestamp()) \
                                   .withColumn("name", concat(col('name.forename'), lit(" "), col('name.surname'))) \
                                   .withColumn("data_source", lit(v_data_source)) \
                                   .withColumn("file_date", lit(v_file_date))
                            

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop unwanted columns
# MAGIC ###1. name.fornamame & name.surname
# MAGIC ###2. url

# COMMAND ----------

final_drivers_df = drivers_column_changed.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write output to to processes container in parquet format 

# COMMAND ----------

#final_drivers_df.write \
  #              .mode("overwrite") \
    #            .parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

#final_drivers_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

final_drivers_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("sucess")
