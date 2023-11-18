# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Circuts file

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
# MAGIC ## Ingest circuts file as csv and apply schema

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                             StructField("circuitRef", StringType(), True),
                             StructField("name", StringType(), True),
                             StructField("location", StringType(), True),
                             StructField("country", StringType(), True),
                             StructField("lat", DoubleType(), True),
                             StructField("lng", DoubleType(), True),
                             StructField("alt", IntegerType(), True),
                             StructField("url", StringType(), True)])

circuts_df = spark.read \
    .option('header', True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Column transformation

# COMMAND ----------

from pyspark.sql.functions import col; 
selected_circuits = circuts_df.select(
    col("circuitid"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt")
    )

# COMMAND ----------

from pyspark.sql.functions import lit

renamed_circuits = selected_circuits.withColumnRenamed("circuitid", "circuit_id") \
                                      .withColumnRenamed("circuitRef", "circuit_ref") \
                                      .withColumnRenamed("lat", "latitude") \
                                      .withColumnRenamed("lng", "longitude") \
                                      .withColumnRenamed("alt", "altitude") \
                                      .withColumn("data_source", lit(v_data_source)) \
                                      .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

circuit_final_df = add_ingestion_date(renamed_circuits)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write transformed df to sink (As Parquet/ Delta)

# COMMAND ----------

#circuit_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

#circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")
circuit_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("sucess")
