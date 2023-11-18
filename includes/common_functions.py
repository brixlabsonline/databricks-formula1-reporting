# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df 

# COMMAND ----------

#new_re_arranged_df = re_arrange_column(final_df, race_id)

def re_arrange_column(input_df, partition_col):
    new_col_list = []
    for col in input_df.schema.names:
        if col != partition_col:
            new_col_list.append(col)
    new_col_list.append(partition_col)
    return(input_df.select(new_col_list))

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_col):
    re_arranged_df = re_arrange_column(input_df, partition_col)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        re_arranged_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        re_arranged_df.write.mode("overwrite").partitionBy(partition_col).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition,partition_col):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true") #this is for merge condition to use partition column as well
    from delta.tables import DeltaTable
    
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
              input_df.alias("src"), 
              merge_condition)\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_col).format("delta").saveAsTable(f"{db_name}.{table_name}")
