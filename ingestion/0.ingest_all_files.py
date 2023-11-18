# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest all files from RAW container 

# COMMAND ----------

return_status = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})


# COMMAND ----------

return_status

# COMMAND ----------

return_status = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

return_status 

# COMMAND ----------

return_status = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

return_status 

# COMMAND ----------

return_status = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

return_status 

# COMMAND ----------

return_status = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

return_status 

# COMMAND ----------

return_status = dbutils.notebook.run("6.ingest_pitstops_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

return_status 

# COMMAND ----------

return_status = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

return_status 

# COMMAND ----------

return_status = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

return_status
