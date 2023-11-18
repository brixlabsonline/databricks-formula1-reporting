-- Databricks notebook source
-- MAGIC %md
-- MAGIC Managed table to be created under the location specified with Create database statement, not default location

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl/processed"

-- COMMAND ----------


