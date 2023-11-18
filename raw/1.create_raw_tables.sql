-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Create managed raw database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create circuite table 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuites
( 
  circuiteId INT, 
  circuitRef STRING, 
  name STRING,
  location STRING, 
  country STRING, 
  lat DOUBLE, 
  lng DOUBLE,
  alt INT,
  url STRING
)
USING CSV
OPTIONS(path "/mnt/formula1/raw/circuites.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create races table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races
(
  raceId INT,
  year INT,
  round INT,
  circuitId INT, 
  name STRING,
  date DATE, 
  time STRING, 
  url STRING
)
USING CSV
OPTIONS(path "/mnt/formula1/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create constructors table
-- MAGIC ####Single Line JSON
-- MAGIC ####Simple structure 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
USING JSON
OPTIONS(path "/mnt/formula1/raw/constructors.json", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create drivers table
-- MAGIC ####Single Line JSON
-- MAGIC ####Complex structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers
(
  driverId INT,
  driverRef STRING, 
  code STRING, 
  name STRUCT<forname: STRING, surname:STRING>,
  dob DATE,
  nationality STRING, 
  number INT,
  url STRING
)
USING JSON
OPTIONS(path "/mnt/formula1/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create results table
-- MAGIC ####Single Line JSON
-- MAGIC ####Simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results
(
  resultId INT,
  driverId INT,
  constructorId INT, 
  fastestLap INT, 
  fastestLapSpeed FLOAT,
  fastestLapTime STRING, 
  grid INT, 
  laps INT, 
  milliseconds INT, 
  number INT, 
  points INT, 
  position INT, 
  positionOrder INT, 
  positionText STRING, 
  raceId INT, 
  rank INT, 
  statusId INT, 
  time STRING
)
USING JSON
OPTIONS(path "/mnt/formula1/raw/results.json", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create pit_stops table
-- MAGIC ####Multi Line JSON
-- MAGIC ####Simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pit_shops
(
    driverId INT, 
    duration STRING, 
    lap INT, 
    milliseconds INT, 
    raceId INT, 
    stop STRING,
    time STRING
) 
USING JSON
OPTIONS(path "/mnt/formula1/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create lap_times table
-- MAGIC ####Multiple files

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_times
(
    raceId INT, 
    driverId INT, 
    lap INT, 
    position INT, 
    milliseconds INT, 
    time STRING
) 
USING CSV
OPTIONS(path "/mnt/formula1/raw/lap_times", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create qualifying table
-- MAGIC ####Multiline Json
-- MAGIC ####Multiple files

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
    constructorId INT, 
    driverId INT,
    number INT, 
    position INT, 
    q1 STRING, 
    q2 STRING, 
    q3 STRING, 
    qualifyId INT,  
    raceId INT
) 
USING JSON
OPTIONS(path "/mnt/formula1/raw/qualifying", multiLine true)
