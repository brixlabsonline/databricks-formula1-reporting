-- Databricks notebook source
REFRESH TABLE f1_presentation.calculated_race_results;

-- COMMAND ----------

select * from f1_presentation.calculated_race_results;

-- COMMAND ----------

SELECT driver_name,
       count(1) AS total_races,
       SUM(calculated_points) AS total_points, 
       AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC; 
