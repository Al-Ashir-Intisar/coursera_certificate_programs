-- Partitioned Tables in Google BigQuery
-- This file contains SQL queries and descriptions for creating and managing partitioned tables in BigQuery.
-- Author: [Your Name]

-- ####################################
-- Task 1: Create a New Dataset
-- ####################################
-- Create a dataset named 'ecommerce'.
CREATE SCHEMA ecommerce;

-- ####################################
-- Task 2: Creating Tables with Date Partitions
-- ####################################
-- Query non-partitioned table to view sample visitor data for 2017.
SELECT DISTINCT
    fullVisitorId,
    date,
    city,
    pageTitle
FROM `data-to-insights.ecommerce.all_sessions_raw`
WHERE date = '20170708'
LIMIT 5;

-- Query non-partitioned table for 2018 data to compare processing costs.
SELECT DISTINCT
    fullVisitorId,
    date,
    city,
    pageTitle
FROM `data-to-insights.ecommerce.all_sessions_raw`
WHERE date = '20180708'
LIMIT 5;

-- Create a partitioned table based on date.
CREATE OR REPLACE TABLE ecommerce.partition_by_day
PARTITION BY date_formatted
OPTIONS (
    description = "A table partitioned by date"
) AS
SELECT DISTINCT
    PARSE_DATE("%Y%m%d", date) AS date_formatted,
    fullVisitorId
FROM `data-to-insights.ecommerce.all_sessions_raw`;

-- ####################################
-- Task 3: View Data Processed with a Partitioned Table
-- ####################################
-- Query a specific partition for 2016-08-01.
SELECT *
FROM `data-to-insights.ecommerce.partition_by_day`
WHERE date_formatted = '2016-08-01';

-- Query a non-existent partition for 2018-07-08 to observe processing behavior.
SELECT *
FROM `data-to-insights.ecommerce.partition_by_day`
WHERE date_formatted = '2018-07-08';

-- ####################################
-- Task 4: Creating an Auto-Expiring Partitioned Table
-- ####################################
-- Create a partitioned table with a 60-day expiration.
CREATE OR REPLACE TABLE ecommerce.days_with_rain
PARTITION BY date
OPTIONS (
    partition_expiration_days = 60,
    description = "Weather stations with precipitation, partitioned by day"
) AS
SELECT
    DATE(CAST(year AS INT64), CAST(mo AS INT64), CAST(da AS INT64)) AS date,
    (SELECT ANY_VALUE(name)
     FROM `bigquery-public-data.noaa_gsod.stations` AS stations
     WHERE stations.usaf = stn) AS station_name,
    prcp
FROM `bigquery-public-data.noaa_gsod.gsod*`
WHERE prcp < 99.9 -- Filter unknown values
  AND prcp > 0    -- Filter stations/days with no precipitation
  AND _TABLE_SUFFIX >= '2021';

-- ####################################
-- Task 5: Querying the Partitioned Table
-- ####################################
-- Query average monthly precipitation for a specific station.
SELECT
    AVG(prcp) AS average,
    station_name,
    date,
    CURRENT_DATE() AS today,
    DATE_DIFF(CURRENT_DATE(), date, DAY) AS partition_age,
    EXTRACT(MONTH FROM date) AS month
FROM ecommerce.days_with_rain
WHERE station_name = 'WAKAYAMA' -- Japan
GROUP BY station_name, date, today, month, partition_age
ORDER BY date DESC; -- Most recent days first

-- Query to find the oldest partition age.
SELECT
    AVG(prcp) AS average,
    station_name,
    date,
    CURRENT_DATE() AS today,
    DATE_DIFF(CURRENT_DATE(), date, DAY) AS partition_age,
    EXTRACT(MONTH FROM date) AS month
FROM ecommerce.days_with_rain
WHERE station_name = 'WAKAYAMA' -- Japan
GROUP BY station_name, date, today, month, partition_age
ORDER BY partition_age DESC; -- Oldest partitions first
