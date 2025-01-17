-- **********************************************************************
-- SQL Guide: Working with JSON and Array Data in BigQuery
-- Description: This file contains SQL queries and best practices 
--              based on the "Working with JSON and Array data in BigQuery 2.5" lab.
-- Author: [Your Name]
-- Created: [Date]
-- GitHub: [Your Repository Link]
-- **********************************************************************

-- **********************************************************************
-- Task 1: Create a New Dataset
-- Create a dataset called `fruit_store` to store tables.
-- **********************************************************************
CREATE SCHEMA `fruit_store`;

-- **********************************************************************
-- Task 2: Practice Working with Arrays
-- Use an array to store multiple fruit values in a single row.
-- **********************************************************************
-- Example 1: Creating an array
SELECT ['raspberry', 'blackberry', 'strawberry', 'cherry'] AS fruit_array;

-- Example 2: Error Example (Array with multiple data types)
-- The following query demonstrates the need for a common data type in arrays.
-- Uncomment to test:
-- SELECT ['raspberry', 'blackberry', 1234567] AS fruit_array;

-- Example 3: Querying an array from a public dataset
SELECT person, fruit_array, total_cost 
FROM `data-to-insights.advanced.fruit_store`;

-- **********************************************************************
-- Task 3: Creating Arrays with ARRAY_AGG()
-- Use ARRAY_AGG() to create arrays from existing rows.
-- **********************************************************************
-- Example: Aggregate product names and page titles into arrays
SELECT
  fullVisitorId,
  date,
  ARRAY_AGG(v2ProductName) AS products_viewed,
  ARRAY_AGG(pageTitle) AS pages_viewed
FROM `data-to-insights.ecommerce.all_sessions`
WHERE visitId = 1501570398
GROUP BY fullVisitorId, date
ORDER BY date;

-- Example: Count elements in an array using ARRAY_LENGTH()
SELECT
  fullVisitorId,
  date,
  ARRAY_LENGTH(ARRAY_AGG(v2ProductName)) AS num_products_viewed,
  ARRAY_LENGTH(ARRAY_AGG(pageTitle)) AS num_pages_viewed
FROM `data-to-insights.ecommerce.all_sessions`
WHERE visitId = 1501570398
GROUP BY fullVisitorId, date;

-- **********************************************************************
-- Task 4: Querying Datasets with Arrays
-- Unnest arrays into rows using UNNEST().
-- **********************************************************************
-- Example: Query repeated fields from Google Analytics sample dataset
SELECT DISTINCT
  visitId,
  h.page.pageTitle
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
UNNEST(hits) AS h
WHERE visitId = 1501570398;

-- **********************************************************************
-- Task 5: Introduction to STRUCTs
-- Use STRUCTs to group related fields together.
-- **********************************************************************
-- Example 1: Creating a STRUCT
SELECT STRUCT("Rudisha" AS name, [23.4, 26.3, 26.4, 26.1] AS splits) AS runner;

-- Example 2: Query STRUCT fields
SELECT race, totals.*, device.*
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
WHERE visitId = 1501570398
LIMIT 10;

-- **********************************************************************
-- Task 6: Practice with STRUCTs and Arrays
-- Create a dataset and table for racing results.
-- **********************************************************************
-- Create dataset and table for racing data
CREATE SCHEMA `racing`;

CREATE TABLE `racing.race_results` (
  race STRING,
  participants ARRAY<STRUCT<
    name STRING,
    splits ARRAY<FLOAT64>
  >>
);

-- Ingest JSON data (replace with your GCS JSON path if needed)
-- Example path: cloud-training/data-insights-course/labs/optimizing-for-performance/race_results.json

-- Query: Unnest STRUCTs and Arrays
SELECT
  race,
  p.name,
  p.splits
FROM `racing.race_results`,
UNNEST(participants) AS p;

-- Example: Find the fastest lap
SELECT
  p.name,
  split_time
FROM `racing.race_results`,
UNNEST(participants) AS p,
UNNEST(p.splits) AS split_time
WHERE split_time = 23.2;

-- Example: Count the total racers
SELECT COUNT(p.name) AS racer_count
FROM `racing.race_results`, 
UNNEST(participants) AS p;

-- **********************************************************************
-- Recap and Notes
-- BigQuery supports Arrays and Structs natively:
-- - Use ARRAY_AGG() to create arrays from rows.
-- - Use UNNEST() to flatten arrays or repeated fields.
-- - Use STRUCTs to group related fields into logical containers.
-- 
-- This lab also demonstrated:
-- - JSON ingestion into BigQuery
-- - Querying nested and repeated fields
-- 
-- More Resources:
-- - BigQuery Documentation: https://cloud.google.com/bigquery/docs
-- **********************************************************************
