-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # variable declaration
-- MAGIC
-- MAGIC clinicaltrial_2023 = "clinicaltrial_2023"
-- MAGIC pharma = "pharma"
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # copy to a temp folder
-- MAGIC
-- MAGIC dbutils.fs.cp("/FileStore/tables/" + clinicaltrial_2023  + ".zip", "file:/tmp/")
-- MAGIC dbutils.fs.cp("/FileStore/tables/" + pharma + ".zip", "file:/tmp/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #  make variable accessible by the command line
-- MAGIC
-- MAGIC import os
-- MAGIC os.environ['clinicaltrial_2023'] = clinicaltrial_2023
-- MAGIC
-- MAGIC import os
-- MAGIC os.environ['pharma'] = pharma

-- COMMAND ----------

-- MAGIC
-- MAGIC %sh
-- MAGIC
-- MAGIC ls /tmp

-- COMMAND ----------

-- MAGIC
-- MAGIC
-- MAGIC %sh
-- MAGIC unzip -d /tmp/ /tmp/clinicaltrial_2023.zip
-- MAGIC unzip -d /tmp/ /tmp/pharma.zip
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC
-- MAGIC %sh
-- MAGIC
-- MAGIC ls /tmp/$clinicaltrial_2023.csv
-- MAGIC ls /tmp/$pharma.csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # making a new directory
-- MAGIC
-- MAGIC dbutils.fs.mkdirs("/FileStore/tables/" + clinicaltrial_2023)
-- MAGIC dbutils.fs.mkdirs("/FileStore/tables/" + pharma)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Moving to DBFS
-- MAGIC
-- MAGIC dbutils.fs.mv("file:/tmp/" +  clinicaltrial_2023 + ".csv", "/FileStore/tables/" +  clinicaltrial_2023 + ".csv", True)
-- MAGIC dbutils.fs.mv("file:/tmp/" + pharma + ".csv", "/FileStore/tables/" + pharma + ".csv", True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Check to confirm that the files are in the directory directory
-- MAGIC
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.head("/FileStore/tables/" + clinicaltrial_2023 + ".csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # CREATE RDD FOR CLINICAL TRIALS
-- MAGIC
-- MAGIC # defines a function to read and convert the clinicaltrial data into RDD
-- MAGIC
-- MAGIC def load_clinical_trial_data(clinicaltrial_2023):
-- MAGIC     """
-- MAGIC     Load clinical trial data from a CSV file into an RDD.
-- MAGIC
-- MAGIC     Parameters:
-- MAGIC         clinicaltrial_2023 (str): The name of the CSV file to load.
-- MAGIC
-- MAGIC     Returns:
-- MAGIC         pyspark.rdd.RDD: An RDD containing the clinical trial data.
-- MAGIC     """
-- MAGIC     rdd = sc.textFile("/FileStore/tables/" + clinicaltrial_2023 + ".csv")
-- MAGIC     return rdd
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # CREATE RDD FOR pharmaceutical data
-- MAGIC
-- MAGIC # defines a function to read and convert the pharmaceutical data into RDD
-- MAGIC
-- MAGIC
-- MAGIC def load_pharma_data(pharma):
-- MAGIC     """
-- MAGIC     Load pharmaceutical data from a CSV file into an RDD.
-- MAGIC
-- MAGIC     Parameters:
-- MAGIC         pharma (str): The name of the pharmaceutical data CSV file.
-- MAGIC
-- MAGIC     Returns:
-- MAGIC         pyspark.rdd.RDD: An RDD containing the pharmaceutical data.
-- MAGIC     """
-- MAGIC     prdd = sc.textFile("/FileStore/tables/" + pharma + ".csv")
-- MAGIC     return prdd
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Removing the delimeters
-- MAGIC
-- MAGIC delimiter_selector = {
-- MAGIC     "clinicaltrial_2023": "\t",
-- MAGIC     "clinicaltrial_2021": "|",
-- MAGIC     "clinicaltrial_2020": "|",
-- MAGIC     "pharma": ","
-- MAGIC }
-- MAGIC
-- MAGIC def clean_clinical_rdd(crdd, clinicaltrial_2023):
-- MAGIC     """
-- MAGIC     Clean the clinical trial RDD by splitting each line using the delimiter
-- MAGIC     specified by the file type and removing unwanted characters.
-- MAGIC
-- MAGIC     Parameters:
-- MAGIC         crdd (pyspark.rdd.RDD): The RDD containing clinical trial data.
-- MAGIC         clinicaltrial_2023 (str): The name of the clinical trial file.
-- MAGIC
-- MAGIC     Returns:
-- MAGIC         pyspark.rdd.RDD: The cleaned RDD.
-- MAGIC     """
-- MAGIC     CLEAN_CRDD = crdd.map(lambda x: x.split(delimiter_selector[clinicaltrial_2023])).map(lambda x: [i.replace(",",'').replace('"','')for i in x])
-- MAGIC     return CLEAN_CRDD
-- MAGIC
-- MAGIC def clean_pharma_rdd(prdd, pharma):
-- MAGIC     """
-- MAGIC     Clean the pharmaceutical RDD by splitting each line using the delimiter
-- MAGIC     specified by the file type and removing unwanted characters.
-- MAGIC
-- MAGIC     Parameters:
-- MAGIC         prdd (pyspark.rdd.RDD): The RDD containing pharmaceutical data.
-- MAGIC         pharma_file (str): The name of the pharmaceutical file.
-- MAGIC
-- MAGIC     Returns:
-- MAGIC         pyspark.rdd.RDD: The cleaned RDD.
-- MAGIC     """
-- MAGIC     CLEAN_PRDD = prdd.map(lambda x: x.split(delimiter_selector[pharma])).map(lambda x: [i.replace(",",'').replace('"','')for i in x])
-- MAGIC     return CLEAN_PRDD
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Creating new RDD  after cleaning the data
-- MAGIC
-- MAGIC # Creating the new RDD
-- MAGIC Clinical_RDD = load_clinical_trial_data(clinicaltrial_2023)
-- MAGIC PHARMA_RDD = load_pharma_data(pharma)
-- MAGIC
-- MAGIC # Cleaning the RDD
-- MAGIC Clinical_RDD_Clean = clean_clinical_rdd(Clinical_RDD, clinicaltrial_2023)
-- MAGIC PHARMA_RDD_Clean = clean_pharma_rdd(PHARMA_RDD, pharma)
-- MAGIC
-- MAGIC # Take first 5 elements from cleaned Clinical RDD
-- MAGIC Clinical_RDD_Clean.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC PHARMA_RDD_Clean.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #filtering empty row in pharma
-- MAGIC pharma_rdd_filtered = PHARMA_RDD_Clean.filter(lambda x: len(x) == 34)

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC # Create a SparkSession
-- MAGIC spark = SparkSession.builder \
-- MAGIC     .appName("Create Pharma DataFrame") \
-- MAGIC     .getOrCreate()
-- MAGIC
-- MAGIC # Define the schema based on the provided columns
-- MAGIC pharma_schema = StructType([
-- MAGIC     StructField("Company", StringType(), True),
-- MAGIC     StructField("Parent_Company", StringType(), True),
-- MAGIC     StructField("Penalty_Amount", StringType(), True),
-- MAGIC     StructField("Subtraction_From_Penalty", StringType(), True),
-- MAGIC     StructField("Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting", StringType(), True),
-- MAGIC     StructField("Penalty_Year", StringType(), True),
-- MAGIC     StructField("Penalty_Date", StringType(), True),
-- MAGIC     StructField("Offense_Group", StringType(), True),
-- MAGIC     StructField("Primary_Offense", StringType(), True),
-- MAGIC     StructField("Secondary_Offense", StringType(), True),
-- MAGIC     StructField("Description", StringType(), True),
-- MAGIC     StructField("Level_of_Government", StringType(), True),
-- MAGIC     StructField("Action_Type", StringType(), True),
-- MAGIC     StructField("Agency", StringType(), True),
-- MAGIC     StructField("Civil_Criminal", StringType(), True),
-- MAGIC     StructField("Prosecution_Agreement", StringType(), True),
-- MAGIC     StructField("Court", StringType(), True),
-- MAGIC     StructField("Case_ID", StringType(), True),
-- MAGIC     StructField("Private_Litigation_Case_Title", StringType(), True),
-- MAGIC     StructField("Lawsuit_Resolution", StringType(), True),
-- MAGIC     StructField("Facility_State", StringType(), True),
-- MAGIC     StructField("City", StringType(), True),
-- MAGIC     StructField("Address", StringType(), True),
-- MAGIC     StructField("Zip", StringType(), True),
-- MAGIC     StructField("NAICS_Code", StringType(), True),
-- MAGIC     StructField("NAICS_Translation", StringType(), True),
-- MAGIC     StructField("HQ_Country_of_Parent", StringType(), True),
-- MAGIC     StructField("HQ_State_of_Parent", StringType(), True),
-- MAGIC     StructField("Ownership_Structure", StringType(), True),
-- MAGIC     StructField("Parent_Company_Stock_Ticker", StringType(), True),
-- MAGIC     StructField("Major_Industry_of_Parent", StringType(), True),
-- MAGIC     StructField("Specific_Industry_of_Parent", StringType(), True),
-- MAGIC     StructField("Info_Source", StringType(), True),
-- MAGIC     StructField("Notes", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # Convert RDD to DataFrame with defined schema
-- MAGIC pharma_df = spark.createDataFrame(pharma_rdd_filtered, schema=pharma_schema)
-- MAGIC
-- MAGIC # Show the DataFrame
-- MAGIC pharma_df.show(5, truncate=False)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #### filtering
-- MAGIC clinical_rdd_filtered = Clinical_RDD_Clean.filter(lambda x: len(x) == 14)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *
-- MAGIC myschema = StructType([
-- MAGIC     StructField("Id", StringType(), True),
-- MAGIC     StructField("Study Title", StringType(), True),
-- MAGIC     StructField("Acronym", StringType(), True),
-- MAGIC     StructField("Status", StringType(), True),
-- MAGIC     StructField("Conditions", StringType(), True),
-- MAGIC     StructField("Interventions", StringType(), True),
-- MAGIC     StructField("Sponsor", StringType(), True),
-- MAGIC     StructField("Collaborators", StringType(), True),
-- MAGIC     StructField("Enrollment", StringType(), True),
-- MAGIC     StructField("Funder Type", StringType(), True),
-- MAGIC     StructField("Type", StringType(), True),
-- MAGIC     StructField("Study Design", StringType(), True),
-- MAGIC     StructField("Start", StringType(), True),
-- MAGIC     StructField("Completion", StringType(), True)
-- MAGIC ])
-- MAGIC # Convert RDD to DataFrame with defined schema
-- MAGIC clinical_df = spark.createDataFrame(clinical_rdd_filtered, schema=myschema)
-- MAGIC
-- MAGIC # Show the DataFrame
-- MAGIC clinical_df.show(5, truncate=False)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Creating a database
-- MAGIC
-- MAGIC clinical_df . createOrReplaceTempView ("sqldata")

-- COMMAND ----------

SELECT * FROM sqldata LIMIT 10

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE TABLE default.clinical_data AS
SELECT
    Id,
    "Study Title" AS Study_Title,
    Acronym,
    Status,
    Conditions,
    Interventions,
    Sponsor,
    Collaborators,
    Enrollment,
    "Funder Type" AS Funder_Type,
    Type,
    "Study Design" AS Study_Design,
    Start,
    Completion
FROM sqldata;


-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS clinical_database

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

--CREATE TABLE IF NOT EXISTS clinical_database.clinical_data AS SELECT * FROM default.clinical_data

-- COMMAND ----------

CREATE OR REPLACE TABLE clinical_database.clinical_data AS SELECT * FROM default.clinical_data;


-- COMMAND ----------

SHOW TABLES IN clinical_database

-- COMMAND ----------

SELECT * FROM clinical_data LIMIT 10

-- COMMAND ----------

--Q1
SELECT COUNT(DISTINCT Id) AS Number_of_Studies
FROM default.clinical_data;


-- COMMAND ----------

--Q2
--Filter out rows where 'Type' column is null, empty, or equals to 'Type'
CREATE OR REPLACE TEMP VIEW filtered_df AS
SELECT *
FROM clinical_data
WHERE Type IS NOT NULL AND Type <> '' AND Type <> 'Type';

-- Group by 'Type' column and count the occurrences
CREATE OR REPLACE TEMP VIEW type_counts_df AS
SELECT Type, COUNT(*) AS Frequency
FROM filtered_df
GROUP BY Type
ORDER BY Frequency DESC;

-- Show the result
SELECT * FROM type_counts_df;


-- COMMAND ----------

-- Q3
SELECT Conditions, COUNT(*) AS Frequency
FROM clinical_data
WHERE Conditions <> ''
GROUP BY Conditions
ORDER BY Frequency DESC
LIMIT 5;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Creating a database for pharma
-- MAGIC
-- MAGIC pharma_df . createOrReplaceTempView ("pharmasql")

-- COMMAND ----------

SELECT * FROM pharmasql LIMIT 10

-- COMMAND ----------

CREATE OR REPLACE TABLE default.pharma_data AS SELECT * FROM pharmasql

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS pharma_database

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

CREATE OR REPLACE TABLE pharma_database.pharma_data AS SELECT * FROM default.pharma_data

-- COMMAND ----------

SHOW TABLES IN pharma_database

-- COMMAND ----------

--Q4
-- Join the clinical_data table with pharma_data table
CREATE OR REPLACE TEMP VIEW joined_df AS
SELECT c.*, p.*
FROM clinical_data c
LEFT JOIN pharma_data p ON c.Sponsor = p.Parent_Company;
-- Filter out rows where Parent_Company is null (non-pharmaceutical companies)
CREATE OR REPLACE TEMP VIEW filtered_df AS
SELECT *
FROM joined_df
WHERE Parent_Company IS NULL;
-- Count the number of clinical trials sponsored by each sponsor
CREATE OR REPLACE TEMP VIEW sponsors_count AS
SELECT Sponsor, COUNT(*) AS TotalTrials
FROM filtered_df
GROUP BY Sponsor;
-- Get the top 10 sponsors by the number of trials
CREATE OR REPLACE TEMP VIEW top_10_sponsors AS
SELECT Sponsor, TotalTrials
FROM sponsors_count
ORDER BY TotalTrials DESC
LIMIT 10;
-- Show the result
SELECT * FROM top_10_sponsors;


-- COMMAND ----------

--Q5
-- Define a table to store month abbreviations and corresponding full names
CREATE OR REPLACE TEMP VIEW month_mapping AS
SELECT *
FROM (VALUES
    ('01', 'Jan'),
    ('02', 'Feb'),
    ('03', 'Mar'),
    ('04', 'Apr'),
    ('05', 'May'),
    ('06', 'Jun'),
    ('07', 'Jul'),
    ('08', 'Aug'),
    ('09', 'Sept'),
    ('10', 'Oct'),
    ('11', 'Nov'),
    ('12', 'Dec')
) AS t(month_num, month_name);

-- Count completed studies for each month in 2023
WITH completed_studies AS (
    SELECT
        EXTRACT(MONTH FROM Completion) AS completion_month,
        COUNT(*) AS study_count
    FROM clinical_data
    WHERE EXTRACT(YEAR FROM Completion) = 2023
      AND Status IN ('COMPLETED', 'Completed')
    GROUP BY EXTRACT(MONTH FROM Completion)
)

-- Map the month abbreviations to their full names
, mapped_months AS (
    SELECT
        mm.month_name AS month,
        COALESCE(cs.study_count, 0) AS completed_studies
    FROM month_mapping mm
    LEFT JOIN completed_studies cs
    ON mm.month_num = LPAD(CAST(cs.completion_month AS STRING), 2, '0')
)

SELECT * FROM mapped_months ORDER BY
    CASE month
        WHEN 'Jan' THEN 1
        WHEN 'Feb' THEN 2
        WHEN 'Mar' THEN 3
        WHEN 'Apr' THEN 4
        WHEN 'May' THEN 5
        WHEN 'Jun' THEN 6
        WHEN 'Jul' THEN 7
        WHEN 'Aug' THEN 8
        WHEN 'Sept' THEN 9
        WHEN 'Oct' THEN 10
        WHEN 'Nov' THEN 11
        WHEN 'Dec' THEN 12
    END;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC # Data
-- MAGIC months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec"]
-- MAGIC completed_studies = [1494, 1272, 1552, 1324, 1415, 1619, 1360, 1230, 1152, 1058, 909, 1082]
-- MAGIC
-- MAGIC # Create bar plot with visual distinction
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC bars = plt.bar(months, completed_studies, color=['skyblue', 'lightgreen', 'salmon', 'cornflowerblue', 'lightcoral', 'mediumaquamarine', 'lightsalmon', 'lightsteelblue', 'lightseagreen', 'lightpink', 'lightskyblue', 'lightgoldenrodyellow'])
-- MAGIC plt.title('Number of Completed Studies in 2023')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Number of Completed Studies')
-- MAGIC plt.xticks(rotation=45)
-- MAGIC plt.grid(axis='y', linestyle='--', alpha=0.7)
-- MAGIC
-- MAGIC # Add data labels
-- MAGIC for bar in bars:
-- MAGIC     yval = bar.get_height()
-- MAGIC     plt.text(bar.get_x() + bar.get_width()/2, yval + 20, round(yval), ha='center', va='bottom')
-- MAGIC
-- MAGIC # Show plot
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()

-- COMMAND ----------

--FURTHER ANALYSIS RECRUITING STUDIES
CREATE OR REPLACE TEMP VIEW month_mapping AS
SELECT *
FROM (VALUES
    ('01', 'Jan'),
    ('02', 'Feb'),
    ('03', 'Mar'),
    ('04', 'Apr'),
    ('05', 'May'),
    ('06', 'Jun'),
    ('07', 'Jul'),
    ('08', 'Aug'),
    ('09', 'Sept'),
    ('10', 'Oct'),
    ('11', 'Nov'),
    ('12', 'Dec')
) AS t(month_num, month_name);
WITH Terminated_studies AS (
    SELECT
        EXTRACT(MONTH FROM Completion) AS completion_month,
        COUNT(*) AS study_count
    FROM clinical_data
    WHERE EXTRACT(YEAR FROM Completion) = 2023
      AND Status IN ('RECRUITING', 'Recruiting')
    GROUP BY EXTRACT(MONTH FROM Completion)
)
, mapped_months AS (
    SELECT
        mm.month_name AS month,
        COALESCE(cs.study_count, 0) AS Recruiting_Studies
    FROM month_mapping mm
    LEFT JOIN Terminated_studies cs
    ON mm.month_num = LPAD(CAST(cs.completion_month AS STRING), 2, '0')
)
SELECT * FROM mapped_months ORDER BY
    CASE month
        WHEN 'Jan' THEN 1
        WHEN 'Feb' THEN 2
        WHEN 'Mar' THEN 3
        WHEN 'Apr' THEN 4
        WHEN 'May' THEN 5
        WHEN 'Jun' THEN 6
        WHEN 'Jul' THEN 7
        WHEN 'Aug' THEN 8
        WHEN 'Sept' THEN 9
        WHEN 'Oct' THEN 10
        WHEN 'Nov' THEN 11
        WHEN 'Dec' THEN 12
    END;


-- COMMAND ----------

--FURTHER ANALYSIS NOT_YET_RECRUITING STUDIES
CREATE OR REPLACE TEMP VIEW month_mapping AS
SELECT *
FROM (VALUES
    ('01', 'Jan'),
    ('02', 'Feb'),
    ('03', 'Mar'),
    ('04', 'Apr'),
    ('05', 'May'),
    ('06', 'Jun'),
    ('07', 'Jul'),
    ('08', 'Aug'),
    ('09', 'Sept'),
    ('10', 'Oct'),
    ('11', 'Nov'),
    ('12', 'Dec')
) AS t(month_num, month_name);
WITH Terminated_studies AS (
    SELECT
        EXTRACT(MONTH FROM Completion) AS completion_month,
        COUNT(*) AS study_count
    FROM clinical_data
    WHERE EXTRACT(YEAR FROM Completion) = 2023
      AND Status IN ('NOT_YET_RECRUITING', 'Not_Yet_Recruiting')
    GROUP BY EXTRACT(MONTH FROM Completion)
)
, mapped_months AS (
    SELECT
        mm.month_name AS month,
        COALESCE(cs.study_count, 0) AS Not_Yet_Recruiting_Studies
    FROM month_mapping mm
    LEFT JOIN Terminated_studies cs
    ON mm.month_num = LPAD(CAST(cs.completion_month AS STRING), 2, '0')
)
SELECT * FROM mapped_months ORDER BY
    CASE month
        WHEN 'Jan' THEN 1
        WHEN 'Feb' THEN 2
        WHEN 'Mar' THEN 3
        WHEN 'Apr' THEN 4
        WHEN 'May' THEN 5
        WHEN 'Jun' THEN 6
        WHEN 'Jul' THEN 7
        WHEN 'Aug' THEN 8
        WHEN 'Sept' THEN 9
        WHEN 'Oct' THEN 10
        WHEN 'Nov' THEN 11
        WHEN 'Dec' THEN 12
    END;


-- COMMAND ----------

--FURTHER ANALYSIS TERMINATED STUDIES
CREATE OR REPLACE TEMP VIEW month_mapping AS
SELECT *
FROM (VALUES
    ('01', 'Jan'),
    ('02', 'Feb'),
    ('03', 'Mar'),
    ('04', 'Apr'),
    ('05', 'May'),
    ('06', 'Jun'),
    ('07', 'Jul'),
    ('08', 'Aug'),
    ('09', 'Sept'),
    ('10', 'Oct'),
    ('11', 'Nov'),
    ('12', 'Dec')
) AS t(month_num, month_name);
WITH Terminated_studies AS (
    SELECT
        EXTRACT(MONTH FROM Completion) AS completion_month,
        COUNT(*) AS study_count
    FROM clinical_data
    WHERE EXTRACT(YEAR FROM Completion) = 2023
      AND Status IN ('TERMINATED', 'Terminated')
    GROUP BY EXTRACT(MONTH FROM Completion)
)
, mapped_months AS (
    SELECT
        mm.month_name AS month,
        COALESCE(cs.study_count, 0) AS Terminated_studies
    FROM month_mapping mm
    LEFT JOIN Terminated_studies cs
    ON mm.month_num = LPAD(CAST(cs.completion_month AS STRING), 2, '0')
)
SELECT * FROM mapped_months ORDER BY
    CASE month
        WHEN 'Jan' THEN 1
        WHEN 'Feb' THEN 2
        WHEN 'Mar' THEN 3
        WHEN 'Apr' THEN 4
        WHEN 'May' THEN 5
        WHEN 'Jun' THEN 6
        WHEN 'Jul' THEN 7
        WHEN 'Aug' THEN 8
        WHEN 'Sept' THEN 9
        WHEN 'Oct' THEN 10
        WHEN 'Nov' THEN 11
        WHEN 'Dec' THEN 12
    END;


-- COMMAND ----------


