# Big-Data---Data-Processing

# Aim:
The goal of this project is to analyse a large dataset using Spark. You will have to load the available data, perform data transformation and analysis on it and finally train a Machine Learning algorithm for predicting a continuous outcome.

# Introduction to the dataset
The New York City Taxi and Limousine Commission (TLC) is the agency responsible for licensing and regulating New York City’s taxi cabs since 1971. TLC has publicly published millions of trip records from both yellow and green taxi cabs. 
Each record includes fields capturing pick-up and drop-off dates/times, locations, trip distances, itemised fares, rate types, payment types, and driver-reported passenger counts.

Yellow taxi cabs are the iconic taxi vehicles from New York city that have the right to pick up street-hailing passengers anywhere in the city. There are around 13,600 authorised taxis in New York City and each taxi must have a yellow medallion affixed to it. 

Green taxis were introduced in August 2013 to improve taxi service and availability in the boroughs. Green taxis may respond to street hails, but only in certain designated areas.

# Tasks:

## PART 1: Data Ingestion and Preparation
1. Download the dataset for yellow and green taxi cabs from 2015 to 2022 and load it into an Azure Blob Storage:
2. On Databricks, read the files from the Azure storage and make a copy of it into DBFS. 
3. Count the total numbers of rows for each taxi colour (yellow and green) by reading the files stored on DBFS:
Green taxi: 66,200,401
Yellow taxi: 663,055,251
4. Download and load the location referential csv: link
5. Convert the “Green” 2015 parquet into a csv file and send it to your Azure Blob Storage. Compare the size of the parquet file against its csv version then explain why parquet format makes more sense than csv.

Explore the dataset and perform any required data cleaning to remove unrealistic trips (You can use pyspark or sparksql) such as:
1. Trips finishing before the starting time
2. Trips where the pickup/dropoff datetime is outside of the range
3. Trips with negative speed
4. Trips with very high speed (look for NYC and outside of NYC speed limit )
5. Trips that are travelling too short or too long (duration wise)
6. Trips that are travelling too short or too long (distance wise)
7. Any other logic you think is important
8. Combine the yellow and green taxi dataset together (their schema are not exactly the same).
9. Combine the new dataframe with the location data (there are two locations in each trip, pick up location and drop off location)
10. Export the combined data into a parquet file in DBFS and then load it as a table or view.


## PART 2: Business Questions (Only use SparkSQL + Take screenshots of results and add in the report)
For each year and month (e.g January 2020 => “2020-01-01” or “2020-01” or “Jan 2020”:
1. What was the total number of trips?
2. Which day of week (e.g. monday, tuesday, etc..) had the most trips?
3. Which hour of the day had the most trips?
4. What was the average number of passengers?
5. What was the average amount paid per trip (using total_amount)?
6. What was the average amount paid per passenger (using total_amount)?
=> In a Single table/dataframe/output

For each taxi colour (yellow and green):
1. What was the average, median, minimum and maximum trip duration in minutes (with 2 decimals, eg. 90 seconds = 1.50 min)?
2. What was the average, median, minimum and maximum trip distance in km?
3. What was the average, median, minimum and maximum speed in km per hour?
=> In a Single table/dataframe/output

For each taxi colour (yellow and green), each pair of pick up and drop off locations (use boroughs not the id), each month, each day of week and each hours:
1. What was the total number of trips?
2. What was the average distance?
3. What was the average amount paid per trip (using total_amount)?
4. What was the total amount paid (using total_amount)?
=> In a Single table/dataframe/output


What was the percentage of trips where drivers received tips?

For trips where the driver received tips, what was the percentage where the driver received tips of at least $5

Classify each trip into bins of durations:
Under 5 Mins
From 5 mins to 10 mins
From 10 mins to 20 mins
From 20 mins to 30 mins
From 30 mins to 60 mins
At least 60 mins *10

Then for each bins, calculate: 
Average speed (km per hour)
Average distance per dollar (km per $)
=> In a Single table/dataframe/output

Which duration bin will you advise a taxi driver to target to maximise his income?

## PART 3: Machine Learning:
Build at least two different ML models (two different algorithms) using Spark ML pipelines + a baseline model to predict the Total amount of a trip:
Build a baseline model by using the answer of Part 2 Q3c (average paid) and calculate its RMSE.
Use all data except October, November, December 2022 to train and validate your models and use the RMSE score to assess your models.
Choose your best model and explain why you chose it (processing time, complexity, accuracy, etc).
Using your best model, predict the October, November, December 2022 trips and calculate the RMSE on your predictions. Does your model beat the baseline model
