# Udacity_DataLake_Sparkify_Project

## INTRODUCTION

This  Project-4 handles data of a music streaming startup, Sparkify.Data set is a set of files in JSON format stored in AWS S3 buckets and contains two parts:

1) s3://udacity-dend/song_data: static data about artists and songs Song-data example: {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

2) s3://udacity-dend/log_data: event data of service usage e.g. who listened what song, when, where, and with which clientThis Project-4 handles data of a music streaming startup, Sparkify. Data set is a set of files in JSON format stored in AWS S3 buckets and contains two parts:

s3://udacity-dend/log_data: event data of service usage e.g. who listened what song, when, where, and with which client

Below, some figures about the example data set (results after running the etl.py):

  s3://udacity-dend/song_data: 14897 files
  s3://udacity-dend/log_data: 31 files

  Project builds an ETL pipeline (Extract, Transform, Load) to Extract data from JSON files stored in AWS S3, process the data with Apache Spark, and write the data back to     AWS S3 as Spark parquet files. As technologies, Project-4 uses python, AWS S3 and Apache Spark.


## Purpose of the database and ETL pipeline

 In context of Sparkify, this Data Lake based ETL solution provides very elastic way of processing data. Some pros that we have observed as following:

1) Collecting input data to AWS S3, process the data as needed, and write it back to S3 without maintaining a separate database for intermediate or final data.

2) Data processing is also fast, since Spark executes data processing in RAM and as a cluster which offers parallel processing capabilities.

3) Spark creates schema on-the-fly, so separate schema design is not necessary needed.
	
 Sparkify analytics database (called here sparkifydb) schema has a star design. Start design means that it has one Fact Table having business data, and supporting Dimensional   Tables. Star database design is maybe the most common schema used in ETL pipelines since it separates Dimension data into their own tables in a clean way and collects         business critical data into the Fact table allowing flexible queries.The Fact Table answers one of the key questions: what songs users are listening to. DB schema is the     following:

![alt text](https://github.com/Snaz786/Udacity_DataLake_Sparkify_Project/blob/master/Images/ER_Design.png)

## Database

1) Fact Table

 songplays - records in log data associated with song plays i.e. records with page NextSong
 (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

2) Dimension Tables

 users - users in the app(user_id, first_name, last_name, gender, level)
 songs - songs in music database(song_id, title, artist_id, year, duration)
 artists - artists in music database(artist_id, name, location, lattitude, longitude)
 time - timestamps of records in songplays broken down into specific units(start_time, hour, day, week, month, year, weekday)


## HOW TO RUN

Project has one script:

 etl.py: This script reads data respective buckets from s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes that data using Spark, and writes them back to S3.


 Script executes Apache Spark SQL commands to read source data (JSON files) from S3 to memory as Spark DataFrames.
 In memory, data is further manipulated to analytics DataFrames.
 Analytics dataFrames are stored back to S4 as Spark parquet files.
 Script writes to console the query it's executing at any given time and if the query was successfully executed.
 Also, script writes to console DataFrame schemas and show a handful of example data.
 In the end, script tells if whole ETL-pipeline was successfully executed.

## Output

  On S3 bucket parquet files created for dimension tables.
![alt text](https://github.com/Snaz786/Udacity_DataLake_Sparkify_Project/blob/master/Images/DataLake_S3_AWS_Parquet.JPG)





