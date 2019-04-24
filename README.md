# Project: Data Warehouse

### Introduction

As the user and song database keeps growing bigger and bigger, moving the data onto the cloud seems to be a better option. Now the data resides in Amazon S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
The task is to build an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs users are listening to.



# Project Objective

Design and structure data to make it available to the others in the team. So they can make use of it easily. Build an ETL pipeline for a database hosted on Redshift.

Throughout this project, I have completed the following tasks:

1. Design schemas for your fact and dimension tables
2. Build an ETL pipeline to load data from S3 to staging tables and analytics tables on Redshift
3. Improve query performance by running analytic queries on Redshift database

# Technology 

<p align="middle">
  <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1d/AmazonWebservices_Logo.svg/1280px-AmazonWebservices_Logo.svg.png" />
  <img src="https://ws2.sinaimg.cn/large/006tNc79ly1g2bsv06jf3j30gp05njtd.jpg" />



# Explore the dataset

##### 1. Song Dataset

The first dataset is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

```txt
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```json
{"num_songs": 1, 
 "artist_id": "ARJIE2Y1187B994AB7", 
 "artist_latitude": null, 
 "artist_longitude": null, 
 "artist_location": "", 
 "artist_name": "Line Renaud", 
 "song_id": "SOUPIRU12A6D4FA1E1", 
 "title": "Der Kleine Dompfaff", 
 "duration": 152.92036, 
 "year": 0}
```

##### 2. Log Dataset

The second dataset consists of log files in JSON format. These describe app activity logs from a music streaming app based on specified configurations.

The log files in the dataset are partitioned by year and month. 

For example, here are file paths to two files in this dataset.

```txt
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![](https://ws3.sinaimg.cn/large/006tNc79ly1g2bsvkkb18j316d0cstbp.jpg)



# Methodology 

### Star Schema Design - Optimized for queries on song play analysis

![](https://ws2.sinaimg.cn/large/006tNc79ly1g2bsvrjxy1j30hg0c2aax.jpg)

#### Fact Table

1. songplays

   \- records in event data associated with song plays i.e. records with page

   ```
   NextSong
   ```

   - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables

1. users

   \- users in the app

   - *user_id, first_name, last_name, gender, level*

2. songs

   \- songs in music database

   - *song_id, title, artist_id, year, duration*

3. artists

   \- artists in music database

   - *artist_id, name, location, lattitude, longitude*

4. time

   \- timestamps of records in songplays table, broken down into specific units

   - *start_time, hour, day, week, month, year, weekday*



# Go Directories

### `/func`

Main applications for this project

### `/query`

All queries for this project

### `/config`

Setting config data for this project

# How to use this project

1. Run `func/create_cluster.ipynb` to create your own redshift cluster

   (Include your aws setting in `dwh.cfg` in `config` folder)

2. Run `func/create_tables.py` to create database and tables 

   (Alter queries if you want in `query/sql_queries.py`)

3. Run `func/etl.py` to perform the complete ETL pipeline