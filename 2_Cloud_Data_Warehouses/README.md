# Sparkify Data Warehouse

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal of the project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Project structure

The project constists of the folling files:

- create_table.py: create your fact and dimension tables for the star schema in Redshift.

- etl.py: load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

- sql_queries.py: contains the SQL statements, which will be imported into the two other files above.

## Database design

The star schema consits of the following fact and dimension tables:

### Fact Tables
    
- songplays: records in event data associated with song plays i.e. records with page NextSong

```
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         INT IDENTITY(0,1) PRIMARY KEY, 
        start_time          TIMESTAMP REFERENCES time(start_time) SORTKEY, 
        user_id             INT NOT NULL REFERENCES users(user_id), 
        level               VARCHAR(10), 
        song_id             VARCHAR(25) REFERENCES songs(song_id) DISTKEY, 
        artist_id           VARCHAR(25) REFERENCES artists(artist_id), 
        session_id          INT, 
        location            VARCHAR(50), 
        user_agent          VARCHAR(200)
    )
```

### Dimension Tables

- users: users in the app

```
     CREATE TABLE IF NOT EXISTS users (
        user_id             INT PRIMARY KEY DISTKEY, 
        first_name          VARCHAR(50), 
        last_name           VARCHAR(50), 
        gender              CHAR(1), 
        level               VARCHAR(10)
    )
```

- songs: songs in music database

```
    CREATE TABLE IF NOT EXISTS songs (
        song_id             VARCHAR(25) PRIMARY KEY DISTKEY, 
        title               VARCHAR(200) SORTKEY, 
        artist_id           VARCHAR(25) NOT NULL REFERENCES artists(artist_id), 
        year                SMALLINT, 
        duration            FLOAT
    )
```

- artists: artists in music database

```
    CREATE TABLE IF NOT EXISTS artists (
        artist_id           VARCHAR(25) PRIMARY KEY DISTKEY, 
        name                VARCHAR(200) SORTKEY, 
        location            VARCHAR(200), 
        latitude            FLOAT, 
        longitude           FLOAT
    )
 ```
 
- time: timestamps of records in songplays broken down into specific units

```
    CREATE TABLE IF NOT EXISTS time (
        start_time          TIMESTAMP PRIMARY KEY SORTKEY, 
        hour                SMALLINT, 
        day                 SMALLINT, 
        week                SMALLINT, 
        month               SMALLINT, 
        year                SMALLINT, 
        weekday             SMALLINT
    )
    diststyle all;
```


## ETL pipeline

The ETL pipeline takes from s3 the json files with the songs and the events logs and, 
load with part of that content the staging tables in Redshift: 

- staging_events  
- staging_songs

From those tables, the ETL will load the star schema in Redshift

