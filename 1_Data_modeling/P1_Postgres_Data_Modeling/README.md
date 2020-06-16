# Sparkify: Data Modeling with Postgres

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.
The raw data is in json files. 

## Objectives

We have to design a Postgres model based on the star schema, convinient for the analytical goals, and also the ETL pipelines required to load the database.

## Database Schema

We have used a dimensional star schema. This helps to obtain aggregated analytics. The fact table is connected with the dimension tables through foreign keys so, we improve the integrity. 

These are the fact and dimension tables:

### Fact Tables

*songplays* - records in log data associated with song plays i.e. records with page NextSong

- songplay_id SERIAL PRIMARY KEY : id of each song
- start_time TIMESTAMP REFERENCES time(start_time): timestamp when the user listen to the song
- user_id INT NOT NULL REFERENCES users(user_id): id of the user  
- level VARCHAR(10): user level (Free, Paid)  
- song_id VARCHAR(25) REFERENCES songs(song_id): id of the song
- artist_id VARCHAR(25) REFERENCES artists(artist_id): id of the artist of the song
- session_id INT: id of the session
- location VARCHAR(50): user location 
- user_agent VARCHAR(200): contains information about the medium from which the user listen to the song

### Dimension Tables

*users* - users in the app

- user_id INT PRIMARY KEY: id of the user 
- first_name VARCHAR(50): first name of the user
- last_name VARCHAR(50): surname of the user
- gender CHAR(1): gender of the user (M, F)
- level VARCHAR(10): level of the user (Free, Paid)
        
*songs* - songs in music database

- song_id VARCHAR(25) PRIMARY KEY: id of the song 
- title VARCHAR(100): title of the song
- artist_id VARCHAR(25) NOT NULL REFERENCES artists(artist_id): id of the artist of the song
- year SMALLINT: year of the song
- duration FLOAT: duration of the song in milliseconds

*artists* - artists in music database

- artist_id VARCHAR(25) PRIMARY KEY: id of the artist
- name VARCHAR(100): name of the artist
- location VARCHAR(100): city of the artist
- latitude FLOAT: latitud of the artist location
- longitude FLOAT: longitude of the artist location

*time* - timestamps of records in songplays broken down into specific units

- start_time TIMESTAMP PRIMARY KEY: timestamp of the event (the user listen to a song)
- hour SMALLINT: hour of the event 
- day SMALLINT: day of the event
- week SMALLINT: week of the event
- month SMALLINT: month of the event 
- year SMALLINT: year of the event 
- weekday SMALLINT: weekday of the event
        
  
## Project files

We have used different files to implement the different parts of the project


- create_tables.py: create the database schema
- etl.py: etl pipeline that takes the raw data and load into the database

We also use this auxiliary files:

- sql_queries.py: contains the sql strings required to create the schema and insert data into it
- etl.ipynb: notebook used during the development of the ETL script
- test.ipynb: notebook to check the process during the development.




