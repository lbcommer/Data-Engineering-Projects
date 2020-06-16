# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY, 
        start_time TIMESTAMP REFERENCES time(start_time), 
        user_id INT NOT NULL REFERENCES users(user_id), 
        level VARCHAR(10), 
        song_id VARCHAR(25) REFERENCES songs(song_id), 
        artist_id VARCHAR(25) REFERENCES artists(artist_id), 
        session_id INT, 
        location VARCHAR(50), 
        user_agent VARCHAR(200)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY, 
        first_name VARCHAR(50), 
        last_name VARCHAR(50), 
        gender CHAR(1), 
        level VARCHAR(10)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(25) PRIMARY KEY, 
        title VARCHAR(100), 
        artist_id VARCHAR(25) NOT NULL REFERENCES artists(artist_id), 
        year SMALLINT, 
        duration FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(25) PRIMARY KEY, 
        name VARCHAR(100), 
        location VARCHAR(100), 
        latitude FLOAT, 
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour SMALLINT, 
        day SMALLINT, 
        week SMALLINT, 
        month SMALLINT, 
        year SMALLINT, 
        weekday SMALLINT
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (
        songplay_id, 
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(songplay_id) DO NOTHING;
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(user_id) 
    DO UPDATE SET 
        first_name=EXCLUDED.first_name, 
        last_name=EXCLUDED.last_name, 
        gender=EXCLUDED.gender, 
        level=EXCLUDED.level; 
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(artist_id) 
    DO UPDATE SET 
        name=EXCLUDED.name, 
        location=EXCLUDED.location, 
        latitude=EXCLUDED.latitude, 
        longitude=EXCLUDED.longitude;
""")


time_table_insert = ("""
    INSERT INTO time (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT song_id, artists.artist_id
    FROM songs JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title = %s
    AND artists.name = %s
    AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [
    user_table_create, 
    artist_table_create, 
    song_table_create, 
    time_table_create, 
    songplay_table_create]

drop_table_queries = [
    user_table_drop, 
    artist_table_drop,
    song_table_drop, 
    time_table_drop, 
    songplay_table_drop]