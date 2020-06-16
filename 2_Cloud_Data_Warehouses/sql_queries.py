import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INT,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INT,
        song                VARCHAR,
        status              INT,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INT
)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INT,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INT
)
""")

time_table_create = ("""
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
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id             INT PRIMARY KEY DISTKEY,
        first_name          VARCHAR(50),
        last_name           VARCHAR(50),
        gender              CHAR(1),
        level               VARCHAR(10)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id             VARCHAR(25) PRIMARY KEY DISTKEY,
        title               VARCHAR(200) SORTKEY,
        artist_id           VARCHAR(25) NOT NULL REFERENCES artists(artist_id),
        year                SMALLINT,
        duration            FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id           VARCHAR(25) PRIMARY KEY DISTKEY,
        name                VARCHAR(200) SORTKEY,
        location            VARCHAR(200),
        latitude            FLOAT,
        longitude           FLOAT
    )
""")

songplay_table_create = ("""
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
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
        credentials 'aws_iam_role={}'
        region 'us-west-2' format as JSON {}
        timeformat as 'epochmillisecs';
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT e.ts,
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId,
            e.location,
            e.userAgent
    FROM staging_events e
    JOIN staging_songs  s
    ON (e.song = s.title AND e.artist = s.artist_name)
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE userId IS NOT NULL AND page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id ,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs
    where artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT date_trunc('second', ts),
            EXTRACT(hour FROM ts),
            EXTRACT(day FROM ts),
            EXTRACT(week FROM ts),
            EXTRACT(month FROM ts),
            EXTRACT(year FROM ts),
            EXTRACT(dayofweek FROM ts)
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        user_table_create,
                        artist_table_create,
                        song_table_create,
                        time_table_create,
                        songplay_table_create]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]

