import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Extract data from a song file and load it into a database

    This function gets the info about the song and artist that is in 
    the file filepath and, store that info in the tables "artists" and "songs"
    of the database associated to the cursor cur.

    Args:
        cur (psycopg2.extensions.cursor): database cursor
        filepath (str): filepath of a song file

    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data = df[["artist_id", 
                      "artist_name",
                      "artist_location", 
                      "artist_latitude", 
                      "artist_longitude"]].values[0].tolist()
    
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[["song_id", 
                    "title", 
                    "artist_id", 
                    "year", 
                    "duration"]].values[0].tolist()
    
    cur.execute(song_table_insert, song_data)
    

def process_log_file(cur, filepath):
    """Extract data from a log file and load it into a database

    This function gets info about the users events presents in the log file filepath,
    then it loads the selected data into the tables: time, users and songplays 
    of the database associated to the cursor cur.

    Args:
        cur (psycopg2.extensions.cursor): database cursor
        filepath (str): filepath of a log file

    """    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    
    time_data = [[mt, 
                  mt.hour, 
                  mt.day, 
                  mt.week, 
                  mt.month, 
                  mt.year, 
                  mt.dayofweek] for mt in t]

    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, 
                         pd.to_datetime(row.ts, unit='ms'), 
                         row.userId, 
                         row.level, 
                         songid, 
                         artistid, 
                         row.sessionId,
                         row.location, 
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Gets a file with raw data and apply the requiered function to process it

    Takes a file with raw data (filepath) and applies the function func on it to process it.
    The function func will be resposible of the ETL part associated to that raw file, 
    extracting, loading the selected data and, loading this into the database.

    Args:
        cur (psycopg2.extensions.cursor): database cursor
        conn (psycopg2.extensions.connection): database connection
        filepath (str): filepath to a file with raw data
        func (str): function to process the file filepath

    """  
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()