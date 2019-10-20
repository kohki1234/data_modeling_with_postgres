import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

import datetime
import sys
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

def process_song_file(cur, filepath):
    """
    This function handles song_files to executable format and insert into table.
    
    Args:
        cur (psycopg2.extensions.cursor): cursor instance 
        filepath (str): the filepath to the song file

    Returns:
        This function return nothing
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_id = df.song_id.values[0]
    title = df.title.values[0]
    artist_id = df.artist_id.values[0]
    year = df.year.values[0].astype(int)
    duration = df.duration.values[0].astype(float)
    
    song_data = [song_id,title,artist_id, year, duration]
    
    cur.execute(song_table_insert, song_data)
    
    
    # insert artist record
    artist_id = df.artist_id.values[0]
    name = df.artist_name.values[0]
    location= df.artist_location.values[0]
    latitude = df.artist_latitude.values[0]
    longtitude = df.artist_longitude.values[0]
    
    
    artist_data = [artist_id, name,location, latitude, longtitude]
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    This function handles log files to executable format and insert into table.
    
    Args:
        cur (psycopg2.extensions.cursor): cursor instance 
        filepath (str): the filepath to the song file

    Returns:
        This function return nothing
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'].isin(['NextSong'])]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    start_time = t
    hour = t.dt.hour
    day = t.dt.day
    week_of_year = t.dt.week
    month = t.dt.month
    year = t.dt.year
    weekday = t.dt.weekday_name
    time_data = ([start_time,hour,day, week_of_year, month, year, weekday])
    column_labels = (['start_time','hour', 'day','week_of_year','month','year','weekday'])
    time_df = pd.DataFrame({column_labels[0]:time_data[0],
                        column_labels[1]:time_data[1],
                        column_labels[2]:time_data[2],
                        column_labels[3]:time_data[3],
                        column_labels[4]:time_data[4],
                        column_labels[5]:time_data[5],
                        column_labels[6]:time_data[6]})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.read_json(filepath, lines=True).loc[:, ['userId','firstName','lastName','gender','level']]

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
        songplay_data = (index, row.ts,row.userId, row.level, songid, artistid, row.sessionId, row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function go over all the files with path destination, and execute each functions \
    (process_song_file() and process_log_file())
    
    Args:
        cur (psycopg2.extensions.cursor): psycopg2 cursor instance
        conn(psycopg2.extensions.connection) : psycopg2 connection instance
        filepath (str): the filepath to the song file
        func(function): function name will be fit here

    Returns:
        This function return nothing
    """
    print(type(func))
    
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
    """
    This main function calls process_data() function and retrive all necessary information, and insert them into\
    each tables.
    
    Args:
        this function dosn't have any arguments to fill in with.

    Returns:
        This function return nothing
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()