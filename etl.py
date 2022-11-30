import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: 
        This function is responsible for:
            - Performing ETL on song and file
            - Extracting the data for the songs and artists tables
            - Inserting data to the songs and artists tables
    
    Arguments:
        cur: the cursor object.
        filepath: song data file path.
        
    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description:
        This function is resposible for:
            - Performing ETL for the log data file.
            - Filter Records by `NextSong` action.
            - Extract Data for Time Table.
            - Creating a dataframe containing the time data for this file by combining `column_labels` and `time_data` into a dictionary and converting this into a dataframe.
            - Inserting records into Time Table.
            - Extract data for the user table
            - Inserting records into the user table
            - Extract data for the songplay table
            - Inserting data into the songplay table
    
    Arguments:
        cur: the cursor of the object.
        filepath: log data file path
    
    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    time_data = [df['ts'].values,t.dt.hour, t.dt.day.values, t.dt.isocalendar().week.values, t.dt.month.values, t.dt.year.values,t.dt.weekday.values]
    column_labels = ['start_time','hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

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
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description:
        This function is responsible for:
            - Listing the files in a directory
            - Executing the ingest process for each file according to the function that performs the transformation to save it to the database.
            
    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        func: function that transfroms the data and inserts it into the database.
    
    Returns:
        None
    
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
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=1234")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()