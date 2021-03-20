import os
import glob
import psycopg2
import pandas as pd
import db_creds
from sql_queries import *


def process_song_file(cur, filepath):
    """Processes a single song json file.
    
    Parameters
    ----------
    cur : `psycopg2.extensions.cursor`
        Postgres database cursor
    filepath : `string`
        Filepath of the file to be processed
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print("Error in songs table insert")
        print(e)
        
    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print("Error in artists table insert")
        print(e)

def process_log_file(cur, filepath):
    """Processes a single log json file.
    
    Parameters
    ----------
    cur : `psycopg2.extensions.cursor`
        Postgres database cursor
    filepath : `string`
        Filepath of the file to be processed
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df["page"] == "NextSong", ]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")
    
    # insert time data records
    time_data = (df["ts"], t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("timestamp", "hour", "day", "weekofyear", "month", "year", "weekday") 
    time_df = pd.DataFrame(
        {
            column_labels[0]: time_data[0],
            column_labels[1]: time_data[1],
            column_labels[2]: time_data[2],
            column_labels[3]: time_data[3],
            column_labels[4]: time_data[4],
            column_labels[5]: time_data[5],
            column_labels[6]: time_data[6]
        }   
    )

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].drop_duplicates()

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
        songplay_data = (index, row['ts'], row['userId'], 
                         row['level'], songid, 
                         artistid, row['sessionId'], 
                         row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Processes all files is a specified filepath.
    
    Parameters
    ----------
    cur : `psycopg2.extensions.cursor`
        Postgres database cursor.
    conn : `psycopg2.extensions.connection`
        Postgres database connection handle.
    filepath : `string`
        Path of the directory of files to be processed.
    func : `function`
        The defined function that will process each individual file.
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('\n{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect(f"host={db_creds.host} \
                            dbname=sparkifydb \
                            user={db_creds.user} \
                            password={db_creds.password}")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()