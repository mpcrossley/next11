import urllib.request
import os
import zipfile
import sqlite3
import pandas as pd
import os

# check if static folder exists
if not os.path.exists('static'):
    os.makedirs('static')

# if static/google_transit.zip exists, skip download
if not os.path.exists('static/google_transit.zip'):
    # download file from url
    urllib.request.urlretrieve("http://victoria.mapstrat.com/current/google_transit.zip", "static/google_transit.zip")
    # unzip file
    os.chdir('static') # change directory to
    unzip = zipfile.ZipFile('google_transit.zip')
    unzip.extractall()

    # create database from csv files
    # list of all GTFS static text files
    gtfs_files = [
        'agency.txt',
        'stops.txt',
        'routes.txt',
        'trips.txt',
        'stop_times.txt',
        'calendar.txt',
        'calendar_dates.txt',
        'fare_attributes.txt',
        'fare_rules.txt',
        'shapes.txt',
        'feed_info.txt',
    ]

    #delete existing database
    if os.path.exists('gtfs.db'):
        os.remove('gtfs.db')

    # create a new SQLite database
    conn = sqlite3.connect('gtfs.db')

    for file in gtfs_files:
        # read the csv file into a pandas DataFrame
        df = pd.read_csv(file)
        # remove .txt from the file name to use as the table name
        table_name = os.path.splitext(file)[0]
        # load the DataFrame into the SQLite database
        df.to_sql(table_name, conn, if_exists='replace', index=False)

# close the SQLite database
conn.close()




