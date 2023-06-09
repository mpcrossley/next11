#!/usr/bin/env python3
import urllib.request
import os
import zipfile
import sqlite3
import pandas as pd

# if google_transit.zip exists, skip download
if not os.path.exists('google_transit.zip'):
    # download file from url
    urllib.request.urlretrieve("http://victoria.mapstrat.com/current/google_transit.zip", "google_transit.zip")
    # unzip file
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
        os.remove(file) # clean up but leave the zip file

# close the SQLite database
conn.close()




