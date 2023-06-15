#!/usr/bin/env python3
import sqlite3
from flask import Flask, jsonify, g, request
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import requests
import sqlite3
import pandas as pd
import os
import time
import zipfile
import urllib
import json

## This back-end component will poll the realtime feed every 30 seconds and store the results in a SQLite database.
## The database will be queried by the front-end component to display the realtime data.

DATABASE = 'gtfs.db'
ZIPURL = 'https://bct.tmix.se/Tmix.Cap.TdExport.WebApi/gtfs/?operatorIds=12'
REGION = 'Campbell River'
RTUPDATES_URL = 'https://bct.tmix.se/gtfs-realtime/tripupdates.pb?operatorIds=12'
RTVEHICLES_URL = 'https://bct.tmix.se/gtfs-realtime/vehiclepositions.pb?operatorIds=12'

# create table for realtime data
conn = sqlite3.connect(DATABASE)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS realtime
                (trip_id text, route_id text, route_short_name text, route_long_name text, direction_id text, trip_headsign text, vehicle_id text, vehicle_label text, vehicle_license_plate text, arrival_time text, departure_time text, stop_id text, stop_sequence text, stop_name text, stop_lat text, stop_lon text, timestamp text)''')
conn.commit()
conn.close()

conn = sqlite3.connect('gtfs.db')
if conn is None:
    print('Error: Could not connect to database');
c = conn.cursor()

# pull realtime data every 30 seconds
while True:
    response = requests.get(RTUPDATES_URL, allow_redirects=True)
    # add some error checking here
    
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    # print(response.content)
    # feed_dict = MessageToDict(feed)
    # print(json.dumps(feed_dict, indent=4))

    # open the SQLite database containing the static schedule
    conn = sqlite3.connect('gtfs.db')
    c = conn.cursor()

    predictions = []

    for entity in feed.entity:
        # print(entity)
        if entity.HasField('trip_update'):
            update = entity.trip_update.stop_time_update[0]
            # print(update)
            # print(entity.trip_update.trip.trip_id)
            # print(stop_id)
            if update.stop_id == stop_id:
                arrival_time = update.arrival.time

                # get the scheduled arrival time for the next stop from the static schedule
                print(entity.trip_update.trip.trip_id)
                print(stop_id)
                c.execute(f"SELECT arrival_time FROM stop_times WHERE trip_id = '{entity.trip_update.trip.trip_id}' AND stop_id = '{stop_id}'")
                scheduled_arrival_time = c.fetchall()[0][0]
                # add some error checking here
                if scheduled_arrival_time is None:
                    print('Error: Could not find scheduled arrival time for stop')

                # print(scheduled_arrival_time)

                # calculate the delay
                delay = arrival_time - scheduled_arrival_time

                # get the scheduled arrival times for the next five stops
                c.execute(f"SELECT stop_id, arrival_time FROM stop_times WHERE trip_id = '{entity.trip_update.trip.trip_id}' AND stop_sequence > (SELECT stop_sequence FROM stop_times WHERE trip_id = '{entity.trip_update.trip.trip_id}' AND stop_id = '{stop_id}') ORDER BY stop_sequence LIMIT 5")
                for row in c.fetchall():
                    future_stop_id, future_scheduled_arrival_time = row
                    # adjust the scheduled arrival time by the delay to get a predicted arrival time
                    future_predicted_arrival_time = future_scheduled_arrival_time + delay

                    predictions.append({
                        'trip_id': entity.trip_update.trip.trip_id,
                        'future_stop_id': future_stop_id,
                        'future_predicted_arrival_time': future_predicted_arrival_time
                    })

    jsonify(predictions)
    time.sleep(30) # wait 30 seconds before pulling data again
                