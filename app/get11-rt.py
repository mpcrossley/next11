#!/usr/bin/env python3
from google.transit import gtfs_realtime_pb2
import requests
import sqlite3
import pandas as pd
import os
import time
from flask import Flask, jsonify

# replace this with the URL for your GTFS real-time feed
feed_url = 'http://victoria.mapstrat.com/current/gtfrealtime_TripUpdates.bin'

app = Flask(__name__)
@app.route('/predictions/<stop_id>', methods=['GET'])
def get_predictions(stop_id):

    response = requests.get(feed_url)
    # add some error checking here
    if response.status_code != 200:
        return 'Error: {}'.format(response.status_code)
    
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    # open the SQLite database containing the static schedule
    conn = sqlite3.connect('gtfs.db')
    if conn is None:
        return 'Error: Could not connect to database'
    c = conn.cursor()

    predictions = []

    for entity in feed.entity:
        if entity.HasField('trip_update'):
            update = entity.trip_update.stop_time_update[0]
            if update.stop_id == stop_id:
                arrival_time = update.arrival.time

                # get the scheduled arrival time for the next stop from the static schedule
                c.execute(f"SELECT arrival_time FROM stop_times WHERE trip_id = '{entity.trip_update.trip.trip_id}' AND stop_id = '{stop_id}'")
                scheduled_arrival_time = c.fetchall()[0][0]

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

    conn.close()

    return jsonify(predictions)


if __name__ == '__main__':
    app.run(port=5000)  # run the server on port 5000