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


DATABASE = 'gtfs.db'
ZIPURL = 'https://bct.tmix.se/Tmix.Cap.TdExport.WebApi/gtfs/?operatorIds=12'
REGION = 'Campbell River'
RTUPDATES_URL = 'https://bct.tmix.se/gtfs-realtime/tripupdates.pb?operatorIds=12'
RTVEHICLES_URL = 'https://bct.tmix.se/gtfs-realtime/vehiclepositions.pb?operatorIds=12'

# if google_transit.zip exists, skip download
if not os.path.exists('google_transit.zip'):
    # download file from url
    urllib.request.urlretrieve(ZIPURL, "google_transit.zip")
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
        # 'calendar.txt',
        'calendar_dates.txt',
        'fare_attributes.txt',
        'fare_rules.txt',
        'shapes.txt',
        'feed_info.txt',
    ]

    #delete existing database
    if os.path.exists(DATABASE):
        os.remove(DATABASE)

    # create a new SQLite database
    conn = sqlite3.connect(DATABASE)

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



app = Flask(__name__)

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row  # This allows to access the rows as dictionaries instead of tuples
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

# Get all the stops
@app.route('/api/static/stops', methods=['GET'])
def get_stops():
    cur = get_db().cursor()
    stops = cur.execute('SELECT * FROM stops').fetchall()  
    if stops:
        return jsonify(dict(row) for row in cur.fetchall())
    else:
        return jsonify({'error': 'Stops not found'}), 404
    
@app.route('/api/static/stops/<stop_id>', methods=['GET'])
def get_stop(stop_id):
    cur = get_db().cursor()
    stop = cur.execute('SELECT * FROM stops WHERE stop_id = ?', (stop_id,)).fetchone() 
    if stop:
        return jsonify(dict(stop))
    else:
        return jsonify({'error': 'Stop not found'}), 404

@app.route('/api/static/routes/', methods=['GET'])
def get_routes():
    cur = get_db().cursor()
    routes = cur.execute('SELECT * FROM routes').fetchall()
    if routes:
        return jsonify(dict(routes))
    else:
        return jsonify({'error': 'Route not found'}), 404
    
@app.route('/api/static/routes/<route_id>', methods=['GET'])
def get_route(route_id):
    cur = get_db().cursor()
    route = cur.execute('SELECT * FROM routes WHERE route_id = ?', (route_id,)).fetchone()
    if route:
        return jsonify(dict(route))
    else:
        return jsonify({'error': 'Route not found'}), 404

@app.route('/api/static/trips/<route_id>', methods=['GET'])
def get_trips(route_id):
    # example of a query with parameters
    route_id = request.args.get('route_id')
    if route_id:
        cur = get_db().cursor()
        trips = cur.execute('SELECT * FROM trips WHERE route_id = ?', (route_id,)).fetchall()
        return jsonify([dict(trip) for trip in trips])
    else:
        return jsonify({'error': 'Invalid parameters'}), 400
    
@app.route('/api/static/routes/<route_id>/stops', methods=['GET'])
def get_stops_for_route(route_id):
    db = get_db()
    cur = db.cursor()

    # Single query to get all stops for the given route
    query = """
    SELECT stops.*
    FROM stops
    JOIN stop_times ON stops.stop_id = stop_times.stop_id
    JOIN trips ON stop_times.trip_id = trips.trip_id
    WHERE trips.route_id = ?
    GROUP BY stops.stop_id
    """
    cur.execute(query, (route_id,))
    stops = [dict(row) for row in cur.fetchall()]

    return jsonify(stops)

@app.route('/api/static/stops', methods=['GET'])
def search_stops():
    search = request.args.get('q', '')
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT * FROM stops WHERE stop_name LIKE ?', ('%' + search + '%',))
    stops = [dict(row) for row in cur.fetchall()]
    return jsonify(stops)

@app.route('/api/static/routes/<route_id>/trips', methods=['GET'])
def get_trips_for_route(route_id):
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT * FROM trips WHERE route_id = ?', (route_id,))
    trips = [dict(row) for row in cur.fetchall()]
    return jsonify(trips)

@app.route('/api/static/routes/<route_id>/schedule', methods=['GET'])
def get_schedule_for_route(route_id):
    print("called")
    db = get_db()
    cur = db.cursor()

    # Get all trips for the route
    cur.execute('SELECT trip_id FROM trips WHERE route_id = ?', (route_id,))
    trip_ids = [row['trip_id'] for row in cur.fetchall()]
    print(trip_ids)

    # Convert the trip_ids into a string of comma-separated question marks
    placeholders = ', '.join('?' for trip_id in trip_ids)

    # Execute a single query to fetch all the stop_times for the trip_ids
    cur.execute(f'SELECT trip_id, stop_id, arrival_time FROM stop_times WHERE trip_id IN ({placeholders}) ORDER BY trip_id, stop_sequence', trip_ids)

    # Initialize an empty dictionary to hold the schedules
    schedules = {}

    # Process the rows from the query
    for row in cur.fetchall():
        trip_id, stop_id, arrival_time = row

        # If this trip_id is not already in the schedules dictionary, add it with an empty list
        if trip_id not in schedules:
            schedules[trip_id] = []

        # Add this stop's data to the trip's schedule
        schedules[trip_id].append({
            'stop_id': stop_id,
            'arrival_time': arrival_time,
        })

    # Convert the schedules dictionary to a list of dictionaries for compatibility with the previous code
    schedule = [{'trip_id': trip_id, 'stop_times': stop_times} for trip_id, stop_times in schedules.items()]


    return jsonify(schedule)


@app.route('/api/realtime/predictions/<stop_id>', methods=['GET'])
def get_predictions(stop_id):
    response = requests.get(RTUPDATES_URL, allow_redirects=True)
    # add some error checking here
    if response.status_code != 200:
        return 'Error: {}'.format(response.status_code)
    
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    # print(response.content)
    # feed_dict = MessageToDict(feed)
    # print(json.dumps(feed_dict, indent=4))

    # open the SQLite database containing the static schedule
    conn = sqlite3.connect('gtfs.db')
    if conn is None:
        return 'Error: Could not connect to database'
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

    conn.close()

    return jsonify(predictions)

if __name__ == '__main__':
    app.run(host='0.0.0.0')
