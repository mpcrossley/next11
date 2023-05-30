from google.transit import gtfs_realtime_pb2
import requests
import sqlite3
import pandas as pd
import os
import time
from flask import Flask, jsonify


# stop_id = '2517'  # replace this with your stop id
# replace this with the URL for your GTFS real-time feed
feed_url = 'http://victoria.mapstrat.com/current/gtfrealtime_TripUpdates.bin'

os.chdir('static') # change directory to

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



# # get all the stops for the 11
# stops = pd.read_sql_query('SELECT * FROM stop_times WHERE stop_id=2517 AND stop_headsign="11 Tillicum Ctr Via Gorge"', c)
# # print(stops.trip_id)

# # create a loop that runs every 30 seconds
# while True:
#     # get the RT feed
#   feed = gtfs_realtime_pb2.FeedMessage()
#   response = requests.get('http://victoria.mapstrat.com/current/gtfrealtime_TripUpdates.bin')
#   # add some error checking here if response.status_code != 200:
#   if response.status_code != 200:
#       print('error - could not retrieve GTFS-RT')
#       exit()
#   feed.ParseFromString(response.content)
#   # if feed is empty, exit
#   if not feed:
#       print('error - empty feed')
#       exit()
#   for entity in feed.entity:
#     if entity.HasField('trip_update'):
#       print(entity.trip_update)

#   for entity in feed.entity:
#       if entity.HasField('trip_update'):
#           update = entity.trip_update.stop_time_update[0]
#           if update.stop_id == stop_id:
#               arrival_time = update.arrival.time
#               # convert arrival time from unix time to a human-readable format
#               arrival_time = pd.to_datetime(arrival_time, unit='s')
#               print(f'Trip {entity.trip_update.trip.trip_id} will arrive at stop {stop_id} at {arrival_time}.')

#               # get the scheduled arrival time for the next stop from the static schedule
#               c.execute(f"SELECT arrival_time FROM stop_times WHERE trip_id = '{entity.trip_update.trip.trip_id}' AND stop_id = '{stop_id}'")
#               #scheduled_arrival_time = c.fetchone()[0]
#               scheduled_arrival_time = c.fetchall()[0][0]
#               # calculate the delay
#               delay = arrival_time - scheduled_arrival_time

#               # get the scheduled arrival times for the next five stops
#               c.execute(f"SELECT stop_id, arrival_time FROM stop_times WHERE trip_id = '{entity.trip_update.trip.trip_id}' AND stop_sequence > (SELECT stop_sequence FROM stop_times WHERE trip_id = '{entity.trip_update.trip.trip_id}' AND stop_id = '{stop_id}') ORDER BY stop_sequence LIMIT 5")
#               for row in c.fetchall():
#                   future_stop_id, future_scheduled_arrival_time = row
#                   # adjust the scheduled arrival time by the delay to get a predicted arrival time
#                   future_predicted_arrival_time = future_scheduled_arrival_time + delay
#                   print(f'Trip {entity.trip_update.trip.trip_id} will arrive at stop {future_stop_id} at approximately {future_predicted_arrival_time}.')
#   # wait 30 seconds
#   print('waiting 30 seconds...')
#   time.sleep(30)

# c.close()










## Example one arriving to yates and fernwood:
# trip {
#   trip_id: "1643051"
# }
# vehicle {
#   id: "991"
# }
# stop_time_update {
#   stop_sequence: 40
#   stop_id: "2517"
#   arrival {
#     time: 1685427300
#   }
# }

# print('searching for trip_id in stops.trip_id')
# for entity in feed.entity:
#   if entity.HasField('trip_update'):
#     # look for trip_id in stops.trip_id
#     print(entity.trip_update.trip.trip_id)
#     # if entity.trip_update.trip.trip_id in stops.trip_id.values:
#     if entity.trip_update.trip.trip_id == "1644378":
#         print(entity.trip_update)
#         print(entity.trip_update.trip.trip_id)
#         print(entity.trip_update.stop_time_update)
#         print(entity.trip_update.stop_time_update[0].arrival.time)
#         print(entity.trip_update.stop_time_update[0].stop_id)
#         print(entity.trip_update.stop_time_update[0].stop_sequence)
#         print(entity.trip_update.stop_time_update[0].departure.time)
#         print(entity.trip_update.stop_time_update[0].schedule_relationship)
#         print(entity.trip_update.stop_time_update[0].arrival.delay)
#         print(entity.trip_update.stop_time_update[0].departure.delay)
    # else:
    #     print("not found")
