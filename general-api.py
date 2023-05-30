import sqlite3
from flask import Flask, jsonify, g, request

DATABASE = 'static/gtfs.db'  # replace with your database path

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

@app.route('/api/stops/<stop_id>', methods=['GET'])
def get_stop(stop_id):
    cur = get_db().cursor()
    stop = cur.execute('SELECT * FROM stops WHERE stop_id = ?', (stop_id,)).fetchone()  # replace with your table and column names
    if stop:
        return jsonify(dict(stop))
    else:
        return jsonify({'error': 'Stop not found'}), 404

@app.route('/api/routes/<route_id>', methods=['GET'])
def get_route(route_id):
    cur = get_db().cursor()
    route = cur.execute('SELECT * FROM routes WHERE route_id = ?', (route_id,)).fetchone()  # replace with your table and column names
    if route:
        return jsonify(dict(route))
    else:
        return jsonify({'error': 'Route not found'}), 404

@app.route('/api/trips', methods=['GET'])
def get_trips():
    # example of a query with parameters
    route_id = request.args.get('route_id')
    if route_id:
        cur = get_db().cursor()
        trips = cur.execute('SELECT * FROM trips WHERE route_id = ?', (route_id,)).fetchall()  # replace with your table and column names
        return jsonify([dict(trip) for trip in trips])
    else:
        return jsonify({'error': 'Invalid parameters'}), 400
    
@app.route('/api/routes/<route_id>/stops', methods=['GET'])
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

@app.route('/api/stops', methods=['GET'])
def search_stops():
    search = request.args.get('q', '')
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT * FROM stops WHERE stop_name LIKE ?', ('%' + search + '%',))
    stops = [dict(row) for row in cur.fetchall()]
    return jsonify(stops)

@app.route('/api/routes/<route_id>/trips', methods=['GET'])
def get_trips_for_route(route_id):
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT * FROM trips WHERE route_id = ?', (route_id,))
    trips = [dict(row) for row in cur.fetchall()]
    return jsonify(trips)

@app.route('/api/routes/<route_id>/schedule', methods=['GET'])
def get_schedule_for_route(route_id):
    db = get_db()
    cur = db.cursor()

    # Get all trips for the route
    cur.execute('SELECT trip_id FROM trips WHERE route_id = ?', (route_id,))
    trip_ids = [row['trip_id'] for row in cur.fetchall()]

    # Get stop times for these trips
    schedule = []
    for trip_id in trip_ids:
        cur.execute('SELECT stop_id, arrival_time FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence', (trip_id,))
        trip_schedule = [dict(row) for row in cur.fetchall()]
        schedule.append({
            'trip_id': trip_id,
            'stop_times': trip_schedule,
        })

    return jsonify(schedule)


if __name__ == '__main__':
    app.run(debug=True)
