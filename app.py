import os
import json
import boto3
import redis
import pandas as pd
from flask import Flask, request, jsonify
from dotenv import load_dotenv

app = Flask(__name__)
r = redis.StrictRedis(host='localhost', port=6379, db=0)

def store_csv_data_in_redis(bucket_name, file_name):
    try:
        ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
        SECRET_KEY = os.environ['AWS_ACCESS_SECRET_ID']
    except KeyError:
        return "AWS credentials not found", 500

    try:
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        
        # reading the csv file from s3 bucket
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        df = pd.read_csv(obj['Body'])
    except Exception as e:
        return e, 500

    # convert time_stamp and sts dates to datetime
    df['time_stamp'] = pd.to_datetime(df['time_stamp'])
    df['sts'] = pd.to_datetime(df['sts'])

    df = df.sort_values(by='sts', ascending=True) # sorting data

    for _, row in df.iterrows():
        device_id = str(int(row['device_fk_id']))
        data = {
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'time_stamp': row['time_stamp'].isoformat(),
            'sts': row['sts'].isoformat(),
            'speed': row['speed']
        }

        r.set(device_id, json.dumps(data)) 

    # store the csv raw data in Redis
    r.set('raw_data', df.to_json(orient='records', date_format='iso'))


def get_raw_data():
    try:
        # fetched the raw data from Redis
        raw_data = r.get('raw_data')
        if raw_data:
            #converting it back to the dataframe
            #start_end_locations and location_points needs raw data
            return pd.read_json(raw_data.decode('utf-8'), convert_dates=['time_stamp', 'sts'])
        else:
            return None
    except redis.exceptions.ConnectionError as e:
        print(e)
        return None


@app.route('/get_all_data', methods=['GET'])
def get_all_data():
    try:
        # we already store the complete data for later use inside redis
        # get it from reds
        raw_data = r.get('raw_data')
        if not raw_data:
            return "No data in Redis cache", 404

        # convert the value to a dataframe and return as JSON
        df = pd.read_json(raw_data.decode('utf-8'), convert_dates=['time_stamp', 'sts'])
        return df.to_json(orient='records', date_format='iso')
    except redis.exceptions.ConnectionError as e:
        return e, 500


@app.route('/latest_device_info', methods=['GET'])
def latest_device_info():
    device_id = request.args.get('device_id')
    if device_id:
        try:
            # fetching the latest information for the given device id
            data = r.get(device_id)
            if not data:
                return "Device ID not found", 404
            data = json.loads(data.decode('utf-8')) 
            return jsonify(data)
        except redis.exceptions.ConnectionError as e:
            return e, 500
    else:
        return "Device ID not provided", 400


@app.route('/fetch_start_end_location', methods=['GET'])
def fetch_start_end_location():
    device_id = request.args.get('device_id')
    if device_id:
        try:
            raw_data = get_raw_data()
            if raw_data is None:
                return "Unable to fetch raw data", 500

            # filter raw data for the given device id
            device_data = raw_data[raw_data['device_fk_id'] == int(device_id)]
            if device_data.empty:
                return "Device ID not found", 404

            # start and endpoint (lat, lon) which is a tuple
            start_location = (device_data.iloc[0]['latitude'], device_data.iloc[0]['longitude'])
            end_location = (device_data.iloc[-1]['latitude'], device_data.iloc[-1]['longitude'])

            print({'start_location': start_location, 'end_location': end_location})
            return jsonify({'start_location': tuple(start_location), 'end_location': tuple(end_location)})
        except Exception as e:
            return e, 500
    else:
        return "Device ID not provided", 400


@app.route('/fetch_location_points', methods=['GET'])
def fetch_location_points():
    device_id = request.args.get('device_id')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')

    if device_id and start_time and end_time:
        try:
            raw_data = get_raw_data()
            if raw_data is None:
                return "Unable to fetch raw data", 500

            # filter raw data for the given device id
            device_data = raw_data[raw_data['device_fk_id'] == int(device_id)]
            # filter all the records ranging inside the start and ed timestamp
            device_data = device_data[(device_data['time_stamp'] >= start_time) & (device_data['time_stamp'] <= end_time)]

            location_points = device_data[['latitude', 'longitude', 'time_stamp']].to_dict('records')
            return jsonify(location_points)
        except Exception as e:
            return e, 500
    else:
        return "Required parameters not provided", 400


if __name__ == '__main__':
    load_dotenv()
    # bucket is not public, you need a key and secret to access the file of bucket
    bucket_name = 'carnot-bucket'
    file_name = 'data/data.csv'

    store_csv_data_in_redis(bucket_name, file_name)
    app.run(host='0.0.0.0', port=8000)
