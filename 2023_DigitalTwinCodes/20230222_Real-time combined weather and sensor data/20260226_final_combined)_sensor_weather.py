import csv
import json
import boto3
import requests
import datetime as dt
import pytz
#import joblib
import io

def lambda_handler(event, context):
    # Replace <API_KEY>, <LATITUDE>, and <LONGITUDE> with your own API key, latitude, and longitude
    api_key = '5216ae97235c334c1499bb33170d0500'
    latitude = 38.94
    longitude = -92.32

    # Get the current timestamp in US Central Time
    central = pytz.timezone('US/Central')
    now = dt.datetime.now(central)
    timestamp = now.strftime('%d/%m/%Y %H:%M:%S')

    # Read the header row from the CSV file stored in S3
    s3 = boto3.client('s3')
    bucket_name = 'attdigitaltwin'
    weather_data_key_name = 'weather_data.csv'
    sensor_data_key_name = 'sensor_data.csv'
    combined_weather_sensor_key_name = 'combined_weather_sensor.csv'

    # Read data from weather_data.csv and append the current weather data
    response = s3.get_object(Bucket=bucket_name, Key=weather_data_key_name)
    header_row = response['Body'].read().decode('utf-8').splitlines()[0].split(',')
    data_list = [timestamp, '', latitude, longitude, '', '', '', '', '', '', '', '', '', '']
    response = requests.get(f'https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}')
    if response.status_code == 200:
        data = response.json()
        temperature = round(data['main']['temp'] - 273.15, 2) # Convert from Kelvin to Celsius and round to 2 decimal places
        humidity = data['main']['humidity']
        wind_speed = data['wind']['speed']
        wind_direction = data['wind']['deg']
        pressure = data['main']['pressure']
        data_list[1] = data["name"]
        
        #data_list[2] = latitude
        #data_list[3] = longitude
        
        data_list[4] = temperature
        data_list[5] = humidity
        data_list[6] = wind_speed
        data_list[7] = wind_direction
        data_list[8] = pressure

        # Read data from sensor_data.csv
        response = s3.get_object(Bucket=bucket_name, Key=sensor_data_key_name)
        sensor_data_reader = csv.reader(response['Body'].read().decode('utf-8').splitlines())
        next(sensor_data_reader) # Skip header row

        # Read data from combined_weather_sensor.csv and append the combined data
        s3_resource = boto3.resource('s3')
        s3_object = s3_resource.Object(bucket_name, combined_weather_sensor_key_name)
        csv_body = s3_object.get()['Body'].read().decode('utf-8')
        csv_lines = csv_body.strip().split('\n')
        combined_data_list = []
        for row in sensor_data_reader:
            if row[0][:16] == timestamp[:16]:
                #data_list[3] = row[0]
                data_list[3] = longitude
                data_list[9] = row[1]
                data_list[10] = row[2]
                data_list[11] = row[3]
                data_list[12] = row[4]
                data_list[13] = row[5]
                
                
                combined_data_list.append(data_list)
        csv_lines.extend([','.join([str(i) for i in row]) for row in combined_data_list])
        csv_body = '\n'.join(csv_lines)
        s3_object.put(Body=csv_body)
        
        

