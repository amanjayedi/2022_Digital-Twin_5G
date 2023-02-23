import csv
import json
import boto3

# Import requests and pytz from the AWS Lambda layer named python
import requests
import datetime as dt
import pytz

def get_weather_data():
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
    key_name = 'weather_data.csv'
    response = s3.get_object(Bucket=bucket_name, Key=key_name)
    header_row = response['Body'].read().decode('utf-8').splitlines()[0].split(',')

    # Make a request to the API and get the response
    response = requests.get(f'https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}')

    # Check if the response was successful
    if response.status_code == 200:
        # Extract the relevant data from the response
        data = response.json()
        temperature = round(data['main']['temp'] - 273.15, 2) # Convert from Kelvin to Celsius and round to 2 decimal places
        humidity = data['main']['humidity']
        wind_speed = data['wind']['speed']
        wind_direction = data['wind']['deg']
        pressure = data['main']['pressure']
        
        # Create a list of the data to write to the CSV file
        data_list = [timestamp, data["name"], latitude, longitude, temperature, humidity, wind_speed, wind_direction, pressure]

        # Write the data to the CSV file stored in S3
        s3_resource = boto3.resource('s3')
        s3_object = s3_resource.Object(bucket_name, key_name)
        csv_body = s3_object.get()['Body'].read().decode('utf-8')
        csv_lines = csv_body.strip().split('\n')
        csv_lines.append(','.join([str(i) for i in data_list]))
        csv_body = '\n'.join(csv_lines)
        s3_object.put(Body=csv_body)

        # Print the data
        print(f'{header_row[0]}: {timestamp}') # Use the name of the city returned by the API
        print(f'{header_row[1]}: {data["name"]}') # Use the name of the city returned by the API
        print(f'{header_row[2]}: {latitude}')
        print(f'{header_row[3]}: {longitude}')
        print(f'{header_row[4]}: {temperature} °C') # Add units to the print statement
        print(f'{header_row[5]}: {humidity}')
        print(f'{header_row[6]}: {wind_speed} m/s')
        print(f'{header_row[7]}: {wind_direction} degrees')
        print(f'{header_row[8]}: {pressure} hPa')
    else:
        # If the response was not successful, print the status code
        print(f'Request failed with status code {response.status_code}')
        
def read_and_write_csv():
    # Define the S3 bucket name and file names
    s3_bucket_name = 'attdigitaltwin'
    weather_data_file_name = 'weather_data.csv'
    other_data_file_name = 'other_data.csv'

    # Get the latest timestamp from the weather data file
    s3 = boto3.client('s3')
    weather_data_response = s3.get_object(Bucket=s3_bucket_name, Key=weather_data_file_name)
    weather_data_reader = csv.reader(weather_data_response['Body'].read().decode('utf-8').splitlines())
    next(weather_data_reader)  # Skip the header row
    latest_timestamp = max(row[0] for row in weather_data_reader)

    # Get the other data from the other data file
    other_data_response = s3.get_object(Bucket=s3_bucket_name, Key=other_data_file_name)
    other_data_reader = csv.reader(other_data_response['Body'].read().decode('utf-8').splitlines())
    other_data_header_row = next(other_data_reader)  # Get the header row
    other_data_rows = [row for row in other_data_reader if row[0][:16] == latest_timestamp[:16]] # Filter by latest timestamp

    # Combine the weather data and other data
    combined_rows = [[latest_timestamp] + row[1:] for row in other_data_rows]

    # Write the combined data back to the weather data file
    weather_data_rows = [[latest_timestamp] + row[1:] for row in combined_rows]
    weather_data_body = '\n'.join([','.join(row) for row in weather_data_rows])
    s3_resource = boto3.resource('s3')
    weather_data_object = s3_resource.Object(s3_bucket_name, weather_data_file_name)
    weather_data_object.put(Body=weather_data_body)

    # Print the combined data
    for row in combined_rows:
        print(f'{other_data_header_row[0]}: {row[0]}')  # Use the timestamp from the weather data file
        print(f'{other_data_header_row[1]}: {row[1]}')
        print(f'{other_data_header_row[2]}: {row[2]}')
        # Add more columns as needed


def lambda_handler(event, context):
    get_weather_data()
    read_and_write_csv()
    return {
        'statusCode': 200,
        'body': json.dumps('Data successfully written to CSV')
    }

