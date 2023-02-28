import boto3
import csv
import joblib
import io

s3_client = boto3.client('s3')
bucket_name = 'attdigitaltwin'
prediction_model_key_name = 'DigitalTwin_DT2.joblib'
combined_weather_sensor_key_name = 'combined_weather_sensor.csv'

def lambda_handler(event, context):
    
    # Download the DigitalTwin_DT.joblib file from S3
    prediction_model_obj = s3_client.get_object(Bucket=bucket_name, Key=prediction_model_key_name)
    prediction_model_data = prediction_model_obj['Body'].read()
    
    # Load the ML-based prediction model
    prediction_model = joblib.load(io.BytesIO(prediction_model_data))

    # Download the combined_weather_sensor.csv file from S3
    combined_weather_sensor_obj = s3_client.get_object(Bucket=bucket_name, Key=combined_weather_sensor_key_name)
    combined_weather_sensor_data = combined_weather_sensor_obj['Body'].read().decode('utf-8')

    # Parse the contents of the CSV file into a list of dictionaries
    csv_data = []
    with io.StringIO(combined_weather_sensor_data) as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            csv_data.append(row) 

    # Process the data as needed
    last_row = csv_data[-1]
    prediction_realtime = prediction_model.predict([[float(last_row['Temperature (degC)']), float(last_row['Humidity (%)']), float(last_row['Wind Speed (m/s)']), float(last_row['Wind Direction (deg)']), float(last_row['Pressure (hPa)']), float(last_row['PeoplePerArea(p/m2)']), float(last_row['LightingPowerIntensity(W/m2)']), float(last_row['EquipmentPowerIntensity(W/m2)']), float(last_row['ACH'])]])
    last_row['CarbonEmissions(lb-co2/h)'] = prediction_realtime[0]

    # Combine the data into a string for writing back to the file
    csv_write_data = io.StringIO()
    csv_writer = csv.DictWriter(csv_write_data, fieldnames=csv_data[0].keys())
    csv_writer.writeheader()
    for row in csv_data[:-1]:
        csv_writer.writerow(row)
    csv_writer.writerow(last_row)

    # Save the updated data back to the combined_weather_sensor.csv file in S3
    s3_client.put_object(Bucket=bucket_name, Key=combined_weather_sensor_key_name, Body=csv_write_data.getvalue().encode('utf-8'))

    return {
        'statusCode': 200,
        'body': 'Data is appended to combined csv file after ML based prediction.'
    }
