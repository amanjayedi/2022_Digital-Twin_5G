import json
import os
import csv
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import urllib3
import ssl
from socket import socket
import urllib3
from urllib3 import request
from urllib3 import PoolManager
# from API import api_result

# with open('D:/AT&T/result.json') as json_file:

#     jsondata = json.load(json_file)


# df1 = pd.DataFrame(["CreatedTime","Req1" ], columns=['device'])
# df2 = pd.DataFrame(["CreatedTime","Req1" ], columns=['device'])
# df3 = pd.DataFrame(["CreatedTime","Req1" ], columns=['device'])

url="https://128.206.9.104/uaa/oauth/token?grant_type=password&username=Public&password=BXLinx!7_Public"

payload={}
files=[

]
headers = {
  'Authorization': 'Basic ZWF0b24tY2xpZW50OkFDdHZRQzFoSE4='
}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
resp = requests.post(url, headers = headers, verify=False)
# print(resp.text)
# resp = requests.post(url, headers = headers,data=json.dumps(params), cert=(certificate_file,certificate_key))
data_json = resp.json()

token = data_json["access_token"]


bearer_headers = {
  'Authorization': f'Bearer {token}'
}
refresh_url = "https://128.206.9.104/v2/public/C1B1/devices/display"

bearer_resp = requests.get(refresh_url, headers = bearer_headers, verify=False)
if bearer_resp.status_code == 200:
  bearer_resp_json = bearer_resp.json()
else:
  print("error")
  
# print(bearer_resp.text)
# print(type(bearer_resp.text))
api_result=bearer_resp.text

ind = 0
jsondata=json.loads(api_result)
# print(type(jsondata))

for data in jsondata:
    if('endpoints' not in data.keys()):
        continue

    endpoints = data['endpoints']
    createdTime = ''
    isOccupied = ''
    actualLevel = ''
    reading = ''

    if('deviceDecorators' in data.keys()):
        createdTime = data['deviceDecorators'][0]['createdTime']

    if ind == 0:
        df1 = pd.DataFrame(["CreatedTime", createdTime ], columns=['device'])
        df2 = pd.DataFrame(["CreatedTime",createdTime ], columns=['device'])
        df3 = pd.DataFrame(["CreatedTime",createdTime ], columns=['device'])

    for endpoint in endpoints:
        deviceId = endpoint['device']['publicId']
        endpoint_decorator = endpoint['endpointDecorators'][0]

        if('isOccupied' in endpoint_decorator.keys()):
            isOccupied = endpoint_decorator['isOccupied']
            break

    for endpoint in endpoints:
        deviceId = endpoint['device']['publicId']
        endpoint_decorator = endpoint['endpointDecorators'][0]
        
        if('actualLevel' in endpoint_decorator.keys()):
            actualLevel = endpoint_decorator['actualLevel']
            break

    for endpoint in endpoints:
        deviceId = endpoint['device']['publicId']
        endpoint_decorator = endpoint['endpointDecorators'][0]
        
        if('reading' in endpoint_decorator.keys()):
            reading = endpoint_decorator['reading']
            break

    data = pd.DataFrame([deviceId, isOccupied], columns=["device" + str(ind)])
    data2 = pd.DataFrame([deviceId, actualLevel], columns=["device" + str(ind)])
    data3 = pd.DataFrame([deviceId, reading], columns=["device" + str(ind)])
    df1 = df1.join(data)
    df2 = df2.join(data2)
    df3 = df3.join(data3)
    ind = ind + 1

# print(df1.iloc[1:len(df1),:])
if(os.path.exists('isOccupied.csv')):
    df1.iloc[1:len(df1),:].to_csv('isOccupied.csv', index=False,mode='a', header= False)
else:
    df1.to_csv('isOccupied.csv', index=False, header= False)

if(os.path.exists('actualLevel.csv')):
    df2.iloc[1:len(df2),:].to_csv('actualLevel.csv', index=False,mode='a', header= False)
else:
    df2.to_csv('actualLevel.csv', index=False, header= False)

if(os.path.exists('reading.csv')):
    df3.iloc[1:len(df3),:].to_csv('reading.csv', index=False,mode='a', header= False)
else:
    df3.to_csv('reading.csv', index=False, header= False)


