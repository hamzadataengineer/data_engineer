import sys
import os

BASE_PATH = os.environ.get('JADOOP_HOME')
if BASE_PATH is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)

import datetime
import json
import random
import time
import csv
from binV2.publishers import Publisher
from utils import Bearing_DATA, intersect

GMP = []
Node = []
Sensor_latitude = []
Sensor_longitude = []
Emitter_latitude = []
Emitter_longitude = []
lat1 = []
lon1 = []
lat2 = []
lon2 = []

global x, y

filename = '../utils/DATA_Sensor.csv'
with open(filename, mode='r') as file:
    # reading the CSV file
    csvFile = csv.DictReader(file)
    x = 0
    y = 0

    # displaying the contents of the CSV file
    for row in csvFile:
        GMP.append(int(row['Group']))
        Node.append(row['Node'])
        Sensor_latitude.append(float(row['Sensor Latitude']))
        Sensor_longitude.append(float(row['Sensor Longitude']))
        if (row['Emitter Latitude']) != '':
            Emitter_latitude.append(float(row['Emitter Latitude']))
            Emitter_longitude.append(float(row['Emitter Longitude']))
            y = y + 1
        x = x + 1

global u
global max
w = 0
q = 0

for q in range(0, x):
    lat1.append(Sensor_latitude[q])
    lon1.append(Sensor_longitude[q])
for w in range(0, y):
    lat2.append(Emitter_latitude[w])
    lon2.append(Emitter_longitude[w])

uuid = "d7b166be-13a0-41a0-9fd1-b54b3f207aee"
task_ID = "5b746e13-bafe-4d2b-a3c0-5bc961d2617b"
KAFKA_TOPIC = "crfs_static_topic"


def Bearing_Data():
    u = 0
    S_lat = []
    S_lon = []
    Bear = []
    event = "bearing.BearingData"
    iotmsg_data = """{\
         "data": {
         "Event": "%s",
         "eventTime": "%s",
         "GMP": "%s",
         "Active Nodes":"%s",
         "Master Node":"%s",
         "Node":"%s",
         "Lat":"%s",
         "long":"%s",
         "bearing":"%s",
         "Node":"%s",
         "Lat":"%s",
         "long":"%s",
         "bearing":"%s",
         "Node":"%s",
         "Lat":"%s",
         "long":"%s",
          "bearing":"%s",
          "Emitter Lat":"%s",
          "Emitter Lon":"%s",
          "Center Frequency":"%s",
          "Bandwidth":"%s",
          "event_uuid":"%s",
          "task_id":"%s"
         }}"""
    payload = 'Group Mission Processor'
    f = random.choice(GMP)
    time.sleep(0.3)
    u = (f * 3) - 1
    E_lat = lat2[f - 1]
    E_lon = lon2[f - 1]
    for i in range(0, 3):
        S_lat.append(lat1[u - i])
        S_lon.append(lon1[u - i])
        Bear.append(Bearing_DATA(S_lat[i], E_lat, S_lon[i], E_lon))

    res = intersect(S_lat[0], S_lon[0], Bear[0], S_lat[1], S_lon[1], Bear[1], S_lat[2], S_lon[2], Bear[2])
    freq = random.uniform(2031982336.0, 3031982336.0)
    BW = 2197265.0
    k = random.randint(40, 60)
    l = random.randint(0, 2)
    for i in range(0, k):
        today = datetime.datetime.today()
        datestr = today.isoformat()
        re = iotmsg_data % (event, datestr, payload, GMP[u], Node[u - l], Node[u - 2], S_lat[0], S_lon[0],
                            Bear[0], Node[u - 1], S_lat[1], S_lon[1], Bear[1],
                            Node[u], S_lat[2], S_lon[2], Bear[2], res[0], res[1], freq, BW, uuid, task_ID)
        print('\n\n')
        json_data = json.loads(re)
        base_parser_obj = Publisher(KAFKA_TOPIC)
        base_parser_obj.publish_data(json_data, None, None)


while True:
    Bearing_Data()
    time.sleep(random.randint(8, 15))
