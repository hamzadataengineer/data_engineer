import sys
import os

BASE_PATH = os.environ.get('JADOOP_HOME')
if BASE_PATH is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)
import json
import time
import datetime
import random
from binV2.simulated_scripts.utils import dist, calculation
import csv
from binV2.publishers import Publisher

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
Speed = []
Heading = []

global x, y

filename = '../utils/Lat_Long.csv'
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
            Speed.append(float(row['Speed']))
            Heading.append(float(row['Heading']))
            y = y + 1
        x = x + 1

iotmsg_data = """{\
        "data": {
            "eventTime": "%s",
            "Group":"%s",
            "Node-1":"%s",
            "Sensor-1 Latitude":"%s",
            "Sensor-1 Longitude":"%s",
            "LOB-1":"%s",
            "Operating Voltage Sensor-1":"%s",
            "Operating Current Sensor-1":"%s",
            "Node-2":"%s",
            "Sensor-2 Latitude":"%s",
            "Sensor-2 Longitude":"%s",
            "LOB-2":"%s",
            "Operating Voltage Sensor-2":"%s",
            "Operating Current Sensor-2":"%s",
            "Node-3":"%s",
            "Sensor-3 Latitude":"%s",
            "Sensor-3 Longitude":"%s",
            "LOB-3":"%s",
            "Operating Voltage Sensor-3":"%s",
            "Operating Current Sensor-3":"%s",
            "Emitter Latitude":"%s",
           "Emitter Longitude":"%s"
        }}"""
w = 0
q = 0

for q in range(0, x):
    lat1.append(Sensor_latitude[q])

    lon1.append(Sensor_longitude[q])
    # marker(Sensor_latitude[q],Sensor_longitude[q])
# print(lat1)
# print(lon1)

for w in range(0, y):
    lat2.append(Emitter_latitude[w])

    lon2.append(Emitter_longitude[w])
    # marker1(Emitter_latitude[w],Emitter_longitude[w])

R = 6378.1  # Radius of the Earth

# b = -163.95226514961917 #Bearing is 90 degrees converted to radians.

KAFKA_TOPIC = "crfs_moving_topic"


def method_name():
    print("Enter the speed")
    v = int(input())
    t = 2
    d = (v * t) / 3600
    print(d)
    print("Enter the Heading")
    b = float(input())
    print("*****")
    return b, d, t, v


r = 0
for i in range(0, y):

    # print(i)
    for j in range(0, 3):
        # print(GMP[r])
        if GMP[r] == i + 1:
            dist1 = dist(lat1[r], lon1[r], lat2[i], lon2[i])
            # print(Bearing_DATA(lat1[r], lat2[i],lon1[r], lon2[i]))
            # print(dist1)
            r = r + 1


def Print_Data():
    today = datetime.datetime.today()
    datestr = today.isoformat()  # print(p)
    print(iotmsg_data % (
        datestr, Node[p - 3], lat1[p - 3], lon1[p - 3], Bearing[p - 3], Node[p - 2], lat1[p - 2], lon1[p - 2],
        Bearing[p - 2], Node[p - 1], lat1[p - 1], lon1[p - 1], Bearing[p - 1], lat2[i], lon2[i]))


z = 0
u = random.randint(105, 175)
array = ['a', 'b']

while True:
    p = 0
    f = 0
    Bearing = []
    volt = []
    current = []
    t = 15
    for i in range(0, y):

        v = (Speed[i])
        d = (v * t) / 3600
        # print(d)
        b = Heading[i]

        # print(i)

        for j in range(0, 3):
            if z < u:
                volt.append(random.randint(24, 28))
                current.append(random.uniform(1, 1.5))
            else:
                f = random.choice(array)

                if (f == 'a'):
                    volt.append(random.randint(29, 38))
                    current.append(random.uniform(1, 1.5))
                else:
                    volt.append(random.randint(24, 28))
                    current.append(random.uniform(0.5, 0.9))

                u = u + random.randint(75, 95)

            if GMP[p] == i + 1:

                ret = calculation(lat1[p], lon1[p], lat2[i], lon2[i], d, b, v)

                try:
                    Bearing.append(ret[2])
                    if p == f + 2:
                        lat2[i] = ret[0]
                        lon2[i] = ret[1]
                        f = f + 3
                except Exception as e:
                    print("")

                p = p + 1
                z = z + 1

        today = datetime.datetime.today()
        datestr = today.isoformat()

        try:
            re = iotmsg_data % (datestr, GMP[p - 1], Node[p - 3], lat1[p - 3], lon1[p - 3], Bearing[p - 3],
                                volt[p - 3], current[p - 3], Node[p - 2], lat1[p - 2], lon1[p - 2], Bearing[p - 2],
                                volt[p - 2],
                                current[p - 2], Node[p - 1], lat1[p - 1], lon1[p - 1], Bearing[p - 1], volt[p - 1],
                                current[p - 1],
                                lat2[i], lon2[i])
            print('\n')
            print(re)
            json_data = json.loads(re)
            base_parser_obj = Publisher(KAFKA_TOPIC)
            base_parser_obj.publish_data(json_data, None, None)
        except Exception as e:
            print(e)
            print("Error")
    time.sleep(t)
