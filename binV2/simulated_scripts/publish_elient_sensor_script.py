import sys
import os

BASE_PATH = os.environ.get('JADOOP_HOME')
if BASE_PATH is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)
import json
import datetime
import random
import time
import csv
from binV2.publishers import Publisher

RADAR_Type = []
Role = []
Band = []
PRF_L = []
PRF_H = []
Peak_power_H = []
Peak_power_L = []
Range = []
Paltform = []
Freq_L = []
Freq_H = []
Pulse_Width = []

filename = '../utils/Radar_Data.csv'
with open(filename, mode='r') as file:
    # reading the CSV file
    csvFile = csv.DictReader(file)
    for row in csvFile:
        RADAR_Type.append(row['Radar Type/Code'])
        Role.append(row['Role'])
        Band.append(row['Band'])
        PRF_L.append(float(row['PRF Low']))
        PRF_H.append(float(row['PRF High']))
        Peak_power_L.append(row['Peak Pwr Low'])
        Peak_power_H.append(row['Peak Pwr High'])
        Range.append(row['Range'])
        Freq_L.append(int(row['Frequency Low']))
        Freq_H.append(int(row['Frequency High']))
        Pulse_Width.append(row['Pulse Width'])

# ---------------------------Frequency----------------------------


IP_array = ['192.168.1.15', '192.168.1.155', '192.168.1.100', '192.168.1.60', '192.168.1.180', '192.168.1.190']
Port_array = [5300, 5800, 5666, 5342, 5100, 5555]
Lat_array = [33.738045, 34.168751, 24.860966, 35.920834, 34.025917, 29.297670]
Long_array = [73.084488, 73.221497, 66.990501, 74.308334, 71.560135, 64.706734]


def select():
    global n
    n = 6
    # print("Enter the number of sensor u want to deploy")
    # n = int(input())
    if n > 6:
        print("out of range")
        select()
    elif n < 0:
        print("out of range")
        select()

    elif n != 0:
        return n


KAFKA_TOPIC = "elient_sensor_topic"


def Radar():
    if n == 1:
        y = 0
    else:
        y = random.randint(0, n - 1)

    q = len(RADAR_Type)
    global Frequency, PW, PRF
    guidStr = "0-ZZZ12345678"
    destinationStr = "0-AAA12345678"
    formatStr = "RADAR_SIMULATOR"

    letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

    iotmsg_data = """{\
           "data": {
           "guid": "%s",
           "destination": "%s",
           "eventTime": "%s", 
           "format": "%s",
           "Port No":"%s",
           "IP":"%s",
           "Latitude":"%s",
           "Longitude":"%s",
           "Frequency": "%s",
           "Pulse Width":" %s",
           "Pulse Repetition Frequency": "%s",
           "Range":"%s",
           "Peak Power":"%s"
           
       }
    }"""

    lat = Lat_array[y]

    lon = Long_array[y]

    port = Port_array[y]

    IP = IP_array[y]

    today = datetime.datetime.today()

    datestr = today.isoformat()
    r = random.randint(0, q - 1)
    Frequency = random.uniform(Freq_L[r], Freq_H[r])
    PW = Pulse_Width[r]
    PRF = random.uniform(PRF_L[r], PRF_H[r])
    if Peak_power_L[r] != '-':
        P_Power = random.uniform(int(Peak_power_L[r]), int(Peak_power_H[r]))
    else:
        P_Power = (Peak_power_L[r])
    range = Range[r]
    res = iotmsg_data % (
        guidStr, destinationStr, datestr, formatStr, port, IP, lat, lon, Frequency, PW, PRF, range, P_Power)
    res_json = json.loads(res)
    base_parser_obj = Publisher(KAFKA_TOPIC)
    base_parser_obj.publish_data(res_json, None, None)


def main():
    Radar()
    time.sleep(random.randint(5, 20))


select()
while True:
    main()
