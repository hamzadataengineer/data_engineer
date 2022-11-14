import csv
import json
import time
from datetime import datetime, timedelta
import pytz
from binV2.utils.common import convertToUTC
from binV2.publishers import Publisher
import pandas as pd

KAFKA_TOPIC = "air_craft_test"


class PublishAirCrafts:
    def __init__(self):
        self.document = list()
        self.schema = ['icao24', 'lat', 'lon', 'velocity', 'heading', 'vertrate', 'callsign',
                       'onground', 'alert', 'spi', 'squawk', 'baroaltitude', 'geoaltitude',
                       'lastposupdate', 'lastcontact']

    def start(self):
        with open("../states_2022-06-27-00.csv", 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                base_parser_obj = Publisher(KAFKA_TOPIC)
                base_parser_obj.publish_data(row, self.schema, 'csv')


###############Main function#################
if __name__ == '__main__':
    obj = PublishAirCrafts()
    while True:
        try:
            obj.start()
        except Exception as e:
            print(e)
            obj.start()
