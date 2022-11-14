import csv
import time

from binV2.publishers import Publisher

KAFKA_TOPIC = "moving_vehicle_topic"


class PublishVesselsScript:

    def __init__(self):
        self.document = list()
        self.schema = ['VehicleID', 'gpsvalid', 'lat', 'lon', 'timestamp_internal', 'speed', 'heading',
                       'for_hire_light', 'engine_acc']

    def publishCsv(self):
        with open("../utils/moving_vehicle.csv") as file:
            reader = csv.reader(file)
            base_parser_obj = Publisher(KAFKA_TOPIC)
            for row in reader:
                base_parser_obj.publish_data(row, self.schema, 'csv')


############ Main Function ###########
if __name__ == '__main__':
    publish_vessels_csv = PublishVesselsScript()
    while True:
        try:
            publish_vessels_csv.publishCsv()
        except Exception as e:
            print(e)
            publish_vessels_csv.publishCsv()
