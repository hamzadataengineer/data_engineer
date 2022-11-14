import csv
from datetime import datetime, timedelta
from binV2.utils.common import convertToUTC
from pytz import timezone
import pytz
from binV2.publishers import Publisher
import time

KAFKA_TOPIC = "vessel_topic_test"


class PublishVesselsScript:

    def __init__(self):
        self.document = list()
        self.schema = ['MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG', 'COG', 'Heading',
                       'VesselName', 'IMO','CallSign','VesselType', 'Status','Length',
                       'Width','Draft','Cargo','TransceiverClass']

    def publishCsv(self):
        with open("../vessels.csv") as file:
            reader = csv.DictReader(file)
            for row in reader:
                base_parser_obj = Publisher(KAFKA_TOPIC)
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
