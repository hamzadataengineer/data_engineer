import sys
import os

BASE_PATH = os.environ.get('JADOOP_HOME')
if BASE_PATH is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)
from binV2.datawarehouse.jadoop_storage import JadoopStorage
from datetime import datetime
from binV2.utils.common import convertToUTC
import json
from binV2.transformers.base_transformer import BaseTransformer
from pyspark.sql.functions import col, udf

air_craft_transformation = udf(lambda s: AirCraft().transform(s))
write_back_to_kafka = False


class AirCraft(BaseTransformer):
    def __init__(self):
        super().__init__()
        self.timestamp = None
        self.offset = "latest"
        self.processed_topic = "processed_air_craft_topic"

    def load_data(self, spark, bootstrap_server, topic, offset=None):
        ##############This function is the concreate method which load data from base class##################
        ##############and apply transformation using udf function and write back data to kafka.###############
        super().load_data(spark, bootstrap_server, topic, self.offset)
        value_df = self.df.withColumn('value', col('value').cast('string')). \
            withColumn('value', air_craft_transformation('value'))
        self.write_back_to_kafka(value_df=value_df, bootstrap_server=bootstrap_server,
                                 processed_topic=self.processed_topic, checkpoint_dir="check-point-air-craft")

    def write_back_to_kafka(self, value_df, bootstrap_server, processed_topic, checkpoint_dir):
        super().write_back_to_kafka(value_df, bootstrap_server, processed_topic, checkpoint_dir)

    def clean_data(self):
        pass

    def transform(self, y):
        final_doc = dict()
        raw_date = datetime.now()
        timestamp = raw_date.strftime('%Y-%m-%d %H:%M:%S')
        date_str = convertToUTC(timestamp)
        y = json.loads(y)
        y['timestamp'] = date_str[0]
        lat = y['lat']
        lon = y['lon']
        if lat is not None and lon is not None:
            coordinates = {'lat': lat, 'lon': lon}
            y['coordinates'] = coordinates
            final_doc['icao24'] = y["icao24"]
            final_doc['velocity'] = y['velocity']
            final_doc['heading'] = y['heading']
            final_doc['vertrate'] = y['vertrate']
            final_doc['callsign'] = y['callsign']
            final_doc['onground'] = y['onground']
            final_doc['alert'] = y['alert']
            final_doc['spi'] = y['spi']
            final_doc['squawk'] = y['squawk']
            final_doc['baroaltitude'] = y['baroaltitude']
            final_doc['geoaltitude'] = y['geoaltitude']
            final_doc['lastposupdate'] = y['lastposupdate']
            final_doc['lastcontact'] = y['lastcontact']
            final_doc['coordinates'] = y['coordinates']
            final_doc['timestamp'] = y['timestamp']
            JadoopStorage().saveToES(self.es_connection, 'air_craft_index', 'getAirCraftMapping', final_doc)
        return json.dumps(y, sort_keys=True, indent=1, default=str)
