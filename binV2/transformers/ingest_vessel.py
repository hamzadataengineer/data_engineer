import sys
import os

BASE_PATH = os.environ.get('JADOOP_HOME')
if BASE_PATH is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)
from binV2.datawarehouse.jadoop_storage import JadoopStorage
from datetime import datetime
from binV2.utils.common import convertToUTC, covertDTtoUTC
import json
from binV2.transformers.base_transformer import BaseTransformer
from pyspark.sql.functions import col, udf

vessel_transformation = udf(lambda s: Vessel().transform(s))
write_back_to_kafka = False


class Vessel(BaseTransformer):
    def __init__(self):
        super().__init__()
        self.timestamp = None
        self.offset = "latest"
        self.processed_topic = "processed_vessel_topic"

    def load_data(self, spark, bootstrap_server, topic, offset=None):
        ##############This function is the concreate method which load data from base class##################
        ##############and apply transformation using udf function and write back data to kafka.###############
        super().load_data(spark, bootstrap_server, topic, self.offset)
        value_df = self.df.withColumn('value', col('value').cast('string')). \
            withColumn('value', vessel_transformation('value'))
        self.write_back_to_kafka(value_df=value_df, bootstrap_server=bootstrap_server,
                                 processed_topic=self.processed_topic, checkpoint_dir="check-point-vessel")

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
        base_date_time = y['BaseDateTime']
        BaseDateTimeObj = convertToUTC(base_date_time, '%Y-%m-%dT%H:%M:%S')
        y['timestamp'] = date_str[0]
        lat = y['LAT']
        lon = y['LON']
        if lat is not None and lon is not None:
            coordinates = {'lat': lat, 'lon': lon}
            y['coordinates'] = coordinates
            final_doc['BaseDateTime'] = BaseDateTimeObj[0]
            final_doc['CallSign'] = y['CallSign']
            final_doc['Cargo'] = y['Cargo']
            final_doc['COG'] = y['COG']
            final_doc['coordinates'] = y['coordinates']
            final_doc['Draft'] = y['Draft']
            final_doc['Heading'] = y['Heading']
            final_doc['IMO'] = y['IMO']
            final_doc['Length'] = y['Length']
            final_doc['MMSI'] = y['MMSI']
            final_doc['SOG'] = y['SOG']
            final_doc['Status'] = y['Status']
            final_doc['TransceiverClass'] = y['TransceiverClass']
            final_doc['timestamp'] = y['timestamp']
            final_doc['VesselName'] = y['VesselName']
            final_doc['VesselType'] = y['VesselType']
            final_doc['Width'] = y['Width']
            JadoopStorage().saveToES(self.es_connection, 'vessel_index', 'getVesselMapping', final_doc)
        return json.dumps(y, sort_keys=True, indent=1, default=str)
