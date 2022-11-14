import os
import sys
BASE_PATH = os.environ.get('JADOOP_HOME')
if BASE_PATH is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)
from binV2.datawarehouse.jadoop_storage import JadoopStorage
import psycopg2
from datetime import datetime
from binV2.utils.common import convertToUTC
import json
from binV2.transformers.base_transformer import BaseTransformer
from pyspark.sql.functions import col, udf

signal_shark_transformation = udf(lambda s: SignalSharkTransformer().transform(s))


class SignalSharkTransformer(BaseTransformer):

    def __init__(self):
        super().__init__()
        self.conn = psycopg2.connect(database="jadoop_main", user='jadoop_main', password='1234',
                                     host='10.20.20.107', port='5432')
        self.lob = list()
        self.lob_weight = None
        self.timestamp = None
        self.offset = "earliest"
        self.processed_topic = "processed_signal_shark_sensor_topic"

    def load_data(self, spark, bootstrap_server, topic, offset=None):
        ##############This function is the concreate method which load data from base class##################
        ##############and apply transformation using udf function and write back data to kafka.###############
        super().load_data(spark, bootstrap_server, topic, self.offset)
        value_df = self.df.withColumn('value', col('value').cast('string')). \
            withColumn('value', signal_shark_transformation('value'))
        self.write_back_to_kafka(value_df=value_df, bootstrap_server=bootstrap_server, processed_topic=self.processed_topic, checkpoint_dir="check-point-signal-shark")

    def write_back_to_kafka(self, value_df, bootstrap_server, processed_topic, checkpoint_dir):
        super().write_back_to_kafka(value_df, bootstrap_server, processed_topic, checkpoint_dir)

    def clean_data(self):
        pass

    def transform(self, y):
        lob_weight = None
        raw_date = datetime.now()
        timestamp = raw_date.strftime('%Y-%m-%d %H:%M:%S')
        date_str = convertToUTC(timestamp)
        x = json.loads(y)
        x['timestamp'] = date_str[0]
        lat = x['data']['GnssLatitude']
        lon = x['data']['GnssLongitude']
        azimuth = x['data']['Azimuth']
        azimuth_val = str(azimuth).split('.')[0]
        # Creating a cursor object using the cursor() method
        cursor = self.conn.cursor()
        # Executing an MYSQL function using to execute() method
        cursor.execute("select norm_val from lob_normalization where degrees ='" + str(azimuth_val) + "'")
        # Fetching 1st row from the table
        result = cursor.fetchone()
        if result is not None:
            for res in result:
                self.lob.append(res)
                lob_weight = res
        if lat is not None and lon is not None:
            coordinates = {'lat': lat, 'lon': lon}
            x['coordinates'] = coordinates
        else:
            coordinates = {'lat': 0.0, 'lon': 0.0}
            x['coordinates'] = coordinates
        x['lob_weight'] = lob_weight
        JadoopStorage().saveToES(self.es_connection, 'signal_shark_sensor_index_test',
                                 'getSensorMapping', x)
        return json.dumps(x)
