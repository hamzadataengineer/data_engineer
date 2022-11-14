import sys
import os
import time

from pyspark.sql.types import StructType, StringType, StructField

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
from pyspark.sql.functions import col, udf, from_json, to_json, struct

moving_vehicle_transformation = udf(lambda s: MovingVehicleTransformation().transform(s))
write_back_to_kafka = True


class MovingVehicleTransformation(BaseTransformer):
    def __init__(self):
        super().__init__()
        self.timestamp = None
        self.offset = "latest"
        self.processed_topic = "processed_moving_vehicle_topic"

    def load_data(self, spark, bootstrap_server, topic, offset=None):
        schema = StructType([
            StructField("VehicleID", StringType()),
            StructField("gpsvalid", StringType()),
            StructField("lat", StringType()),
            StructField("lon", StringType()),
            StructField("timestamp_internal", StringType()),
            StructField("speed", StringType()),
            StructField("heading", StringType()),
            StructField("for_hire_light", StringType()),
            StructField("engine_acc", StringType()),
            StructField("timestamp", StringType()),
            StructField("coordinates", StructType([
                StructField("lat", StringType()),
                StructField("lon", StringType()),
            ])),
            StructField("_index", StringType()),
            StructField("_type", StringType())
        ])
        super().load_data(spark, bootstrap_server, topic, self.offset)
        value_df = self.df.withColumn('value', col('value').cast('string')). \
            withColumn('value', moving_vehicle_transformation('value'))
        json_df = value_df.select(from_json(col("value").cast("string"), schema).alias("value"))
        final_df = json_df.selectExpr('value.*').drop('lat', 'lon', 'timestamp', '_index', '_type').withColumn("value",to_json(struct("*")).cast("string"))
        self.write_back_to_kafka(value_df=final_df, bootstrap_server=bootstrap_server,
                                 processed_topic=self.processed_topic, checkpoint_dir="check-point-moving-vehicle")

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
            final_doc['VehicleID'] = y["VehicleID"]
            final_doc['engine_acc'] = y["engine_acc"]
            final_doc['for_hire_light'] = y['for_hire_light']
            final_doc['gpsvalid'] = y['gpsvalid']
            final_doc['heading'] = y['heading']
            final_doc['coordinates'] = y['coordinates']
            final_doc['speed'] = y['speed']
            final_doc['timestamp_internal'] = y['timestamp_internal']
            final_doc['timestamp'] = y['timestamp']
            JadoopStorage().saveToES(self.es_connection, 'moving_vehicle_index', 'getmovingvehicleMapping', final_doc)
        return json.dumps(y, sort_keys=True, indent=1, default=str)
