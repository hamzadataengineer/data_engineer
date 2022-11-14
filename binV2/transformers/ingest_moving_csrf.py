import os
import sys

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

moving_crfs_transformation = udf(lambda s: MovingCrfsTransformation().transform(s))


class MovingCrfsTransformation(BaseTransformer):
    def __init__(self):
        super().__init__()
        self.timestamp = None
        self.offset = "latest"
        self.processed_topic = "processed_crfs_moving_topic"

    def load_data(self, spark, bootstrap_server, topic, offset=None):
        ##############This function is the concreate method which load data from base class##################
        ##############and apply transformation using udf function and write back data to kafka.###############
        super().load_data(spark, bootstrap_server, topic, self.offset)
        value_df = self.df.withColumn('value', col('value').cast('string')). \
            withColumn('value', moving_crfs_transformation('value'))
        self.write_back_to_kafka(value_df=value_df, bootstrap_server=bootstrap_server,
                                 processed_topic=self.processed_topic, checkpoint_dir="check-point-moving-crfs")

    def write_back_to_kafka(self, value_df, bootstrap_server, processed_topic, checkpoint_dir):
        super().write_back_to_kafka(value_df, bootstrap_server, processed_topic, checkpoint_dir)

    def clean_data(self):
        pass

    def transform(self, y):
        raw_date = datetime.now()
        timestamp = raw_date.strftime('%Y-%m-%d %H:%M:%S')
        date_str = convertToUTC(timestamp)
        x = json.loads(y)
        x['timestamp'] = date_str[0]
        JadoopStorage().saveToES(self.es_connection, 'crfs_moving_index', 'getMovingCrfsMapping', x)
        return json.dumps(x)
