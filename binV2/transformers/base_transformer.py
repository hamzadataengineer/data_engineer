import os
import sys

BASE_PATH = os.environ.get('JADOOP_HOME')
if BASE_PATH is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)
from abc import ABC, abstractmethod
from binV2.coupling.Connections import getElasticSearchObject


class BaseTransformer(ABC):

    def __init__(self):
        self.df = None
        self.es_connection = getElasticSearchObject('http://10.20.20.105:9200')

    @abstractmethod
    def load_data(self, spark, bootstrap_server, topic, offset):
        try:
            self.df = spark \
                .readStream \
                .format("kafka") \
                .option("failOnDataLoss", "false") \
                .option("kafka.bootstrap.servers", bootstrap_server) \
                .option("subscribe", topic) \
                .option("startingOffsets", offset) \
                .load()
            print('Printing Kafka Data on Console')
        except Exception as e:
            print(e)
            import traceback
            traceback.print_exc()

    @abstractmethod
    def clean_data(self):
        raise NotImplementedError

    @abstractmethod
    def transform(self, y):
        raise NotImplementedError

    @abstractmethod
    def write_back_to_kafka(self, value_df, bootstrap_server, processed_topic, checkpoint_dir):
        query = value_df \
            .selectExpr("CAST(value AS STRING)") \
            .writeStream \
            .format("kafka") \
            .option("checkpointLocation", './checkpoint-locations/' + checkpoint_dir) \
            .option("kafka.bootstrap.servers", bootstrap_server) \
            .option("topic", processed_topic) \
            .start()
        query.awaitTermination()
