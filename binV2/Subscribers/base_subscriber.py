import os
import sys

BASE_PATH = os.environ.get('JADOOP_HOME')
if BASE_PATH is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)
from configparser import ConfigParser
from binV2.coupling.Connections import getSparkObject


class KafkaConnectionConfigMixin:
    KAFKA_SERVER = None
    KAFKA_PORT = None
    KAFKA_HOST = None

    def configure(self, properties):
        self.KAFKA_HOST = properties.get('config', 'jadoop_kafka_host')
        self.KAFKA_PORT = properties.get('config', 'jadoop_kafka_port')
        self.KAFKA_SERVER = self.KAFKA_HOST + ":" + self.KAFKA_PORT
        return self.KAFKA_SERVER


class SparkConnectionConfigMixin:
    spark = None
    MASTER = None
    APP_NAME = None

    def configure(self, properties):
        self.MASTER = properties.get('setting', 'jadoop_master_name')
        self.APP_NAME = properties.get('setting', 'jadoop_app_name')
        print('Main Is Starting')
        self.spark = getSparkObject(self.MASTER, self.APP_NAME)
        print(f'..Spark Object is created.. : {self.spark}')
        return self.spark


class BaseSubscriber(KafkaConnectionConfigMixin, SparkConnectionConfigMixin):
    topic = None
    transformer = None

    def __init__(self):
        property_file = BASE_PATH + '/binV2/config/properties.ini'
        properties = ConfigParser()
        properties.read(property_file)
        self.kafka = KafkaConnectionConfigMixin().configure(properties)
        self.spark = SparkConnectionConfigMixin().configure(properties)

    def subscribe(self):
        self.transformer().load_data(self.spark, self.kafka, self.topic)
