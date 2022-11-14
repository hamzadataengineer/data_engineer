import json
from kafka import KafkaProducer
from binV2.parsers.generic_parser import GenericParser

KAFKA_SERVER = ["10.20.20.104:9092"]


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


producer = KafkaProducer(
    value_serializer=json_serializer,
    bootstrap_servers=KAFKA_SERVER
)


class Publisher:
    def __init__(self, topic):
        self.socket_conn = None
        self.topic = topic

    def parse_response(self, data, data_format, data_type):
        if type(data) == dict:
            return data
        parser = GenericParser(data, data_format, data_type)
        return parser.parse()

    def publish_data(self, data=None, schema=None, data_type=None):
        parsed_data = self.parse_response(data, schema, data_type)
        self.publish(parsed_data)

    def publish(self, data: dict):
        producer.send(self.topic, value=data)
        print(f'kafka send data to the topic: {self.topic}')
