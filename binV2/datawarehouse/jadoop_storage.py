from elasticsearch.helpers import bulk
import json
from kafka import KafkaProducer
from binV2.mapping.index_mappings import IndexMappings
from datetime import datetime


class JadoopStorage:
    def __init__(self):
        self.index_mapping_obj = IndexMappings()

    def createDocument(self, documents, es_index):
        documents['_index'] = es_index
        documents['_type'] = "_doc"
        yield documents

    def getMapping(self, mapping):
        get_mapping = None
        if mapping == "getTemperatureMapping":
            get_mapping = self.index_mapping_obj.getTemperatureMapping()
        if mapping == "getCo2Mapping":
            get_mapping = self.index_mapping_obj.getCo2Mapping()
        if mapping == "getPowerMapping":
            get_mapping = self.index_mapping_obj.getPowerMapping()
        if mapping == "getVesselMapping":
            get_mapping = self.index_mapping_obj.getVesselMapping()
        if mapping == "getSensorMapping":
            get_mapping = self.index_mapping_obj.getSensorMapping()
        if mapping == "getweatherMapping":
            get_mapping = self.index_mapping_obj.getweatherMapping()
        if mapping == "getElientSensorMapping":
            get_mapping = self.index_mapping_obj.getElientSensorMapping()
        if mapping == "getStaticCrfsMapping":
            get_mapping = self.index_mapping_obj.getStaticCrfsMapping()
        if mapping == "getMovingCrfsMapping":
            get_mapping = self.index_mapping_obj.getMovingCrfsMapping()
        if mapping == "getblightersensorMapping":
            get_mapping = self.index_mapping_obj.getBlighterSensorMapping()
        if mapping == "getAirCraftMapping":
            get_mapping = self.index_mapping_obj.getAirCraftMapping()
        if mapping == "getmovingvehicleMapping":
            get_mapping = self.index_mapping_obj.getmovingvehicleMapping()
        return get_mapping

    def saveToES(self, es, index, mapping, documents=None):
        try:
            raw_date = datetime.now()
            es_index = index + "_" + raw_date.strftime("%Y.%m.%d")
            get_mapping = self.getMapping(mapping)
            if not es.indices.exists(index=es_index):
                es.indices.create(index=es_index, body=get_mapping)
            result = bulk(es, self.createDocument(documents, es_index))
            if result[0] is not None and result[0] > 0:
                print("Successfully Inserted " + str(result[0]) + " documents in the " + str(index) + " index")
        except Exception as e:
            import traceback
            traceback.print_exc()
            print('Error:', e)

    def insertAlertInKafka(self, write_topic, documents=None):
        self.pushDataToKafka(documents, write_topic)

    def saveToKafka(self, write_topic, documents=None):
        # for doc in documents:
        # doc['correlation_id'] = correlation_id
        try:
            self.pushDataToKafka(documents, write_topic)
        except Exception as e:
            print(e)

    def json_serializer(self, data):
        return json.dumps(data).encode('utf-8')

    def pushDataToKafka(self, processed_event, write_topic):
        producer = KafkaProducer(bootstrap_servers='10.20.20.104:9092',
                                 value_serializer=self.json_serializer,
                                 compression_type='gzip')
        try:
            print("Attempting to Write on :" + write_topic)
            producer.send(write_topic, value=processed_event)
            producer.flush()
        except Exception as e:
            print("Error: ", e)
        finally:
            producer.close()
