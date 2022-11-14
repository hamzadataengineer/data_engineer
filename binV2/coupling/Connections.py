import traceback
from pyspark.sql import SparkSession
import elasticsearch


def getElasticSearchObject(es_server):
    client = None
    # Create the client instance
    client = elasticsearch.Elasticsearch(es_server)

    # Successful response!
    print(client.info())

    return client


def getSparkObject(envn, appName):
    try:
        if envn == 'Development':
            master = 'local'
        else:
            master = 'yarn'
        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(e)
        traceback.print_exc()


if __name__ == "__main__":
    try:
        pass
        # es = getElasticSearchObject('http://10.20.20.105:9200')
        # print(es)
    except Exception as e:
        print(e)
