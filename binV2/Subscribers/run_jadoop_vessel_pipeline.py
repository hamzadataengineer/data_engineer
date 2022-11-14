import sys
import os

BASE_PATH = os.environ.get('JADOOP_HOME')
if BASE_PATH is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)
import time
import traceback
from binV2.Subscribers.base_subscriber import BaseSubscriber
from binV2.transformers.ingest_vessel import Vessel


class RunJadoopVessel(BaseSubscriber):
    topic = 'vessel_topic_test'
    transformer = Vessel


if __name__ == '__main__':
    while True:
        try:
            run_jadoop_pipline_obj = RunJadoopVessel()
            run_jadoop_pipline_obj.subscribe()
        except Exception as e:
            time.sleep(15)
            print('Subscription Error:', e)
            traceback.print_exc()
