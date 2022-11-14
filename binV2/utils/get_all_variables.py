import os
import sys

# set jadoop home environment variable
# current_working_directory = os.getcwd()
os.environ['Jadoop_Home'] = '/home/hamza-rnd/PycharmProjects/jadoops'
BASE_PATH = os.getenv("Jadoop_Home")
if os.getenv("Jadoop_Home") is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)
