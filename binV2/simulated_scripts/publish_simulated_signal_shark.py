import sys
import os

BASE_PATH = os.environ.get('JADOOP_HOME')
if BASE_PATH is None:
    print("Please set the Jadoop_Home Environment Variables it is missing")
    exit(-1)
sys.path.insert(0, BASE_PATH)
import json
import datetime
import csv
import random
from binV2.publishers import Publisher

KAFKA_TOPIC = "signal_shark_topic"
filename = '../utils/bearings.csv'

iotmsg_data = """{\
        "data": {
            "TimeStampSyuncFlag":"%s",
            "TimeStamp":"%s",
            "ScanNumber":"%s",
            "BearingID":"%s",
            "BearingElements":"%s",
            "Overdriven":"%s",
            "VectorLost":"%s",
            "ValidBearing":"%s",
            "Azimuth":"%s",
            "AzimuthCorrection":"%s",
            "Elevation":"%s",
            "DFQuality":"%s",
            "DetectorValue":"%s",
            "CompassID":"%s",
            "CompassElements":"%s",
            "CompassAzimuth":"%s",
            "CompassElevation":"%s",
            "CompassRoll":"%s",
            "GnssID":"%s",
            "GnssElements":"%s",
            "GnssFrozenFlag":"%s",
            "Gnss3DFlag":"%s",
            "GnssSatellite":"%s",
            "GnssLatitude":"%s",
            "GnssLongitude":"%s",
            "GnssAltitude":"%s",
            "GnssSpeed":"%s",
            "GnssCourse":"%s",
            "TimeStampSyncFlag":"%s",
            "TimeStampSeconds":"%s",
            "TimeStampFractional":"%s",
            "ScanNumber":"%s",
            "FreqID":"%s",
            "FreqElements":"%s",
            "FreqList":"%s",
            "Operating Voltage":"%s",
            "Operating Current":"%s",
            "Power Level":"%s"

        }}"""


def main():
    while True:
        with open(filename, mode='r') as file:
            # reading the CSV file
            csvFile = csv.DictReader(file)
            print(csvFile)
            # displaying the contents of the CSV file
            for row in csvFile:
                Timestamp = row['Time stamp']
                ScanNumber = row['Scan no']
                BearingID = row['Bearing ID']
                BearingElements = row['Bearing elements']
                Overdriven = row['Overdriven']
                VectorLost = row['Vector Lost']
                ValidBearing = row['Valid Bearing']
                Azimuth = row['Azimuth']
                AzimuthCorrection = row['Azimuth Correction']
                Elevation = row['Elevation']
                DFQuality = row['DF quality']
                DetectorValue = row['Detector Value']
                CompassID = row['Compass ID']
                CompassElements = row['Compass elements']
                CompassAzimuth = row['Compass azimuth']
                CompassElevation = row['Compass elevation']
                CompassRoll = row['Compass roll']
                GnssID = row['Gnss ID']
                GnssElements = row['Gnss elements']
                GnssFrozenFlag = row['Gnss frozen flag']
                Gnss3DFlag = row['Gnss 3d flag']
                GnssSatellite = row['Gnss satellite']
                GnssLatitude = row['Gnss latitue']
                GnssLongitude = row['Gnss logitude']
                GnssAltitude = row['Gnss altitude']
                GnssSpeed = row['Gnss speed']
                GnssCourse = row['Gnss course']
                TimeSt = row['TimeStampSyncFlag']
                TimeSts = row['TimeStampSeconds']
                TimeStF = row['TimeStampFractional']
                SNo = row['ScanNumber']
                FID = row['FreqID']
                Freqele = row['FreqElements']
                FreqList = row['FreqList']
                Volt = random.uniform(24, 28)
                Current = random.uniform(1, 1.5)
                Power = random.uniform(-40, -120)

                today = datetime.datetime.today()
                datestr = today.isoformat()
                res = iotmsg_data % (Timestamp, datestr, ScanNumber, BearingID, BearingElements, Overdriven,
                                     VectorLost, ValidBearing, Azimuth, AzimuthCorrection, Elevation, DFQuality,
                                     DetectorValue, CompassID, CompassElements, CompassAzimuth, CompassElevation,
                                     CompassRoll, GnssID, GnssElements, GnssFrozenFlag, Gnss3DFlag, GnssSatellite,
                                     GnssLatitude, GnssLongitude, GnssAltitude, GnssSpeed, GnssCourse, TimeSt,
                                     TimeSts, TimeStF, SNo, FID, Freqele, FreqList, Volt, Current, Power)
                json_data = json.loads(res)
                base_parser_obj = Publisher(KAFKA_TOPIC)
                base_parser_obj.publish_data(json_data, None, None)


if __name__ == '__main__':
    try:
        print('main function is started')
        print(main())
    except Exception as e:
        print(e)
        import traceback
        traceback.print_exc()
