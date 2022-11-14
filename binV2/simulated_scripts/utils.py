import mpu
import geopy

from pygeodesy import sphericalNvector
from geopy.distance import geodesic
from geographiclib.geodesic import Geodesic



def Print():
    print("Ok")


def Bearing_DATA(lat1, lat2, long1, long2):
     brng = Geodesic.WGS84.Inverse(lat1, long1, lat2, long2)['azi1']
     return brng


def dist(lat1, lon1, lat2, lon2):
    dist = mpu.haversine_distance((lat1, lon1), (lat2, lon2))
    return dist


def calculation(lat1,lon1,lat2,lon2,d,b,v):

        dist1=dist(lat1, lon1, lat2, lon2)

        if dist1 < 10:

            origin = geopy.Point(lat2, lon2)
            destination = geodesic(kilometers=d).destination(origin, b)

            lat, lon = destination.latitude, destination.longitude


            #print(" Latitude: ",lat,"\n","Longitude: ",lon)
            lat2=lat
            lon2=lon




            dist1 = dist(lat1, lon1, lat2, lon2)
            bearing=Bearing_DATA(lat1, lat2, lon1, lon2)


            return lat2,lon2,bearing


def intersect(lat1, long1, b1, lat2, long2, b2, lat3, long3, b3):
    s = sphericalNvector.LatLon(lat1, long1)
    e1 = sphericalNvector.LatLon(lat2, long2)
    e2 = sphericalNvector.LatLon(lat3, long3)
    g1 = s.intersection(b1, e1, b2)
    g2 = s.intersection(b1, e2, b3)

    lat = (g1.lat + g2.lat) / 2

    lon = (g1.lon + g2.lon) / 2

    return (lat, lon)