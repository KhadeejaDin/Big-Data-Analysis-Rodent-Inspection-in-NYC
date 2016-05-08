import datetime
import operator
import os
import sys
import time
import heapq
import pyspark

### trip_YEAR.csv

###  0: vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
###  4: trip_distance, pickup_longitude, pickup_latitude,
###  7: dropoff_longitude, dropoff_latitude, fare_amount, surcharge,
### 11: mta_tax, tip_amount, tolls_amount, total_amount, payment_type
###
### CMT,2011-01-28 10:38:19,2011-01-28 10:42:18,1,0.4,-73.968582,40.759171,-73.964333,40.764664,1,N,4.1,0,0.5,0,0,4.6,CSH

def indexZones(shapeFilename):
    import rtree
    import fiona.crs
    import geopandas as gpd
    index = rtree.Rtree()
    zones = gpd.read_file(shapeFilename).to_crs(fiona.crs.from_epsg(2263))
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findNeighborhoodZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if any(map(lambda x: x.contains(p), zones.geometry[idx])):
            return zones['neighborhood'][idx]
    return -1

def findBoroughZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if any(map(lambda x: x.contains(p), zones.geometry[idx])):
            return zones['borough'][idx]
    return -1

def top3(data):
    return heapq.nlargest(3, data, key=lambda k: k[1])

def mapToZone(parts):
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = indexZones('neighborhoods.geojson')

    for line in parts:

        fields = line.strip().split(',')
        if all((fields[5],fields[6],fields[9],fields[10])):
            # pickup_time = datetime.datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S").timetuple()
            # date = (pickup_time.tm_year,
            #       pickup_time.tm_mon,
            #        pickup_time.tm_mday)

            # Trips 2011 May
            # if (date>=(2011, 05, 01)) and (date<=(2011, 05, 31)):
            #    passenger_count = int(fields[3])
            pickup_location  = geom.Point(proj(float(fields[5]), float(fields[6])))
            dropoff_location = geom.Point(proj(float(fields[9]), float(fields[10])))

            pickup_zone = findNeighborhoodZone(pickup_location, index, zones)
            dropoff_zone = findBoroughZone(dropoff_location, index, zones)
            #    dow = pickup_time.tm_wday
            #    tod = pickup_time.tm_hour

            #if pickup_zone>=0 and dropoff_zone>=0:
            yield (( pickup_zone, dropoff_zone), 1)

if __name__=='__main__':
    if len(sys.argv)<3:
        print "Usage: <input files> <output path>"
        sys.exit(-1)

    sc = pyspark.SparkContext()

    lines = sc.textFile(','.join(sys.argv[1:-1]))
    trips = lines.filter(lambda x: not x.startswith('vendor_id') and x != '')

    output = trips.mapPartitions(mapToZone).reduceByKey(lambda a, b: a+b)

    output.saveAsTextFile(sys.argv[-1])
