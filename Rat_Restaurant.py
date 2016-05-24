import datetime
import operator
import os
import sys
import time
import heapq
import pyspark


def indexZones(shapeFilename):
    import rtree
    import fiona.crs
    import geopandas as gpd
    index = rtree.Rtree()
    zones = gpd.read_file(shapeFilename).to_crs(fiona.crs.from_epsg(2263))
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if any(map(lambda x: x.contains(p), zones.geometry[idx])):
            return zones['neighborhood'][idx]
    return -1



def mapToZone(parts):
    import csv
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = indexZones('neighborhoods.geojson')
    reader = csv.reader(parts)
    for line in reader:
        if all((line[1],line[2])):
            
            restaurant_location  = geom.Point(proj(float(line[2]), float(line[1])))
            
            
            restaurant_zone = findZone(restaurant_location, index, zones)
            
            if restaurant_zone != -1:
                yield ( str(restaurant_zone), 1)

def mapToZone2(parts):
    import pyproj #only to conver long lat to x y in feets
    import shapely.geometry as geom #only to convert (x,y) to Point (x y)
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = indexZones('neighborhoods.geojson')
    #return zones.geometry
    for line in parts: #convert long lat to x y
        fields = line.strip().split(',')
        #50 is lat, #51 is long
        if all((fields[50],fields[51])):
            location  = geom.Point(proj(float(fields[51]), float(fields[50])))
            zone = findZone(location, index, zones)
            if zone!=-1:
                yield (str(zone), 1)


if __name__=='__main__':
    if len(sys.argv)<3:
        print "Usage: <input files> <output path>"
        sys.exit(-1)

    sc = pyspark.SparkContext()
    # read 311 call dataset and map calls to neighborhoods, finally count the number of calls in each neighborhoods.
    call = sc.textFile(sys.argv[1])
    call = call.filter(lambda x: not x.startswith('Unique Key') and x != '')
    callOuput = call.mapPartitions(mapToZone2).reduceByKey(lambda a, b: a+b) 
    
     # read restaurant dataset and map restaurants to neighborhoods, finally count the number of restaurants in each neighborhoods.
    restaurant = sc.textFile(sys.argv[2], use_unicode=False)
    restaurantOutput = restaurant.mapPartitions(mapToZone).reduceByKey(lambda a, b: a+b)
  
    joinResult = callOuput.join(restaurantOutput)
    
    joinResult.saveAsTextFile(sys.argv[-1])
