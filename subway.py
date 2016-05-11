import pyspark
import datetime
import operator
import os
import sys
import time

def indexZones(shapeFilename):  ##creates rtree
    import rtree
    import fiona.crs
    import geopandas as gpd
    index = rtree.Rtree()
    zones = gpd.read_file(shapeFilename).to_crs(fiona.crs.from_epsg(2263))
    g = zones.geometry.buffer(900)
    zones = zones.set_geometry(g)
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    #return match
    for idx in match:
        if (zones.geometry[idx]).contains(p):
            return zones['name'][idx]
    return -1

def mapToZone(parts):
    import pyproj #only to conver long lat to x y in feets
    import shapely.geometry as geom #only to convert (x,y) to Point (x y)
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = indexZones('SubwayStations.geojson')
    #return zones.geometry
    for line in parts: #convert long lat to x y
        fields = line.strip().split(',')
        #50 is lat, #51 is long
        if all((fields[50],fields[51])):
            location  = geom.Point(proj(float(fields[51]), float(fields[50])))
            zone = findZone(location, index, zones)
            yield (zone, 1)

if __name__=='__main__':
    if len(sys.argv)<3:
        print "Usage: <input files> <output path>"
        sys.exit(-1)

    sc = pyspark.SparkContext()
    lines = sc.textFile('311_Service_Requests_from_2010_to_Present.csv')
    trips = lines.filter(lambda x: not x.startswith('Unique Key') and x != '')

    output = trips.mapPartitions(mapToZone).reduceByKey(lambda a, b: a+b)
    output.saveAsTextFile(sys.argv[-1])