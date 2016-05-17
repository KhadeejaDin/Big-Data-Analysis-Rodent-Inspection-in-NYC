import datetime
import operator
import os
import sys
import time
import heapq
import pyspark

def indexZones(buildingfiles):  ##creates rtree 
    import rtree
    import fiona.crs
    import geopandas as gpd
    import shapely.geometry as geom
    index = rtree.Rtree()
    import csv
    #if index==0:
        #lines.next()
    dic ={}
    with open(buildingfiles,'rb') as f:
        reader = csv.DictReader(f)
        inx =0
        for row in reader:
        #if row[2]!='s':
        #if row[0] =='Borough': continue
            if row['YearBuilt'] !='0':
                if row['XCoord'].strip() != '' and row['YCoord'].strip() != '':

                    
                    point  = geom.Point(float(row['XCoord']), float(row['YCoord']))  #point=POINT (1012703.999983049 255827.0144377612)
                    g = point.buffer(20) # create a polygon,Polygon has a list of Points which correspond to polygon corners (self.corners)
            
                    index.insert(inx, g.bounds)
                    dic[inx] = (row['YearBuilt'],g)
                    inx +=1
            
    return (index, dic)




def findZone(p, index, dic):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if (dic[idx][1]).contains(p): # this is the line I added
            return dic[idx][0]
    return -1






def mapToZone(parts):
    import pyproj #only to conver long lat to x y in feets
    import shapely.geometry as geom #only to convert (x,y) to Point (x y)
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = indexZones('BoroughsBuildingAge.csv')
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

    #lines = sc.textFile('311_Service_Requests_from_2010_to_Present.csv')

    lines = sc.textFile(','.join(sys.argv[1:-1]))
    trips = lines.filter(lambda x: not x.startswith('Unique Key') and x != '')   
    output = trips.mapPartitions(mapToZone).reduceByKey(lambda a, b: a+b)
    output.saveAsTextFile(sys.argv[-1])
    
    
    
