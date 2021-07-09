#!\bin\python

#Script to load points from ascii text file to PostGIS FC 
#Assumes point source is in lat-long
#User specifices the target coord system epsg code and filename at run time
from __future__  import with_statement
from pyproj import *
from pyGEarth import pyGEarthPostGISTools as PGISTools
from pyGEarth import pyGEarthGeometry as PGGeo


def getDSDetails():
    lstDSDetails = []
    f =open('./DEM_Files.txt')
    lstLines = f.readlines()
    for line in lstLines:
        lstStuff = line.split(',')
        lstDSDetails.append(lstStuff)
    return lstDSDetails
def getDBHost():
    f =open('./DBHostName.txt')
    strDBPath = f.read()
    return strDBPath.strip()
   

if __name__ == '__main__':
    #epsgcode = sys.argv[1] 
    #filepath = sys.argv[2].strip()
    #Get the details from the config file
    lstDetails = getDSDetails()
    lstFields=['shape']
    #Iterate through the list loding each file sequentially
    for x in range (0,len(lstDetails)):
       # p1 = Proj(init='epsg:4269')
        p1 = Proj(proj='longlat',  ellps='GRS80',  datum='NAD83')
        print lstDetails[x]
        print lstDetails[x][1].strip()
        p2 = Proj(init='epsg:'+lstDetails[x][1].strip())
        #Loop through file and read 1 line at a time
        lstData = []
        intx = 0
        with open(lstDetails[x][0]) as f:
            for line in f:
                intx = intx + 1
                dctData={}
                lstFields = ['shape']
                #split the line by "," into X,Y,Z
                LLX =  line.split('|')[0]
                LLY =  line.split('|')[1]
                #Reproject the point into user specified cooridnate system
                PX,PY = transform(p1,p2,LLX,LLY)
                #Now create the point object to add to the database
                objPoint = PGGeo.Point()
                objCoord = (PX,PY,float(line.split("|")[2].strip()))
                objPoint.addPoint(objCoord)
                dctData['shape'] = objPoint
                dctData['elevation'] = float(line.split("|")[2].strip())
                lstData.append(dctData)
                del objPoint
                del objCoord
                del dctData
                if intx%1000==0:
                    #print intx,  'Points Read from ',  lstDetails[x][0]
                    #Write dataset to the database
                    #print "Writing Points to Database table ",  lstDetails[x][2]
                    objPOSTGIS = PGISTools.PostGISWrite('Thesis', 'ebpowell',getDBHost(), 'geo1ogy1', "POINT3D", lstData, lstDetails[x][2].strip(), lstFields, lstDetails[x][1].strip(),  'objectid')
                    del lstData
                    lstData = []
                
