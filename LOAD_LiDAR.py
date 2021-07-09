#!/python
#*******************************************************
#
#Simple script to use pyLAS and pyGEarth to load
#LiDAR data into the geodatabase.
#
#4/1/2008
#
#******************************************************


#from pyGEarthPostGISTools import *
from pyGEarth import pyGEarthPostGISTools as PGISTools
from pyLAS import pylas
import sys,  os
import psycopg2
from time import strftime
#from Logger import Logger

import psycopg2

class Logger:
    def __init__(self  ):
        self.connection = psycopg2.connect("dbname='Thesis' host='" + getDBHost() +"' user='ebpowell' password='geo1ogy1'")
        self.cursor = self.connection.cursor()
        
    def Update(self,  strFilename,  strMachineName,  intFilePos,  strTimeStamp):
        strSQL = "Update status set fileposition = " + str(intFilePos) + ", updatetime = '"+strTimeStamp +"' WHERE filename = '"+strFilename + "';"
        self.cursor.execute(strSQL)
        self.connection.commit()
        return 0
        
    def InsertRec(self,   strFilename,  strMachineName,  intFilePos,  strTimeStamp):
       strSQL = "INSERT INTO status (fileposition, filename, machine, updatetime, locked) VALUES(" + str(intFilePos) + ",'"+strFilename +"','"+strMachineName +"', '"+strTimeStamp +"',1);"
       self.cursor.execute(strSQL)
       self.connection.commit()
       return 0        
    def GetPosition(self,  strFileName):
        strSQL = "SELECT fileposition, finished, locked FROM status WHERE filename ='"+strFileName +"';"
        self.cursor.execute(strSQL)
        self.connection.commit()
        rows = self.cursor.fetchall()
        if len(rows)>0:
            if rows[0][1] == 1 or rows[0][2]==1:
                intPosition = -999
            else:
                try:
                    intPosition = rows[0][0] + 1
                except: #Consider record screwed...ignore this file
                    intPosition = -999
        else:
            intPosition = 0
        if intPosition > -1: #Lock files on resume
            strSQL = "UPDATE status set locked = 1 where filename = '"+strFileName+"';"
            self.cursor.execute(strSQL)
            self.connection.commit()
        return intPosition
        
    def SetFinished(self,  strFileName,  strTimeStamp):
        strSQL = "Update status set finished = 1, locked = 0, updatetime = '"+strTimeStamp+"' WHERE FileName = '"+strFileName+"';"
        self.cursor.execute(strSQL)
        self.connection.commit()
        return 0
        
        return 0
    def __del__(self):
        del self.cursor
        self.connection.close()  


def movedata():
   #Now, select the points within the roads layer and write to permanent table
    ConnString = "dbname='Thesis' user='ebpowell' host='" + getDBHost() +"' password='geo1ogy1'"
    conn=psycopg2.connect(ConnString)
    conn.set_isolation_level(0)
    cur = conn.cursor()
    for table in (getTableName(), "lidarpoints"):
        print strftime("%Y-%m-%d %H:%M:%S")," Vacuuming: ",  table
        cur.execute("VACUUM ANALYZE %s"%table)
    del cur
    conn.close()
    print strftime("%Y-%m-%d %H:%M:%S"), "Selecting records to move to lidarpoints table"
    strSQL = 'INSERT INTO lidarpoints (elevation, extra_1, extra_2, return_grp,intensity, scan_angle_rank, classification, shape) select elevation, extra_1, extra_2, return_grp,intensity, scan_angle_rank, classification, shape from '+getTableName() +' as a, scroadsbuffer as b where within( a.shape, b.the_geom ) = True;'
    #Connect to postgresql and run query....
    conn=psycopg2.connect(ConnString)
    cur = conn.cursor()
    #Perform the query
    cur.execute(strSQL)
    conn.commit()
    #Now purge the points from lidar_import before continuing...
    print strftime("%Y-%m-%d %H:%M:%S"),"Purging un-needed data"
    strSQL = "delete from "+ getTableName() +" ;"
    cur.execute(strSQL)
    conn.commit()
    #Now  vacuum the  tables to increase performance
    print "Performing Database maintanence"
    #conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    conn.set_isolation_level(0)
    for table in (getTableName(), "lidarpoints"):
        print strftime("%Y-%m-%d %H:%M:%S")," Vacuuming: ",  table
        cur.execute("VACUUM ANALYZE %s"%table)
    conn.close()
    del cur
    del conn
    return

def getMachine():
    f =open('./MachineName.txt')
    return f.read()
def getTableName():
    f =open('./ImportTableName.txt')
    strTableName = f.read()
    return strTableName.strip()
def getDBHost():
    f =open('./DBHostName.txt')
    strDBPath = f.read()
    return strDBPath.strip()
def getFilePath():
    f =open('./Datapath.txt')
    strPath = f.read()
    return strPath.strip()
    
if __name__ == '__main__':
    #Standard pyLAS read options
    numpnts = None
    rand = False
    dim=3
    sample = 1
    verbose=False
    #Generate the file list from the source directory
    strPath = getFilePath().strip()
    lstFiles = os.listdir(strPath)
    x = 0
    #lstFiles = ["680590.las"]
    intRecCount = 5000
    for file in lstFiles:
        #Only process LAS files
        if file[-3:] == 'las':
            #Look up wether to process file and starting position in the file  - coordinate efforts between machines
            logger = Logger()
            intStartPos = logger.GetPosition(file)
            #Add a lock on the current file
            if intStartPos ==0:
                logger.InsertRec(file,  getMachine(),  0, strftime("%Y-%m-%d %H:%M:%S"))
            del logger
            if intStartPos ==-999: #Skip file - already processed, in use by another machine
                continue
            strFile = getFilePath() + file
            objLAS = pylas.LAS(strFile)
            print "Processing: ",  file
            #Define pyLAS parameters  -spelled out for clarity
            #First, read the header to determine the number of records in the file
            intSRID = 2273
            h=objLAS.parseHeader(False)
            if  h['numptrecords']  > intRecCount: #For files of greater than intRecCount points, load in batches
                #Modified to use new method - readLASRecs
                intIterations = h['numptrecords']/intRecCount
                #Check to see if loading terminated before file was completed...
                if intStartPos == 0:
                    intStartPos = h['offset']
                for x in range (0, intIterations):
                    print "Processing",  strFile
                    print "Start Position ", intStartPos,  'Number of Records',  h['numptrecords']
                    #Now determine the start and end postions in the file...
                    lstResults = objLAS.readLASRecs(intStartPos,  intStartPos+intRecCount,  numpnts,  rand,  dim, sample,  verbose)
                    print  strftime("%Y-%m-%d %H:%M:%S"),"Adding points to temporary table"
                    objPOSTGIS = PGISTools.PostGISWrite('Thesis', 'ebpowell',  getDBHost() , 'geo1ogy1', "POINT", lstResults[0], getTableName(), lstResults[1], intSRID,  'objectid')
                    del lstResults
                    #Ensure we don't read past the end of file
                    if intStartPos + intRecCount < h['numptrecords'] :
                        intStartPos = intStartPos + intRecCount
                    #else:
                    movedata()
                    logger = Logger()
                    logger.Update(file,  getMachine(),  intStartPos, strftime("%Y-%m-%d %H:%M:%S"))
                    del logger
                    if intStartPos + intRecCount >= h['numptrecords']:#Quit at end of file...
                        print 'End of file detected'
                        intEOF = 1
                        #continue
                        break
                    else:
                        intEOF = 0
                        
            else:
                lstResults = objLAS.readLAS(numpnts,  rand,  dim, sample,  verbose)
                objPOSTGIS = PGISTools.PostGISWrite('Thesis', 'ebpowell',getDBHost(), 'geo1ogy1', "POINT", lstResults[0], getTableName(), lstResults[1], intSRID,  'objectid')
                del lstResults
                movedata()
            logger = Logger()
            logger.SetFinished(file, strftime("%Y-%m-%d %H:%M:%S") )
            del logger
 
