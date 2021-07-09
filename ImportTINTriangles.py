#Script to convert the output of the tinlib into a layer of triangles.
#Assumes the target table requires SRID, has autonumbering PK via sequence
import psycopg2
import math
class TINInput:
    def __init__(self, strTINTableName, strFileName, intSRID,  intRouteID):
        self.ConnString = "dbname='Thesis' user='ebpowell' host='gimili' password='geo1ogy1'"
        f = open(strFileName, 'r')
        self.connection = self.connect()
        lstData = f.readlines()
        for line in lstData:
            strSQL = "INSERT INTO "+strTINTableName +" (the_geom, routeid) VALUES ( st_geometryfromtext('POLYGON(("+self.parseString(line)+"))', "+str(intSRID)+"), "+str(intRouteID)+")"
            print strSQL
            self.runQuery(strSQL) 
    def connect(self):
        conn=psycopg2.connect(self.ConnString)
        return conn
        
    def runQuery(self, strCommand):
        try:
            cur = self.connection.cursor()
            print strCommand
            #run the query
            cur.execute(strCommand)
            #commit each 
            self.connection.commit()
            del cur    
        except:
            print 'Error Performing action query'
            print strCommand
            #del cur
            raise
    def parseString(self,strLine):
        #Split into three tuples by the ')' character
        tplInitialStrings = strLine.split(')')
        #For each of the three, extrtact the value array
        lstpoints=[]
        for x in range (0, 3):
            string = tplInitialStrings[x]
            strSec = string.split('(')[1]
            tplSec = strSec.split(',')
            strpnt = tplSec[0].strip()+' '+tplSec[1].strip()+' '+tplSec[2].strip()
            lstpoints.append(strpnt)
        strgeometry = lstpoints[0]+ ','+lstpoints[1]+','+lstpoints[2]+','+lstpoints[0]
        #Strip the extra comma
        return strgeometry
        
if __name__ == '__main__':
    #Original file list - not all would TIN
   # lstFiles = [[1,  utmseventeentin,  26917], [10, utmeighteentin, 26918][14, utmeighteentin, 26918 ], [15,  utmeighteentin, 26918], [16, utmsixteentin, 26916 ], [17, utmseventeentin,  26917],  [3, utmseventeentin,  26917 ], [4,utmseventeentin,  26917 ], [8, utmeighteentin, 26918  ], [9, utmeighteentin, 26918 ]]
   #trimmed down list of TINS generated
    #lstFiles = [[1,  'utmseventeentin',  26917], [10, 'utmeighteentin', 26918], [14, 'utmeighteentin', 26918 ], [15,  'utmeighteentin', 26918], [16, 'utmsixteentin', 26916 ],  [17, 'utmseventeentin',  26917]]
    lstFiles = [[14, 'utmeighteentin', 26918 ], [15,  'utmeighteentin', 26918], [16, 'utmsixteentin', 26916 ],  [17, 'utmseventeentin',  26917]]
    for file in lstFiles:
        strFileName = 'route'+str(file[0])+'_triangles.txt'
        strTableName = file[1]
        intSRID = file[2]
        intRouteID = file[0]
        objTIN = TINInput(strTableName,  strFileName,  intSRID,  intRouteID)
