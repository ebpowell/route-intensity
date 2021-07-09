#Script to poll point elevation data for a given route and write a file suitable for reading by TINLib

import psycopg2
import math


class TINInput:
    def __init__(self, strRouteTableName, strRouteIDFld, intRouteID, strRouteGeoFld,  strPntTblNm,  strPntGeoFld ):
        self.ConnString = "dbname='Thesis' user='ebpowell' host='gimili' password='geo1ogy1'"
        self.strSQL = 'select st_x(a.'+strPntGeoFld+') , st_y(a.'+strPntGeoFld+'), st_z(a.'+strPntGeoFld+') from ' + strPntTblNm+'  a, ' + strRouteTableName + ' b WHERE st_dwithin(b.'+strRouteGeoFld+', a.'+strPntGeoFld+',75) and b.'+strRouteIDFld+' in (select  '+strRouteIDFld+' from '+strRouteTableName +' WHERE '+strRouteIDFld+' = '+str(intRouteID) +')'
        #print self.strSQL
        self.connection = self.connect()
        
    def connect(self):
        conn=psycopg2.connect(self.ConnString)
        return conn
    
    def getData(self,  strSQL):
        cur = self.connection.cursor()
        #Perform the query
        cur.execute(strSQL)
        intCnt = cur.rowcount
        rows = cur.fetchall()
        return [intCnt,  rows]
        
    def genFile(self, strFileName): 
        lstData = self.getData(self.strSQL)
        f=open(strFileName, 'w')
        #First, write the number iof  records
        f.write('num_points='+str(lstData[0])+'\n')
        for row in lstData[1]: #Write the points
            f.write('      '+str(row[0]) + '      '+str(row[1])+'   '+str(row[2])+'\n')
        f.close()
    
if __name__ == '__main__':
    lsttables = [['route_utmsixteen',  'demutmsixteen'], ['route_utmseventeen', 'demutmseventeen'], ['route_utmeighteen', 'demutmeighteen']]
    #Iterate through the table list, get the route ids and generate the point files
    for tpltables in lsttables:
        #Firsrt determine the routeids to run
        strSQL = 'select distinct routeid from ' +  tpltables[0]
        connection=psycopg2.connect("dbname='Thesis' user='ebpowell' host='gimili' password='geo1ogy1'")
        cur = connection.cursor()
        cur. execute(strSQL)
        rows = cur.fetchall()
        for row in rows:
            objTIN = TINInput(tpltables[0],  'routeid',  row[0],  'the_geom',  tpltables[1],  'shape' )
            objTIN.genFile('route'+str(row[0])+'_pnts.txt')
        del cur
        connection.close()
