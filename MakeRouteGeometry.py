import psycopg2
import math
class Route:
    
    def __init__(self,  strTableName, strGeoColName, strOrderColName, strRouteIDColName, intRouteID):
        #Get the arcs from the sourcetable into a temp table
         #self.ConnString = "dbname='"+dbName+"' user='"+UserName+"' host='"+HostName+"' password='"+Password+"'"
        self.ConnString = "dbname='Thesis' user='ebpowell' host='gimili' password='geo1ogy1'"
        self.strGeoColName =strGeoColName
        self.datatable = 'arctemp'
        self.strTableName = strTableName
        self.OrderColName = strOrderColName
        self.strRouteIDColName = strRouteIDColName
        self.intRouteID = intRouteID
        self.connection = self.connect()
        #Make sure old table is dropped
        #try:
         #   self.runQuery("select dropgeometrytable ('public', 'arctemp')")
        #except:
         #   print 'Temporary Table not present'
        strSQL = 'select ' + strGeoColName +', ' + strOrderColName +' into '+self.datatable + ' FROM ' + strTableName +' WHERE '+strRouteIDColName + '='+str(self.intRouteID)
        #print strSQL
       # try:
        self.runQuery(strSQL)
        #except: 
          #  print 'Error Initalizing Script'
            #raise
        
    def connect(self):
            conn=psycopg2.connect(self.ConnString)
            return conn
            
    def CreateRoute(self,  strOrigTableName, strOrigColName,  intSnapTol):
    #First get the coordinates of the starting point
        strSQL = 'SELECT st_x('+strOrigColName+'), st_Y( '+strOrigColName+') FROM '+strOrigTableName + ' WHERE '+self.strRouteIDColName+'='+ str(self.intRouteID)
        #Get the data
        lstData = self.getData(strSQL)
        tplStartPt = [lstData[0][0],lstData[0][1]]
        print 'Start Point =',  tplStartPt
        try:
        #Get the first arc
            strSQL = 'SELECT st_x(st_startpoint('+self.strGeoColName+')),st_Y(st_startpoint('+self.strGeoColName+')) FROM '+self.datatable+' where '+self.OrderColName+'=1'
            #Call the database to get the data
            lstData = self.getData(strSQL)
            tplLnStart =  [lstData[0][0],lstData[0][1]]
            print 'Line Start point:',  tplLnStart
            #Now check to see if it has the same start point as the startpoint table, if so carry on otherwise,reverse the arc
            if self.pythagoreandistance([[tplStartPt[0] , tplLnStart[0]], [tplStartPt[1] , tplLnStart[1]] ] )>intSnapTol:
                strSQL= 'SELECT st_x(st_endpoint('+self.strGeoColName+')),st_Y(st_endpoint('+self.strGeoColName+')) FROM '+self.datatable+' where '+self.OrderColName+'=1'
                lstData = self.getData(strSQL)
                tplLnEnd =  [lstData[0][0],lstData[0][1]]
                print 'Line End Point: ', tplLnEnd
                
                #Check the end point if it does not match either, throw error
                if self.pythagoreandistance([[tplStartPt[0] , tplLnEnd[0]], [tplStartPt[1] , tplLnEnd[1]] ] )>intSnapTol:
                    #Error condition
                    print "Geoemtry Error Detected! Line start does not match origin point"
                    raise
                else:
                    #Reverse the arc...
                    x = self.runQuery('UPDATE ' + self.datatable+' SET '+self.strGeoColName+'=st_reverse('+self.strGeoColName+') where '+self.OrderColName+'=1')
                    
        except:
            self.runQuery("select dropgeometrytable ('public', 'arctemp')")
            self.connection.close()
            raise
        try:
            #Determine the range of values in the arcorder column - assumes a single integer stepping progression but can start at any number.
            strSQL = 'SELECT min('+self.OrderColName+'),max('+self.OrderColName+') FROM '+self.datatable
            #strsql = 'SELECT count(' +self.OrderColName +') FROM '+self.datatable
            #get the data
            row = self.getData(strSQL)
            print row
            #Iterate through the list two at a time and determine if the are oriented head-to-tail, always acts on the NEXT arc (x+1)
            for x in range (row[0][0],row[0][1]):
                strSQL= 'SELECT st_x(st_startpoint('+self.strGeoColName+')),st_Y(st_startpoint('+self.strGeoColName+')), st_x(st_endpoint('+self.strGeoColName+')),st_Y(st_endpoint('+self.strGeoColName+')), '+self.OrderColName+' FROM '+self.datatable+' where '+self.OrderColName +' in ('+str(x)+','+str(x+1)+') order by '+self.OrderColName
                lsrows = self.getData(strSQL)
                print 'Row: ', x
                print lsrows
                if self.pythagoreandistance([[lsrows[0][2], lsrows[1][0] ], [lsrows[0][3],lsrows [1][1] ] ] )>intSnapTol :
                    #IF not, reverse the second arc and retry
                    strSQL1 = 'UPDATE ' + self.datatable+' SET '+self.strGeoColName+'=st_reverse('+ self.strGeoColName +') where '+self.OrderColName+'='+str(x+1)
                    run = self.runQuery(strSQL1)
                    #now repoll and check 
                    lsrows = self.getData(strSQL)
                    if self.pythagoreandistance([[lsrows[0][2], lsrows[1][0] ], [lsrows[0][3], lsrows[1][1] ] ] )>intSnapTol:
                    #FAIL - break in the topology
                        print 'Geometry Error Detected between arcs ', str(x) ,  ' and ', str( x+1)
                        print lsrows
                        raise 
        except:
            #Drop the temp table
            self.runQuery("select dropgeometrytable ('public', 'arctemp')")
            self.connection.close()
            raise 
        #Once all arcs have been visited, select them all again, reduce them to points and generate a contiguous linestring
        try:
            strsql = 'SELECT st_astext(' + self.strGeoColName+') as shape FROM ' +self.datatable +' ORDER BY ' + self.OrderColName
            lstrows = self.getData(strsql)
            strGeometry = 'LINESTRING('
            #for x in range (1, 3):
            x=0
            for row in lstrows:
                #Extract points from the geoemtry and write to the makeline function
                strgeo = row[0]
                #print 
                #print 
                #print
                #print strgeo[11:-1]
                if x == 0:
                    strGeometry = strGeometry + strgeo[11:-1]	
                    x = x + 1
                else:
                    strGeometry = strGeometry +','+ strgeo[11:-1]
            #When all points entered, create the geometry and return 
            #strGeometry = strGeometry + ')'
            #self.runQuery("select dropgeometrytable ('public', 'arctemp)")
            return strGeometry
        except:
            #print strGeometry
            self.runQuery("select dropgeometrytable ('public', 'arctemp')")
            self.connection.close()
            raise
    def getData(self,  strSQL):
        cur = self.connection.cursor()
        #print strSQL
        #Perform the query
        cur.execute(strSQL)
        rows = cur.fetchall()
        return rows
        
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
            del cur
            raise
    
    def  pythagoreandistance(self,  lstpoints):
        #Calcuate the pythagorean distance between two points
        return math.sqrt((lstpoints[0][0]-lstpoints[0][1])**2 +(lstpoints[1][0]-lstpoints[1][1])**2)
        
    def __delete__(self):
            self.runQuery("select dropgeometrytable ('public', 'arctemp')")
            self.connection.close()

if __name__ == '__main__':
    lstRoutes = [ 15]
    #strTableName = 'route_utmsixteen_l'
    strTableName = 'route_utmeighteen_l'
    strGeoColName = 'the_geom'
    strOrderColName = 'arcorder'
    strRouteIDColName ='routeid'
    strRouteTable = 'route_utmeighteen'
    strRtOriginTableName  = 'utmeighteen_origins'
    #intRouteID = 18
    intSRID = 26918
    #intRouteID = 16
    for rtid in lstRoutes:
        objroute = Route(strTableName,strGeoColName, strOrderColName, strRouteIDColName, rtid)
        #conn=psycopg2.connect("dbname='Thesis' user='ebpowell' host='gimili' password='geo1ogy1'")
        #cur = conn.cursor()
            
            #run the query
        f=open('route'+str(rtid)+'.sql', 'w')
        
        #f.write('insert into route_utmsixteen (arcid, the_geom) values( '+ str(intRouteID)+", st_geoemtryfromtext('"+objroute.CreateRoute('utmsixteen_origins',  'start_pt', 1)+")', 26916)")
        f.write('insert into '+strRouteTable+' (routeid, the_geom) values( '+ str(rtid)+", st_geometryfromtext('"+objroute.CreateRoute(strRtOriginTableName,  'start_pt', 1)+")', "+str(intSRID)+"))")
        f.close()
        
            #commit each 
        #conn.commit()
        #del cur   
        #conn.close() 
        del objroute
        #print objroute.CreateRoute('utmsixteen_origins',  'start_pt', 1)
    
    
