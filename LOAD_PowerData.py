#!\bin\python

import math,  psycopg2

class DBObject:
    def  __init__(self):
        self.connection = psycopg2.connect("dbname='Thesis' host='gimili' user='ebpowell' password='geo1ogy1'")
    def getData(self,  strSQL):
        cursor = self.connection.cursor()
        try:
            cursor.execute(strSQL)
            self.connection.commit()
            return [cursor.rowcount,  cursor.fetchall()]
        except:
            raise
    #Function for executing update and insert queries...
    def runQuery(self,  strSQL):
        try:
            cur = self.connection.cursor()
            #run the query
            cur.execute(strSQL)
            #commit each 
            self.connection.commit()
            del cur    
        except:
            print 'Error Performing action query:'
            print strSQL
            raise

            
            
    def __delete__(self):
        self.connect.close()

if __name__ =='__main__':
    dctZoneTables={16:['utmsixteenpower',  'route_utmsixteen',  26916],  17:['utmseventeenpower',  'route_utmseventeen',   26917], 18:['utmeighteenpower',  'route_utmeighteen',   26918]}
    lstRides = [14,15,16,17]
    db = DBObject()
    for ride in lstRides:
        #Get the route data...
        lstData = db.getData('SELECT a.srid from thesis.ridelocation a inner join thesis.rides b on b.startlocid = a.locationid where b.routeid = '+str(ride))
        lstTable = dctZoneTables[lstData[1][0][0]]
        strTable =lstTable [0]
        strroutetablename =lstTable[1]
        lstData = db.getData('select st_length(the_geom) from '+strroutetablename+' where routeid ='+str(ride))
        tdist = lstData[1][0][0]
        #Query to extract the tabular data
        strSQL = 'SELECT time_min, torq_n_m, speed_km_h, power_watts, cadence, hrate, distance_route from powerdata where routeid ='+str(ride)+'order by distance_route'
        
        lstData = db.getData(strSQL)
        x = 0
        sumpower = 0
        sumspeed = 0
        sumtorq = 0
        sumcadence = 0
        sumhr = 0
        summin = 0
        for row in lstData[1]:
            if x== 0: #Run only the first time
                sdist = row[6] * 1000
            x = x + 1
            sumpower = sumpower + row[3]
            sumspeed = sumspeed + row[2]
            sumtorq = sumtorq + row[1]
            sumcadence = sumcadence+row[4]
            sumhr = sumhr + row[5]
            summin = summin + row[0]
            if x ==8:
                enddist = row[6]*1000
                #Generate the averages and store in a list
                if enddist > sdist:
                    strSQL = 'INSERT INTO ' + strTable+' (time_min, torq_n_m, speed_km_h, power_watts, cadence, hrate, routeid, distance_route, the_geom) VALUES ('+str(summin/8.0)+', '+str(sumtorq/8.0)+', '+str(sumspeed/8.0)+', '+str(sumpower/8.0)+', '+str(sumcadence/8.0)+', '+str(sumhr/8)+', '+str(ride)+', '+str(enddist)+', (select st_line_substring(the_geom, '+str(sdist/tdist)+', '+str(enddist/tdist)+') from '+strroutetablename+' where routeid = '+str(ride)+'))'
                    db.runQuery(strSQL)
                else:
                    print strSQL
                    
                sdist=row[6]*1000
                #Reset everything
                strSQL = ''
                sumpower = 0
                sumspeed = 0
                sumtorq = 0
                sumcadence = 0
                sumhr = 0
                summin = 0
                x = 0
                enddist = 0
                
    
