#!\bin\python

import math,  psycopg2

class DBObject:
    def  __init__(self):
        self.connection = psycopg2.connect("dbname='Thesis' host='localhost' user='ebpowell' password='geo1ogy1'")
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

#This class creates a somewhat generalized geometry representing the Slope and Power required for a given ride on a given day
#The arcs in the route store the slope of the segment, the rideid and and the ORIGNAL SEGMENT LENGTH (even though the geometry is 
#Simplified to just the points of intersection with the elevation datatset. May increase detail is some cases...

#The proper (future) solution is to store the data in a linear reference systems. Requires more time that I have at present
class Ride(DBObject):
    def __init__(self,  rideid):
        DBObject.__init__(self)
        self.rideid = rideid
        #self.outtablename  = outtablename
        #based on rideid, determine which utm zone to look into an assign the proper tables
        dctZoneTables={16:['utmsixteentin',  'route_utmsixteen',  'utmsixteenride', 'vw_sixteen_int',  26916],  17:['utmseventeentin',  'route_utmseventeen',  'utmseventeenride', 'vw_seventeen_int',   26917], 18:['utmeighteentin',  'route_utmeighteen',  'utmeighteenride', 'vw_eighteen_int',   26918]}
        #Retrieve the the SRID of route based on the RideID
        strSQL = 'SELECT a.srid from thesis.ridelocation a inner join thesis.rides b on b.startlocid = a.locationid where b.rideid = '+str(rideid)
        self.lstTables = dctZoneTables[self.getData(strSQL)[1][0][0]]
    def getRideTable(self):
        return self.lstTables[2]
        
class TINRide(Ride):
    def __init__(self,  intRideID):
        Ride.__init__(self,  intRideID)
  
        strSQL= 'insert into '+self.lstTables[2]+'(rideid, elevdtype, the_geom, slope) select '+str(self.rideid)+',1, c.the_geom, atan((st_z(st_endpoint(c.the_geom)) - st_z(st_startpoint(c.the_geom)))/st_length(c.the_geom)) as slope from (select (st_dump(st_intersection(b.the_geom, a.the_geom))).geom as the_geom from '+self.lstTables[1]+' a, '+self.lstTables[0]+' b where a.routeid =  (select routeid from thesis.rides where rideid = '+str(self.rideid)+')) c where isempty(c.the_geom ) = false and ST_Dimension(c.the_geom) = 1'

        self.runQuery(strSQL)
 
class ContourRide(Ride):
    def __init__(self,  intRideID):
        Ride.__init__(self, intRideID)
        strSQL = 'select st_line_locate_point(a.the_geom, b.the_geom)*st_length(a.the_geom) as dist, st_x(b.the_geom),st_Y(b.the_geom), st_z(b.the_geom) as elev from '+self.lstTables[1]+' a inner join '+self.lstTables[3]+' b on a.routeid = b.routeid where a.routeid = (select routeid from thesis.rides where rideid = '+str(self.rideid)+') order by dist'
        lstData = self.getData(strSQL)
        x = self.calcSlope(lstData[1], lstData[0])
        
    def calcSlope(self,  lstRows,  intRowCount):
        lstQueries = []
        #Assume that subclass has pulled the appropriate dataset in the form of [routeid, point X, point Y, distance, elevation, srid]
        #Slope angle calculated in radians
        for x in range (0,  intRowCount-1):
            try: #Incse a zero - lenght arc is encountered, trap the error
                strSQL = "INSERT INTO "+self.lstTables[2] +" (rideid, elevdtype, slope, the_geom) VALUES ("+str(self.rideid)+", 2, "+str(math.atan((lstRows[x+1][3] - lstRows[x][3])/(lstRows[x+1][0]-lstRows[x][0])))+",  st_geometryfromtext('LINESTRING("+str(lstRows[x][1])+" "+str(lstRows[x][2])+", "+str(lstRows[x+1][1])+" "+str(lstRows[x+1][2])+")', "+str(self.lstTables[4])+"))"
            #print strSQL
                self.runQuery(strSQL)
            except:
                print 'Zero - length arc detected'
                continue
        return  0

class Arc (DBObject):
    def __init__(self, arcid,  strTableName,  intRideID):
        DBObject.__init__(self)
        self.ArcID = arcid
        self.strTableName = strTableName
        self.Slope = self.getSlope()
        self.RideID =intRideID
        self.Length = self.getLength()
   
    def getSlope(self):
        strSQL = 'SELECT slope from '+self.strTableName+' WHERE arcid = '+str(self.ArcID)
        lstRes = self.getData(strSQL)
        return lstRes[1][0][0]
        
    def getLength(self):
        strSQL = 'SELECT st_length(the_geom) FROM '+self.strTableName+' where arcid = '+str(self.ArcID)
        lstRes = self.getData(strSQL)
        return lstRes[1][0][0]
 
#This class uses the rider tables in the database to build a rider object 
class Rider (DBObject):
    def __init__(self, rideid):
        #Instatiate the Database object
        DBObject.__init__(self)
        strsql = 'SELECT riderid, weight, bikeweight, height, avgflatroadpace, restingheartrate, bodytype from thesis.riderinformation where riderid =  (select riderid from thesis.rides where rideid =  '+str(rideid)+')'
        lstData = self.getData(strsql)[1]
        self.riderid = lstData[0][0]
        self.Weight = (lstData[0][1] + lstData[0][2])/2.2 #Convert pounds to kilograms on the way in
        self.Height = lstData[0][3] #In inches
        self.FlatRoadSpeed = lstData[0][4]*0.44704 # from mph Convert to m/s
        self.RHR = lstData[0][5]
        self.BodyType = lstData[0][6]
        if self.Weight >81.81: #Cmpare in kilograms
            self.BodyType = 3
        lstSpd = self.calcSpeedEquation()
        print 'Speed Cofficents', lstSpd
        self.HillSpeedM = lstSpd[0]
        self.HillSpeedB = lstSpd[1]
        self.HillHRM = 0.417
        self.HillHRB = 44
        
    def calcSpeedEquation(self): #Generate a person specific speed/slope curve. 
    #Convert from MPH to m/s
        lstData = self.getData('select slope, speed*0.044704  from thesis.heartrate where riderid ='+ str(self.riderid) +'and slope >0')
        try:
            lstResults  = self.calcRegression(lstData)
            if lstResults[0]>0: #Relationship is negative linear. Low density data causes Linear Regression to fail returning a possitive slope, use default values derived from averages of available
        #data derived from survey - average slope and average speed
                print 'linear regression Failed, default values used'
                lstResults[0] =-50.85
                lstResults[1]= 9.6857
        except:
            lstResults = [0, 0]
            print 'No regression data available, defaults used'
            lstResults[0] =-50.85
            lstResults[1]= 9.6857
        return lstResults
        
    def Speed(self,  slope,  length,  initialspeed):
        if slope <0: #Negative slopes - add component of acceleration acceleration of gravity along plane
            initialspeed = self.HillSpeedM*slope + self.HillSpeedB
            speed = math.sqrt(initialspeed**2+ 2*(abs(math.sin(slope)*9.8*length)))
        if  slope == 0:#Flat to possitive slopes, use speed / slope data from database
            speed = self.HillSpeedM*slope + self.HillSpeedB
            if speed< initialspeed: #Assume constant velocity on flats
                speed = initialspeed
        if slope > 0:
            speed = self.HillSpeedM*slope + self.HillSpeedB
        #This function assures that on really steep downhills, a modicom of braking is applied
        if speed > 2.0 * self.FlatRoadSpeed:
            speed = 2.0 * self.FlatRoadSpeed
        if speed<2.5: #Set a minimum speed threshhold - below this speed you will fall over....
            speed = 2.5
        return speed

    def SurfaceArea(self):
        dBodyType={1:.40, 2:.495, 3:.84}
        #Calculated using the Mosteller formula combined with percentages for adults from O'Sullivan and Smith
        BSA = math.sqrt((self.Height*2.54)*(self.Weight))/3600*dBodyType[self.BodyType]
        return BSA
        
    def HeartRate(self, power):
        heartrate =self.HillHRM*power + self.HillHRB #Coggan
        if heartrate > 201: #Set a maximum threshhold. Unbounded linear expressions do bad things...Coggan's data based on power range from 0 - 300 watts
            heartrate = 201
        return heartrate
        
    def calcRegression(self,  lstData): #Based on M.G. Bulmer pp 212-213
        #Least squares linear regression
        #First, determine the averages of x and y
        sumx  = 0
        sumy = 0
        for i in range (0,  lstData[0]):
            sumx = sumx + lstData[1][i][0]
            sumy = sumy + lstData[1][i][1]
        xbar = sumx/lstData[0]
        ybar = sumy/lstData[0]
        sumxy = 0
        sumx = 0
        for i in range (0,  lstData[0]):
            #Calculate the sum of x(i)*y(i)
            sumxy = sumxy  + (lstData[1][i][0]*lstData[1][i][1] - lstData[0]*xbar*ybar)
            sumx = sumx + (lstData[1][i][0]**2-(lstData[0]*xbar)**2)
        if sumx > 0:
            m = sumxy/sumx
            b = ybar - m*xbar
        else:
            m = 0
            b = 0
        return [m, b]
        
#Uses meteorological data and 
class AirLoad (DBObject):
    def __init__(self,time,  startlocation,  traveldirection,  surfacearea,  bikespeed):
        DBObject.__init__(self)
        self.Density = 1.18
        #Convert windspeed to m/s when reading
        strSQL = "SELECT windspeed*0.44704, winddirection FROM thesis.winddata WHERE locationid = "+str(startlocation) + " AND meastime = '" +str( time) + "'"
        lstData = self.getData(strSQL)
        self.WindSpeed = lstData[1][0][0]
        self.WindDir = lstData[1][0][1]
        self.dragcofficient = 0.88 #From http://www.engineeringtoolbox.com
        self.TravelDirection = traveldirection  
        self.SurfaceArea = surfacearea
        self.BikeSpeed = bikespeed
        
    def Drag(self):
        windvelocity = self.calcWindVelocity()
        #print windvelocity
        drag = (self.Density*self.dragcofficient*self.SurfaceArea*(windvelocity)**2)/2.0 #Powell equation 2.15
        #print 'Drag', drag
        return drag
        
    def calcWindVelocity(self):
        #print 'Wind Velocity Bike Speed',  self.BikeSpeed
        #print 'Wind Speed',  self.WindSpeed/0.44704
        #print 'Wind Direction from',  self.WindDir
        #Turn wind around to find direction of the force
        WindDir = self.WindDir +180
        #print 'Wind TO direction',  WindDir
        #print 'Travel Direction',  math.degrees(self.TravelDirection[0])
        try:
            delta_wind_dir = WindDir - math.degrees(self.TravelDirection[0])
            windvelocity = self.BikeSpeed - self.WindSpeed*math.cos(math.radians(delta_wind_dir))
        except:
            #For zero length records, set to velocity to 0
            windvelocity = 0
        #print 'Wnd Direction Delta',delta_wind_dir
        #print 'Effective Wind Velocity',  windvelocity
        return  windvelocity 
  
class Acceleration:  
    def __init__(self,  initialv,  finalv,  length):
        self.Vo = initialv
        self.Vf = finalv
        self.length = length
    def getAcceleration(self,  slope):
        #Rearrangment of V^2 = Vo^2+2ax
        accel = (self.Vf**2-self.Vo**2)/(2*self.length)
        if accel > 1.0 and slope >=0: #AASHTO study sets acceleration for bicycles between 0.5 and 1.4 m/s2, use center of range - should mitigate power spikes. Do not apply for downhill accleration
            accel = 1.0
        if accel > 0 :
            veloc = math.sqrt(self.Vo**2+(2*accel*self.length))
        else:
            veloc = math.sqrt(self.Vo**2+(2*accel*self.length))
        print 'Acceleration', accel, 'Final Velocity:',  veloc
        return  accel,  veloc

class Normal:
    def __init__(self,  mass,  slope):
        self.Gravity = 9.8 #m/s2
        self.mass = mass
        self.slope = slope
    def Normal(self):
        return self.mass * self.Gravity * math.cos(self.slope)
        
class Impulse: #Actually impulse
    def __init__(self,  mass,  speedi,  speedf,  length):
        self.mass = mass
        self.init_speed = speedi
        self.fin_speed = speedf
        self.length = length
    def Impulse(self):
        try:
            impulse = (self.mass * (self.fin_speed - self.init_speed))
            deltaT =self.length/((self.fin_speed + self.init_speed)/2) #Time is estimated as length divided by average speed
            force =impulse/deltaT
        except:
            force = 0
        return force
        
class Gravity:
    def __init__(self,  mass,  slope):
        self.Acceleration = 9.8 #m/s2
        self.mass = mass
        self.slope = slope    
    def Gravity(self):
        return self.mass*self.Acceleration*math.sin(self.slope)
        
class Friction:
    def __init__(self, Normal):
        self.Crr = 0.0063 #Averaged from data in table ??? 
        self.Normal = Normal
    def Friction(self):
        return self.Crr * self.Normal.Normal()
        
#Calculates the calories required to traverse a route based on the power linework
class Work (DBObject):
    def __init__(self, Power, Arc, Rider):
        self.Rider = Rider
        self.Power = Power
        self.Arc = Arc
        
    def calcWork(self):
        #Work is defined as Power * time (e.g. w/s * s = w)
        return self.Power*self.Arc.Length/self.Rider.Speed(self.Arc.Slope)
        
class Power (DBObject):
    def __init__(self, Rider,  Arc,  StartSpeed,  endspeed):
        DBObject.__init__(self)
        self.Rider =Rider
        self.Arc = Arc
        #self.Acceleration = Acceleration
        self.StartSpeed = StartSpeed
        self.EndSpeed = endspeed
        #Pull vaues from Database - RideDetails, Route and Arc tables
        lstRide = self.getRide()
        self.Drag = AirLoad(lstRide[0][0],  lstRide[0][1], self.getDirection() , Rider.SurfaceArea(),  self.EndSpeed)
        self.Gravity = Gravity(self.Rider.Weight,  self.Arc.Slope)
        self.Normal = Normal(self.Rider.Weight,  self.Arc.Slope)
        self.Impulse = Impulse(self.Rider.Weight,  StartSpeed,  endspeed,  Arc.Length)
        self.Friction = Friction(self.Normal)
    def calcPower(self):
        friction = self.Friction.Friction()
        gravity =self.Gravity.Gravity() 
        drag =self.Drag.Drag()
        impulse= self.Impulse.Impulse()
        #ma =(self.Rider.Weight * self.Acceleration)/(self.Arc.Length/((self.EndSpeed+self.StartSpeed)/2))#Needs to be converted from impulse to force by dividing by time- estimated using delta speed over time. 
        print 'Power.calcPowerValues'
        print 'Slope',  self.Arc.Slope
        #Use average speed across segment for power calcuation NOT maximum...
        avgSpeed = (self.StartSpeed + self.EndSpeed)/2
        # a btter approach would be to calcuated based on continuous acceleration
        power =avgSpeed*(friction+gravity + drag +impulse) #Ma dropped - impulse measures the change in momentum along plane of travel
        if power < 0: #If power is negative, coasting on inertia
            power = 0
        if power > 900: #Set a maximum power - a little high for hobby cyclists, low for the elite...average maximum for 4 power meter rides
            self.EndSpeed = 900/(friction+gravity + drag +impulse)
            if self.EndSpeed < 3.5: #If slowing beyonbd a minimum, set to minimum sped and recalculate
                self.endspeed = 3.5
                avgSpeed = (self.StartSpeed + self.EndSpeed)/2
                power =avgSpeed*(friction+gravity + drag +impulse)
            power = 900
        #Assume power approaches 0 on downhills
        if self.Arc.Slope < 0:
            power = 0
        print 'Power',  power,   'Speed',  self.EndSpeed, 'Friction',  friction,'Drag',   drag, 'Gravity',  gravity,'Impulse',   impulse
        return [power,  self.EndSpeed,  friction,  drag,  gravity,  impulse]
        
    def getDirection(self):
        strSQL = 'SELECT st_azimuth(st_startpoint(the_geom), st_endpoint(the_geom)) from '+self.Arc.strTableName+' WHERE arcid = '+ str(self.Arc.ArcID)
        #print strSQL
        lstData = self.getData(strSQL)
        return lstData[1][0]
        
    def getRide(self):
        strSQL = 'SELECT starttime, startlocid FROM thesis.rides WHERE rideid = '+str(self.Arc.RideID)
        lstData = self.getData(strSQL)
        print lstData
        return lstData[1]
    
if __name__ == '__main__':
    #First, inital the route by calling the proper Ride constructor...
    #inteltype =2
    #intRideID = 10
    #lstRides=[[1, 1],[1, 10], [2, 1], [2, 3], [2, 4], [2, 8], [2, 9], [2, 10] ]
    #lstRides=[[2, 9], [2, 8], [1, 10], [2, 10] ]
    lstRides=[[1, 15], [2, 15], [1, 16], [2, 16], [1, 17], [2, 17], [1, 18], [2, 18]]
    #lstRides=[[2, 15], [2, 16], [2, 17], [2, 18]]
#    for ride in lstRides:
#        print "Processing :",  ride
#        if ride[0] == 1:
#            print 'Intersecting TIN'
#            objjRide = TINRide(ride[1])
#        else:
#            print 'Intersecting COntours'
#            objRide = ContourRide(ride[1])
     #Build a route based on path followed and desired elevation model
     #if  elev_type = 1: #TIN
        #objRide = TINRide(intRideID)
    #else
        #objRide = ContourRide(intRideID)
    #Next, instatiate the rider object
    for item in lstRides:   
        objRider = Rider(item[1])
        #
        #Now poll the arcs generated earlier and process
        #First, get the Ride Table name from the Ride Object...
        objRide = Ride(item[1])
        strRideTable = objRide.getRideTable()
        #strRideTable = 'utmeighteenride'
        Db = DBObject()
        #Get arcs from database, assume that st_intersection routine returned them in proper order...
        lstArcs = Db.getData('SELECT * from '+strRideTable+' where rideid ='+str(item[1])+'and elevdtype='+str(item[0]) + 'order by arcid')
        endspeed = 0
        x = 0
        for arc in lstArcs[1]:
            x = x + 1
            
            if x == 14:
                print 'Fucked up spot'
            objArc = Arc(arc[0], strRideTable, item[1])
            print 'Inital Speed',  endspeed/.44704,  endspeed
            #print 'Final Speed',  objRider.Speed(objArc.Slope,  objArc.Length)/.44704,  objRider.Speed(objArc.Slope,  objArc.Length)
            print 'Arc Length (m)', objArc.Length
            objAccel = Acceleration(endspeed,  objRider.Speed(objArc.Slope,  objArc.Length,  endspeed),  objArc.Length) #dubiously names endspeed, which is actually inital speed, is only used for cases of negative slopes
            lstAcceleration = objAccel.getAcceleration(objArc.Slope)
            print 'Acceleration',  lstAcceleration[0]
            objPower = Power(objRider,  objArc,  endspeed,  lstAcceleration[1]) #Limit power cacluatuions to a REASONABLE acceleration...
            #Now update the arc in the database with the calculated parameters
            lstPower = objPower.calcPower()
            del objPower
            strSQL = 'UPDATE '+ strRideTable+' SET power ='+str(lstPower[0])+', speed = '+str(lstPower[1]/.44704)+', percent_hr_max = '+str(objRider.HeartRate(lstPower[0]))+', startspeed = '+str(endspeed/.44704)+',  friction='+str(lstPower[2])+', drag='+str(lstPower[3])+',  gravity='+str(lstPower[4])+', impulse='+str(lstPower[5])+',  acceleration='+str(lstAcceleration[0])+' where arcid= '+str(objArc.ArcID)
            print strSQL
            Db.runQuery(strSQL)
            endspeed = lstAcceleration[1]
            del objArc
            del objAccel
            print
            print
            print
            print 'End Record'
            print
            print
            
