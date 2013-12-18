import QuantTechnologies.PyAPI as api
import QuantTechnologies.DB as DB
from  datetime  import  *  
import time as Time

swBegin=Time.time()
symbols=[]

#f = file("securities.csv")
#while True:
    #line = f.readline()
    #if len(line) == 0: # Zero length indicates EOF
        #break
    #symbols.append(line.replace("\n",""))
    # Notice comma to avoid automatic newline added by Python
#f.close() # close the file


#print symbols
#symbols.append('KMB')
symbols.append('SPY')
#symbols.append('GOOG')
#symbols.append('MSFT')
startdate= date(2013, 10, 21)
enddate= date(2013 ,11, 21)
starttime=time(0 ,  0,  0 ) 
endtime=time(16 ,  0 ,  0 ) 
Flags=DB.SubscriptionFlags.TRADE

#while (startdate<=enddate):
	#print startdate
	#startdate=startdate.replace(day=startdate.day+1)
client=DB.Client()
client.Client("user","user","108.60.133.9")
api.Subscribe(Flags,symbols,startdate,enddate,starttime,endtime,client)
swEnd=Time.time()
print str(swEnd-swBegin)


