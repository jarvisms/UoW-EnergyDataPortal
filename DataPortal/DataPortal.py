#!/usr/bin/env python
# Remember to change EOL convention to suit Windows or Linux depending on where this will run
print 'Importing Libraries...'
import MySQLdb	# REAL DATABASE
#import sqlite3	# TEST DATABASE
import urllib2, base64, json, xlrd, datetime, csv
from SocketServer import ThreadingTCPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
from urlparse import urlsplit, parse_qs
from operator import itemgetter

# Put all "constants" up here as there is no need to repeat them
dcsmaster ='http://URL_TO_ENERGY_DATA/'
dcsusername = 'USERNAME'
dcspassword = 'PASSWORD'
dcsheaders={'Authorization':'Basic ' + base64.b64encode(dcsusername + ':' + dcspassword), 'Content-Type':'application/json, text/json'}
SQLserver = "SERVER"
SQLUser = "USERNAME"
SQLPassword = "PASSWORD"
SQLdb = "db_EnergyDataPortal"
AvailableData=tuple(['elec', 'gas', 'water', 'heat', 'cool', 'wifi', 'temp', 'humidity', 'windspeed', 'wind_dir', 'gustspeed', 'pressure', 'solar', 'rain'])	# Sets what data is available and order the coloumns should appear in
units = {"gas":' (kWh)', "water":' (m3)', "elec":' (kWh)', "heat":' (kWh)', "cool":' (kWh)', "wind_dir":' (deg from N)', "windspeed":' (ms-1)', "gustspeed":' (ms-1)', "temp":' (deg C)', "humidity":' (%)', "pressure": ' (mB)', "solar":' (Wm-2)', "rain":' (mm since 00:00)', "wifi":' (devices)'}	# Defines the units to use for coloumns
# Instead of just reading it from the server every time, the following try to read from cached files, but if they dont exist, then use the server and save it for next time
try:
	print "Trying to load VirtualMeters from cached file"
	with open('VirtualMeters.json','rb') as VirtualMeters:	# Try loading it from a cached file
		v = json.loads(VirtualMeters.read())
		print 'Done'
except IOError:
	with open('VirtualMeters.json','wb') as VirtualMeters:	# If that fails, fetch it from the server, save it and use that
		vms = urllib2.urlopen(urllib2.Request(dcsmaster+'VirtualMeters', headers=dcsheaders)).read()
		VirtualMeters.write(vms)
		v = json.loads(vms)
		print 'Not chached so fetched VirtualMeter data from Server instead and saved it'
		del(vms)

try:
	print "Trying to load Meters from cached file"
	with open('Meters.json','rb') as Meters:	# Try loading it from a cached file
		r = json.loads(Meters.read())
		print 'Done'
except IOError:
	with open('Meters.json','wb') as Meters:	# If that fails, fetch it from the server, save it and use that
		ms = urllib2.urlopen(urllib2.Request(dcsmaster+'Meters', headers=dcsheaders)).read()
		Meters.write(ms)
		r = json.loads(ms)
		print 'Not chached so fetched Meter data from Server instead and saved it'
		del(ms)

def jsondatetime(dt):	# Converts datetime objects to JavaScript datetime notation so json supports it (with additional parameter)
	return datetime.datetime.strftime(dt,'%Y-%m-%dT%H:%M:%S.%fZ')

'''
def adapt_datetime(dt): # Adapter so that SQLite3 can accept datetimes but store them in the db file as efficient integers
	return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())

def convert_datetime(b): # Converter so that SQLite3 can retreive efficient integers and return them as datetimes
	return datetime.datetime.fromtimestamp(int(b))

sqlite3.register_adapter(datetime.datetime, adapt_datetime)
sqlite3.register_converter("DATETIME", convert_datetime)
'''

class JSONGen(list):	# Create a subclass of list so that the json module will think its a list and encode it
 def __init__(self,gen,len=0):	# There will be two items, a standard generator to make appear as a list, and what should end up being the length of the list/generator, although this can be any number
  self.gen = gen	# The input generator
  self.len = abs(int(len))	# The input predicted length, which will be converted to a positive integer just in case
 def __iter__(self):	# As required by the iterator protocol
  return self
 def __next__(self):	# Future Python3 compatible, just an alias for .next()
  return self.next()
 def next(self):	# Iterates through the underlying input generator
  return next(self.gen)
 def __len__(self):	# Gives the predicted length
  return self.len

print 'Reading Excel spreadsheet...'
#Load in dictionary with all information on location types and meters. Result is list of dictionaries 'LocsRef', with 'loc', and type-, wifi-, gas- etc lists eg 'wifilist', 'gaslist', 'eleclist'
File = xlrd.open_workbook('LocationIdRefs.xls')
sheet = File.sheet_by_index(0)

typelist, wifilist,gaslist,waterlist,eleclist,heatlist,coollist = [],[],[],[],[],[],[] #lists accodomate for multiple types/meters corresponding to single locations
LocRefs = [{'loc':'', 'loctype':[], 'wifilist':[], 'gaslist':[], 'waterlist':[], 'eleclist':[], 'heatlist':[], 'coollist':[]}]   #the master list initialization with an empty location
for x in xrange(1, sheet.nrows):  # opens spreadsheet and iterates over rows but skips the 0th
	vals = sheet.row_values(x)
	if len(vals[0]) != 0: ### loads in the first entry
		loc = str(vals[0]).lower()
	if vals[1] != '':
		typelist.append(str(vals[1]))	# Empty list was initialised at the start so can just append to it
	if vals[2] != '':
		wifilist.append(int(vals[2]))
	if vals[3] != '':
		gaslist.append(vals[3])
	if vals[4] != '':
		waterlist.append(vals[4])
	if vals[5] != '':
		eleclist.append(vals[5])
	if vals[6] != '':
		heatlist.append(vals[6])
	if vals[7] != '':
		coollist.append(vals[7])
	if (x == sheet.nrows - 1) or (len(sheet.row_values(x+1)[0]) != 0):      ## end of one data set, add entries in OR get to the end and add the entries in
		LocRefs.append({'loc':loc, 'loctype':typelist, 'wifilist':wifilist, 'gaslist':gaslist, 'waterlist':waterlist, 'eleclist':eleclist, 'heatlist':heatlist, 'coollist':coollist})
		loc, typelist, wifilist,gaslist,waterlist,eleclist,heatlist,coollist = '',[],[],[],[],[],[],[] #Clear it all out ready for the next thing
File.release_resources()	# Free's up memory

convfactors = { 'gas':  {'Cubic meters':11.19222667, 'm3':11.19222667, 'ft3':0.3169280441}, 'water': {'Cubic meters':1, 'm3':1}, None: {'MWh':1000,'kWh':1} }	# Store conversion factors as a dictionary and then look up as required
Regs={ int(rs['Id']):rs['Unit'] for item in r for rs in item['Registers'] }	# Dictionary of IDs:Units from the DCS Register List
VMs={ int(item['Id']):item['Unit'] for item in v }	# Dictionary of IDs:Units from the DCS VirtualMeters List

def getUnits(meterinfo):
	if meterinfo['MeterType'] == 'Register':
		if meterinfo['DataType'] in convfactors:
			convfact = convfactors[meterinfo['DataType']][Regs[int(meterinfo['Meter'])]]	#First index is the utility from DataType, second is the unit from the dict of id:units, looking up convfactors this results in the appropriate conversion factor for the meter
		else:
			convfact = convfactors[None][Regs[int(meterinfo['Meter'])]]	#First index is None (any thing but gas and water), second is the unit from the dict of id:units, looking up convfactors this results in the appropriate conversion factor for the meter
	elif meterinfo['MeterType'] == 'VirtualMeter':
		if meterinfo['DataType'] in convfactors:
			convfact = convfactors[meterinfo['DataType']][VMs[int(meterinfo['Meter'])]]	#First index is the utility from DataType, second is the unit from the dict of id:units, looking up convfactors this results in the appropriate conversion factor for the meter
		else:
			convfact = convfactors[None][VMs[int(meterinfo['Meter'])]]	#First index is None (any thing but gas and water), second is the unit from the dict of id:units, looking up convfactors this results in the appropriate conversion factor for the meter
	return convfact

def GetResults(FormResults):
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "GetResults begin"
	End = datetime.datetime.strptime(FormResults['enddate'][0], "%Y-%m-%d") + datetime.timedelta(days=1)
	Start = datetime.datetime.strptime(FormResults['startdate'][0], "%Y-%m-%d")
	Locations = FormResults['locations[]'] if 'locations[]' in FormResults else ['']
	IntPeriod = FormResults['integrationperiod']
	WantedDataTypes = []	# Build a list of datatypes which are required - effectively the headings of the table
	if "metereddata[]" in FormResults:
		WantedDataTypes.extend(FormResults["metereddata[]"])
	if "occupancydata" in FormResults:
		WantedDataTypes.append('wifi')
	if "weatherdata[]" in FormResults:
		WantedDataTypes.extend(FormResults["weatherdata[]"])
	WantedDataTypes = list(set(WantedDataTypes))	# Removed any duplications
	if not set(WantedDataTypes) <= set(AvailableData):	# If whats wanted is not a subset of what's available...
		print "The following data is not valid", set(WantedDataTypes)-set(AvailableData)	# Highlight it
		WantedDataTypes = list(set(WantedDataTypes) & set(AvailableData))	# strip out anything thats not actually valid
	AllData = { loc:{ datatype:{} for datatype in WantedDataTypes } for loc in Locations }	# Build an empty dictionary to accomodate the data. Keyed by location, then datatype which will then hold the datetime:data pairs
	TempStore = { s['loc']:s for s in LocRefs if str(s['loc']) in Locations }	# s will be a dictionary of lists of ids, so now TempStore is a dictionary of Locations, containing dictionary of types containing lists of IDs
	Count=0	# Just keep a tally of how much was grabbed overall for informatino
	if "metereddata[]" in FormResults:
		print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'Fetching DCS Metered data'
		Metered = FormResults["metereddata[]"]
		WantId = []	# Will be a list of dictionaries containing the DCS IDs and associated details
		for datatype in Metered:		# Feels like this loop could also be streamlined but not sure how yet!
			for loc in TempStore:
				for meter in TempStore[loc][datatype+'list']:	# For each meter (123, V123 etc) for that meter type for that given location
					if type(meter) != float:
						mtype = 'VirtualMeter'
						meter = meter[1:]	# This should be the numeric part such as 123 in V123
					else:
						mtype = 'Register'	# meter = the number so leave as is
					WantId.append({'Location':loc, 'DataType':datatype, 'Meter':int(meter), 'MeterType':mtype})
		TempIntPeriod = "halfhour" if IntPeriod[0] in ["minute","quarterhour"] else IntPeriod[0]	# Only HH data is available so finer stuff must default to this
		for item in WantId:
			conversionFactor = getUnits(item)
			UrlEnd = item['MeterType'] + 'Readings?Id=' + str(item['Meter']) + '&StartDate=' + Start.strftime("%Y-%m-%d") + '&EndDate=' + End.strftime("%Y-%m-%d") + '&IntegrationPeriod=' + TempIntPeriod
			obj = json.loads(urllib2.urlopen(urllib2.Request(dcsmaster + UrlEnd, headers=dcsheaders)).read())	# Get the json data and convert it to python native objects 
			for entry in obj:
				dt = datetime.datetime.strptime(entry['StartTime'][:19], "%Y-%m-%dT%H:%M:%S")	# Convert it to a native datetime ob. [:19] will ignore the Z if its there but wont mind if its not
				if entry['IsGenerated']:
					continue	# Skip over Null data
				else:
					Count+=1	# Keep a tally of what's been obtained
					if dt in AllData[item['Location']][item['DataType']]: # Check if this timestamp is there from another meter, just add it to get the total while applying the conversion factor on the way
						try:
							AllData[item['Location']][item['DataType']][dt] += (entry['PeriodValue'] * conversionFactor)
						except TypeError:
							pass
					else:	# If its not there, then create the first entry
						AllData[item['Location']][item['DataType']][dt] = (entry['PeriodValue'] * conversionFactor)
		print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'DCS Metered data fetched and collated!'
	if "occupancydata" in FormResults:
		print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'Fetching Wifi data from SQL'
		Occupancy = FormResults["occupancydata"]  #will need editing to add the [] if other occupancy set is added
		conn = MySQLdb.connect(SQLserver, SQLUser, SQLPassword, SQLdb)	# REAL DATABASE
#		conn=sqlite3.connect('EnergyDataPortal.db',detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)	# TEST DATABASE
		cursor = conn.cursor()
		if len(Locations) == 0:    #if place empty, retreive nothing
			cursor = []	# Give an empty list so the for loop at the end doesn't crash
			print "No Locations were requested"
		else:   #if place not empty, return data for places
			WantId = {}	# Will become a dictinoary of wifi location IDs as keys as they appear in the SQL database, with the value being the Location it corresponds to
			for loc in TempStore:
				for wifiid in TempStore[loc]['wifilist']:
					WantId[wifiid] = loc
			#cursor.execute('SELECT dateTime as "dateTime [DATETIME]", locId, count FROM WifiData WHERE (dateTime BETWEEN ? AND ?) AND (locId in (?))', (Start, End, ','.join(str(id) for id in WantId))) # TEST DATABASE
			cursor.execute("SELECT dateTime,locId,count FROM WifiData WHERE (dateTime BETWEEN %s AND %s) AND (locId in %s)", (Start, End, WantId.keys()))	# REAL DATABASE ######## CHECK TO MAKE SURE DATES WORK WHEN DATABASES CHANGED '%Y-%m-%d %H:%M:%S' is MySQL format
			print "Items from WifiData SQL query:", cursor.rowcount
		for row in cursor:	# row[0] should be the datetime (native), row[1] is the locId and row[2] the counts. May be possible to reference these by coloumn name rather than index. 
			# No need for Null Check as the entry simply wouldn't exist
			Count+=1	# Just count the valid item for info
			if row[0] in AllData[WantId[row[1]]]['wifi']: # Check if this timestamp is there from another ID, just add it to get the total
				AllData[WantId[row[1]]]['wifi'][row[0]] += row[2]
			else:	# If its not there, then create the first entry
				AllData[WantId[row[1]]]['wifi'][row[0]] = row[2]
#			AllData.append({'Time':unicode(row[0].isoformat()+'Z'), 'Place':row[1], 'Data':row[2], 'Type':'Wifi'})	# ORIGINAL REAL DATABASE
		conn.close()
		print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'Wifi data fetched and collated!'
	if "weatherdata[]" in FormResults:
		print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'Fetching Weather data from SQL'
		Weather = FormResults["weatherdata[]"]	# Collects up the data requestd
		#front = 'SELECT datetime as "datetime [DATETIME]", '
		front = "SELECT datetime, "
		#back = " FROM RTWeather WHERE (datetime BETWEEN ? AND ?)" # TEST DATABASE
		back = " FROM RTWeather WHERE (datetime BETWEEN %s AND %s)"	# REAL DATABASE
		command = front + ','.join(Weather) + back	# This will grab all weather items in one go
		conn = MySQLdb.connect(SQLserver, SQLUser, SQLPassword, SQLdb)	# REAL DATABASE
		#conn=sqlite3.connect('EnergyDataPortal.db',detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)	# TEST DATABASE
		cursor = conn.cursor()
		cursor.execute(command, (Start, End))	# CHECK TO MAKE SURE DATES WORK WHEN DATABASES CHANGED '%Y-%m-%d %H:%M:%S' is MySQL format
		print "Items from RTWeather SQL query:", cursor.rowcount
		WeatherData = { item:{} for item in Weather}	# You can only iterate through the cursor once so collect data here and then duplicate it over all locations after
		for row in cursor:	# Each row may have multiple weather items
			for subset in enumerate(Weather,1):	# enumerate creates tuples like (1,wind_dir),(2,windspeed)... index starts at 1 since 0 will be the datetime
				if row[subset[0]] == None:
					continue	# Skip Null Entries - moves onto the next weather parameter
				else:
					Count+=1	# Just count the valid item for info
					WeatherData[subset[1]].update({row[0]:row[subset[0]]})	# The enumerate generator will give subset[0] as the number representing the row number from the SQL query, and subset[1] as the datatype
#					AllData.append({'Time':unicode(row[0].isoformat()+'Z'), 'Place':loc, 'Data':row[subset[0]], 'Type':subset[1]})	# ORIGINAL REAL DATABASE
		conn.close()
		for loc in AllData:	# Now the WeatherData can be duplicated in all Locations
			AllData[loc].update(WeatherData)	# Update the data for the location to include all Weather Data
		print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), ' Weather data fetched and collated!'
	print "Total items retreived:", Count
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "GetResults end"
	return AllData

def Integegrate(data,Start,End,Period):
	Intervals = (End - Start).total_seconds()/Period.total_seconds()
	Averages = dict()	# Empty Directory to be populated by averages shortly
	for bin in ( Start+i*Period for i in xrange(int(Intervals)) ):	# For each integration period with a with the start datetimes given by this generator...
		Total=0	# Total for the average
		Count=0	# Number of elements for the average
		for i in (data[dt] for dt in data if bin <= dt < bin+Period):	# Generator gives elements along the way. Includes start datetime but not end.
			Total+=i	# Totalise as you go
			Count+=1		# Count as you go
		try:
			Averages[bin] = float(Total)/float(Count)	# Calculate Mean Average
		except ZeroDivisionError:
			Averages[bin] = None	# Divide by zero error means Count was zero and so there was nothing in this period so store None
	return Averages

def IntegrationCalc(data, FormResults):		#matches integration period input by averaging. for all inputs smaller than day, returns first entry as is, then all other results come from averaging data over previous increment in time. ie 03/07/2017 12:00 for HalfHour is average of data from 11:31 --> 12:00 inclusive.  For higher increments it is the opposite - 03/07/2017 00:00 for Day is the average from 00:00 --> 23:59 that day.
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "IntegrationCalc begin"
	IntPeriod = FormResults['integrationperiod'][0]
	Start = datetime.datetime.strptime(FormResults['startdate'][0], "%Y-%m-%d")  # convert starttime into datetime object to allow timedelta
	End = datetime.datetime.strptime(FormResults['enddate'][0], "%Y-%m-%d") + datetime.timedelta(days=1)
	Increments = {'quarterhour':datetime.timedelta(minutes=15), 'halfhour':datetime.timedelta(minutes=30), 'hour':datetime.timedelta(hours=1), 'day':datetime.timedelta(days=1), 'week':datetime.timedelta(days=7)}	# Dictionary lookup of increments as this is quicker than if, elif, elif etc.
	for loc in data:	# Each location key in AllData
		for datatype in data[loc]:
			if IntPeriod == 'minute' or ( datatype == 'wifi' and IntPeriod == 'quarterhour' ) or len(data[loc][datatype]) == 0:	# Order OR conditions with most likely true first (as later checks will be skipped if earlier ones are true as OR will already be satisfied)
				continue	# Minute data is smallest so no need to re-process. Likewise Wifi@Quarterhourly needs no re-processing, and empty sets can be skipped although this shouldn't happen
			Increment = Increments[IntPeriod]	# Set the timedelta from the dictionary
			number = (End - Start).total_seconds()/Increment.total_seconds()
			Averages = dict()	# Empty Directory to be populated by averages shortly
			for bin in ( Start+i*Increment for i in xrange(int(number)) ):	# For each integration period with a with the start datetimes given by this generator...
				Total=0	# Total for the average
				Count=0	# Number of elements for the average
				for i in (data[loc][datatype][dt] for dt in data[loc][datatype] if bin <= dt < bin+Increment):	# Generator gives elements along the way. Includes start datetime but not end.
					Total+=i	# Totalise as you go
					Count+=1		# Count as you go
				try:
					Averages[bin] = float(Total)/float(Count)	# Calculate Mean Average
				except ZeroDivisionError:
					Averages[bin] = None	# Divide by zero error means Count was zero and so there was nothing in this period so store None
			data[loc][datatype]=Averages	# Replace the existing data with the new averages - may potentially be an empty dict
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "IntegrationCalc end"
	return data

def InLineResults(data, FormResults):
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "InLineResults begin"
	#InLineDict = {}	# Will be keyed by (datetime, location) tuples, and contains dicts with all coloumns data
	WhatWeGot =  tuple ( col for col in AvailableData if col in set(datatype for loc in data for datatype in data[loc] ) )	# All of the data types that we have in an order tuple
	Timestamps = tuple ( sorted ( set ( timestamp for loc in data for datatype in data[loc] for timestamp in data[loc][datatype] ) ) )	# All of the timestanmps we have sorted into a tuple
	#for loc in data:
	#	for datatype in data[loc]:
	#		for dt in data[loc][datatype]:
	#			if (dt,loc) in InLineDict:	# If this (dt,loc) combination already has some data coloumnd, add these ones to it
	#				InLineDict[(dt,loc)].update({datatype:data[loc][datatype][dt]})
	#			else:	# Otherwise this is the first bit of data
	#				InLineDict[(dt,loc)] = {datatype:data[loc][datatype][dt]}
	#data = None
	#del data	# Free up some resources - its all now reshaped into InLineDict
	#for item in InLineDict:	# Run through again and fill in missing fields with None
	#	InLineDict[item].update({datatype:None for datatype in WhatWeGot if datatype not in InLineDict[item]})
	TableHead = tuple( ['Time', 'Place'] + [col.title()+units[col] for col in WhatWeGot] )	# Table Headings will be of the title and units for the datasets in the order given by AvailableData
	print 'Proposed Table Heading',TableHead
	#if len(WhatWeGot) == 1:
	#	TableContents = [[dt, loc, InLineDict[dt,loc][WhatWeGot[0]]] for dt,loc in sorted(InLineDict.keys(), key=itemgetter(1,0))]
	#else:
	#	TableContents = [[dt, loc]+list(itemgetter(*WhatWeGot)(InLineDict[dt,loc])) for dt,loc in sorted(InLineDict.keys(), key=itemgetter(1,0))]	# Creates a row list from the timestamp and location, followed by a list of values from the dictionary as required by the table coloumns. Keys from WhatWeGot which is in the correct order, and the data itself is added in order of location first, and then datetime
	TCGen = ( [dt,loc] + [ data[loc][datatype][dt] if (datatype in data[loc] and dt in data[loc][datatype]) else None for datatype in WhatWeGot ] for loc in data for dt in Timestamps ) # Generator creates each line as it goes when iterated over
	rowcount = len(data) * len(Timestamps)	# This should be how many rows there will be, partly just for information
	TableContents = JSONGen(TCGen,rowcount)	# Wrap the generator and predicted length into the JSONGen class so that the json module treats the generator as if its a list.
	print 'There should be {} rows of data'.format(rowcount)
	InLineList = {'headers':TableHead, 'contents':TableContents}
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "InLineResults end"
	return InLineList

'''	# Maybe this function could be used?
def DownloadData(FormResults, data):    #location of this file? will it download to where the code is running?
	if "download" in FormResults and FormResults["download"] == ["true"]:
		headings = []
		tdata = data
		for key in tdata[0]:
			headings.append(key) # creates headings - this could change if many types get displayed in one entry
		with open('portaldata.csv', 'wb') as csvfile:
			writer = csv.DictWriter(csvfile, fieldnames=headings)
			writer.writeheader()
			for row in tdata:
				row['Time'] = row['Time'].strftime('%d/%m/%Y %H:%M:%S')	# Convert datetime object to text for a csv file
				row['Place'] = row['Place'].title()
				writer.writerow(row)
		print "File downloaded"
'''
class CustomHandler(SimpleHTTPRequestHandler):	# Based on Python Standard Library
	def do_GET(self):	# Handles HTTP GET Verb
		UrlSplit = urlsplit(self.path.lower())	# Splits the URL and Query parts.
		print "UrlSplit: "+str(UrlSplit)
		QuerySplit = parse_qs(UrlSplit.query)	# Drops to lower case and splits apart the parameters and variables into a dictionary
		print "QuerySplit: "+str(QuerySplit)
		if UrlSplit.path == "/getloclist" or UrlSplit.netloc == "getloclist":
			print "Found GetLocList!!"
			self.send_response(200)	# Request must be exactly the right API path, be asking for the right parameter and the ID must be valid
			self.send_header('Access-Control-Allow-Origin', '*')
			self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
			self.send_header("Access-Control-Allow-Headers", "X-Requested-With")
			self.send_header("Access-Control-Allow-Headers", "Content-Type")
			self.send_header('Content-Type','application/json')
			self.end_headers()	# CORS compatible headers given
			loclist = {"All" : ["Conference Center 1", "Conference Center 2", "Main Campus Building A", "Main Campus Building B", "Residential Block One", "Residential Block Two"],
					"Conferences" : ["Conference Center 1", "Conference Center 2"],
					"Main" : ["Main Campus Building A", "Main Campus Building B"],
					"Residencies" : ["Residential Block One", "Residential Block Two"]}
			self.wfile.write(json.dumps(loclist))
			print "Sent loclist"
		elif UrlSplit.path == "/getdata" or UrlSplit.netloc == "getdata":
			print "Found GetData!!"
			self.send_response(200)	# Request must be exactly the right API path, be asking for the right parameter and the ID must be valid
			self.send_header('Access-Control-Allow-Origin', '*')
			self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
			self.send_header("Access-Control-Allow-Headers", "X-Requested-With")
			self.send_header("Access-Control-Allow-Headers", "Content-Type")
			self.send_header('Content-Type','application/json')
			self.end_headers()	# CORS compatible headers given
			Data = GetResults(QuerySplit) #function that gets the raw data
			#Data = AddReads(Data, QuerySplit) #function that adds the readings together where needed
			Data = IntegrationCalc(Data, QuerySplit)
			Data = InLineResults(Data, QuerySplit)
			#Data.sort(key=itemgetter('Time'), reverse=False)  # sorts the data into ascending order by date
			self.wfile.write(json.dumps(Data,default=jsondatetime))	# Returns the data as JSON format
			print "Sent Data"
			#DownloadData(QuerySplit, Data) - uncommenting this causes data to be downloaded to computer where code is running, not where user is viewing interface
			return
		else:	# If serving files is allowed, then the original Python library does this.
			print "Doing what it would normally do"
			SimpleHTTPRequestHandler.do_GET(self)
			#else:
			#self.send_error(404)	# File not Found for anything else.
		return
	def do_OPTIONS(self):	# This is purely for the CORS Preflight and is only given on valid API data requests.
		UrlSplit = urlsplit(self.path.lower())
		QuerySplit = parse_qs(UrlSplit.query)
		if (UrlSplit.path == "/getdata" or UrlSplit.netloc == "getdata") and "id" in QuerySplit and set(QuerySplit.get('id',[])) <= set(meters.keys()):
			self.send_response(200, "OK")	# Request must be exactly the right API path, be asking for the right parameter and the ID must be valid
			self.send_header('Access-Control-Allow-Origin', '*')
			self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
			self.send_header("Access-Control-Allow-Headers", "X-Requested-With")
			self.send_header("Access-Control-Allow-Headers", "Content-Type")
			self.end_headers()
			return
		else:
			self.send_error(405)	# Method not allowed
			return
				#	def list_directory(self, path):	# Patches the list_directory method so that files an be served but directories not listed.
				#		if True:
				#			SimpleHTTPRequestHandler.list_directory(self, path)	# If listing is allowed, then do what the original method does
				#		else:
				#			self.send_error(403) #No permission to list directory
#			return None	# Effectively, all directory listing is blocked

print "Starting WebServer..."
httpd = ThreadingTCPServer(('', 8080),CustomHandler)	# Start the HTTP Server
try:
	print "Ready"
	httpd.serve_forever()
except KeyboardInterrupt:	# Allow Ctrl+C locally to close it gracefully
	print "Shutting down..."
	httpd.shutdown()
	print "Done"
httpd.server_close()	# Finally close everything off
raise SystemExit	# Ensure explicit termination at this point
