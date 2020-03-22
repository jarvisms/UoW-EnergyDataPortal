#!/usr/bin/env python
# Remember to change EOL convention to suit Windows or Linux depending on where this will run
print 'Importing Libraries...'
#import MySQLdb	# REAL DATABASE
import sqlite3	# TEST DATABASE
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
units = {"gas":' (kWh)', "water":' (m3)', "elec":' (kWh)', "heat":' (kWh)', "cool":' (kWh)', "wind_dir":' (deg from N)', "windspeed":' (ms-1)', "gustspeed":' (ms-1)', "temp":' (deg C)', "humidity":' (%)', "pressure": ' (mB)', "solar":' (Wm-2)', "rain":' (mm since 00:00)', "Wifi":' (devices)'}

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

def adapt_datetime(dt): # Adapter so that SQLite3 can accept datetimes but store them in the db file as efficient integers
	return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())

def convert_datetime(b): # Converter so that SQLite3 can retreive efficient integers and return them as datetimes
	return datetime.datetime.fromtimestamp(int(b))

sqlite3.register_adapter(datetime.datetime, adapt_datetime)
sqlite3.register_converter("DATETIME", convert_datetime)

print 'Reading Excel spreadsheet...'
#Load in dictionary with all information on location types and meters. Result is list of dictionaries 'LocsRef', with 'loc', and type-, wifi-, gas- etc lists eg 'wifilist', 'gaslist', 'eleclist'
File = xlrd.open_workbook('LocationIdRefs.xls')
sheet = File.sheet_by_index(0)

typelist, wifilist,gaslist,waterlist,eleclist,heatlist,coollist = [],[],[],[],[],[],[] #lists accodomate for multiple types/meters corresponding to single locations
LocRefs = [{'loc':'', 'loctype':typelist, 'wifilist':wifilist, 'gaslist':gaslist, 'waterlist':waterlist, 'eleclist':eleclist, 'heatlist':heatlist, 'coollist':coollist}]   #the master list initialization with an empty location
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
	Start = FormResults['startdate'][0]
	End = FormResults['enddate'][0]
	Locations = FormResults['locations[]'] if 'locations[]' in FormResults else ['']
	IntPeriod = FormResults['integrationperiod']
#	AllData = []  #master list will contain all the data in uniform format - dictionary with 4 keys Data Time Place Type
	WantedDataTypes = []
	if "metereddata[]" in FormResults:
		WantedDataTypes.extend(FormResults["metereddata[]"])
	if "occupancydata" in FormResults:
		WantedDataTypes.append('wifi')
	if "weatherdata[]" in FormResults:
		WantedDataTypes.extend(FormResults["weatherdata[]"])
	AllData = { loc:{ datatype:{} for datatype in WantedDataTypes } for loc in Locations }
	Count=0
	if "metereddata[]" in FormResults:
		Metered = FormResults["metereddata[]"]
		WantId = []
		TempStore = (set for set in LocRefs if str(set['loc']) in Locations)	# ListComprehension/Generators executes faster and "if item in list" is much faster than looping through things repeatedly
		for part in Metered:		# Feels like this loop could also be streamlined but not sure how yet!
			search = part + 'list'
			for thing in TempStore:
				meters = thing[search]
				for meter in meters:
					if type(meter) != float:
						mtype = 'VirtualMeter'
						meter = str(meter[1:])
					else:
						mtype = 'Register'
					WantId.append({'Location':thing['loc'], 'DataType':part, 'Meter':int(meter), 'MeterType':mtype})
		TempData =[]
		TempIntPeriod = "halfhour" if IntPeriod[0] in ["minute","quarterhour"] else IntPeriod[0]
		for item in WantId:
			conversionFactor = getUnits(item)
			UrlEnd = item['MeterType'] + 'Readings?Id=' + str(int(item['Meter'])) + '&StartDate=' + Start + '&EndDate=' + End + '&IntegrationPeriod=' + TempIntPeriod
			print 'Fetching DCS Metered data:', dcsmaster + UrlEnd
			obj = json.loads(urllib2.urlopen(urllib2.Request(dcsmaster + UrlEnd, headers=dcsheaders)).read())
			for entry in obj:
				entry['Time'] = datetime.datetime.strptime(entry['StartTime'][:19], "%Y-%m-%dT%H:%M:%S")	# Convert it to a native datetime ob. [:19] will ignore the Z if its there but wont mind if its not
				if entry['Time'] > datetime.datetime.now():
					del entry	# Throw it away if its not needed before any further processing is done on it
					continue
				Count+=1
				try:
					AllData[item['Location']][item['DataType']][datetime.datetime.strptime(entry['StartTime'][:19], "%Y-%m-%dT%H:%M:%S")] += (entry['PeriodValue'] * conversionFactor)
				except KeyError:
					AllData[item['Location']][item['DataType']][datetime.datetime.strptime(entry['StartTime'][:19], "%Y-%m-%dT%H:%M:%S")] = (entry['PeriodValue'] * conversionFactor)
#				entry['Data'] = (entry['PeriodValue'] * conversionFactor)
#				if item['MeterType'] == 'VirtualMeter':
#					entry['Place'] = "v" + str(item['Meter'])
#				else:
#					entry['Place'] = item['Meter']
#				entry['Type'] = item['DataType']
#				del entry['Duration'], entry['IsGenerated'], entry['IsEstimated'], entry['PeriodValue'], entry['TotalValue'], entry['StartTime']
#				AllData.append(entry)
	if "occupancydata" in FormResults:
		End = datetime.datetime.strptime(FormResults['enddate'][0], "%Y-%m-%d") + datetime.timedelta(days=1)
		Start = datetime.datetime.strptime(FormResults['startdate'][0], "%Y-%m-%d")
######## CHECK TO MAKE SURE DATES WORK WHEN DATABASES CHANGED
#		End = date.strftime('%Y-%m-%d %H:%M:%S')	# Convert the date into MySQL format
#		Start = datetime.datetime.strptime(Start[0], "%Y-%m-%d").strftime('%Y-%m-%d %H:%M:%S')	# Convert the date into MySQL format
		Occupancy = FormResults["occupancydata"]  #will need editing if other occupancy sets added
#		conn = MySQLdb.connect(SQLserver, SQLUser, SQLPassword, SQLdb)	# REAL DATABASE
		conn=sqlite3.connect('EnergyDataPortal.db',detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)	# TEST DATABASE
		cursor = conn.cursor()
		if len(Locations) == 0:    #if place empty, retreive nothing
			cursor = []	# Give an empty list so the for loop at the end doesn't crash
			print "No Locations were requested"
		elif len(Locations) >= 1:   #if place not empty, return data for places
			IdList = []
			TempStore = []
			IDLocs = {}
			for x in Locations:
				for a in LocRefs:
					if x == str(a['loc']):
						TempStore.append(a)
			for item in TempStore:
				for wmeter in item['wifilist']:
					IdList.append(wmeter)
					IDLoc[wmeter] = item['loc']
			cursor.execute('SELECT dateTime as "dateTime [DATETIME]", locId, count FROM WifiData WHERE (dateTime BETWEEN ? AND ?) AND (locId in (?))', (Start, End, ','.join(str(id) for id in IdList))) # TEST DATABASE
#			cursor.execute("SELECT * FROM WifiData WHERE (dateTime BETWEEN %s AND %s) AND (locId in %s)", (Start, End, IdList))	# REAL DATABASE
			print "Items from SQL query:", cursor.rowcount
		for row in cursor:
			Count+=1
			try:
				AllData[IDLoc[row[1]]]['wifi'][row[0]] += row[2]
			except KeyError:
				AllData[IDLoc[row[1]]]['wifi'][row[0]] = row[2]
#			AllData.append({'Time':row[0], 'Place':row[1], 'Data':row[2], 'Type':'Wifi'})	# TEST DATABASE
#			AllData.append({'Time':row[0], 'Place':row[1], 'Data':row[2], 'Type':'Wifi'})	# REAL DATABASE
#			AllData.append({'Time':unicode(row[0].isoformat()+'Z'), 'Place':row[1], 'Data':row[2], 'Type':'Wifi'})	# ORIGINAL REAL DATABASE
		conn.close()
	if "weatherdata[]" in FormResults:
		End = datetime.datetime.strptime(FormResults['enddate'][0], "%Y-%m-%d") + datetime.timedelta(days=1)
		Start = datetime.datetime.strptime(FormResults['startdate'][0], "%Y-%m-%d")
######## CHECK TO MAKE SURE DATES WORK WHEN DATABASES CHANGED
#			End = date.strftime('%Y-%m-%d %H:%M:%S')	# Convert the date into MySQL format
#			Start = datetime.datetime.strptime(Start[0], "%Y-%m-%d").strftime('%Y-%m-%d %H:%M:%S')	# Convert the date into MySQL format
		Weather = FormResults["weatherdata[]"]
#		conn = MySQLdb.connect(SQLserver, SQLUser, SQLPassword, SQLdb)	# REAL DATABASE
		conn=sqlite3.connect('EnergyDataPortal.db',detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)	# TEST DATABASE
		cursor = conn.cursor()
		front = 'SELECT datetime as "datetime [DATETIME]", '
#		front = "SELECT datetime, "
		back = " FROM RTWeather WHERE (datetime BETWEEN ? AND ?)" # TEST DATABASE
#		back = " FROM RTWeather WHERE (datetime BETWEEN %s AND %s)"	# REAL DATABASE
		command = front + ','.join(Weather) + back	# This will grab all weather items in one go
		cursor.execute(command, (Start, End))
		print "Items from SQL query:", cursor.rowcount
		for row in cursor:	# Each row may have multiple weather items
			for subset in enumerate(Weather,1):	# enumerate creates tuples like (1,wind_dir),(2,windspeed)... index starts at 1 since 0 will be the datetime
				for x in Locations:
					Count+=1
					AllData[x][subset[1]][row[0]] = row[subset[0]]
#					AllData.append({'Time':row[0], 'Place':x, 'Data':row[subset[0]], 'Type':subset[1]})	# NEW DATABASE
#					AllData.append({'Time':unicode(row[0].isoformat()+'Z'), 'Place':x, 'Data':row[subset[0]], 'Type':subset[1]})	# ORIGINAL REAL DATABASE
		conn.close()
	print "Total items retreived:", Count
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "GetResults end"
	return AllData
'''
def AddReads(data, FormResults):   #function adds readings from multiple meters to give total for buildings
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "AddReads begin"
	NewData = []
	listlist = ["wifilist", "gaslist", "waterlist", "eleclist", "heatlist", "coollist"]  #these are the lists that might contain multiples for adding
	weathertypes = ["wind_dir", "windspeed", "gustspeed", "temp", "humidity", "pressure", "solar", "rain"]   #these will only ever contain single entries
	Locations = FormResults['locations[]'] if 'locations[]' in FormResults else ['']
	for item in Locations:  # iterates over list of locations requested
		for entry in LocRefs:
			if item == entry['loc']:
					store = entry  #stores the entry containing information about that location
		for givenlist in listlist:
			copy = data # !!! Why do we need a copy?
			for x in data:	# !!! Checked every data point (repeatedly for each givenlist? Better to check every data point and put it in the right list
				if x['Type'] in weathertypes:   #if a weather data point is found, append as is and continue
					if x in NewData:	# !! Should be no need to repeatedly check
						continue
					else:
						NewData.append(x)
					continue
				if x['Type'].lower() not in givenlist:  # if the data point isn't in this particular list, continue - it will be picked up in the list it belongs to
					continue
				if type(x['Place']) == int or type(x['Place']) == float:    # if the meter number is saved as a number
					if int(x['Place']) not in store[givenlist] and float(x['Place']) not in store[givenlist]: #and is not in the list we are currently looking at, move on
						continue
				if type(x['Place']) == str or type(x['Place']) == unicode:  # if meter number is virtual or saved as string or unicode, and is not in current list, move on
					for number in store[givenlist]:
						same = 0
						if str(number) in x['Place']:
							same += 1
					if same == 0:
						continue
				if len(givenlist) == 1:   # if the list only contains one meter, append that entry as it is 
					NewData.append(x)
				elif len(givenlist) == 0:   #  if the list is empty, carry on - this data wasn't requested
					continue
				else:
					counter = x['Data']  # otherwise, initialise the counter
					for y in copy:	# !!! Nested loop again. O**2 operation now
						if x['Time'] == y['Time']  and x['Type'] == y['Type'] and x['Place'] != y['Place']: # time and type must be the same but place must be different
							if x['Place'] in store[givenlist] and y['Place'] in store[givenlist]:  # if both places (meters) belong to the same list, add to the counter
								counter += y['Data']
					if {'Time':x['Time'], 'Place':item, 'Type':x['Type'], 'Data':counter} in NewData:  # if the entry is already in there, continue
						continue
					else:
						NewData.append({'Time':x['Time'], 'Place':item, 'Type':x['Type'], 'Data':counter})   # if entry is new, append to NewData list
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "AddReads end"
	return NewData
'''

def IntegrationCalc(data, FormResults):		#matches integration period input by averaging. for all inputs smaller than day, returns first entry as is, then all other results come from averaging data over previous increment in time. ie 03/07/2017 12:00 for HalfHour is average of data from 12:31 --> 12:00 inclusive.  For higher increments it is the opposite - 03/07/2017 00:00 for Day is the average from 00:00 --> 23:59 that day.
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "IntegrationCalc begin"
#	ignorelist = ['gas', 'water', 'elec', 'heat', 'cool']   # the data that comes from the DCS already has intperiod built in, so we don't want to change entries of these types.
#	Locations = FormResults['locations[]'] if 'locations[]' in FormResults else ['']
	IntPeriod = FormResults['integrationperiod'][0]
	Start = datetime.datetime.strptime(FormResults['startdate'][0], "%Y-%m-%d")  # convert starttime into datetime object to allow timedelta
	End = datetime.datetime.strptime(FormResults['enddate'][0], "%Y-%m-%d") + datetime.timedelta(days=1)
	'''IntData = []  # initialise the list that will be returned
	for place in Locations:
		wind_dirl, windspeedl, gustspeedl, templ, humidityl, pressurel, solarl, rainl, wifil = [], [], [], [], [], [], [], [], []  # lists for each type of data to be put into so that list can be iterated over
		IntPeriod = FormResults['integrationperiod'][0]
		Start = datetime.datetime.strptime(FormResults['startdate'][0], "%Y-%m-%d")  # convert starttime into datetime object to allow timedelta
		data.sort(key=itemgetter('Time'), reverse=False)  # sorts the data into ascending order by date
		for item in data:				# in this section the entries get sorted into lists by type
			if item['Type'].lower() == "wind_dir":
				wind_dirl.append(item)
			if item['Type'].lower() == "windspeed":
				windspeedl.append(item)
			if item['Type'].lower() == "gustspeed":
				gustspeedl.append(item)
			if item['Type'].lower() == "temp":
				templ.append(item)
			if item['Type'].lower() == "humidity":
				humidityl.append(item)
			if item['Type'].lower() == "pressure":
				pressurel.append(item)
			if item['Type'].lower() == "solar":
				solarl.append(item)
			if item['Type'].lower() == "rain":
				rainl.append(item)
			if item['Type'].lower() == "wifi":
				wifil.append(item)
			if item['Type'].lower() in ignorelist:  #if the item belongs to the ignorelist, append it as it is and continue
				IntData.append(item)
				continue
		alllists = [wind_dirl, windspeedl, gustspeedl, templ, humidityl, pressurel, solarl, rainl, wifil]  #list of lists
		for thelist in alllists:'''
	for loc in data:
		for datatype in data[loc]:
			if len(data[loc][datatype]) == 0:
				continue
			if IntPeriod == 'minute':  #custom - minute is finest precision --> all sets in original frequency
				#IntData[loc][datatype]=data[loc][datatype]
				continue
			elif IntPeriod == 'quarterhour':  #custom - continue at end to skip all bottom bit
				if datatype == 'wifi':
					#IntData[loc][datatype]=data[loc][datatype]
					continue
				else:
					Increment = datetime.timedelta(minutes=15)
			elif IntPeriod == 'halfhour':
				Increment = datetime.timedelta(minutes=30)
			elif IntPeriod == 'hour':
				Increment = datetime.timedelta(hours=1)
			elif IntPeriod == 'day':
				Increment = datetime.timedelta(days=1)
			elif IntPeriod == 'week':
				Increment = datetime.timedelta(days=7)
			number = (End - Start).total_seconds()/Increment.total_seconds()
			Averages = dict()	# Empty Directory to be populated by averages shortly
			for bin in ( Start+i*Increment for i in xrange(int(number)) ):	# For each integration period with a with the start datetimes given by this generator...
				Total=0	# Total for the average
				Count=0	# Number of elements for the average
				#for i in (item['Data'] for item in thelist if item['Time'] >= bin and item['Time'] < bin+Increment):	# For each element given by the generator which gives all data points within the given time window
				for i in (data[loc][datatype][dt] for dt in data[loc][datatype] if bin+Increment > dt >= bin):
					Total+=i	# Totalise as you go
					Count+=1		# Count as you go
				try:
					Averages[bin] = float(Total)/float(Count)	# Calculate Mean Average
				except ZeroDivisionError:
					pass	# Divide by zero error means Count was zero and so there was nothing in this time so just move on
			data[loc][datatype]=Averages
#			for entry in Averages:
#				IntData.append({'Data':Averages[entry], 'Time':entry, 'Type':thelist[0]['Type'], 'Place':place})
#			del Averages
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "IntegrationCalc end"
	return data

def InLineResults(data, FormResults):
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "InLineResults begin"
	AddCols = []
	if 'metereddata[]' in FormResults:
		AddCols.extend(FormResults['metereddata[]'])	# Just stick the whole list in
	if 'weatherdata[]' in FormResults:
		AddCols.extend(FormResults['weatherdata[]'])
	if 'occupancydata' in FormResults:
		AddCols.extend(FormResults['occupancydata'])
	InLineDict = {}
	for loc in data:
		for datatype in data[loc]:
			InLineDict[(dt,loc)].update({(datatype+units[datatype]):data[loc][datatype][dt]})
	InLineList = sorted( {'Time':dt, 'Place':loc}.update(InLineDict[(dt,loc)]) for dt,loc in InLineDict )
	'''for x in data:
		#x['Type'] = x['Type'] + units[x['Type']]
		datastore = {'Time':x['Time'], 'Place':x['Place'], x['Type']+units[x['Type']]:x['Data']}
		for y in data:
			if x['Time'] == y['Time'] and x['Place'] == y['Place'] and x['Type'] != y['Type']:
				datastore[y['Type']+units[y['Type']]] = y['Data']
		if datastore in InLineData:
			continue
		else:
			InLineData.append(datastore)'''
	print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), "InLineResults end"
	return InLineList



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
			Data.sort(key=itemgetter('Time'), reverse=False)  # sorts the data into ascending order by date
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
