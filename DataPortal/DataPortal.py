#!/usr/bin/env python
# Remember to change EOL convention to suit Windows or Linux depending on where this will run
import urllib2, base64, json, xlrd, MySQLdb, datetime, csv
from SocketServer import ThreadingTCPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
from json import dumps
from urlparse import urlsplit, parse_qs
from operator import itemgetter

master ='http://URL_TO_ENERGY_DATA/'

units = {"gas":' (kWh)', "water":' (m3)', "elec":' (kWh)', "heat":' (kWh)', "cool":' (kWh)', "wind_dir":' (deg from N)', "windspeed":' (ms-1)', "gustspeed":' (ms-1)', "temp":' (deg C)', "humidity":' (%)', "pressure": ' (mB)', "solar":' (Wm-2)', "rain":' (mm since 00:00)', "Wifi":' (devices)'}

#Load in dictionary with all information on location types and meters. Result is list of dictionaries 'LocsRef', with 'loc', and type-, wifi-, gas- etc lists eg 'wifilist', 'gaslist', 'eleclist'
File = xlrd.open_workbook('LocationIdRefs.xls')
sheet = File.sheet_by_index(0)
LocRefs = []   #the master list initialization
typelist = []  #lists accodomate for multiple types/meters corresponding to single locations
wifilist = []
gaslist = []
waterlist = []
eleclist = []
heatlist = []
coollist = []
for x in range(sheet.nrows):  # opens spreadsheet and iterates over rows
	vals = sheet.row_values(x)
	if x == 0:
		continue
	if len(vals[0]) != 0: ### loads in the first entry
		loc = str(vals[0]).lower()
		if vals[1] != '':
			typelist = [str(vals[1])]
		if vals[2] != '':
			wifilist = [int(vals[2])]
		if vals[3] != '':
			gaslist = [vals[3]]
		if vals[4] != '':
			waterlist = [vals[4]]
		if vals[5] != '':
			eleclist = [vals[5]]
		if vals[6] != '':
			heatlist = [vals[6]]
		if vals[7] != '':
			coollist = [vals[7]]
	else:
		if vals[1] != '':     ### if there are more, loads in the other entries
			typelist.append(str(vals[1]))
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
	if x == sheet.nrows - 1:      ## get to the end and add the entries in
		LocRefs.append({'loc':loc, 'loctype':typelist, 'wifilist':wifilist, 'gaslist':gaslist, 'waterlist':waterlist, 'eleclist':eleclist, 'heatlist':heatlist, 'coollist':coollist})
		loc, typelist, wifilist,gaslist,waterlist,eleclist,heatlist,coollist = '','',[],[],[],[],[],[]
	elif len(sheet.row_values(x+1)[0]) != 0:  #end of one data set, add entries in
		LocRefs.append({'loc':loc, 'loctype':typelist, 'wifilist':wifilist, 'gaslist':gaslist, 'waterlist':waterlist, 'eleclist':eleclist, 'heatlist':heatlist, 'coollist':coollist})
		loc, typelist, wifilist,gaslist,waterlist,eleclist,heatlist,coollist = '','',[],[],[],[],[],[]


# function gets units for meter and figures out conversion to standard units kWh and (m3 for water)
def getUnits(meterinfo):
	if meterinfo['MeterType'] == 'Register':
		for entry in r:
			for item in entry['Registers']:
				if meterinfo['Meter'] == item['Id']:
					if item['Unit'] == 'MWh':
						convfact = 1000
					elif item['Unit'] == 'kWh':
						convfact = 1
					elif meterinfo['DataType'] == 'gas':
						if item['Unit'] == 'Cubic meters' or item['Unit'] == 'm3':
							convfact = 11.19222667
						elif item['Unit'] == 'ft3':
							convfact = 0.3169280441
						else:
							print "invalid unit on meter" + item['Unit']
					elif meterinfo['DataType'] == 'water':
						if item['Unit'] == 'Cubic meters' or item['Unit'] == 'm3':
							convfact = 1
					else:
						print "invalid meter unit" + item['Unit']
	if meterinfo['MeterType'] == 'VirtualMeter':
		for item in v:
			if meterinfo['Meter'] == item['Id']:
				if item['Unit'] == 'MWh':
					convfact = 1000
				elif item['Unit'] == 'kWh':
					convfact = 1
				elif meterinfo['DataType'] == 'gas':
					if item['Unit'] == 'Cubic meters' or item['Unit'] == 'm3':
						convfact = 11.19222667
					elif item['Unit'] == 'ft3':
						convfact = 0.3169280441
					else:
						print "invalid unit on meter" + item['Unit']
				elif meterinfo['DataType'] == 'water':
					if item['Unit'] == 'Cubic meters' or item['Unit'] == 'm3':
						convfact = 1
				else:
					print "invalid meter unit" + item['Unit']

	return convfact
username = 'USERNAME'
password = 'PASSWORD'
v = obj = json.loads(urllib2.urlopen(urllib2.Request(master+'VirtualMeters', headers={'Authorization':'Basic ' + base64.b64encode(username + ':' + password), 'Content-Type':'application/json, text/json'})).read())
r = obj = json.loads(urllib2.urlopen(urllib2.Request(master+'Meters', headers={'Authorization':'Basic ' + base64.b64encode(username + ':' + password), 'Content-Type':'application/json, text/json'})).read())
def GetResults(FormResults):
	Start = FormResults['startdate']
	End = FormResults['enddate']
	LocationSets = FormResults['locationsets']
	if 'locations[]' in FormResults:
		Locations = FormResults['locations[]']
	else:
		Locations = ""
	IntPeriod = FormResults['integrationperiod']
	AllData = []  #master list will contain all the data in uniform format - dictionary with 4 keys Data Time Place Type
	if "metereddata[]" in FormResults:
		Metered = FormResults["metereddata[]"]
		WantId = []
		TempStore = []
		for x in Locations:
			for set in LocRefs:
				if x == str(set['loc']):
					TempStore.append(set)
		for part in Metered:
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
		if IntPeriod[0] == "minute" or IntPeriod[0] == "quarterhour":
			TempIntPeriod = "halfhour"
		else:
			TempIntPeriod = IntPeriod[0]
		for item in WantId:
			conversionFactor = getUnits(item)
			UrlEnd = item['MeterType'] + 'Readings?Id=' + str(int(item['Meter'])) + '&StartDate=' + Start[0] + '&EndDate=' + End[0] + '&IntegrationPeriod=' + TempIntPeriod
			url = master + UrlEnd
			username = 'USERNAME'
			password = 'PASSWORD'
			obj = json.loads(urllib2.urlopen(urllib2.Request(url, headers={'Authorization':'Basic ' + base64.b64encode(username + ':' + password), 'Content-Type':'application/json, text/json'})).read())
			for entry in obj:
				entry['Data'] = (entry['PeriodValue'] * conversionFactor)
				if item['MeterType'] == 'VirtualMeter':
					entry['Place'] = "v" + str(item['Meter'])
					x = entry['StartTime']   ## virtual meter datetime objs don't have Z on end, marking GMT
					x = unicode(str(x+'Z'))  ## if that gets fixed this will need to change

				else:
					entry['Place'] = item['Meter']
					x = entry['StartTime']
				entry['Time'] = x
				entry['Type'] = item['DataType']
				del entry['Duration'], entry['IsGenerated'], entry['IsEstimated'], entry['PeriodValue'], entry['TotalValue'], entry['StartTime']
				temptime = datetime.datetime.strptime(entry['Time'], "%Y-%m-%dT%H:%M:%SZ")
				if temptime > datetime.datetime.now():
					del entry
					continue
				AllData.append(entry)

	if "occupancydata" in FormResults:
		for date in End:
			date = datetime.datetime.strptime(date, "%Y-%m-%d")
			date = date + datetime.timedelta(days=1) #+ datetime.timedelta(minutes=-15)
		del(End)
		End = date.strftime('%Y-%m-%d %H:%M:%S')	# Convert the date into MySQL format
		Start = datetime.datetime.strptime(Start[0], "%Y-%m-%d").strftime('%Y-%m-%d %H:%M:%S')	# Convert the date into MySQL format
		Occupancy = FormResults["occupancydata"]  #will need editing if other occupancy sets added
		server = "SERVER"
		SQLUser = "USERNAME"
		SQLPassword = "PASSWORD"
		conn = MySQLdb.connect(server, SQLUser, SQLPassword, "db_EnergyDataPortal")
		cursor = conn.cursor()
		if len(Locations) == 0:    #if place empty, select all data
			cursor.execute("SELECT * FROM WifiData WHERE (dateTime BETWEEN %s AND %s)", (Start, End))
			print "Items from SQL query:", cursor.rowcount
		elif len(Locations) >= 1:   #if place not empty, return data for places
			IdList = []
			TempStore = []
			for x in Locations:
				for set in LocRefs:
					if x == str(set['loc']):
						TempStore.append(set)
			for item in TempStore:
				for wmeter in item['wifilist']:
					IdList.append(wmeter)
			cursor.execute("SELECT * FROM WifiData WHERE (dateTime BETWEEN %s AND %s) AND (locId in %s)", (Start, End, IdList))
			print "Items from SQL query:", cursor.rowcount
		for row in cursor:
			AllData.append({'Time':unicode(row[0].isoformat()+'Z'), 'Place':row[1], 'Data':row[2], 'Type':'Wifi'})
		conn.close()

	if "weatherdata[]" in FormResults:
		if "occupancydata" not in FormResults:
			for date in End:
				date = datetime.datetime.strptime(date, "%Y-%m-%d")
				date = date + datetime.timedelta(days=1) #+ datetime.timedelta(minutes=-1)
			del(End)
			End = date.strftime('%Y-%m-%d %H:%M:%S')	# Convert the date into MySQL format
			Start = datetime.datetime.strptime(Start[0], "%Y-%m-%d").strftime('%Y-%m-%d %H:%M:%S')	# Convert the date into MySQL format
		Weather = FormResults["weatherdata[]"]
		server = "SERVER"
		SQLUser = "USERNAME"
		SQLPassword = "PASSWORD"
		conn = MySQLdb.connect(server, SQLUser, SQLPassword, "db_EnergyDataPortal")
		cursor = conn.cursor()
		for subset in Weather:
			front = "SELECT datetime, "
			back = " FROM RTWeather WHERE (datetime BETWEEN %s AND %s)"
			command = subset.join([front, back])
			cursor.execute(command, (Start, End))
			print "Items from SQL query:", cursor.rowcount
			for row in cursor:
				for x in Locations:
					AllData.append({'Time':unicode(row[0].isoformat()+'Z'), 'Place':x, 'Data':row[1], 'Type':subset})
	return AllData

def AddReads(data, FormResults):   #function adds readings from multiple meters to give total for buildings
	NewData = []
	listlist = ["wifilist", "gaslist", "waterlist", "eleclist", "heatlist", "coollist"]  #these are the lists that might contain multiples for adding
	weathertypes = ["wind_dir", "windspeed", "gustspeed", "temp", "humidity", "pressure", "solar", "rain"]   #these will only ever contain single entries
	if 'locations[]' in FormResults:
		Locations = FormResults['locations[]']
	for item in Locations:  # iterates over list of locations requested
		for entry in LocRefs:
			if item == entry['loc']:
					store = entry  #stores the entry containing information about that location
		for givenlist in listlist:
			copy = data
			for x in data:
				if x['Type'] in weathertypes:   #if a weather data point is found, append as is and continue
					if x in NewData:
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
					for y in copy:
						if (x['Time'] in y['Time'] or y['Time'] in x['Time']) and x['Type'] == y['Type'] and x['Place'] != y['Place']: # time and type must be the same but place must be different
							if x['Place'] in store[givenlist] and y['Place'] in store[givenlist]:  # if both places (meters) belong to the same list, add to the counter
								counter += y['Data']
					if {'Time':x['Time'], 'Place':item, 'Type':x['Type'], 'Data':counter} in NewData:  # if the entry is already in there, continue
						continue
					else:
						NewData.append({'Time':x['Time'], 'Place':item, 'Type':x['Type'], 'Data':counter})   # if entry is new, append to NewData list
	return NewData


def IntegrationCalc(data, FormResults):		#matches integration period input by averaging. for all inputs smaller than day, returns first entry as is, then all other results come from averaging data over previous increment in time. ie 03/07/2017 12:00 for HalfHour is average of data from 12:31 --> 12:00 inclusive.  For higher increments it is the opposite - 03/07/2017 00:00 for Day is the average from 00:00 --> 23:59 that day.
	ignorelist = ['gas', 'water', 'elec', 'heat', 'cool']   # the data that comes from the DCS already has intperiod built in, so we don't want to change entries of these types.
	if 'locations[]' in FormResults:
		Locations = FormResults['locations[]']
	IntData = []  # initialise the list that will be returned
	for place in Locations:
		wind_dirl, windspeedl, gustspeedl, templ, humidityl, pressurel, solarl, rainl, wifil = [], [], [], [], [], [], [], [], []  # lists for each type of data to be put into so that list can be iterated over
		IntPeriod = FormResults['integrationperiod'][0]
		Start = FormResults['startdate'][0]
		Start = datetime.datetime.strptime(Start, "%Y-%m-%d")  # convert starttime into datetime object to allow timedelta
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
		End = FormResults['enddate'][0]
		End = datetime.datetime.strptime(End, "%Y-%m-%d")
		End = End + datetime.timedelta(days=1) #+ datetime.timedelta(minutes=-1)
		for thelist in alllists:
			if len(thelist) == 0:
				continue
			if IntPeriod == 'minute':  #custom - minute is finest precision --> all sets in original frequency
				for entry in thelist:
					IntData.append(entry)
				continue
			if IntPeriod == 'quarterhour':  #custom - continue at end to skip all bottom bit
				if thelist != wifil:
					Increment = datetime.timedelta(minutes=15)
					i = 1   ## for the day, average is whole day beginning x y 00:00:00
					counter = 0
					howmany = 0
					for entry in thelist:
						target = Start + i*Increment
						currenttime = datetime.datetime.strptime(entry['Time'], "%Y-%m-%dT%H:%M:%SZ")
						if currenttime != target:
							if entry['Place'] == place or entry['Place'] == 'All':
								counter += entry['Data']
								howmany += 1
						elif currenttime == target:
							if entry['Place'] == place or entry['Place'] == 'All':
								IntData.append({'Data':float(counter)/float(howmany), 'Time':str((target-Increment).isoformat())+'Z', 'Type':entry['Type'], 'Place':entry['Place']})
								counter = 0
								howmany = 0
								i += 1
								howmany += 1
								counter += entry['Data']
				else:
					for entry in thelist:
						IntData.append(entry)
				continue
			if IntPeriod == 'halfhour':
				Increment = datetime.timedelta(minutes=30)
			elif IntPeriod == 'hour':
				Increment = datetime.timedelta(hours=1)
			elif IntPeriod == 'day':
				Increment = datetime.timedelta(days=1)
			elif IntPeriod == 'week':
				Increment = datetime.timedelta(days=7)
			number = (End - Start).total_seconds()/Increment.total_seconds()

			Bins = { Start+i*Increment:[] for i in range(int(number)) } # Creates a dictionary of bins with the key being the start of the target window. the contents are empty lists at the moment
			Averages = { bin:0 for bin in Bins }
			for bin in Bins:    # For each bin which represents the start of the value at the start of the window
				Bins[bin] = [item['Data'] for item in thelist if datetime.datetime.strptime(item['Time'], "%Y-%m-%dT%H:%M:%SZ") >= bin and datetime.datetime.strptime(item['Time'], "%Y-%m-%dT%H:%M:%SZ") < bin+Increment]
				if len(Bins[bin])==0:
					 del Averages[bin]
				else:
					Averages[bin] = float(sum(Bins[bin])) / max(len(Bins[bin]),1)
			for entry in Averages:
				IntData.append({'Data':Averages[entry], 'Time':str(entry.isoformat())+'Z', 'Type':thelist[0]['Type'], 'Place':place})
			del Bins, Averages
	return IntData

def InLineResults(data, FormResults):
	Start = FormResults['startdate']
	End = FormResults['enddate']
	LocationSets = FormResults['locationsets']
	if 'locations[]' in FormResults:
		Locations = FormResults['locations[]']
	else:
		Locations = ""
		IntPeriod = FormResults['integrationperiod']
	AddCols = []
	if 'metereddata[]' in FormResults:
		for item in FormResults['metereddata[]']:
			AddCols.append(item)
	if 'weatherdata[]' in FormResults:
		for item in FormResults['weatherdata[]']:
			AddCols.append(item)
	if 'occupancydata' in FormResults:
		for item in FormResults['occupancydata']:
			AddCols.append(item)
	InLineData = []
	for x in data:
		#x['Type'] = x['Type'] + units[x['Type']]
		datastore = {'Time':x['Time'], 'Place':x['Place'], x['Type']+units[x['Type']]:x['Data']}
		for y in data:
			if x['Time'] == y['Time'] and x['Place'] == y['Place'] and x['Type'] != y['Type']:
				datastore[y['Type']+units[y['Type']]] = y['Data']
		if datastore in InLineData:
			continue
		else:
			InLineData.append(datastore)
	return InLineData



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
				row['Time'] = ' '.join(str(row['Time'])[0:19].split('T'))
				row['Place'] = row['Place'].title()
				writer.writerow(row)
		print "File downloaded"


class CustomHandler(SimpleHTTPRequestHandler):	# Based on Python Standard Library
	def do_GET(self):	# Handles HTTP GET Verb
		UrlSplit = urlsplit(self.path.lower())	# Splits the URL and Query parts.
		print "UrlSplit: "+str(UrlSplit)
		QuerySplit = parse_qs(UrlSplit.query)	# Drops to lower case and splits apart the parameters and variables into a dictionary
		print "QuerySplit: "+str(QuerySplit)
		if UrlSplit.path == "/getdata" or UrlSplit.netloc == "getdata":
			print "Found GetData!!"
			self.send_response(200)	# Request must be exactly the right API path, be asking for the right parameter and the ID must be valid
			self.send_header('Access-Control-Allow-Origin', '*')
			self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
			self.send_header("Access-Control-Allow-Headers", "X-Requested-With")
			self.send_header("Access-Control-Allow-Headers", "Content-Type")
			self.send_header('Content-Type','application/json')
			self.end_headers()	# CORS compatible headers given
			Data = GetResults(QuerySplit) #function that gets the raw data
			Data = AddReads(Data, QuerySplit) #function that adds the readings together where needed
			Data = IntegrationCalc(Data, QuerySplit)
			Data = InLineResults(Data, QuerySplit)
			Data.sort(key=itemgetter('Time'), reverse=False)  # sorts the data into ascending order by date
			self.wfile.write(dumps(Data))	# Returns the data as JSON format
			print Data
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

if __name__ == '__main__':
	httpd = ThreadingTCPServer(('localhost', 8080),CustomHandler)	# Start the HTTP Server
	try:
		print "Starting..."
		httpd.serve_forever()
	except KeyboardInterrupt:	# Allow Ctrl+C locally to close it gracefully
		print "Shutting down..."
		httpd.shutdown()
		print "Done"
	httpd.server_close()	# Finally close everything off
	raise SystemExit	# Ensure explicit termination at this point
