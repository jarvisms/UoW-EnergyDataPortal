# This code takes the live wifi access point data and stores it in the SQL server

import xml.etree.ElementTree as ET, datetime, MySQLdb, urllib2, re

link = "URL_TO_WIFI_XML"
SQLuser = "USERNAME"
SQLpassword = "PASSWORD"

def GetRootXML(link, tries=3):
	'''Get Root XML object from document on website'''
	root = None # Setting this to None means that other things will break if this fails so the error cannot propagate
	while root is None and tries > 0:
		try:	
			root = ET.fromstring(urllib2.urlopen(link).read())
			tries = 0
			print "Got xml file"
		except Exception as err:
			tries -= 1
			print "Failed to get xml file, remaining attempts: {}\n{}".format(tries,err)
	return root

def GetClientCounts(root):
	'''Provides a dictionary of clientcounts given an XML root'''
	clientcounts = None # Setting this to None means that other things will break if this fails so the error cannot propagate
	try:
		# Makes a list of dictionaries per site with all tag:text elements within
		sites = [ {sub1.tag:sub1.text for sub1 in site} for site in root ]
		print "Found {} sites before filtering".format(len(sites))
		# Makes another list of dictionaries like above but only where siteType==Floor Area
		floorareas = [ site for site in sites if 'siteType' in site and site['siteType']=='Floor Area' ]
		print "Found {} sites after filtering those where siteType=Floor Area".format(len(floorareas))
		# Makes a dictionary of clean names:clientCounts
		clientcounts = { ', '.join(site['name'].split('/')[2:]):int(site['clientCount']) for site in floorareas }
		print "Names all cleaned up and clientCounts stored"
	except Exception as err:
		print "Something went wrong: \n{}".format(err)
	return clientcounts

# ==== Now grab list of locations from server

def ConnectSQL(user,password, tries=3):
	'''Makes connection to SQL server and gets location ids'''
	conn, cursor, idref = None, None, None	# Setting these to None means that other things will break if this fails so the error cannot propagate
	while tries > 0:
		try:
			conn = MySQLdb.connect(host='SERVER', user=user, passwd=password, db="db_EnergyDataPortal", connect_timeout=15)
			cursor = conn.cursor()
			tries = 0
			print "Connected to SQL Server"
			cursor.execute("SELECT id,name FROM WifiLocations")
			idref = { row[1]:row[0] for row in cursor }	# Keys are the names, and the values are the ids
			print "Got IDs of {} location names".format(len(idref))
		except Exception as err:
			tries -= 1
			print "Failed to connect"
	return conn, cursor, idref

def CheckMissing(compare):
	'''Given a dictionary to compare, this finds location names not in the database and adds them'''
	global conn, cursor, idref	# I am explicitly refering to the global objects
	missing = set(compare.keys()) - set(idref.keys())	# Get a list/set of the names in the XML file which are not on the server
	print "Found {} locations not already known in the database".format(len(missing))
	for loc in missing:
		cursor.execute("INSERT INTO WifiLocations (name) VALUES (%s)", (loc,))   # New entry, allowing the server to generate the id
		cursor.execute("SELECT id FROM WifiLocations WHERE name = (%s)", (loc,)) # Get id out
		idref[loc] = cursor.fetchone()[0]
		print "Location '{}' was unknown but is now ID {}".format(loc, idref[loc])
	conn.commit()	# Can commit at the end

def tidytime(now):
	'''Cleans up the time'''
	now = now.replace(second=0, microsecond=0)
	if 0 <= now.minute < 15:
		now = now.replace(minute=0)
	elif 15 <= now.minute < 30:
		now = now.replace(minute=15)
	elif 30 <= now.minute < 45:
		now = now.replace(minute=30)
	else:
		now = now.replace(minute=45)
	return now

def UploadClientCounts(now, clientcounts):
	'''Put all of the client counts into the database'''
	global conn, cursor, idref	# I am explicitly refering to the global objects
	# Prepares the data
	data = [ {'dateTime':now.strftime('%Y-%m-%d %H:%M:%S'), 'locId':idref[loc], 'count':clientcounts[loc]} for loc in clientcounts ]	# MySQLdb wants date in string format
	print "{} items ready to push into database".format(len(data))
	# Insert the new data
	for item in data:
		try:	# The SQL Server will protest if an of the location id's are not valid or if there is a duplicate
			cursor.execute("INSERT INTO WifiData (dateTime, locId, count) VALUES (%(dateTime)s, %(locId)s, %(count)s)", item)	# MySQLdb wants everything as strings
		except Exception as err:
			print "Something went wrong with the item '{}'\n{}".format(item,err)
	conn.commit()	# Anything that did manage to go in will be stored

if __name__ == "__main__":
	root = GetRootXML(link)	# link was declared at the top
	clientcounts = GetClientCounts(root)	# Root and clientcounts are now global variables
	conn, cursor, idref = ConnectSQL(SQLuser,SQLpassword)	# username and password were declared at the top, conn, cursor and idref are now globals
	try:
		CheckMissing(clientcounts)	# Should be called after ConnectSQL but before UploadClientCounts
	except Exception as err:
		conn.close()
		raise SystemExit("something failed: {}".format(err))
	now = tidytime(datetime.datetime.utcnow())	# Assume the data is value for right now in UTC/GMT
	UploadClientCounts(now, clientcounts)	# Actually pushes data in
	conn.close()	# Close the connection
	print "All done!"
