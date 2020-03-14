#!/usr/bin/env python
## Mark Jarvis 14 June 2018
## Written for Python3, but has been adapted to be backward compatible with Python2

from sys import version_info
if version_info[0] < 3: # Check if it Python 2 or 3
  import ConfigParser as configparser # In Python2, its Title case but we'll alias it so we have single code
else:
  import configparser # In Python3, its lower case
import socket
from operator import itemgetter
from datetime import datetime, timedelta
from time import sleep
begin = datetime.utcnow()
print('Starting at: {} UTC'.format(begin))
settings = configparser.ConfigParser(allow_no_value=True) # Setup a config parser
#settings.read_dict(defaults) # Bring in all default values
cfgfiles = settings.read('LiveWeather.cfg')
if len(cfgfiles) > 0:
  for file in cfgfiles: print('Read config from external file: {}'.format(file))  # Bring in settings from file

try:  # Establish if the test SQLite3 database should be used, assume yes.
  TEST = settings.getboolean('DEFAULT','TEST')  # Establish if this for testing 
except ValueError:
  TEST = True
  print('TEST Parameter in the DEFAULT section was not found or malformed')

if TEST:  # If the test database is being used, then get SQLite3 ready and connect
  import sqlite3
  def adapt_datetime(dt): # Adapter so that SQLite3 can accept datetimes but store them in the db file as efficient integers (unix timestamp in seconds)
    return int(((dt - datetime(1970, 1, 1)).total_seconds())*1000000)
  def convert_datetime(b): # Converter so that SQLite3 can retreive efficient integers (unix timestamp in seconds) and return them as datetimes
    return datetime.utcfromtimestamp(int(b)/1000000)
  sqlite3.register_adapter(datetime, adapt_datetime)  # TEST DATABASE
  sqlite3.register_converter("DATETIME", convert_datetime)  # TEST DATABASE
  findlastcommand = 'SELECT MAX(datetime) as "dateTime [DATETIME]" FROM {};'.format(settings.get('SQLite3','table'))
  #insertcommand = "INSERT OR IGNORE INTO RTWeather VALUES (?,?,?,?,?,?,?,?,?);"  # If duplicates are found, they are IRGNORED
  insertcommand = "INSERT OR REPLACE INTO RTWeather VALUES (?,?,?,?,?,?,?,?,?);"  # If duplicates are found, they are REPLACED
  try:
    database = sqlite3.connect(settings.get('SQLite3','file'),detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    db=database.cursor()
    db.executescript('''
      CREATE TABLE IF NOT EXISTS RTWeather (
      datetime INTEGER PRIMARY KEY,
      wind_dir REAL,
      windspeed REAL,
      gustspeed REAL,
      temp REAL,
      humidity REAL,
      pressure REAL,
      solar REAL,
      rain REAL
      );''')  # Create an empty table in the file if there isn't one already there
    database.commit()
    print('Test Database being used')
  except sqlite3.Error:
    raise SystemExit('Database Load failed')
else: # If the REAK database is being usedm then load mysql modeuls and connect
  import  MySQLdb # pymysql
  findlastcommand = 'SELECT MAX(datetime) FROM {};'.format(settings.get('MySQL','table'))
  insertcommand = "INSERT IGNORE INTO RTWeather VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);" # If duplicates are found, they are IGNORED
  #insertcommand = "REPLACE INTO RTWeather VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);"  # If duplicates are found, they are REPLACED
  try:
    database = MySQLdb.connect(settings.get('MySQL','host'), settings.get('MySQL','username'), settings.get('MySQL','password'), settings.get('MySQL','database'))
    db=database.cursor()
    print('Live Database being used')
  except pymysql.Error:
    raise SystemExit('Database connection failed')

def floatornone(x):
  '''Tries to convert number or string x into a float, or returns None if x is of the wrong type or value'''
  try:
    return float(x)
  except (ValueError,TypeError):
    return None

def dateornone(s):
  '''Tries to convert string s into a date assuming the format "YYYY/MM/DD hh:mm:ss.nnnnnn", or returns None if s is of the wrong type or value'''
  try:
    return datetime.strptime(s,'%Y/%m/%d %H:%M:%S.%f')
  except (ValueError,TypeError):
    return None

#dumpfile = open('DumpFile.txt','wb')

db.execute(findlastcommand) #Run the command
dbfinaldata = db.fetchone()[0]  # The most recent data point

try:
  maxdays = settings.getint('DT80','maxdays')
except (ValueError,TypeError):
  maxdays = 1

if settings.get('DT80','forcestartdate') not in  ['',None] :
  startdate = settings.get('DT80','forcestartdate') # Taken directly as provided so its parsed directly to the logger
  maxdays=0
  print('Starting from the start time specified by the config file: {}'.format(startdate))
elif dbfinaldata is not None:
  startdate = dbfinaldata + timedelta(seconds=1)
  print('Starting from 1 second after the final item in the database: {}'.format(startdate))
else:
  startdate = '-{}T'.format(maxdays)  # Go back the number of days specified by maxdays, which defaults to 1
  print('Starting from {} days ago'.format(maxdays))

if settings.get('DT80','forceenddate') not in  ['',None] :
  enddate = settings.get('DT80','forceenddate') # Taken directly as provided so its parsed directly to the logger
  maxdays = 0
  print('Ending at the end time specified by the config file: {}'.format(enddate))
elif maxdays > 0:
  enddate = startdate + timedelta(days=maxdays) # This limits how much data will be downloaded in one go, however if there is a gap in the source data larger than this, then it will never progress beyond the gap
  print('Ending at {} days after the start date: {}'.format(maxdays,enddate))
else:
  enddate = '-0'  # Defaults to the most recent data point
  print('Ending at the most recent data in the logger')

if version_info[0] < 3: # Check if it Python 2 or 3
  startdate = startdate.isoformat(sep='T') if type(startdate) == datetime else startdate
  enddate = enddate.isoformat(sep='T') if type(enddate) == datetime else enddate
else:
  startdate = startdate.isoformat(sep='T', timespec='milliseconds') if type(startdate) == datetime else startdate
  enddate = enddate.isoformat(sep='T', timespec='milliseconds') if type(enddate) == datetime else enddate
  
command = 'COPYD job={job} sched={sched} archive=Y data=Y live=Y alarms=N id={id} start={start} end={end}\r\n'  # According to DT80 manual. id is arbitrary and optional but set to my phone number for some traceability. start time is to be calculated from what is last in the database but has .01 seconds forced in so its after the last actual data point, end date is simply midnight several days later (may be in the future
command = command.format(job=settings.get('DT80','job'), sched=settings.get('DT80','sched'), id=settings.get('DT80','id'), start=startdate,end=enddate) # Create the command string using the dates in the predfined format YYYY-MM-DDTHH:MM:SS.mmm

maxtries, tries, err = settings.getint('DT80','maxtries'), 0, None
while tries < maxtries:
  try:
    s = socket.create_connection((settings.get('DT80','host'),settings.getint('DT80','port')),settings.getint('DT80','timeout')) # Timeout of 5 seconds
    print('\r\nConnected to DT80 Logger')
    signoncmd = 'SIGNON\r\n{username}\r\n{password}\r\n'.format(username=settings.get('DT80','username'),password=settings.get('DT80','password'))
    if s.send(signoncmd.encode()) == len(signoncmd):
      print('Sign On command sent')
      rec = b'' # Collect responses in here
      try:
        while b'Login succeed\r\n' not in rec:  # Keep trying until we see the success string and its end of line
          rec += s.recv(1024)
        print('Sign On Successful')
        tries = None
        break # Break out of the outer loop
      except socket.timeout:  # Otherwise, something went wrong.
        tries += 1
        err = 'Timeout occurred, Signon Failed. Attempt {} of {}'.format(tries, maxtries)
        print(err)
        #dumpfile.write(rec)
        del(rec)  # Get rid of this now
        s.close() # Close it to attempt to reopen
        sleep(1*tries)
    else: # If the buffer didn't fully send, something went wrong
      tries += 1
      err = 'Error while sending SIGNON command. Attempt {} of {}'.format(tries, maxtries)
      print(err)
      s.close() # Close it to attempt to reopen
      sleep(1*tries)
  except socket.error:
    tries += 1
    err = 'Error establishing connection. Attempt {} of {}'.format(tries, maxtries)
    print(err)
    sleep(1*tries)
if tries != None: # If this isn't None, its because an error occured
  raise SystemExit('Error connecting to DT80: {}'.format(err))
else:
  del(err)

if s.send(command.encode()) == len(command):  # This will send the COPYD command which obtains the data
  print('\r\nSending following command to download data:\r\n{}'.format(command))
  lines = [b''] # Declaration: This will contain each line but is empty for now. Must have an element for [-1] index to work, and must be type(bytes) since thats what the socket will recv.
  LineCount = 0 # Tally of lines found
  InsertCount = 0 # Tally of rows uploaded
  z = 0
  packet=b''
  while True: # Loop until timeout breaks the loop
    try:
      packet = s.recv(4096) # Get the next packet of bytes from the buffer. Enclose this in a Try to capture the timeout when finished
    except socket.timeout:
      print('Timeout occured, assume thats the end')
      break # Break out of the loop when the data logger stops sending data as this should be the end, otherwise it would run forever
    finally:  # This block is executed with or without an exception so it will still execute before the break stops the loop
      #dumpfile.write(packet) # Write it out to the dumpfile
      lines[-1] += packet # Append the recently received packet to the last partial line
      packet = b''  # Clear this so it doesn't get repeatedly processed during the final timeout
      lines += lines.pop(-1).split(b'\r\n') # Pop will remove the last element which may now contain complete lines, it will be split by the line endings (bytes) to create a new list of complete lines and re-appended to the list of lines. The new last entry may now be a partial line, but all others should be complete lines.
      if b'Unload complete.' in lines:
        lines = [ l for l in lines if l != b'Unload complete.' ]  # Strips out any "Unload complete." strings. This should be at index -2 if it exists at all, however if there is no data provided it will cause the db.executemany to fail as it will iterate over an empty set.
      readycount = len(lines[1:-1])
      if readycount > 0:  # If there are actually proper lines to work with, then process and upload them
        ToUpload = ([dateornone(row[0].decode()),None]+[value for value in map(floatornone,row[2:])] for row in (row.replace(b'"',b'').split(b',') for row in lines[1:-1])) # Convert each line of bytes into nested lists of strings, then convert all strings to datetime, None for TimeZone, and Floats for everything else, staying consistent with the original coloumns for clarity, all using generators rather than list comprehensions for memory efficiency). Malformed strings will be captured by exception and replaced with None. lines[1:-1] will skip the absolute first line which should be the headers, and the final which is the partial line.
        db.executemany(insertcommand, (itemgetter(0,3,4,5,6,7,8,9,10)(row) for row in ToUpload if row[0] != None))  # Only upload relevent cells with indices consistent with original download file, iterating over the original generator with another generator which also filters out None Dates since the SQL Primary Key cannot be NULL
        LineCount += readycount
        InsertCount += db.rowcount
        del(lines[1:-1])  # This removes everything we should have just uploaded keeping only the first line (headings) and the last line (partial)
        database.commit()
      y = z
      z = (LineCount % 100) # Every hundred lines, report back
      if z < y:
        print('Items found so far: {}, Items submitted to Database so far: {}'.format(LineCount,InsertCount)) # Print running progress
  db.execute(findlastcommand) #Run the command
  lastdata = db.fetchone()[0] # The most recent data point
  print('Items found in total: {}, Items submitted to Database in total: {}, Most recent data is now {}\r\n'.format(LineCount,InsertCount,lastdata))  # Print final stats
  #dumpfile.close()
else: # If the buffer didn't fully send, something went wrong
  raise SystemExit('Error while sending COPYD command')

signoffcmd = 'SIGNOFF\r\n'
if s.send(signoffcmd.encode()) == len(signoffcmd):
  print('Sign Off command sent')
else:
  print("Error while sending SIGNOFF command, but it probably doesn't matter")  # If this failed, it doesn't really matter, after this we exit the with-blok context manager so the socket will close anyway

s.close()

# Won't bother checking if anything was received back. At this point exist the context manager will close the socket
print('\r\nFor information, here are the headings:')
for key in enumerate( tuple(lines[0].decode().replace('"','').split(',')) ):  # The first element of lines should be the header to help identify what's what. Remove quotes and make it comma seperated list. Then enumerate it and show the keys and index numbers for information
  print('{:2d} : {}'.format(*key))
finished = datetime.utcnow()
print('\r\nFinished at: {} UTC. Iime taken: {}\r\nDONE'.format(finished,finished-begin))
