import sqlite3, MySQLdb
from datetime import datetime

def adapt_datetime(dt):
	return int((dt - datetime(1970, 1, 1)).total_seconds())

def convert_datetime(b):
	return datetime.fromtimestamp(int(b))

sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("DATETIME", convert_datetime)

connsqlite3=sqlite3.connect('EnergyDataPortal.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
lite=connsqlite3.cursor()

connMySQL = MySQLdb.connect("SERVER", "USERNAME", "PASSWORD", "db_EnergyDataPortal")
my = connMySQL.cursor()

lite.executescript('''
DROP TABLE IF EXISTS RTWeather;
DROP TABLE IF EXISTS WifiData;
DROP TABLE IF EXISTS WifiLocations;
DROP INDEX IF EXISTS idx_WifiData_locId_dateTime;

CREATE TABLE IF NOT EXISTS WifiLocations (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS WifiData (
	dateTime INTEGER,
	locId INTEGER,
	count INTEGER,
	PRIMARY KEY (dateTime, locId),
	FOREIGN KEY (locId) REFERENCES WifiLocations(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_WifiData_locId_dateTime ON WifiData (locId, dateTime);

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
);
''')

my.execute("SELECT * FROM WifiLocations")
lite.executemany("INSERT INTO WifiLocations VALUES (?,?)", my.fetchall())
my.execute("SELECT * FROM WifiData")
lite.executemany("INSERT INTO WifiData VALUES (?,?,?)", my.fetchall())
my.execute("SELECT * FROM RTWeather")
lite.executemany("INSERT INTO RTWeather VALUES (?,?,?,?,?,?,?,?,?)", my.fetchall())

connsqlite3.commit()
connsqlite3.close()
connMySQL.close()

