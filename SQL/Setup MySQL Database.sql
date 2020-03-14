CREATE DATABASE db_EnergyDataPortal;

USE db_EnergyDataPortal;

CREATE TABLE WifiLocations (
	id SMALLINT UNSIGNED UNIQUE NOT NULL AUTO_INCREMENT,
	name TINYTEXT NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE WifiData (
	dateTime DATETIME,
	locId SMALLINT UNSIGNED,
	count SMALLINT UNSIGNED,
	PRIMARY KEY (dateTime, locId),
	UNIQUE INDEX (locId, dateTime),
	FOREIGN KEY (locId) REFERENCES WifiLocations(id)
);

CREATE TABLE RTWeather (
	datetime DATETIME UNIQUE NOT NULL,
	wind_dir FLOAT,
	windspeed FLOAT,
	gustspeed FLOAT,
	temp FLOAT,
	humidity FLOAT,
	pressure FLOAT,
	solar FLOAT,
	rain FLOAT,
	PRIMARY KEY (datetime)
);

GRANT INSERT, SELECT ON db_EnergyDataPortal.* to 'USERNAME'@'SERVER' IDENTIFIED BY 'PASSWORD';
