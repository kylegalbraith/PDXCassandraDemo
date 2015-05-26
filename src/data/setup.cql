CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use demo;
create table highways (highwayid int primary key, shortdirection text, direction text, highwayname text);
create table stations_northbound (stationid int primary key, milepost float, locationtext text, upstream int, downstream int, numberlanes int, latlon text, length float);
create table stations_southbound (stationid int primary key, milepost float, locationtext text, upstream int, downstream int, numberlanes int, latlon text, length float);
create table detector_for_stations (
	stationid int,
	lanenumber int, 
	locationtext text, 
	detectorid int,
	primary key (stationid, lanenumber)
);
create table loopdata_by_detectorid (
	detectorid int,
	starttime timestamp,
	volume int,
	speed int,
	occupancy int,
	status int,
	dqflags int,
	primary key (detectorid, starttime, volume, speed)
);

copy highways (highwayid, shortdirection, direction, highwayname) 
from 'C:\Users\Kyle\Documents\GitHub\PDXCassandraDemo\src\data\highways.csv' 
with header = true;

copy stations_northbound (stationid, milepost, locationtext, upstream, downstream, numberlanes, latlon, length)
from 'C:\Users\Kyle\Documents\GitHub\PDXCassandraDemo\src\data\freeway_stations_northbound.csv' 
with header = true;

copy stations_southbound (stationid, milepost, locationtext, upstream, downstream, numberlanes, latlon, length)
from 'C:\Users\Kyle\Documents\GitHub\PDXCassandraDemo\src\data\freeway_stations_southbound.csv' 
with header = true;

copy detector_for_stations (stationid, lanenumber, locationtext, detectorid)
from 'C:\Users\Kyle\Documents\GitHub\PDXCassandraDemo\src\data\freeway_detectors_manip1.csv' 
with header = true;

copy loopdata_by_detectorid (detectorid, starttime, volume, speed, occupancy, status, dqflags)
from 'C:\Users\Kyle\Documents\GitHub\newportfolio\Sping15\cs410-sp15\project\ProjectData-Cloud2015\freeway_loopdata_test.csv' 
with header = true;

