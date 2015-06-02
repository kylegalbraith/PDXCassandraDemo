CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use demo;
create table highways (highwayid int primary key, shortdirection text, direction text, highwayname text);
create table stations_northbound (stationid int primary key, milepost float, locationtext text, upstream int, downstream int, numberlanes int, latlon text, length float);
create table stations_southbound (stationid int primary key, milepost float, locationtext text, upstream int, downstream int, numberlanes int, latlon text, length float);

create index on stations_northbound (locationtext);
create index on stations_southbound (locationtext);


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
	primary key (detectorid, starttime)
);

create index on loopdata_by_detectorid (speed);
create index on loopdata_by_detectorid (occupancy);