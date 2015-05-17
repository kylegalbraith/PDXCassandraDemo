CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use demo;
create table highways (highwayid int primary key, shortdirection text, direction text, highwayname text);
create table stations_northbound (stationid int primary key, milepost float, locationtext text, upstream int, downstream int, numberlanes int, latlon text, length float);
create table stations_southbound (stationid int primary key, milepost float, locationtext text, upstream int, downstream int, numberlanes int, latlon text, length float);

copy highways (highwayid, shortdirection, direction, highwayname) 
from 'C:\Users\Kyle\Documents\GitHub\PDXCassandraDemo\src\data\highways.csv' 
with header = true;

copy stations_northbound (stationid, milepost, locationtext, upstream, downstream, numberlanes, latlon, length)
from 'C:\Users\Kyle\Documents\GitHub\PDXCassandraDemo\src\data\freeway_stations_northbound.csv' 
with header = true;

copy stations_southbound (stationid, milepost, locationtext, upstream, downstream, numberlanes, latlon, length)
from 'C:\Users\Kyle\Documents\GitHub\PDXCassandraDemo\src\data\freeway_stations_southbound.csv' 
with header = true;
