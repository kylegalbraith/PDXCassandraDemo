using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryDemoApp
{
    class StationsSbRepo
    {
        //tations_northbound

        private readonly ISession _session;
        public StationsSbRepo(ISession session)
        {
            _session = session;
        }

        public int LoadFromCsv(string path)
        {
            var result = _session.Execute("select * from stations_southbound limit 1;");

            if (result.Any())
            {
                return 0;
            }
            //create table stations_northbound (stationid int primary key, milepost float, locationtext text, upstream int, downstream int, numberlanes int, latlon text, length float);
            var loader = new CsvLoader(_session, path, @"insert into stations_southbound (stationid, milepost, locationtext, upstream, downstream, numberlanes, latlon, length) 
                                                        values ({0},{1},'{2}',{3},{4},{5},'{6}',{7})");

            return loader.Load();
        }
    }
}
