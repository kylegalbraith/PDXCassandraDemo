using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryDemoApp
{
    class DetectorRepo
    {
        //tations_northbound

        private readonly ISession _session;
        public DetectorRepo(ISession session)
        {
            _session = session;
        }

        public int LoadFromCsv(string path)
        {
            var result = _session.Execute("select * from detector_for_stations limit 1;");

            if (result.Any())
            {
                return 0;
            }
            //detectorid,highwayid,milepost,locationtext,detectorclass,lanenumber,stationid
            var loader = new CsvLoader(_session, path, @"insert into detector_for_stations (stationid, lanenumber,	locationtext, detectorid) values ({6},{5},'{3}',{0})");

            return loader.Load();
        }
    }
}
