using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryDemoApp
{
    class LoopdataRepo
    {
        //tations_northbound

        private readonly ISession _session;
        public LoopdataRepo(ISession session)
        {
            _session = session;
        }

        public int LoadFromCsv(string path)
        {
            var result = _session.Execute("select * from loopdata_by_detectorid limit 1;");

            if (result.Any())
            {
                return 0;
            }
            //detectorid,starttime,volume,speed,occupancy,status,dqflags

            var loader = new CsvLoader(_session, path, @"insert into loopdata_by_detectorid (detectorid, starttime, volume,	speed, occupancy, status, dqflags) values ({0},'{1}',{2},{3},{4},{5},{6})");

            return loader.Load();
        }
    }
}
