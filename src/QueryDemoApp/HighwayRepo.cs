using Cassandra;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryDemoApp
{
    public class HighwayRepo
    {
        private readonly ISession _session;
        public HighwayRepo(ISession session)
        {
            _session = session;
        }

        public int LoadFromCsv(string path)
        {
            var result = _session.Execute("select * from highways limit 1;");

            if (result.Any())
            {
                return 0;
            }

            var loader = new CsvLoader(_session, path, "insert into highways (highwayid, shortdirection, direction, highwayname) values ({0},'{1}','{2}','{3}')");

            return loader.Load();
        }
    }
}
