using Cassandra;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace QueryDemoApp
{
    class Program
    {
        private static readonly string KEYSPACE_NAME = "demo";
        static void Main(string[] args)
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            ISession session = cluster.Connect(KEYSPACE_NAME);

            var hRepo = new HighwayRepo(session);
            int count = hRepo.LoadFromCsv(@"../../../data/highways.csv");

            if (count > 0)
                Console.WriteLine("Loaded {0} highways", count);


            var sNbRepo = new StationsNbRepo(session);
            count = sNbRepo.LoadFromCsv(@"../../../data/freeway_stations_northbound.csv");

            if (count > 0)
                Console.WriteLine("Loaded {0} NB stations", count);

            var sSbRepo = new StationsSbRepo(session);
            count = sSbRepo.LoadFromCsv(@"../../../data/freeway_stations_southbound.csv");

            if (count > 0)
                Console.WriteLine("Loaded {0} SB stations", count);


            var detectorRepo = new DetectorRepo(session);
            count = detectorRepo.LoadFromCsv(@"../../../data/freeway_detectors.csv");

            if (count > 0)
                Console.WriteLine("Loaded {0} detectors", count);

            var loopRepo = new LoopdataRepo(session);
            count = loopRepo.LoadFromCsv(@"../../../data/freeway_loopdata_clean.csv");

            if (count > 0)
                Console.WriteLine("Loaded {0} detectors", count);


            Console.WriteLine("Done");

            Console.ReadKey();
        }

        static void QueryOne() { }

        static void QueryTwo() { }

        static void QueryThree() { }

        static void QueryFour() { }

        static void QueryFive() { }

        static void QuerySix() { }
    }
}
