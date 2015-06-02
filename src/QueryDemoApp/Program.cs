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

            Console.WriteLine("---------------- QUERY ONE ----------------");
            QueryOne(session);
            Console.WriteLine("-------------------------------------------");

            Console.WriteLine("---------------- QUERY TWO ----------------");
            QueryTwo(session);
            Console.WriteLine("-------------------------------------------");

            Console.WriteLine("---------------- QUERY FOUR ----------------");
            QueryFour(session);
            Console.WriteLine("-------------------------------------------");

            Console.WriteLine("---------------- QUERY FIVE ----------------");
            QueryFive(session);
            Console.WriteLine("-------------------------------------------");

            Console.WriteLine("Done");

            Console.ReadKey();
        }

        static void QueryOne(ISession session)
        {

            /* Expect 6972 records to come back with speed > 100 because the naive implementation
             * comes back with 6972 records, so if the range is 101 - 150 then 6972 records come 
             * back in 2 second rather than 11 minutes */
            int overHundredCount = 0;
            for (int i = 101; i < 150; i++)
            {
                string query = "select speed from loopdata_by_detectorid where speed = " + i;
                RowSet result = session.Execute(query);
                foreach (var row in result)
                {
                    overHundredCount++;
                }
            }
            Console.WriteLine("Number of results with speed > 100: {0}", overHundredCount);
        }

        static void QueryTwo(ISession session)
        {
            var query = new StationVolumeQuery(session);

            DateTime startDate = new DateTime(2011, 9, 21);
            DateTime endDate = new DateTime(2011, 9, 22);

            //Temp code (Data is off)

            //startDate = startDate.AddDays(-15).AddMonths(-8);
            //endDate = endDate.AddDays(-15).AddMonths(-8);

            long result = query.Run("Foster NB", startDate, endDate);

            Console.WriteLine("Query 2 result: {0}", result);
        }

        static void QueryThree() { }

        static void QueryFour(ISession session)
        {
            DateTime date = new DateTime(2011, 9, 22);

            //temp code
            //date = date.AddDays(-15).AddMonths(-8);

            int[] results = GetPeakTravelTimes(date, "Foster NB", session);

            Console.WriteLine("Travel times for Foster NB are {0} and {1} seconds", results[0], results[1]);
        }

        static void QueryFive(ISession session)
        {
            DateTime date = new DateTime(2011, 9, 22);

            //temp code
            //date = date.AddDays(-15).AddMonths(-8);

            int[] results = GetPeakTravelTimes(date, "Foster NB", session);

            Console.WriteLine("Travel times for Foster NB are {0:##0.00} and {1:##0.00} minutes", results[0] / 60M, results[1] / 60M);
        }

        private static int[] GetPeakTravelTimes(DateTime date, String stationName, ISession session)
        {
            var query = new TravelTimesQuery(session);

            DateTime morningStart = date.Date.AddHours(7);
            DateTime eveningStart = date.Date.AddHours(16);

            //Temp code (Data is off)

            int morningTimes = query.Run(stationName, morningStart, morningStart.AddHours(2));
            int eveningTimes = query.Run(stationName, eveningStart, eveningStart.AddHours(2));

            return new int[] { morningTimes, eveningTimes };
        }


        static void QuerySix() { }
    }
}
