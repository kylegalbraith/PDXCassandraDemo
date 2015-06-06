using Cassandra;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            count = detectorRepo.LoadFromCsv(@"../../../data/freeway_detectors_manip1.csv");

            if (count > 0)
                Console.WriteLine("Loaded {0} detectors", count);

//            Console.WriteLine("---------------- QUERY ONE ----------------");
//            QueryOne(session);
//            Console.WriteLine("-------------------------------------------");
//
//            Console.WriteLine("---------------- QUERY TWO ----------------");
//            QueryTwo(session);
//            Console.WriteLine("-------------------------------------------");
//
//            Console.WriteLine("---------------- QUERY THREE ----------------");
//            //QueryFour(session);
//            Console.WriteLine("-------------------------------------------");
//
//            Console.WriteLine("---------------- QUERY FOUR ----------------");
//            QueryFour(session);
//            Console.WriteLine("-------------------------------------------");
//
//            Console.WriteLine("---------------- QUERY FIVE ----------------");
//            QueryFive(session);
//            Console.WriteLine("-------------------------------------------");
//
//            Console.WriteLine("---------------- QUERY SIX ----------------");
//            //QueryFour(session);
//            Console.WriteLine("-------------------------------------------");

            for (int number = 1; number <= 6; number++)
            {
                Query(number, session);
            }

            Console.WriteLine("Done");
            Console.ReadKey();
        }

        delegate void QueryExecutor(ISession session);

        static void Query(int number, ISession session)
        {
            QueryExecutor executor = null;

            switch (number)
            {
                case 1:
                    executor = QueryOne;
                    break;

                case 2:
                    executor = QueryTwo;
                    break;

                case 3:
                    executor = QueryThree;
                    break;

                case 4:
                    executor = QueryFour;
                    break;

                case 5:
                    executor = QueryFive;
                    break;

                case 6:
                    executor = QuerySix;
                    break;
            }

            if (executor == null)
            {
                throw new ArgumentException(string.Format("Unknown query number: {0}.", number), "number");
            }

            Console.WriteLine("---------------- QUERY {0} ----------------", number);
            Console.WriteLine();

            Stopwatch timer = Stopwatch.StartNew();

            try
            {
                executor(session);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: Query {0} terminated with a {1}.", number, ex.GetType().FullName);
                Console.WriteLine("Exception Message: {0}", ex.Message);
                Console.WriteLine(ex.StackTrace);
            }

            timer.Stop();

            Console.WriteLine();
            Console.WriteLine("-------------------------------------------");
            Console.WriteLine("Query {0} executed in {1}ms", number, timer.ElapsedMilliseconds);
            Console.WriteLine("-------------------------------------------");
            Console.WriteLine();
        }

        /// <summary>
        /// Number of speeds > 100 in the data set
        /// </summary>
        /// <param name="session"></param>
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

        /// <summary>
        /// Find the total volume for the station Foster NB for Sept. 21, 2011
        /// </summary>
        /// <param name="session"></param>
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

        /// <summary>
        /// Find travel time for station Foster NB for 5-minute intervals for Sept. 22, 2011. Report travel time in seconds.
        /// </summary>
        /// <param name="session"></param>
        static void QueryThree(ISession session)
        {
            string stationName = @"Foster NB";
            var query = new TravelTimesIntervalQuery(session, stationName);

            DateTime when = new DateTime(2011, 9, 22);
            int minuteInterval = 5;
            List<double> travelTimes = query.Run(when, minuteInterval);

            for (int interval = 0; interval < travelTimes.Count; interval++)
            {
                Console.WriteLine(
                    "Travel time at {0} on {1} was {2} seconds.",
                    when.AddMinutes(minuteInterval * interval),
                    stationName,
                    travelTimes[interval]
                );
            }
        }

        /// <summary>
        /// Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 for station Foster NB. Report travel time in seconds.
        /// </summary>
        /// <param name="session"></param>
        static void QueryFour(ISession session)
        {
            DateTime date = new DateTime(2011, 9, 22);

            //temp code
            //date = date.AddDays(-15).AddMonths(-8);

            int[] results = GetPeakTravelTimes(date, "Foster NB", session);

            Console.WriteLine("Travel times for Foster NB are {0} and {1} seconds", results[0], results[1]);
        }

        /// <summary>
        /// Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 for station I-205 NB freeway. Report travel time in minutes.
        /// </summary>
        /// <param name="session"></param>
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

        /// <summary>
        /// Find a route from Johnson Creek to Columbia Blvd on I-205 NB using the upstream and downstream fields.
        /// </summary>
        /// <param name="session"></param>
        static void QuerySix(ISession session)
        {
            var query = new RouteFindingQuery(session);

            string startPoint = "Johnson Cr NB";
            string endPoint = "Columbia to I-205 NB";

            query.run(startPoint, endPoint);
        }
    }
}
