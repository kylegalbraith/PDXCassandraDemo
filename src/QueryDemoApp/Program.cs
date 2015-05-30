using Cassandra;
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
            Console.WriteLine("--------------- QUERY ONE ---------------");
            QueryOne(session);
            Console.WriteLine("-----------------------------------------");
            Console.ReadKey();
        }

        public static void QueryOne(ISession session) 
        {
            /* Expect 3284 records to come back with speed > 100 because the naive implementation
             * comes back with 3284 records, so if the range is 101 - 150 then 3284 records come 
             * back in 2 second rather than 11 minutes */
            Stopwatch timer = new Stopwatch();
            timer.Start();
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
            timer.Stop();
            Console.WriteLine("Number of results with speed > 100: {0}", overHundredCount);
            Console.WriteLine("Query 1 took {0} to execute", timer.Elapsed);
        }

        static void QueryTwo() { }

        static void QueryThree() { }

        static void QueryFour() { }

        static void QueryFive() { }

        static void QuerySix() { }
    }
}
