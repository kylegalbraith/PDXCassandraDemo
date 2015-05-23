using Cassandra;
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
        }

        static void QueryOne() { }

        static void QueryTwo() { }

        static void QueryThree() { }

        static void QueryFour() { }

        static void QueryFive() { }

        static void QuerySix() { }
    }
}
