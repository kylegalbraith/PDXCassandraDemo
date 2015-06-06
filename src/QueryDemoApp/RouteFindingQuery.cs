using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryDemoApp
{
    class RouteFindingQuery
    {
        private readonly ISession _session;

        public RouteFindingQuery(ISession session)
        {
            _session = session;
        }

        public void run(string starPoint, string endPoint)
        {
            if (!(searchUpStream(starPoint, endPoint)))
                if (!(searchDownStream(starPoint, endPoint)))
                    Console.WriteLine("No path found !");
        }


        private Row GetNextStation(int stationId, string direction)
        {
            String query = @"select stationid, locationtext, {1}stream from stations_northbound where stationid = {0};";

            Row result = _session.Execute(string.Format(query, stationId.ToString(), direction)).SingleOrDefault();

            if (result == null)
                throw new ArgumentNullException("Station doesn't exist");

            return result;
        }

        private Row GetNextStation(string stationName, string direction)
        {
            String query = @"select stationid, locationtext, {1}stream from stations_northbound where locationtext = '{0}';";

            Row result = _session.Execute(string.Format(query, stationName, direction)).SingleOrDefault();

            if (result == null)
                throw new ArgumentNullException("Station doesn't exist");

            return result;
        }

        private bool searchUpStream(String starPoint, String endPoint)
        {
            Queue<string> route = new Queue<string>();
            string station = starPoint;
            route.Enqueue(station);
            try
            {
                var result = GetNextStation(station, "up");

                int nextId = Convert.ToInt32(result[2]);

                while (nextId != 0)
                {
                    result = GetNextStation(nextId, "up");

                    station = result[1].ToString();
                    route.Enqueue(station);
                    nextId = Convert.ToInt32(result[2]);
                    if (station.Equals(endPoint)) break;

                }
            }
            catch (ArgumentNullException e)
            {
                return false;
            }

            if (route.Last().Equals(endPoint))
            {
                foreach (string record in route)
                {
                    Console.WriteLine(record);
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        private bool searchDownStream(String starPoint, String endPoint)
        {
            Queue<string> route = new Queue<string>();
            string station = starPoint;
            route.Enqueue(station);

            try
            {
                var result = GetNextStation(station, "down");

                int nextId = Convert.ToInt32(result[2]);

                while (nextId != 0)
                {
                    result = GetNextStation(nextId, "down");

                    station = result[1].ToString();
                    route.Enqueue(station);
                    nextId = Convert.ToInt32(result[2]);
                    if (station.Equals(endPoint)) break;

                }
            }
            catch (ArgumentNullException e)
            {
                return false;
            }

            if (route.Last().Equals(endPoint))
            {
                foreach (string record in route)
                {
                    Console.WriteLine(record);
                }
                return true;
            }
            else
            {
                return false;
            }
        }

    }
}


