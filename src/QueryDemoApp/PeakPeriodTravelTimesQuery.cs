using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryDemoApp
{
    public class TravelTimesQuery
    {
        private readonly ISession _session;
        public TravelTimesQuery(ISession session)
        {
            _session = session;
        }

        public int Run(string stationName, DateTime startDate, DateTime endDate)
        {
            StationData station = GetStationData(stationName);

            IEnumerable<int> detectors = GetDetectors(station.Id);

            List<int> speeds = new List<int>();

            foreach (int detector in detectors)
            {
                speeds.AddRange(GetSpeedsForDetector(detector, startDate, endDate));
            }

            var averageSeconds = speeds.Where(s=>s>0).Select(speed => (station.Lenght / speed) * 3600).Average();

            return (int)Math.Round(averageSeconds);
        }

        private StationData GetStationData(string stationName)
        {
            string query = @"select stationid, length from {0} where locationtext = '{1}';";

            string tableName = stationName.EndsWith("NB", StringComparison.InvariantCultureIgnoreCase) ? "stations_northbound" : "stations_southbound";

            var result = _session.Execute(string.Format(query, tableName, stationName)).SingleOrDefault();

            if (result == null)
                throw new ArgumentNullException("Station doesn't exist");

            return new StationData()
            {
                Id = Convert.ToInt32(result[0]),
                Lenght = Convert.ToDecimal(result[1])
            };
        }

        private IEnumerable<int> GetDetectors(int stationId) {
            var result = _session.Execute(string.Format("select detectorid from detector_for_stations where stationid = {0};", stationId));

            var retValue = result.Select(r => Convert.ToInt32(r[0])).ToArray();

            if (!retValue.Any())
                throw new ArgumentNullException("No detectors found for the provided station");

            return retValue;
        }

        private IEnumerable<int> GetSpeedsForDetector(int detectorId, DateTime startDate, DateTime endDate)
        {

            string query = @"select speed from loopdata_by_detectorid where detectorid = {0} and starttime > '{1:yyyy-MM-dd HH:mm}'  and starttime < '{2:yyyy-MM-dd HH:mm}' and occupancy = {3} allow filtering;";

            

            List<int> times = new List<int>();

            for (int i = 1; i < 20; i++)
            {
                var result = _session.Execute(string.Format(query, detectorId, startDate, endDate, i));

                //need to ignore records where occupancy is 0, but cassandra can't do "!=" or ">" on this column
                times.AddRange(result.Select(r => Convert.ToInt32(r[0])));
            }

            return times;
        }

        private class StationData {
            public int Id { get; set; }

            public decimal Lenght { get; set; } 
        }
    }
}
