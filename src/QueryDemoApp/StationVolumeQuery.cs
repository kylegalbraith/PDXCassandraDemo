using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryDemoApp
{
    public class StationVolumeQuery
    {
        private readonly ISession _session;

        public StationVolumeQuery(ISession session)
        {
            _session = session;
        }

        public long Run(string stationName, DateTime startDate, DateTime endDate)
        {
            int stationId = GetStationId(stationName);

            IEnumerable<int> detectors = GetDetectors(stationId);

            long volume = 0;

            foreach (int detector in detectors) {
                volume = GetVolumeForDetector(detector, startDate, endDate);
            }

            return volume;
        }

        private int GetStationId(string stationName)
        {
            string query = @"select stationid from {0} where locationtext = '{1}';";

            string tableName = stationName.EndsWith("NB", StringComparison.InvariantCultureIgnoreCase) ? "stations_northbound" : "stations_southbound";

            var result = _session.Execute(string.Format(query, tableName, stationName)).SingleOrDefault();

            if (result == null)
                throw new ArgumentNullException("Station doesn't exist");

            return Convert.ToInt32(result[0]);
        }

        private IEnumerable<int> GetDetectors(int stationId) {
            var result = _session.Execute(string.Format("select detectorid from detector_for_stations where stationid = {0};", stationId));

            var retValue = result.Select(r => Convert.ToInt32(r[0])).ToArray();

            if (!retValue.Any())
                throw new ArgumentNullException("No detectors found for the provided station");

            return retValue;
        }

        private long GetVolumeForDetector(int detectorId, DateTime startDate, DateTime endDate) {

            string query = @"select volume from loopdata_by_detectorid where  detectorid = {0} and starttime > '{1:yyyy-MM-dd HH:mm}'  and starttime < '{2:yyyy-MM-dd HH:mm}' allow filtering;";

            long volume = 0;
            var result = _session.Execute(string.Format(query,detectorId, startDate, endDate));

            volume = result.Sum(r => Convert.ToInt32(r[0]));

            return volume;
        }
    }
}
