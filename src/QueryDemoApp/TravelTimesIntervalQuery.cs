using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QueryDemoApp
{
    public class TravelTimesIntervalQuery
    {
        private readonly ISession session;
        private readonly string stationName;

        public TravelTimesIntervalQuery(ISession session, string stationName)
        {
            this.session = session;
            this.stationName = stationName;
        }

        public List<double> Run(DateTime when, int interval)
        {
            if (interval <= 0)
            {
                throw new ArgumentException("The interval must be greater than 0.", "interval");
            }

            // Get the station ID and length.
            StationInfo station = GetStationInfo();
            
            // Now get all the detectors.
            int[] detectorIds = GetDetectors(station.Id);

            LoopData[] speeds = GetSpeeds(detectorIds, when);
            List<List<LoopData>> intervals = GetIntervals(speeds, interval);

            List<double> travelTimes = new List<double>();
            for (int index = 0; index < intervals.Count; index++)
            {
                travelTimes.Add(GetTravelTime(intervals[index], station.Length));
            }

            return travelTimes;
        }

        private double GetTravelTime(List<LoopData> data, double stationLength)
        {
            if (data == null || data.Count == 0)
            {
                return 0;
            }

            double totalTravelTime = 0;

            foreach (LoopData entry in data)
            {
                totalTravelTime += ((entry.Speed / stationLength) / 3600);
            }

            return totalTravelTime;
        }

        private List<List<LoopData>> GetIntervals(LoopData[] speeds, int interval)
        {
            if (speeds == null || speeds.Length == 0)
            {
                return new List<List<LoopData>>();
            }


            // The number of intervals there are (1,440 minutes in a day).
            int totalIntervals = 1440 / interval;
            List<List<LoopData>> intervals = new List<List<LoopData>>(totalIntervals);

            for (int i = 0; i < totalIntervals; i++)
            {
                intervals.Add(new List<LoopData>());
            }

            foreach (LoopData speed in speeds)
            {
                intervals[GetIntervalIndex(speed.StartDate, interval)].Add(speed);
            }

            return intervals;
        }

        private int GetIntervalIndex(DateTimeOffset timestamp, int interval)
        {
            int minutes = timestamp.Minute + (timestamp.Hour * 60);
            return (int)Math.Floor((double)minutes / interval);
        }

        private LoopData[] GetSpeeds(int[] detectorIds, DateTime when)
        {
            DateTime startDate = new DateTime(when.Year, when.Month, when.Day, 0, 0, 0);
            DateTime endDate = new DateTime(when.Year, when.Month, when.Day, 23, 59, 59);

            string query = @"SELECT detectorid, speed, starttime FROM loopdata_by_detectorid WHERE detectorid IN({0}) AND starttime > '{1:yyyy-MM-dd HH:mm}'  AND starttime < '{2:yyyy-MM-dd HH:mm}' ALLOW FILTERING;";

            RowSet results = session.Execute(string.Format(
                                                query,
                                                string.Join(", ", detectorIds),
                                                startDate,
                                                endDate
                                            ));

            return results.Select(r => new LoopData {
                DetectorId = Convert.ToInt32(r[0]),
                Speed = Convert.ToInt32(r[1]),
                StartDate = (DateTimeOffset) r[2]
            }).ToArray<LoopData>();
        }

        private struct LoopData
        {
            public int DetectorId;
            public int Speed;
            public DateTimeOffset StartDate;
        }

        private int[] GetDetectors(int stationId)
        {
            string query = @"SELECT detectorid FROM detector_for_stations WHERE stationid = {0}";

            RowSet results = session.Execute(string.Format(query, stationId));

            int[] detectorIds = results.Select(r => Convert.ToInt32(r[0])).ToArray();

            if (detectorIds.Length == 0)
            {
                throw new ArgumentException(string.Format("Unable to locate any detectors for station: {0}.", stationName));
            }

            return detectorIds;
        }

        private struct StationInfo
        {
            public int Id;
            public double Length;
        }

        private StationInfo GetStationInfo()
        {
            string query = @"SELECT stationid, length FROM stations_{0}bound WHERE locationtext = '{1}'";
            string direction = stationName.ToUpper().EndsWith("NB") ? "north" : "south";

            Row result = session.Execute(string.Format(
                            query,
                            direction,
                            stationName
                        )).SingleOrDefault();

            if (result == null || result.Length == 0)
            {
                throw new ArgumentException(string.Format("Unable to locate station: {0}.", stationName));
            }

            return new StationInfo
            {
                Id = Convert.ToInt32(result[0]),
                Length = Convert.ToDouble(result[1])
            };
        }
    }
}
