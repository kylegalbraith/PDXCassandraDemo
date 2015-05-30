using Cassandra;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryDemoApp
{
    public class CsvLoader
    {
        private readonly ISession _session;
        private readonly string _path;
        private readonly string _format;

        public CsvLoader(ISession session, string path, string queryFormat)
        {
            _session = session;
            _path = path;
            _format = queryFormat;
        }

        public int Load()
        {
            int counter = 0;

            TextFieldParser parser = new TextFieldParser(_path);
            parser.TextFieldType = FieldType.Delimited;
            parser.SetDelimiters(",");
            parser.ReadLine(); //skip the header

            while (!parser.EndOfData)
            {
                //Process row
                string[] fields = parser.ReadFields();

                _session.Execute(string.Format(_format, fields)); //nulber of fields can't be smaller than the number of placeholders
                counter++;
            }

            parser.Close();

            return counter;
        }
    }
}
