using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeTableRTDFW.Model
{
    public class TopicData
    {
        public TopicData(string key, string field)
        {
            Key = key;
            Field = field;
        }

        public string Field { get; }
        public string Key { get; }
    }
}
