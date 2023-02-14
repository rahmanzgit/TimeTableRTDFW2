using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TimeTableRTDFW.Model;

namespace TimeTableRTDFW2.Model
{
    public class TimeTable
    {
        public string key { get; set; }
        public Time value { get; set; }
        public void Populate(int time)
        {
            this.key = RandomGenerator.GetContainerKey();
            this.value = new Time()
            {
                time = RandomGenerator.GetTimeKey(time),
                value = RandomGenerator.GetTimeValue()
            };
        }
        public void Populate(string objvalue)
        {
            var objTc = JsonConvert.DeserializeObject<TimeTable>(objvalue);
            this.key = objTc.key;
            this.value = objTc.value;
        }
    }
}
