using Confluent.Kafka;
using Newtonsoft.Json;
using RTDTest.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TimeTableRTDFW2;
//using TimeTableRTDFW2;
//using TimeTableRTDFW2.Model;

namespace RTDTest
{
    internal class Program
    {
        static void Main(string[] args)
        {
            TimeTableRTD2 rtd = new TimeTableRTD2();
            int topicid = 0;            
            Array strings = Array.CreateInstance(typeof(string), 4);
            strings.SetValue("localhost:9092", 0);
            //strings.SetValue("sample", 0);
            strings.SetValue("ztesttopic2", 1);
            strings.SetValue("k1", 2);
            strings.SetValue("KEY", 3);
            
            bool newvalu = false;

            rtd.ConnectData(topicid, ref strings, ref newvalu);
            Console.ReadLine();
        }        
    }
}
