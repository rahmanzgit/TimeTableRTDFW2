using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeTableRTDFW.Model
{
    public class RandomGenerator
    {
        private static Random _random = new Random();
        public static string GetContainerKey()
        {
            return "k" + _random.Next(1, 100);
        }
        public static string GetTimeKey(int time)
        {
            //return "t" + _random.Next(1, 100);
            return "t" + time;
        }
        public static double GetTimeValue()
        {
            return _random.NextDouble();
        }
    }
}
