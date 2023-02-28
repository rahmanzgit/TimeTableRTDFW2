using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using System.Threading.Tasks;
using TimeTableRTDFW2.Model;

namespace TimeTableRTDFW.Util
{
    public class Util
    {
        public static Time GetWithLatestTime(Time t1, Time t2)
        {
            if (!t1.time.Equals(t2.time))
            {
                int currentKey = Convert.ToInt32(t1.time.Replace("t", ""));
                int givenKey = Convert.ToInt32(t2.time.Replace("t", ""));
                if (currentKey > givenKey)
                {
                    return t1;
                }
            }
            return t2;
        }
    }
}
