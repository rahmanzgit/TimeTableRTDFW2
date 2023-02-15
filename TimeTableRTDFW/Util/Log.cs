using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeTableRTDFW.Util
{
    public class Log
    {
        public static void Append(string file, string text)
        {
            try {
                //to turn off/on log
                if (1==1)
                {
                    File.AppendAllText(file, "\n" + DateTime.Now.ToString() + "\n" + text);
                }
            }
            catch (Exception e) { }
        }
    }
}
