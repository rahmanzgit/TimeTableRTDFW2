using Newtonsoft.Json;

namespace TimeTableRTDFW.Util
{
    public class JSONHelper
    {
        public T TryParseJson<T>(string jsonStr)
        {
            try
            {
                return JsonConvert.DeserializeObject<T>(jsonStr);
            }catch
            {
                return default(T);
            }
        }
    }
}
