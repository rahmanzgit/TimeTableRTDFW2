using Confluent.Kafka;
using Microsoft.Office.Interop.Excel;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using TimeTableRTDFW.Model;
using TimeTableRTDFW2.Model;
using Time = TimeTableRTDFW2.Model.Time;
using Timer = System.Windows.Forms.Timer;

namespace TimeTableRTDFW2
{
    [ProgId("TimeTableRTDFW2")]
    [ComDefaultInterface(typeof(IRtdServer))]
    public class TimeTableRTD2 : IRtdServer
    {
        Dictionary<int, TopicData> m_topics;
        Timer m_timer;
        IRTDUpdateEvent m_callback;
        Dictionary<string, Time> m_data;
        string logPath = @"Log\\Log"+ DateTime.Now.Day + DateTime.Now.Month + DateTime.Now.Year + "_"+ DateTime.Now.Hour+DateTime.Now.Minute + DateTime.Now.Second+DateTime.Now.Millisecond + ".txt";
        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        bool isSubscribed = false;
        public TimeTableRTD2()
        {
            try
            {
                m_topics = new Dictionary<int, TopicData>();
                m_data = new Dictionary<string, Time>();
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: Constructed");
            }
            catch(Exception ex)
            {
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: " + ex.ToString());
            }
        }

        public int ServerStart(IRTDUpdateEvent callback)
        {
            m_callback = callback;
            m_timer = new Timer();
            m_timer.Tick += new EventHandler(TimerEventHandler);
            m_timer.Interval = 500;
            return 1;
        }

        public void ServerTerminate()        
        {
            if (null != m_timer)
            {
                m_timer.Dispose();
                m_timer = null;
            }
            _cancellationTokenSource.Cancel();
        }

        public object ConnectData(int topicId,
                                  ref Array strings,
                                  ref bool newValues)
        {
            try
            {
                newValues = true;
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Connect Data");
                if (strings.Length == 1)
                {
                    string host = strings.GetValue(0).ToString().ToUpperInvariant();

                    File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Connect Data - string length 1");
        
                    return "ERROR: Expected: host, topic, field";
                }
                else if (strings.Length >= 2)
                {
                    File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Connect Data - string length >=2");                    
                    string host = strings.GetValue(0).ToString();
                    string topic = strings.GetValue(1).ToString();
                    File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Connect Data - host:" + host + "; topic:" + topic);
                    string field = strings.Length > 2 ? strings.GetValue(2).ToString() : "";
                    File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Connect Data - field:" + field);
                    string key = strings.Length > 3 ? strings.GetValue(3).ToString() : "";
                    File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Connect Data - key:" + key);
                    //
                    TopicData data = new TopicData(key, field);
                    m_topics.Add(topicId, data);
                    //
                    if (!host.Equals("sample"))
                    {
                        if (!isSubscribed)
                        {
                            Subscribe(topicId, host, topic, field);
                            isSubscribed = true;
                        }                        
                    }
                    else
                    {
                        SampleGenerator();
                        m_timer.Start();
                    }
                    
                    return GetNextValue(data);                                        
                }
                return "ERROR: Expected: host, topic, field";

            }
            catch (Exception ex)
            {
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Connect Data - Exception:" + ex.ToString());
                return ex.Message;
            }
        }
        private object Subscribe(int topicId, string host, string topic, string field)
        {
            try
            {
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe");
                //
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - topic checked");

                Uri hostUri = new Uri(host);
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Subscribed");

                Task.Run(() =>
                {
                    File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Task Run");
                    try
                    {
                        var config = new ConsumerConfig
                        {
                            BootstrapServers = host,
                            GroupId = Guid.NewGuid().ToString(),
                            EnableAutoOffsetStore = true,
                            EnableAutoCommit = true,
                            StatisticsIntervalMs = 5000,
                            SessionTimeoutMs = 6000,
                            AutoOffsetReset = AutoOffsetReset.Earliest,
                            EnablePartitionEof = true
                        };
                        File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Config Created");

                        using (var consumer = new ConsumerBuilder<Ignore, string>(config)                
                        .Build())
                        {
                            File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Consumer Build");                            
                            consumer.Assign(new TopicPartitionOffset(topic, new Partition(0), Offset.Beginning));
                            consumer.Subscribe(topic);
                            File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Consumer Subscribed : " + topic );
                            try
                            {
                                while (true)
                                {
                                    try
                                    {
                                        File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Waiting to be Retrived");
                                        var consumeResult = consumer.Consume(2000);
                                        File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Retrived");
                                        if (consumeResult != null && consumeResult.IsPartitionEOF)
                                        {
                                            File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - IsPartitionEOF");
                                            continue;
                                        }
                                        else
                                        {
                                            File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Is Not PartitionEOF");
                                        }
                                        if (consumeResult !=null && consumeResult.Message != null)
                                        {
                                            File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Retrived - " + consumeResult.Message.Value);
                                            var timeTable = JsonConvert.DeserializeObject<TimeTable>(consumeResult.Message.Value);
                                            m_data.Add(timeTable.key, timeTable.value);
                                        }
                                        else
                                        {
                                            File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Empty Message or null Message");
                                        }
                                    }
                                    catch (ConsumeException e)
                                    {
                                        File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - ConsumeException - " + e.Error.Reason);                                        
                                    }
                                    catch (Exception e)
                                    {
                                        File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - General Ex - " + e.ToString());
                                    }
                                }
                            }
                            catch (OperationCanceledException oex)
                            {
                                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Retrived Exception : " + "Closing consumer : " + oex.ToString());
                                consumer.Close();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - " + ex.ToString());
                    }
                    finally
                    {
                    }
                });
                return 1;                
            }
            catch (Exception ex)
            {
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Subscribe - Exception:" + ex.ToString());                                
            }
            return null;
        }
        private object SampleGenerator()
        {
            try
            {
                try
                {
                    int time = 1;
                    while (m_data.Count < 100)
                    {
                        try
                        {
                            TimeTable timetable = new TimeTable();
                            timetable.Populate(time++);
                            m_data.Add(timetable.key, timetable.value);
                        }
                        catch (IOException)
                        {
                            break;
                        }
                        //
                        //Thread.Sleep(100);
                    }
                }
                catch (Exception ex)
                {
                    File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In SampleGenerator - " + ex.ToString());
                }
            }
            catch (Exception ex)
            {
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In SampleGenerator - Exception:" + ex.ToString());
            }
            return null;
        }

        public void DisconnectData(int topicId)
        {
            m_topics.Remove(topicId);
            File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In DisconnectData");
        }

        public Array RefreshData(ref int topicCount)
        {
            object[,] data = new object[2, m_topics.Count];

            int index = 0;

            foreach (int topicId in m_topics.Keys)
            {
                data[0, index] = topicId;
                data[1, index] = GetNextValue(m_topics[topicId]);
                ++index;
            }

            topicCount = m_topics.Count;            
            return data;            
        }
        object GetNextValue(TopicData data)
        {
            Time timeData;
            if (m_data.TryGetValue(data.Key, out timeData))
            {
                switch (data.Field)
                {
                    case "key":
                        return data.Key;
                    case "time":
                        return timeData.time;
                    case "value":
                        return timeData.value;
                }
            }
            return "#Invalid Data or No Data";
        }

        public int Heartbeat()
        {
            File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Heartbeat");
            if (m_data != null)
            {
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Heartbeat - " + m_data.Count);
            }
            else
            {
                File.AppendAllText(logPath, "\n" + DateTime.Now.ToString() + "\nTimeTableRTD: In Heartbeat - Data Not loaded yet");
            }            
            return 1;
        }
        void TimerEventHandler(object sender,
                               EventArgs args)
        {
            m_timer.Stop();
            SampleGenerator();
            m_callback.UpdateNotify();
        }
    }
}
