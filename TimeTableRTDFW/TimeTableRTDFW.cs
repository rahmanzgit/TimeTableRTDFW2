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
using TimeTableRTDFW.Util;
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
        string logPath = @"Log\\Log" + DateTime.Now.Day + DateTime.Now.Month + DateTime.Now.Year + "_" + DateTime.Now.Hour + DateTime.Now.Minute + DateTime.Now.Second + DateTime.Now.Millisecond + ".txt";
        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        bool isSubscribed = false;
        JSONHelper jSONHelper;
        public TimeTableRTD2()
        {
            try
            {
                m_topics = new Dictionary<int, TopicData>();
                m_data = new Dictionary<string, Time>();
                jSONHelper = new JSONHelper();
                Log.Append(logPath, "TimeTableRTD: Constructed");
            }
            catch (Exception ex)
            {
                Log.Append(logPath, "TimeTableRTD: " + ex.ToString());
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
                Log.Append(logPath, "TimeTableRTD: In Connect Data");
                if (strings.Length == 1)
                {
                    string host = strings.GetValue(0).ToString().ToUpperInvariant();

                    Log.Append(logPath, "TimeTableRTD: In Connect Data - string length 1");

                    return "ERROR: Expected: host, topic, field";
                }
                else if (strings.Length >= 2)
                {
                    Log.Append(logPath, "TimeTableRTD: In Connect Data - string length >=2");
                    string host = strings.GetValue(0).ToString();
                    string topic = strings.GetValue(1).ToString();
                    Log.Append(logPath, "TimeTableRTD: In Connect Data - host:" + host + "; topic:" + topic);
                    string field = strings.Length > 2 ? strings.GetValue(2).ToString() : "";
                    Log.Append(logPath, "TimeTableRTD: In Connect Data - field:" + field);
                    string key = strings.Length > 3 ? strings.GetValue(3).ToString() : "";
                    Log.Append(logPath, "TimeTableRTD: In Connect Data - key:" + key);
                    //
                    TopicData data = new TopicData(key, field);
                    //m_topics.Add(topicId, data);
                    m_topics[topicId] = data;
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
                    }
                    m_timer.Start();
                    return GetNextValue(data);
                }
                return "ERROR: Expected: host, topic, field";

            }
            catch (Exception ex)
            {
                Log.Append(logPath, "TimeTableRTD: In Connect Data - Exception:" + ex.ToString());
                return ex.Message;
            }
        }
        private object Subscribe(int topicId, string host, string topic, string field)
        {
            try
            {
                Log.Append(logPath, "TimeTableRTD: In Subscribe");
                //
                Log.Append(logPath, "TimeTableRTD: In Subscribe - topic checked");

                Uri hostUri = new Uri(host);
                Log.Append(logPath, "TimeTableRTD: In Subscribe - Subscribed");

                Task.Run(() =>
                {
                    Log.Append(logPath, "TimeTableRTD: In Subscribe - Task Run");
                    try
                    {
                        var config = new ConsumerConfig
                        {
                            BootstrapServers = host,
                            GroupId = Guid.NewGuid().ToString(),
                            AutoOffsetReset = AutoOffsetReset.Earliest,
                            EnableAutoOffsetStore = true,
                            EnableAutoCommit = true,
                            StatisticsIntervalMs = 5000,
                            SessionTimeoutMs = 6000,
                            EnablePartitionEof = true
                        };
                        Log.Append(logPath, "TimeTableRTD: In Subscribe - Config Created");

                        
                        using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                        .Build())
                        {
                            Log.Append(logPath, "TimeTableRTD: In Subscribe - Consumer Build");
                            //consumer.Assign(new TopicPartitionOffset(topic, new Partition(0), Offset.Beginning));
                            consumer.Subscribe(topic);
                            Log.Append(logPath, "TimeTableRTD: In Subscribe - Consumer Subscribed : " + topic);
                            try
                            {
                                while (true)
                                {
                                    try
                                    {
                                        Log.Append(logPath, "TimeTableRTD: In Subscribe - Waiting to be Retrived");
                                        var consumeResult = consumer.Consume(_cancellationTokenSource.Token);
                                        Log.Append(logPath, "TimeTableRTD: In Subscribe - Retrived");
                                        if (consumeResult != null && consumeResult.IsPartitionEOF)
                                        {
                                            Log.Append(logPath, "TimeTableRTD: In Subscribe - IsPartitionEOF");
                                            continue;
                                        }
                                        else
                                        {
                                            Log.Append(logPath, "TimeTableRTD: In Subscribe - Is Not PartitionEOF");
                                        }
                                        if (consumeResult != null && consumeResult.Message != null)
                                        {
                                            Log.Append(logPath, "TimeTableRTD: In Subscribe - Retrived - " + consumeResult.Message.Value);
                                            var timeTable = jSONHelper.TryParseJson<TimeTable>(consumeResult.Message.Value);
                                            if (timeTable != null)
                                            {
                                                this.AddTimeTable(timeTable);
                                            }
                                        }
                                        else
                                        {
                                            Log.Append(logPath, "TimeTableRTD: In Subscribe - Empty Message or null Message");
                                        }
                                    }
                                    catch (ConsumeException e)
                                    {                                        
                                        if (e.ConsumerRecord.Message.Value.Length > 0)
                                        {
                                            string msg = System.Text.Encoding.Default.GetString(e.ConsumerRecord.Message.Value);
                                            var timeTable = jSONHelper.TryParseJson<TimeTable>(msg);
                                            if (timeTable != null)
                                            {
                                                this.AddTimeTable(timeTable);
                                            }                                            
                                        }
                                        Log.Append(logPath, "TimeTableRTD: In Subscribe - ConsumeException - " + e.Error.Reason);                                        
                                    }
                                    catch (Exception e)
                                    {
                                        Log.Append(logPath, "TimeTableRTD: In Subscribe - General Ex - " + e.ToString());                                        
                                    }
                                }
                            }
                            catch (OperationCanceledException oex)
                            {
                                Log.Append(logPath, "TimeTableRTD: In Subscribe - Retrived Exception : " + "Closing consumer : " + oex.ToString());
                                consumer.Close();
                                isSubscribed = false;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Append(logPath, "TimeTableRTD: In Subscribe - " + ex.ToString());
                    }
                    finally
                    {
                    }
                });
                return 1;
            }
            catch (Exception ex)
            {
                Log.Append(logPath, "TimeTableRTD: In Subscribe - Exception:" + ex.ToString());
            }
            return null;
        }
        private void AddTimeTable(TimeTable timeTable)
        {
            if (!m_data.ContainsKey(timeTable.key))
            {
                m_data.Add(timeTable.key, timeTable.value);
            }
            else
            {
                m_data[timeTable.key] = Util.GetWithLatestTime(m_data[timeTable.key], timeTable.value);
            }
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
                    Log.Append(logPath, "TimeTableRTD: In SampleGenerator - " + ex.ToString());
                }
            }
            catch (Exception ex)
            {
                Log.Append(logPath, "TimeTableRTD: In SampleGenerator - Exception:" + ex.ToString());
            }
            return null;
        }

        public void DisconnectData(int topicId)
        {
            m_topics.Remove(topicId);
            Log.Append(logPath, "TimeTableRTD: In DisconnectData");
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
            m_timer.Start();
            return data;
        }
        object GetNextValue(TopicData data)
        {
            Time timeData;
            if (m_data.TryGetValue(data.Key, out timeData))
            {
                switch (data.Field.ToLower())
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
            Log.Append(logPath, "TimeTableRTD: In Heartbeat");
            if (m_data != null)
            {
                Log.Append(logPath, "TimeTableRTD: In Heartbeat - " + m_data.Count);
            }
            else
            {
                Log.Append(logPath, "TimeTableRTD: In Heartbeat - Data Not loaded yet");
            }            
            return 1;
        }
        void TimerEventHandler(object sender,
                               EventArgs args)
        {
            m_timer.Stop();            
            m_callback.UpdateNotify();
        }
    }
}
