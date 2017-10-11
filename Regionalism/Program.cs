using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Regionalism
{
    class Program
    {
        static List<Tuple<string, string, string, string>> regionlist = new List<Tuple<string, string, string, string>>();
        static int index = 0;
        static object _lock = new object();
        static string currentName="";
        static void Main(string[] args)
        {

            GetListRegion("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2016/index.html", "");
            // GetListRegion("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2016/11/01/01/110101001.html");
            Console.WriteLine("行政区域抓取完成，共计：" + index);

            SqlConnection connection = new SqlConnection("Server=192.168.107.100;uid=sa;pwd=cnki.sa123456;database=niezl;");
            connection.Open();
            long ins = 1;
            foreach (var item in regionlist)
            {
                if (item != null)
                {
                    string queryString = "insert into CN_Regionalism values({0},'{1}',{2},{3},{4})";
                    if (string.IsNullOrEmpty(item.Item2))
                    {
                        queryString = string.Format("insert into CN_Regionalism values({0},'{1}',null,{3},{4})", item.Item1, item.Item3, null, string.IsNullOrEmpty(item.Item2) ? 0 : 1, string.IsNullOrEmpty(item.Item4) ? 0 : long.Parse(item.Item4));
                    }
                    else
                    {
                        queryString = string.Format(queryString, item.Item1, item.Item3, item.Item2, string.IsNullOrEmpty(item.Item2) ? 0 : 1, string.IsNullOrEmpty(item.Item4) ? 0 : long.Parse(item.Item4));
                    }
                    Console.WriteLine((ins++)+":"+item.Item3);
                    SqlCommand command = new SqlCommand(queryString, connection);
                    command.ExecuteNonQuery();
                }
            }
            Console.WriteLine("行政区域抓取完成，共计：" + ins);
            Console.ReadLine();
        }
        public static void GetListRegion(string url, string parentid)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            WebClient client = new WebClient();
            string datastr = string.Empty;
            try
            {
                datastr = client.DownloadString(url);
            }
            catch (Exception)
            {
                Console.WriteLine("等待：10000  url:" + url);
                Thread.Sleep(10000);
                try
                {
                    datastr = client.DownloadString(url);
                }
                catch (Exception)
                {
                    Console.WriteLine("等待：100000  url:" + url);
                    Thread.Sleep(100000);
                    try
                    {
                        datastr = client.DownloadString(url);
                    }
                    catch (Exception)
                    {

                        using (StreamWriter sw = new StreamWriter(Path.Combine(Environment.CurrentDirectory, "Exception.txt"), true))
                        {
                            sw.WriteLine(url);
                            sw.Flush();
                            sw.Close();
                        }
                    }
                }
            }
            long c = 0;
            bool lastchile = false;
            if (string.IsNullOrEmpty(parentid))
            {
                string prttern = @"<(?<HtmlTag>a)[^>]*?>((?<Nested><\k<HtmlTag>[^>]*>)|</\k<HtmlTag>>(?<-Nested>)|.*?)*</\k<HtmlTag>>";
                var maths = Regex.Matches(datastr, prttern);
                for (int i = 0; i < maths.Count; i++)
                {
                    //  Parallel.For(0, maths.Count, (i) =>
                    {
                        index = 0;
                        string astr = maths[i].Value;
                        string href = astr.Substring(astr.IndexOf("'") + 1, astr.LastIndexOf("'") - astr.IndexOf("'") - 1);
                        string value = astr.Substring(astr.IndexOf(">") + 1, astr.IndexOf("<", 2) - astr.IndexOf(">") - 1);
                        Console.WriteLine((index++) + "_" + href.Substring(href.LastIndexOf("/") + 1).Replace(".html", "") + ":" + value);
                        regionlist.Add(Tuple.Create(href.Substring(href.LastIndexOf("/") + 1).Replace(".html", ""), "", value, parentid));
                        currentName = value;
                        InsertDB(regionlist[regionlist.Count - 1]);
                        GetListRegion(url.Replace(url.Substring(url.LastIndexOf("/")), "/") + href, href.Substring(href.LastIndexOf("/") + 1).Replace(".html", ""));
                    }
                    //);
                }
            }
            else
            {
                string prttern = @"<(?<HtmlTag>tr)[^>]*?>((?<Nested><\k<HtmlTag>[^>]*>)|</\k<HtmlTag>>(?<-Nested>)|.*?)*</\k<HtmlTag>>";

                var maths = Regex.Matches(datastr, prttern);
                for (int i = 0; i < maths.Count; i++)
                {
                    if (!maths[maths.Count - 1].Value.Contains("href"))
                    {
                        lastchile = true;
                        break;

                    }
                    if (maths[i].Value.Contains("统计用区划代码"))
                    {
                        continue;
                    }
                    string[] trstr = new string[3];
                    string href = "";
                    string value = "";
                    if (!maths[i].Value.Contains("</a></td><td><"))
                    {
                        trstr = maths[i].Value.Split(new string[] { @"</td><td>" }, StringSplitOptions.RemoveEmptyEntries);
                        href = trstr[0].Substring(trstr[0].LastIndexOf(">") + 1);
                        value = trstr[1].Substring(0, trstr[1].IndexOf("<"));
                    }
                    else
                    {
                        trstr = maths[i].Value.Split(new string[] { @"</a></td><td><" }, StringSplitOptions.RemoveEmptyEntries);
                        href = trstr[0].Substring(trstr[0].LastIndexOf(">") + 1);
                        value = trstr[1].Substring(trstr[1].IndexOf(">") + 1, trstr[1].IndexOf("<") - trstr[1].IndexOf(">") - 1);
                    }
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine(currentName+":"+(index++) + "_" + href.Substring(href.LastIndexOf("/") + 1).Replace(".html", "") + ":" + value);
                    regionlist.Add(Tuple.Create(href, "", value, parentid));
                    InsertDB(regionlist[regionlist.Count - 1]);
                    if (maths[i].Value.Contains("href"))
                    {
                        GetListRegion(url.Replace(url.Substring(url.LastIndexOf("/")), "/") + trstr[0].Substring(trstr[0].IndexOf("href='") + 6, trstr[0].IndexOf(".html") - trstr[0].IndexOf("href='") - 1), href);
                    }
                }
            }
            if (lastchile)
            {
                string prttern1 = @"<(?<HtmlTag>tr)[^>]*?>((?<Nested><\k<HtmlTag>[^>]*>)|</\k<HtmlTag>>(?<-Nested>)|.*?)*</\k<HtmlTag>>";
                var maths1 = Regex.Matches(datastr, prttern1);
                for (int i = 0; i < maths1.Count; i++)
                {
                    if (maths1[i].Value.Contains("统计用区划代码"))
                    {
                        continue;
                    }
                    string[] trstr = maths1[i].Value.Split(new string[] { @"</td>" }, StringSplitOptions.RemoveEmptyEntries);
                    string href = trstr[0].Substring(trstr[0].LastIndexOf(">") + 1);
                    if (long.TryParse(href, out c))
                    {
                        href = c.ToString();
                        string code = trstr[1].Substring(trstr[1].LastIndexOf(">") + 1);
                        string value = trstr[2].Substring(trstr[2].LastIndexOf(">") + 1);
                        regionlist.Add(Tuple.Create(href, code, value, parentid));
                        InsertDB(regionlist[regionlist.Count - 1]);
                        //Console.WriteLine((index++) + "_" + href + ":" + value + "(" + code + ")");
                    }
                }
            }

        }

        public static void InsertDB(Tuple<string, string, string, string> item)
        {
            lock (_lock)
            {
                if (item != null)
                {
                    string queryString = "insert into CN_Regionalism values({0},'{1}',{2},{3},{4})";
                    if (string.IsNullOrEmpty(item.Item2))
                    {
                        queryString = string.Format("insert into CN_Regionalism values({0},'{1}',null,{3},{4})", item.Item1, item.Item3, null, string.IsNullOrEmpty(item.Item2) ? 0 : 1, string.IsNullOrEmpty(item.Item4) ? 0 : long.Parse(item.Item4));
                    }
                    else
                    {
                        queryString = string.Format(queryString, item.Item1, item.Item3, item.Item2, string.IsNullOrEmpty(item.Item2) ? 0 : 1, string.IsNullOrEmpty(item.Item4) ? 0 : long.Parse(item.Item4));
                    }
                    //using (SqlConnection connection = new SqlConnection("Data Source=localhost;Initial Catalog=68zg;Integrated Security=True"))
                    //{
                    //    SqlCommand command = new SqlCommand(queryString, connection);
                    //    command.Connection.Open();
                    //    command.ExecuteNonQuery();
                    //}
                    using (StreamWriter sw = new StreamWriter(Path.Combine(Environment.CurrentDirectory, currentName + ".txt"), true))
                    {
                        sw.WriteLine(queryString);
                        sw.Flush();
                        sw.Close();
                    }
                }
            }
        }

    }
}
