using System;
using System.Collections.Generic;
using System.Data;
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
        static string currentName = "";
        static void Main(string[] args)
        {
            DBToDB();
            //Regionalism();
            Console.ReadLine();
        }

        public static void Regionalism()
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
                    Console.WriteLine((ins++) + ":" + item.Item3);
                    SqlCommand command = new SqlCommand(queryString, connection);
                    command.ExecuteNonQuery();
                }
            }
            connection.Close();
            Console.WriteLine("行政区域抓取完成，共计：" + ins);
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
                    Console.WriteLine(currentName + ":" + (index++) + "_" + href.Substring(href.LastIndexOf("/") + 1).Replace(".html", "") + ":" + value);
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


        public static void DBToDB()
        {
            long count = 0;
            SqlConnection connection = new SqlConnection("Server=192.168.107.100;uid=sa;pwd=cnki.sa123456;database=niezl;");
            //(<Id, uniqueidentifier,>,< Name, nvarchar(50),> ,< Alias, nvarchar(200),>,< AreaClass, tinyint,>,< Parent_Id, uniqueidentifier,>,< Sort, int,> ,< Code, varchar(6),>,< Parent_Code, varchar(6),>,< CreateTime, datetime,>,< CreateUser, uniqueidentifier,>,< LastUpdateTime, datetime,> ,< LastUpdateUser, uniqueidentifier,>,< Remark, nvarchar(500),>,< IsDelete, bit,>)"
            //(<Id,>,< Name,> ,< Alias>,< AreaClass, tinyint,>,< Parent_Id>,< Sort, int,> ,< Code>,< Parent_Code>,< CreateTime>,< CreateUser,>,< LastUpdateTime> ,< LastUpdateUser,>,< Remark, nvarchar(500),>,< IsDelete, bit,>)"
            string insertsql = "INSERT INTO TB_AdministrativeRegion VALUES ('{0}','{1}',null,{2},'{3}',{4} ,{5},'{6}','{7}',null,null ,null,null,0)";
            connection.Open();
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connection;
            SqlDataAdapter adapter = new SqlDataAdapter();
            cmd.CommandText = string.Format(insertsql, "2D6DACA1-4D91-B947-87CE-DDF5DB32F388", "中国", 0, "2D6DACA1-4D91-B947-87CE-DDF5DB32F386", 0, 86, "地球", DateTime.Now);
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CN_Regionalism where 父级代码=0";
            DataSet ds = new DataSet();
            adapter.SelectCommand = cmd;
            adapter.Fill(ds); int index1 = 0; int index2 = 0; int index3 = 0;
            foreach (DataRow item in ds.Tables[0].Rows)
            {
              
                string id = Guid.NewGuid().ToString();
                cmd.CommandText = string.Format(insertsql, id, item["区域名称"].ToString().Replace("办事处", "").Replace("建设管理委员会", "").Replace("居委会", ""), 1, "2D6DACA1-4D91-B947-87CE-DDF5DB32F388", index1++, item["区划代码"], 86, DateTime.Now);
                cmd.ExecuteNonQuery();
                Console.WriteLine((count++).ToString()+item["区域名称"]);
                DataSet ds1 = new DataSet();
                cmd.CommandText = "select * from CN_Regionalism where 父级代码=" + item["区划代码"];
                adapter.SelectCommand = cmd;
                adapter.Fill(ds1);
                if (ds1.Tables[0].Rows.Count == 1 && ds1.Tables[0].Rows[0]["区域名称"].ToString().Trim() == "市辖区")
                {
                    cmd.CommandText = "select * from CN_Regionalism where 父级代码=" + ds1.Tables[0].Rows[0]["区划代码"];
                    adapter.SelectCommand = cmd;
                    adapter.Fill(ds1);
                }
                foreach (DataRow item1 in ds1.Tables[0].Rows)
                {
                   
                    if (item1["区域名称"].ToString().Trim() == "市辖区")
                    {
                        continue;
                    }
                    string id1 = Guid.NewGuid().ToString();
                    if (item1["区域名称"].ToString().Trim() == "省直辖县级行政区划")
                    {
                        DataSet ds2 = new DataSet();
                        cmd.CommandText = "select * from CN_Regionalism where 父级代码=" + item1["区划代码"];
                        adapter.SelectCommand = cmd;
                        adapter.Fill(ds2);
                        foreach (DataRow item2 in ds2.Tables[0].Rows)
                        {
                            string id1_1 = Guid.NewGuid().ToString();
                            cmd.CommandText = string.Format(insertsql, id1_1, item2["区域名称"].ToString().Replace("办事处", "").Replace("建设管理委员会", "").Replace("居委会", ""), 2, id, index2++, item2["区划代码"], item["区划代码"], DateTime.Now);
                            cmd.ExecuteNonQuery();
                            Console.WriteLine((count++).ToString() + item2["区域名称"]);
                            DataSet ds2_1 = new DataSet();
                            cmd.CommandText = "select * from CN_Regionalism where 父级代码=" + item2["区划代码"];
                            adapter.SelectCommand = cmd;
                            adapter.Fill(ds2_1);
                            foreach (DataRow item2_1 in ds2_1.Tables[0].Rows)
                            {
                                cmd.CommandText = string.Format(insertsql, Guid.NewGuid().ToString(), item2_1["区域名称"].ToString().Replace("办事处", "").Replace("建设管理委员会", "").Replace("居委会", ""), 3, id1_1, index3++, item2_1["区划代码"], item2["区划代码"], DateTime.Now);
                                cmd.ExecuteNonQuery();
                                Console.WriteLine((count++).ToString() + item2_1["区域名称"]);
                            }
                        }
                    }
                    else
                    {
                        cmd.CommandText = string.Format(insertsql, id1, item1["区域名称"].ToString().Replace("办事处", "").Replace("建设管理委员会", "").Replace("居委会", ""), 2, id, index2++, item1["区划代码"], item["区划代码"], DateTime.Now);
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "select * from CN_Regionalism where 父级代码=" + item1["区划代码"];
                        adapter.SelectCommand = cmd;
                        DataSet ds2_2 = new DataSet();
                        adapter.Fill(ds2_2);
                        foreach (DataRow item2_2 in ds2_2.Tables[0].Rows)
                        {
                            cmd.CommandText = string.Format(insertsql, Guid.NewGuid().ToString(), item2_2["区域名称"].ToString().Replace("办事处","").Replace("建设管理委员会", "").Replace("居委会", ""), 3, id1, index3++, item2_2["区划代码"], item1["区划代码"], DateTime.Now);
                            cmd.ExecuteNonQuery();
                            Console.WriteLine((count++).ToString() + item2_2["区域名称"]);
                        }
                    }
                   
                    Console.WriteLine((count++).ToString() + item1["区域名称"]);
                }

            }

            connection.Close();
        }
    }
}
