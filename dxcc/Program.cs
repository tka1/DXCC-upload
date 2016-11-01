using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace dxcc
{
    class Program
    {
        static void Main(string[] args)
        {
            NpgsqlConnection conn = new NpgsqlConnection("Server=192.168.1.52;User Id=cluster;" +
                                "Password=Saturnus1!;Database=postgres;");
            try // truncate dxcc table
            {
                conn.Open();
                // Define a query
                NpgsqlCommand cmd = new NpgsqlCommand("truncate table cluster.dxcc", conn);
                // Execute a query
                NpgsqlDataReader dr = cmd.ExecuteReader();
                // Close connection
                conn.Close();
            }
            catch (Exception ex)
            {
                System.Console.WriteLine("READING:");
                System.Console.WriteLine("  ERROR:" + ex.Message);
                Console.ReadLine();
           }

            char splitchar = ':';
            char splitchar2 = ',';
            try { // parse country file
                int kountti = 0;
                string[] lines = File.ReadAllLines("wl_cty2.txt");    // read rows to array 

                for (int i=0;i<= lines.Length; i++) // parse array
                {
                    if (":".Equals(lines[i].Substring(lines[i].Length - 1, 1))) // country line
                    {
                        string[] countryline = lines[i].Split(splitchar); // split to columns
                        int prefrow = 1;
                        kountti++;
                        while (!":".Equals(lines[i+prefrow].Substring(lines[i+prefrow].Length - 1, 1))) // loop all prefix lines
                        {
                            string[]prefix = lines[i + prefrow].Split(splitchar2);
                           // Console.WriteLine(lines[i + prefrow]);
                            for(int ii=0; ii < prefix.Length; ii++)
                            {
                                string pref = prefix[ii];
                                //remove (
                                pref = pref.Replace("=", "");
                                if(pref.IndexOf("(") !=-1)
                                {
                                    pref = pref.Substring(0, pref.IndexOf("("));
                                }
                               
                                //remove [
                                pref = pref.Replace("=", "");
                                if (pref.IndexOf("[") != -1)
                                {
                                    pref = pref.Substring(0, pref.IndexOf("["));
                                }
                             

                                pref = pref.Replace(";", "");
                                pref = pref.Trim();
                               
                                Console.WriteLine(pref + "  " + countryline[0] + "  "+ kountti);
                                try
                                {
                                    

                                    conn.Open();
                                    // Define a query

                                    NpgsqlCommand cmd = new NpgsqlCommand("insert into cluster.dxcc(country,prefix,cq_zone,itu_zone,continent,lat,long,local_time,prim_dxcc_prefix) values  ( :value1 ,:value2,:value3,:value4,:value5,:value6,:value7,:value8,:value9)", conn);
                                    cmd.Parameters.Add(new NpgsqlParameter("value1", NpgsqlTypes.NpgsqlDbType.Text));
                                    cmd.Parameters.Add(new NpgsqlParameter("value2", NpgsqlTypes.NpgsqlDbType.Text));
                                    cmd.Parameters.Add(new NpgsqlParameter("value3", NpgsqlTypes.NpgsqlDbType.Text));
                                    cmd.Parameters.Add(new NpgsqlParameter("value4", NpgsqlTypes.NpgsqlDbType.Text));
                                    cmd.Parameters.Add(new NpgsqlParameter("value5", NpgsqlTypes.NpgsqlDbType.Text));
                                    cmd.Parameters.Add(new NpgsqlParameter("value6", NpgsqlTypes.NpgsqlDbType.Text));
                                    cmd.Parameters.Add(new NpgsqlParameter("value7", NpgsqlTypes.NpgsqlDbType.Text));
                                    cmd.Parameters.Add(new NpgsqlParameter("value8", NpgsqlTypes.NpgsqlDbType.Text));
                                    cmd.Parameters.Add(new NpgsqlParameter("value9", NpgsqlTypes.NpgsqlDbType.Text));
                                    cmd.Parameters[0].Value = countryline[0].Trim();
                                    cmd.Parameters[1].Value = pref;
                                    cmd.Parameters[2].Value = countryline[1].Trim();
                                    cmd.Parameters[3].Value = countryline[2].Trim();
                                    cmd.Parameters[4].Value = countryline[3].Trim();
                                    cmd.Parameters[5].Value = countryline[4].Trim();
                                    cmd.Parameters[6].Value = countryline[5].Trim();
                                    cmd.Parameters[7].Value = countryline[6].Trim();
                                    cmd.Parameters[8].Value = countryline[7].Trim();

                                    NpgsqlDataReader dr = cmd.ExecuteReader();
                                    conn.Close();

                                    
                                }
                                catch (Exception ex)
                                {
                                    System.Console.WriteLine("READING:");
                                    System.Console.WriteLine("  ERROR:" + ex.Message);
                                    Console.ReadLine();

                                }


                            }
                            
                            prefrow++;
                        }

                    }
                    //string lastchar = lines[i].Substring(lines[i].Length - 1, 1);
                   
                }

              
                Console.ReadLine();
            }
            catch { }
            Console.ReadLine();
        }
    }
}
