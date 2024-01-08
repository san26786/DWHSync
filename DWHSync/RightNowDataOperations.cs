using DWHSync.RightNowService;
using log4net;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DWHSync.Model;
using System.Web;
using Blackbaud.AppFx;
using Blackbaud.AppFx.WebAPI;
using Blackbaud.AppFx.WebAPI.ServiceProxy;
using Blackbaud.AppFx.XmlTypes;
using Blackbaud.AppFx.XmlTypes.DataForms;
using RestSharp.Serialization;
using RestSharp;
using RestSharp.Authenticators;
using System.Net;
using System.IO;
using Microsoft.Web.Administration;
using Configuration = Microsoft.Web.Administration.Configuration;

namespace DWHSync
{
    public class RightNowDataOperations
    {
        protected static readonly ILog appLogger = LogManager.GetLogger(typeof(RightNowDataOperations));

        public RightNowSyncPortClient rightNowSyncPortClient = new RightNowSyncPortClient();
        // private string myConnectionString = "server=62.138.4.114;uid=oneview_RN;" + "pwd=Geecon0404;database=oneview_rn_backup; convert zero datetime=True;AutoEnlist=false";
        APIAccessRequestHeader api = new APIAccessRequestHeader();
        private string myConnectionString = "server=172.23.161.18;uid=dwh_user;" + "pwd=Geecon0404;database=DWH; convert zero datetime=True;AutoEnlist=false;Connection Lifetime=0;Min Pool Size=0;Max Pool Size=1000;Pooling=true;";
        private string myConnectionStringComuk = "server=185.162.227.108;uid=comuk_websit;" + "pwd=Website2020@;database=comuk_website; convert zero datetime=True;AutoEnlist=false";//wp_sponsorship_details//wp_donation_details
        private string myConnectionStringcuk = "server=62.138.4.114;uid=cuk_tcpt4;" + "pwd=Geecon0404;database=cuk_tcpt4_log; convert zero datetime=True;AutoEnlist=false";//wp_tcpt_online_s2b
        private string myAppConnectionString = "server=172.23.161.30;uid=oneview_comapp;" + "pwd=Geecon0404;database=oneview_comukapp_v2; convert zero datetime=True;AutoEnlist=false";
        private string MyuatConnectionString = "server=212.48.85.22;uid=myuat_Tcpt_Audit;" + "pwd=Geecon0404;database=myuat_Tcpt_Audit_Log; convert zero datetime=True;";

        private string myCUKDEVConnectionString = "server=109.108.159.77;uid=cukdevco_user;" + "pwd=Geecon0404;database=cukdevco_tcpt4_log; convert zero datetime=True;AutoEnlist=false;Connect Timeout=500;";

        private string myDWH_LandingConnectionString = "server=172.23.161.18;uid=dwh_landing_user;" + "pwd=Comp@ss!on@1952;database=dwh_landing; convert zero datetime=True;AutoEnlist=false;Connection Lifetime=0;Min Pool Size=0;Max Pool Size=100;Pooling=true;";

        private Blackbaud.AppFx.WebAPI.ServiceProxy.AppFxWebService _service;
        private string BbDatabase = string.Empty;
        private string ApplicationName = System.Reflection.Assembly.GetCallingAssembly().FullName;


        System.Timers.Timer _timer;
        DateTime _scheduleTime;
        TimeSpan start;
        TimeSpan end;

        string[,] Table_Name = new string[,]
        {
            {"Need","SCBS_CHILD.Need;"},
            {"Supporters","contacts;"},
            {"SupporterGroup","SCBS_SUP.SupporterGroup;"},
            {"SupporterGroupLinks","SCBS_SUP.SupporterGroupLinks;"},
            {"Commitments","SCBS_CM.Commitment;"},
            //{"SupporterEnquiries","Incidents;"},
            {"SchedChildDepartures","SCBS_CM.SchedChildDepartures;"},
            {"BeneficiaryOrICPHold","SCBS_CHILD.BeneficiaryOrICPHold;"},
            {"S2BCommunication","SCBS_SUP.S2BCommunication;"},
            {"Correspondence","SCBS_SUP.Correspondence;"},
            {"Campaigns","SCBS_COMMON.Campaigns;"},
            {"SolicitCodes","SCBS_SUP.SolicitCodes;"},
            {"Organisation","organizations;"},
            {"MiscEventTourDate","Misc.EventTourDate;"},
            {"CPPathwayActivity","CPPathway.Activity;"},
            {"ResourceOrder","SCBS_RM.ResourceOrder;"},
            {"DonationDetails","SCBS_DON.DonationDetails;"},
            {"Donation","SCBS_DON.Donation;"},
            {"DonationType","SCBS_DON.DonationType;"},
            {"MarketingChannels","SCBS_COMMON.MarketingChannels;"},
            {"MarketingManager","SCBS_COMMON.MarketingManager;"},
            {"Events","opportunities;"},
        };


        #region enquiry status
        Dictionary<int, string> dict = new Dictionary<int, string>()
        {
            { 1,"New Enquiry" },
            { 108, "New Enquiry (Automated)" },
            {106, "Outbound Call" },
            {111, "Outbound Call (Automated)" },
            {123, "Outbound Call (Second call needed)" },
            {8,"In Progress" },
            {3, "Supporter Response" },
            {110, "Supporter Response (Automated)" },
            {101, "Field Response" },
            {105, "Future Follow-up" },
            {112, "Updated by Supporter" },
            {125, "Updated Internally" },
            {113, "Pending Confirmation" },
            {2,"Resolved" },
            {107, "Resolved (Automated)" },
            {121, "Resolved (No Contact)" },
            {122, "Resolved (Message Left)" },
            {127, "Resolved (India)" },
            {118, "Meeting Arranged" },
            {114, "EXCEPTION Urgent" },
            {115, "EXCEPTION Immediate" },
            {116, "EXCEPTION Standard" },
            {117 ," Waiting on Support Ticket"},
            {124, "For Processing" },
        };
        #endregion


        #region Manager
        Dictionary<int, string> manager = new Dictionary<int, string>()
        {
            {316 , "Adrian Turk" },
            {3 , "Clare Hartley" },
            {2 , "Clare Nelson" },
            {4 , "Darren Sharp (Northern Ireland)" },
            {17 , "Darren Sharp (Ireland)" },
            {22 , "Don Esson" },
            {29 , "Dora Hanson Amissah" },
            {357 , "James Spencer" },
            {272 , "James Waddell" },
            {358 , "Luke Fleming" },
            {8 , "John Draper" },
            {359 , "Matt Band" },
            {360 , "Paul Wagstaff" },
            {318 , "Luke Gratton" },
            {361 , "Phil Martin" },
            {315 , "Mike Robins" },
            {254 , "Phil Briggs" },
            {362 , "Ruth Morgan" },
            {334 , "Phil Bamber" },
            {323 , "Rona Anderson" },
            {363 , "Wendy Pawsey" },
            {9 , "Matthew Harris" },
            {364 , "RPM North East" },
            {25 , "Hayley Humphreys" },
            {335 , "Harriet Dewey" },
            {31 , "Jeanine Kennedy" },
            {20 , "Justin Dowds" },
            {1 , "Unknown" },
            {10 , "Archive - Natasha Clark" },
            {365 , "RPM Northern Ireland" },
            {270 , "Archive - Steve Bowring" },
            {321 , "Tiffany Rollo" },
            {306 , "Archive - Lisa Bottaro" },
            {277 , "Archive - Darren Allwright" },
            {27 , "Archive - Donna Mack" },
            {19 , "Archive - Wendy Beech,Ward" },
            {14 , "Archive - Steve Bunn" },
            {21 , "Archive - Corporate" },
            {18 , "Archive - Belinda" },
            {319 , "Archive - Gary Mace" },
            {23 , "Archive - Stefan" },
            {317 , "Rachel Johns" },
            {366 , "TBC" },
            {320 , "Tobi Stathers" },
            {28 , "Ian Childs" },
            {15 , "Tim Robertson" },
            {12 , "International" },
            {250 , "Archive - Bekah Legg" },
            {251 , "Archive - Mattias Arvidsson" },
            {253 , "Archive - Gill" },
            {269 , "Archive - Bill Blow" },
            {7 , "Archive - Ian Hamilton" },
            {26 , "Archive - Ginny" },
            {16 , "Archive - Anne" },
            {34 , "Archive - Ruth" },
            {13 , "Archive - Sandy" },
            {5 , "Archive - Hayley M" },
            {11 , "Archive - Olly" },
            {6 , "Archive - Helen" },
            {24 , "Archive - International2" },
            {30 , "Archive - Hannah S" },
            {32 , "Archive - Joanne" },
            {33 , "Archive - Jonathan" },
            {35 , "Archive - Alison" },
            {259 , "Archive - Darren (North England)" },
            {271 , "Archive - Jane 'Alam Sheikh" },
            {0 , "" },

        };
        #endregion

        public RightNowDataOperations()
        {
            Dictionary<string, string> keyValue = new Dictionary<string, string>();

            using (ServerManager serverManager = new ServerManager())
            {
                Configuration config = serverManager.GetWebConfiguration("Default Web Site", "/");
                ConfigurationSection appSettingsSection = config.GetSection("appSettings");

                foreach (ConfigurationElement element in appSettingsSection.GetCollection())
                {
                    string key = element["key"].ToString();
                    string value = element["value"].ToString();
                    keyValue.Add(key, value);
                }
            }

            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            _service = new AppFxWebService();
            _service.Url = keyValue["BB_URI"];
            _service.Credentials = new System.Net.NetworkCredential(keyValue["BB_UID"], keyValue["BB_PWD"], "");
            var req = new Blackbaud.AppFx.WebAPI.ServiceProxy.GetAvailableREDatabasesRequest();
            req.ClientAppInfo = GetRequestHeader();
            try
            {
                var reply = _service.GetAvailableREDatabases(req);
                BbDatabase = reply.Databases[0];
            }
            catch (Exception e)
            {
                appLogger.Info("Error while connecting to BB. " + e.Message);
                appLogger.Info(e.StackTrace);
                System.Threading.Thread.Sleep(5000);// Sleep for 5 sec
                appLogger.Info("Trying again to connect with BB after 5 second.");
                var reply = _service.GetAvailableREDatabases(req);
                BbDatabase = reply.Databases[0];
            }

            // Logging
            log4net.Config.XmlConfigurator.Configure();

            // Authenticate with RightNow Server using RightNow user credentials
            rightNowSyncPortClient = new RightNowSyncPortClient();
            rightNowSyncPortClient.ClientCredentials.UserName.UserName = "GC_2";
            rightNowSyncPortClient.ClientCredentials.UserName.Password = "G33con0404";
            /*_timer = new System.Timers.Timer();
            _scheduleTime = DateTime.Today.AddDays(1).AddDays(-1).AddHours(18); // Schedule to run once a day at 6:00 p.m.
            //_scheduleTime = DateTime.Today.AddDays(1).AddDays(-1).AddHours(7).AddMinutes(5); // Schedule to run once a day at 1:00 a.m.
            start = new TimeSpan(18, 0, 0); //18 o'clock
            end = new TimeSpan(5, 0, 0); //5 o'clock
            */
        }

        private Blackbaud.AppFx.WebAPI.ServiceProxy.ClientAppInfoHeader GetRequestHeader()
        {
            var header = new Blackbaud.AppFx.WebAPI.ServiceProxy.ClientAppInfoHeader();
            header.ClientAppName =
            header.REDatabaseToUse = BbDatabase;
            return header;
        }

        #region Sync DWH
        public int SyncCommitment(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Need";

                do
                {
                    count = 0;


                    string Query = "SELECT SCBS_CM.Commitment.ID, SCBS_CM.Commitment.AcceptDate, SCBS_CM.Commitment.ApplicationCode, SCBS_CM.Commitment.BlackbaudID, SCBS_CM.Commitment.Campaign, SCBS_CM.Commitment.ChurchPartnership, SCBS_CM.Commitment.CompassionExperience, SCBS_CM.Commitment.CreatedTime, SCBS_CM.Commitment.DelinkReason, SCBS_CM.Commitment.DelinkType, SCBS_CM.Commitment.EndDate, SCBS_CM.Commitment.Event, SCBS_CM.Commitment.FirstFundedDate, SCBS_CM.Commitment.GlobalCommitmentID, SCBS_CM.Commitment.GlobalCorrespCommitmentID, SCBS_CM.Commitment.LinkReason, SCBS_CM.Commitment.LinkType, SCBS_CM.Commitment.LinkedToPartnership, SCBS_CM.Commitment.MarketingChannel, SCBS_CM.Commitment.MarketingCode, SCBS_CM.Commitment.Need, SCBS_CM.Commitment.Organisation, SCBS_CM.Commitment.ParentCommitmentID, SCBS_CM.Commitment.PaymentMethod, SCBS_CM.Commitment.RelationshipManager, SCBS_CM.Commitment.ResourceOrder, SCBS_CM.Commitment.Sequence, SCBS_CM.Commitment.SponsorshipAct, SCBS_CM.Commitment.SponsorshipPlus, SCBS_CM.Commitment.SponsorshipType, SCBS_CM.Commitment.StaffMemberName, SCBS_CM.Commitment.StartDate, SCBS_CM.Commitment.Supporter, SCBS_CM.Commitment.SupporterGroup, SCBS_CM.Commitment.Thread, SCBS_CM.Commitment.UpdatedByAccount, SCBS_CM.Commitment.UpdatedTime,SCBS_CM.Commitment.MarketingRegion,SCBS_CM.Commitment.SourceChannel,SCBS_CM.Commitment.CPPActivity FROM SCBS_CM.Commitment";

                    string updatedRecordsFilter = " WHERE SCBS_CM.Commitment.CreatedTime >= '" + fromTime + "' OR SCBS_CM.Commitment.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CM.Commitment.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Commitment records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Commitments");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("CommitmentID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                /*string pattern = @"""?\s*,\s*""?";
                                string[] tokens = System.Text.RegularExpressions.Regex.Split(row, pattern);*/
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                                //Rows.Add(string.Format("({0})", row));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Commitment records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncCommitment - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncNeed(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Need";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_CHILD.Need.ID, SCBS_CHILD.Need.Beneficiary_GlobalID, SCBS_CHILD.Need.Beneficiary_LocalID, SCBS_CHILD.Need.BirthDate, SCBS_CHILD.Need.ContinentName, SCBS_CHILD.Need.CorrespondenceLanguage, SCBS_CHILD.Need.CountryName, SCBS_CHILD.Need.CreatedTime, SCBS_CHILD.Need.EndDate, SCBS_CHILD.Need.Gender, SCBS_CHILD.Need.ICP_ID, SCBS_CHILD.Need.Latitude, SCBS_CHILD.Need.Longitude, SCBS_CHILD.Need.NeedKey, SCBS_CHILD.Need.NeedType, SCBS_CHILD.Need.PlannedCompDateChangeReason, SCBS_CHILD.Need.PlannedCompletionDate, SCBS_CHILD.Need.UpdatedTime,SCBS_CHILD.Need.NeedStatus FROM SCBS_CHILD.Need";


                    string updatedRecordsFilter = " WHERE SCBS_CHILD.Need.CreatedTime >= '" + fromTime + "' OR SCBS_CHILD.Need.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CHILD.Need.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Need records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Need");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("NeedID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                /*string pattern = @"""?\s*,\s*""?";
                                string[] tokens = System.Text.RegularExpressions.Regex.Split(row, pattern);*/
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                                //Rows.Add(string.Format("({0})", row));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            mConnection.Close();
                            appLogger.Info(count + " Need records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncNeed - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncSchedChildDepartures(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SchedChildDepartures";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_CM.SchedChildDepartures.ID,SCBS_CM.SchedChildDepartures.BeneficiaryLifecycleEvent_ID,SCBS_CM.SchedChildDepartures.Beneficiary_GlobalID,SCBS_CM.SchedChildDepartures.DeathCategory,SCBS_CM.SchedChildDepartures.DeathSubcategory,SCBS_CM.SchedChildDepartures.Duplicate,SCBS_CM.SchedChildDepartures.EffectiveDate,SCBS_CM.SchedChildDepartures.FinalLetterSentToSponsor,SCBS_CM.SchedChildDepartures.Need,SCBS_CM.SchedChildDepartures.ReasonForRequest,SCBS_CM.SchedChildDepartures.RecordType,SCBS_CM.SchedChildDepartures.Reinstated,SCBS_CM.SchedChildDepartures.ScheduledDepartureDate,SCBS_CM.SchedChildDepartures.CreatedTime From SCBS_CM.SchedChildDepartures";

                    string updatedRecordsFilter = " WHERE SCBS_CM.SchedChildDepartures.CreatedTime >= '" + fromTime + "' OR SCBS_CM.SchedChildDepartures.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CM.SchedChildDepartures.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SchedChildDepartures records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SchedChildDepartures");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("SchedChildDeparturesID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                /*string pattern = @"""?\s*,\s*""?";
                                string[] tokens = System.Text.RegularExpressions.Regex.Split(row, pattern);*/
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                                //Rows.Add(string.Format("({0})", row));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SchedChildDepartures records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SchedChildDepartures - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncS2BCommunication(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get S2BCommunication";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_SUP.S2BCommunication.ID,SCBS_SUP.S2BCommunication.BatchID,SCBS_SUP.S2BCommunication.CommKitID,SCBS_SUP.S2BCommunication.CommunicationUpdateReceived,SCBS_SUP.S2BCommunication.CreatedByAccount,SCBS_SUP.S2BCommunication.CreatedTime,SCBS_SUP.S2BCommunication.DateProcessed,SCBS_SUP.S2BCommunication.DateScanned,SCBS_SUP.S2BCommunication.DocumentType,SCBS_SUP.S2BCommunication.FileURL,SCBS_SUP.S2BCommunication.ItemNotScannedEligable,SCBS_SUP.S2BCommunication.ItemNotScannedNotEligable,SCBS_SUP.S2BCommunication.MarkedForRework,SCBS_SUP.S2BCommunication.Need,SCBS_SUP.S2BCommunication.ReasonForRework,SCBS_SUP.S2BCommunication.ReworkComments,SCBS_SUP.S2BCommunication.SBCGlobalStatus,SCBS_SUP.S2BCommunication.Source,SCBS_SUP.S2BCommunication.SourceID,SCBS_SUP.S2BCommunication.SupporterGroup,SCBS_SUP.S2BCommunication.Type,SCBS_SUP.S2BCommunication.UpdatedByAccount,SCBS_SUP.S2BCommunication.UpdatedTime,SCBS_SUP.S2BCommunication.TemplateID,SCBS_SUP.S2BCommunication.TemplateName FROM SCBS_SUP.S2BCommunication";

                    string updatedRecordsFilter = " WHERE SCBS_SUP.S2BCommunication.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.S2BCommunication.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.S2BCommunication.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " S2BCommunication records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO S2BCommunication");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("S2BCommunicationID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                /*string pattern = @"""?\s*,\s*""?";
                                string[] tokens = System.Text.RegularExpressions.Regex.Split(row, pattern);*/
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                                //Rows.Add(string.Format("({0})", row));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }

                            appLogger.Info(count + " S2BCommunication records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in S2BCommunication - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncCorrespondence(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Correspondence";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_SUP.Correspondence.ID,SCBS_SUP.Correspondence.CommKitID,SCBS_SUP.Correspondence.ConnectCreateStatus,SCBS_SUP.Correspondence.CreatedByAccount,SCBS_SUP.Correspondence.CreatedTime,SCBS_SUP.Correspondence.DateScanned,SCBS_SUP.Correspondence.DigitalLetterStatus,SCBS_SUP.Correspondence.Duplicate,SCBS_SUP.Correspondence.HoldLetter,SCBS_SUP.Correspondence.HoldLetterReason,SCBS_SUP.Correspondence.InternalLetterSent,SCBS_SUP.Correspondence.IsFinalLetter,SCBS_SUP.Correspondence.IsFinalLetterArchived,SCBS_SUP.Correspondence.IsOriginalLetterArchived,SCBS_SUP.Correspondence.IsOriginalMailed,SCBS_SUP.Correspondence.ItemNotScannedEligible,SCBS_SUP.Correspondence.ItemNotScannedNotEligible,SCBS_SUP.Correspondence.KingslineBatch,SCBS_SUP.Correspondence.KingslineDespatched,SCBS_SUP.Correspondence.KingslineDigitalLetterReceived,SCBS_SUP.Correspondence.KingslineDigitalURN,SCBS_SUP.Correspondence.KingslineLetterReceived,SCBS_SUP.Correspondence.KingslineScanned,SCBS_SUP.Correspondence.KingslineURN,SCBS_SUP.Correspondence.KingslineUpdated,SCBS_SUP.Correspondence.MarkedForRework,SCBS_SUP.Correspondence.Need,SCBS_SUP.Correspondence.NoAlertEmail,SCBS_SUP.Correspondence.NumberOfPages,SCBS_SUP.Correspondence.OldVersion,SCBS_SUP.Correspondence.OriginalLanguage,SCBS_SUP.Correspondence.PhysicalLetterStatus,SCBS_SUP.Correspondence.SBCTypes,SCBS_SUP.Correspondence.SupporterGroup,SCBS_SUP.Correspondence.Template,SCBS_SUP.Correspondence.TranslatedBy,SCBS_SUP.Correspondence.TranslationComplexity,SCBS_SUP.Correspondence.TranslationLanguage,SCBS_SUP.Correspondence.Type,SCBS_SUP.Correspondence.UpdatedByAccount,SCBS_SUP.Correspondence.UpdatedTime,SCBS_SUP.Correspondence.WrittenOnBeneficiaryBehalf FROM SCBS_SUP.Correspondence";

                    string updatedRecordsFilter = " WHERE SCBS_SUP.Correspondence.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.Correspondence.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.Correspondence.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Correspondence records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Correspondence");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("CorrespondenceID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                /*string pattern = @"""?\s*,\s*""?";
                                string[] tokens = System.Text.RegularExpressions.Regex.Split(row, pattern);*/
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                                //Rows.Add(string.Format("({0})", row));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Correspondence records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncCorrespondence - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncSupporterEnquiries(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            string updatingStartPoint = null;
            string updatingEndPoint = null;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SupporterEnquiries";

                do
                {
                    count = 0;

                    string Query = "SELECT Incident.ID,Incident.Queue,Incident.Mailbox,Incident.Category,Incident.Disposition,Incident.CustomFields.c.ContactAttemptsMade,Incident.CustomFields.c.FeedbackRating,Incident.CreatedTime,Incident.UpdatedTime,Incident.ClosedTime,Incident.CreatedByAccount,Incident.CustomFields.c.CampaignFormType,Incident.CustomFields.c.due_date as ResolutionDueDate,Incident.Product as MinistryAspect,Incident.CustomFields.c.sourcechannel,Incident.CustomFields.c.source,Incident.source.level1 as source_lvl1,Incident.source.id as source_lvl2,Incident.CustomFields.c.whitemail as whitemail,Incident.PrimaryContact.contact as PrimaryContact,Incident.assignedTo.account as AssignedTo,Incident.statusWithType.status.id as Status_id,Incident.statusWithType.status.lookupName as Status,Incident.CustomFields.c.InternalSubject,Incident.lookupname as Reference,Incident.customFields.c.sponsorchildref as ChildReference,Incident.customFields.c.rcworkitem as RingCentralWorkItem,Incident.customFields.c.rcworkitemskill as RingCentralWorkItemSkill,Incident.customFields.c.rcworkitemstatus as RingCentralWorkItemStatus,Incident.customFields.c.rcworkitemid as RingCentralWorkItemID,Incident.customFields.c.rcoriginalenquiryref as RingCentralOriginalEnquiryReference,Incident.customFields.c.rcenquiryiteration as RingCentralEnquiryIteration,Incident.statusWithType.statusType.id as StatusType,Incident.customFields.c.resolvestarted as ResolveStarted,Incident.customFields.c.resolveended as ResolveEnded,Incident.customFields.c.setupstarted as SetupStarted,Incident.customFields.c.setupended as SetupEnded,Incident.customFields.c.supportertype as SupporterType,Incident.customFields.c.needid as NeedID FROM Incident";

                    string updatedRecordsFilter = " WHERE Incident.CreatedTime >= '" + fromTime + "' OR Incident.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Incident.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SupporterEnquiries records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SupporterEnquiries");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("IncidentID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string IncidentID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string Queue = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Mailbox = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string Category = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string Disposition = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string ContactAttemptsMade = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string FeedbackRating = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string CreatedTime = "";//7
                                string UpdatedTime = "";//8
                                string ClosedTime = "";//9
                                string CreatedByAccount = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string CampaignFormType = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string ResolutionDueDate = !string.IsNullOrEmpty(values[12]) ? GetDateFromDateTime(values[12]) : "null";
                                string MinistryAspect = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string sourcechannel = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string source = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string source_lvl1 = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string source_lvl2 = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string whitemail = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string PrimaryContact = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string AssignedTo = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string Status_id = string.IsNullOrEmpty(values[21]) ? "null" : values[21];
                                string Status = string.IsNullOrEmpty(values[22]) ? "null" : values[22];
                                string InternalSubject = string.IsNullOrEmpty(values[23]) ? "null" : values[23].Replace(",", "");


                                string Reference = string.IsNullOrEmpty(values[24]) ? "null" : values[24];
                                string ChildReference = string.IsNullOrEmpty(values[25]) ? "null" : values[25];
                                string RingCentralWorkItem = string.IsNullOrEmpty(values[26]) ? "null" : values[26];
                                string RingCentralWorkItemSkill = string.IsNullOrEmpty(values[27]) ? "null" : values[27];
                                string RingCentralWorkItemStatus = string.IsNullOrEmpty(values[28]) ? "null" : values[28];
                                string RingCentralWorkItemID = string.IsNullOrEmpty(values[29]) ? "null" : values[29];
                                string RingCentralOriginalEnquiryReference = string.IsNullOrEmpty(values[30]) ? "null" : values[30];
                                string RingCentralEnquiryIteration = string.IsNullOrEmpty(values[31]) ? "null" : values[31];
                                string StatusType = string.IsNullOrEmpty(values[32]) ? "null" : values[32];
                                string ResolveStarted = "";
                                string ResolveEnded = "";
                                string SetupStarted = "";
                                string SetupEnded = "";
                                string SupporterType = string.IsNullOrEmpty(values[37]) ? "null" : values[37];
                                string NeedID = string.IsNullOrEmpty(values[38]) ? "null" : values[38];

                                if (!string.IsNullOrEmpty(values[33]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[33], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ResolveStarted = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ResolveStarted = "null";
                                }

                                if (!string.IsNullOrEmpty(values[34]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[34], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ResolveEnded = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ResolveEnded = "null";
                                }

                                if (!string.IsNullOrEmpty(values[35]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[35], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    SetupStarted = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    SetupStarted = "null";
                                }

                                if (!string.IsNullOrEmpty(values[36]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[36], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    SetupEnded = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    SetupEnded = "null";
                                }


                                if (!string.IsNullOrEmpty(values[7]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[7], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[8]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[8], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[9]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[9], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ClosedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ClosedTime = "null";
                                }

                                string singleRow = IncidentID + ",'" + Queue + "','" + Mailbox + "','" + Category + "','" + Disposition + "','" + ContactAttemptsMade + "','" + FeedbackRating + "','" + CreatedTime + "','" + UpdatedTime + "','" + ClosedTime + "','" + CreatedByAccount + "','" + CampaignFormType + "','" + ResolutionDueDate + "','" + MinistryAspect + "','" + sourcechannel + "','" + source + "','" + source_lvl1 + "','" + source_lvl2 + "','" + whitemail + "','" + PrimaryContact + "','" + AssignedTo + "','" + Status_id + "','" + Status + "','" + InternalSubject.Replace("'", "") + "','" + Reference + "','" + ChildReference + "'," + RingCentralWorkItem + "," + RingCentralWorkItemSkill + "," + RingCentralWorkItemStatus + ",'" + RingCentralWorkItemID + "','" + RingCentralOriginalEnquiryReference + "'," + RingCentralEnquiryIteration + "," + StatusType + ",'" + ResolveStarted + "','" + ResolveEnded + "','" + SetupStarted + "','" + SetupEnded + "'," + SupporterType + "," + NeedID;
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            updatingStartPoint = getDateTimeWhichProcessUpdatingInDB(Rows[0]);
                            updatingEndPoint = getDateTimeWhichProcessUpdatingInDB(Rows[count - 1]);
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");
                            sCommand.Replace("''s", "s");
                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SupporterEnquiries records updated successfully Between " + updatingStartPoint + " to " + updatingEndPoint);
                            updatingStartPoint = null;
                            updatingEndPoint = null;
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SupporterEnquiries - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SolicitCodes(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SolicitCodes ";

                do
                {
                    count = 0;

                    string Query = "select SCBS_SUP.SolicitCodes.ID, SCBS_SUP.SolicitCodes.CreatedTime, SCBS_SUP.SolicitCodes.CreatedByAccount, SCBS_SUP.SolicitCodes.UpdatedTime, SCBS_SUP.SolicitCodes.UpdatedByAccount, SCBS_SUP.SolicitCodes.ChannelEmail, SCBS_SUP.SolicitCodes.ChannelPhone, SCBS_SUP.SolicitCodes.ChannelPost, SCBS_SUP.SolicitCodes.ChannelSMS, SCBS_SUP.SolicitCodes.SCSubType, SCBS_SUP.SolicitCodes.SCType, SCBS_SUP.SolicitCodes.SolicitCode, SCBS_SUP.SolicitCodes.Source, SCBS_SUP.SolicitCodes.StartDate, SCBS_SUP.SolicitCodes.EndDate, SCBS_SUP.SolicitCodes.Supporter,SCBS_SUP.SolicitCodes.Parameter from SCBS_SUP.SolicitCodes";

                    string updatedRecordsFilter = " where SCBS_SUP.SolicitCodes.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.SolicitCodes.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.SolicitCodes.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SolicitCodes records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SolicitCodes");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("SolicitCodesID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SolicitCodes records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SolicitCodes - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncCampaigns(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Campaigns ";

                do
                {
                    count = 0;

                    string Query = "select SCBS_COMMON.Campaigns.ID, SCBS_COMMON.Campaigns.UpdatedByAccount, SCBS_COMMON.Campaigns.UpdatedTime, SCBS_COMMON.Campaigns.CreatedByAccount, SCBS_COMMON.Campaigns.CreatedTime, SCBS_COMMON.Campaigns.Campaign, SCBS_COMMON.Campaigns.LegacyCampaign, SCBS_COMMON.Campaigns.Region, SCBS_COMMON.Campaigns.Manager, SCBS_COMMON.Campaigns.Channel, SCBS_COMMON.Campaigns.AssignByPostcode, SCBS_COMMON.Campaigns.AssignByEvent, SCBS_COMMON.Campaigns.Event, SCBS_COMMON.Campaigns.Organisation, SCBS_COMMON.Campaigns.Active, SCBS_COMMON.Campaigns.CompassionExperience,SCBS_COMMON.Campaigns.CPPActivity from SCBS_COMMON.Campaigns";

                    string updatedRecordsFilter = " where SCBS_COMMON.Campaigns.CreatedTime >= '" + fromTime + "' OR SCBS_COMMON.Campaigns.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_COMMON.Campaigns.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Campaigns records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Campaigns");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("CampaignsID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Campaigns records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Campaigns - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncSupporterGroup(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SupporterGroup";

                do
                {
                    count = 0;

                    string Query = "select SCBS_SUP.SupporterGroup.ID, SCBS_SUP.SupporterGroup.StartDate, SCBS_SUP.SupporterGroup.Status, SCBS_SUP.SupporterGroup.CompassConID, SCBS_SUP.SupporterGroup.CompassionOffice, SCBS_SUP.SupporterGroup.CreatedTime, SCBS_SUP.SupporterGroup.UpdatedTime, SCBS_SUP.SupporterGroup.Type, SCBS_SUP.SupporterGroup.CreatedByAccount, SCBS_SUP.SupporterGroup.UpdatedByAccount, SCBS_SUP.SupporterGroup.PrioritySupporterId, SCBS_SUP.SupporterGroup.BlackBaudConstituentId, SCBS_SUP.SupporterGroup.GlobalID,SCBS_SUP.SupporterGroup.SubChildren, SCBS_SUP.SupporterGroup.SubPreferences,SCBS_SUP.SupporterGroup.DigitalChoice from SCBS_SUP.SupporterGroup";

                    string updatedRecordsFilter = " WHERE SCBS_SUP.SupporterGroup.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.SupporterGroup.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.SupporterGroup.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SupporterGroup records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SupporterGroup");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("SupporterGroupID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SupporterGroup records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncSupporterGroup - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncSupporterGroupLinks(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SupporterGroupLinks";

                do
                {
                    count = 0;

                    string Query = "select SCBS_SUP.SupporterGroupLinks.ID, SCBS_SUP.SupporterGroupLinks.OrganisationID, SCBS_SUP.SupporterGroupLinks.SupporterGroupID, SCBS_SUP.SupporterGroupLinks.SupporterID, SCBS_SUP.SupporterGroupLinks.Active, SCBS_SUP.SupporterGroupLinks.CreatedTime, SCBS_SUP.SupporterGroupLinks.CreatedByAccount, SCBS_SUP.SupporterGroupLinks.UpdatedTime, SCBS_SUP.SupporterGroupLinks.UpdatedByAccount, SCBS_SUP.SupporterGroupLinks.SupporterGroupHistory from SCBS_SUP.SupporterGroupLinks";

                    string updatedRecordsFilter = " where SCBS_SUP.SupporterGroupLinks.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.SupporterGroupLinks.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.SupporterGroupLinks.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SupporterGroupLinks records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SupporterGroupLinks");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("SupporterGroupLinksID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SupporterGroupLinks records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncSupporterGroupLinks - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncThreads(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Threads";

                do
                {
                    count = 0;

                    string Query = "SELECT Incident.threads.account as Account,Incident.threads.channel Channel,Incident.threads.contact as Contact,Incident.threads.createdTime as CreatedTime,Incident.threads.displayOrder as DisplayOrder,Incident.threads.entryType as EntryType,Incident.threads.id as ThreadID,Incident.ID as IncidentID FROM Incident";

                    string updatedRecordsFilter = " where Incident.CreatedTime >= '" + fromTime + "' OR Incident.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY Incident.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Thread records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Threads");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Threads records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Threads - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncEvents(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Events ";

                do
                {
                    count = 0;

                    string Query = "select opportunity.ID, opportunity.Name, opportunity.Organization,opportunity.CustomFields.c.eventstatus, opportunity.CustomFields.c.eventstart, opportunity.CustomFields.c.eventend,opportunity.AssignedToAccount,opportunity.CustomFields.c.budgetholder,opportunity.Territory,opportunity.UpdatedTime,opportunity.CreatedTime,opportunity.CustomFields.standard.Region,opportunity.Customfields.SCBS_COMMON.Manager.name as Manager,opportunity.customFields.Standard.ExpectedSponsorships,opportunity.customFields.Standard.ReportedSponsorships ,opportunity.customFields.c.agreement_expectedaudiencesize,opportunity.customFields.c.feedback_audiencesize,opportunity.customFields.c.feedback_whatworked,opportunity.customFields.c.feedback_whatdidnotwork,opportunity.customFields.Standard.TourEvent,opportunity.customFields.Standard.ProfileExpiry,opportunity.customFields.Standard.TotalSponsorships,opportunity.customFields.Standard.NumProfilesRequired,opportunity.customFields.Standard.ChildrenOnHoldFrom,opportunity.customFields.Standard.OrderOn,opportunity.customFields.Standard.WebSMSCampaignStart,opportunity.customFields.Standard.WebSMSCampaignEnd,opportunity.customFields.Standard.WebSMSProfilesRequired,opportunity.PrimaryContact.contact as Supporter,opportunity.StageWithStrategy.stage as StageWithStrategy from opportunity";

                    string updatedRecordsFilter = " where opportunity.CreatedTime >= '" + fromTime + "' OR opportunity.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY opportunity.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Events ...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Events ");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                {
                                    Columns.Add("EventID");
                                }
                                else if (data == "eventstatus")
                                {
                                    Columns.Add("Status");
                                }
                                else if (data == "eventstart")
                                {
                                    Columns.Add("EventStart");
                                }
                                else if (data == "eventend")
                                {
                                    Columns.Add("EventEnd");
                                }
                                else if (data == "AssignedToAccount")
                                {
                                    Columns.Add("Assigned");
                                }
                                else if (data == "budgetholder")
                                {
                                    Columns.Add("BudgetHolder");
                                }
                                else
                                {
                                    Columns.Add(data);
                                }
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Events records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Events - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncOrganisations(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Organisations ";

                do
                {
                    count = 0;

                    string Query = "select organization.ID,organization.Name,organization.CustomFields.c.denomination,organization.CustomFields.c.relationship_status,organization.CustomFields.c.church_size,organization.CustomFields.standard.RegionalManager,organization.CustomFields.c.status,organization.CustomFields.c.country,organization.CustomFields.c.orgtype,organization.CustomFields.c.organisationtype as OrganisationType,organization.CustomFields.c.cptype,organization.CustomFields.c.postcodearea,organization.CustomFields.c.add_mailingcountrydrop,organization.CreatedTime, organization.UpdatedTime,organization.CustomFields.standard.Region,organization.CustomFields.c.add_postal_code from organization";

                    string updatedRecordsFilter = " where organization.CreatedTime >= '" + fromTime + "' OR organization.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY organization.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " organization ...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Organisation ");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                {
                                    Columns.Add("OrganisationID");
                                }
                                else if (data == "denomination")
                                {
                                    Columns.Add("Denomination");
                                }
                                else if (data == "relationship_status")
                                {
                                    Columns.Add("RelationshipStatus");
                                }
                                else if (data == "church_size")
                                {
                                    Columns.Add("ChurchSize");
                                }
                                else if (data == "RegionalManager")
                                {
                                    Columns.Add("Manager");
                                }
                                else if (data == "status")
                                {
                                    Columns.Add("Status");
                                }
                                else if (data == "country")
                                {
                                    Columns.Add("Country");
                                }
                                else if (data == "orgtype")
                                {
                                    Columns.Add("OrgType");
                                }
                                else if (data == "cptype")
                                {
                                    Columns.Add("CPType");
                                }
                                else if (data == "postcodearea")
                                {
                                    Columns.Add("PostcodeArea");
                                }
                                else if (data == "add_mailingcountrydrop")
                                {
                                    Columns.Add("MailingCountry");
                                }
                                else
                                {
                                    Columns.Add(data);
                                }

                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Organisations records updated successfully");
                        }

                        // For next set of data
                        Total += count;

                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Organisations - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncSupporters(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Supporters ";

                do
                {
                    count = 0;

                    string Query = "select contact.ID,contact.CustomFields.c.advocate_status,contact.CustomFields.c.country, contact.CustomFields.c.appfirstloggedin, contact.CustomFields.c.applastloggedin, contact.CustomFields.c.appfirstloggedin_android, contact.CustomFields.c.applastloggedin_android, contact.CustomFields.c.postcode_area,contact.CustomFields.c.postcode_district, contact.CustomFields.c.add_mailingcountrydrop, contact.CreatedTime, contact.UpdatedTime,contact.CustomFields.c.status,contact.CustomFields.c.digitalchoice,contact.CustomFields.c.blackbaudid from contact";

                    string updatedRecordsFilter = " where contact.CreatedTime >= '" + fromTime + "' OR contact.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY contact.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Supporters  records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Supporters ");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                {
                                    Columns.Add("SupporterID");
                                }
                                else if (data == "country")
                                {
                                    Columns.Add("Compassion_office");
                                }
                                else if (data == "add_mailingcountrydrop")
                                {
                                    Columns.Add("Country");
                                }
                                else
                                {
                                    Columns.Add(data);
                                }

                            }

                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Supporters records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Supporters - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncEventTourDate(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get MiscEventTourDate";

                do
                {
                    count = 0;

                    string Query = "SELECT Misc.EventTourDate.ID,Misc.EventTourDate.ApproximateAudience,Misc.EventTourDate.Attendance,Misc.EventTourDate.CreatedByAccount,Misc.EventTourDate.CreatedTime,Misc.EventTourDate.Date,Misc.EventTourDate.Event,Misc.EventTourDate.ExpectedSponsorships,Misc.EventTourDate.NumberMonies,Misc.EventTourDate.NumberNoMonies,Misc.EventTourDate.NumberOfProfiles,Misc.EventTourDate.ProfileExpiryDate,Misc.EventTourDate.ReportedSponsorships,Misc.EventTourDate.SourceCode,Misc.EventTourDate.Status,Misc.EventTourDate.UpdatedTime,Misc.EventTourDate.VenueAddress from Misc.EventTourDate";

                    string updatedRecordsFilter = " WHERE Misc.EventTourDate.CreatedTime >= '" + fromTime + "' OR Misc.EventTourDate.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Misc.EventTourDate.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    // //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " MiscEventTourDate records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO EventTourDate");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("EventTourDateID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();

                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string singleRow = "";
                                List<string> values = row.Split('^').ToList();
                                string EventTourDateID = values[0];
                                string ApproximateAudience = string.IsNullOrEmpty(values[1]) ? "0" : values[1];
                                string Attendance = string.IsNullOrEmpty(values[2]) ? "0" : values[2];
                                string CreatedByAccount = values[3];
                                string CreatedTime = string.IsNullOrEmpty(values[4]) ? "" : Convert.ToDateTime(values[4]).ToString("yyyy-MM-dd hh:mm:ss");
                                string Date = values[5];
                                string Event = values[6];
                                string ExpectedSponsorships = string.IsNullOrEmpty(values[7]) ? "0" : values[7];
                                string NumberMonies = string.IsNullOrEmpty(values[8]) ? "0" : values[8];
                                string NumberNoMonies = string.IsNullOrEmpty(values[9]) ? "0" : values[9];
                                string NumberOfProfiles = string.IsNullOrEmpty(values[10]) ? "0" : values[10];
                                string ProfileExpiryDate = values[11];
                                string ReportedSponsorships = string.IsNullOrEmpty(values[12]) ? "0" : values[12];
                                string SourceCode = values[13];
                                string Status = values[14];
                                string UpdatedTime = string.IsNullOrEmpty(values[15]) ? "" : Convert.ToDateTime(values[15]).ToString("yyyy-MM-dd hh:mm:ss");
                                string VenueAddress = string.IsNullOrEmpty(values[16]) ? "" : values[16].Replace("'", "");


                                singleRow = EventTourDateID + "," + ApproximateAudience + "," + Attendance + ",'" + CreatedByAccount + "','" + CreatedTime + "','" + Date + "','" + Event + "'," + ExpectedSponsorships + "," + NumberMonies + "," + NumberNoMonies + "," + NumberOfProfiles + ",'" + ProfileExpiryDate + "'," + ReportedSponsorships + ",'" + SourceCode + "','" + Status + "','" + UpdatedTime + "','" + VenueAddress + "'";

                                //Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " MiscEventTourDate records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncMiscEventTourDate - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncNotifications()
        {
            appLogger.Info(" Updating Notifications records ....");
            int SupCount = 0;
            // int SupCount = GetTotalCountOfNotifications();

            int lastID = GetLastIDNotifications();

            SupCount = GetLastIDtificationsCount(lastID);

            StringBuilder sCommand = new StringBuilder();
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            //int startlimit = 0;
            if (SupCount != 0)
            {
                //sCommand = GetInsertStringNotifications(startlimit);
                sCommand = GetInsertStringNotificationsNew(lastID);//overnyte
                                                                   //MySql.Data.MySqlClient.MySqlConnection conn;
                string Query = "";
                long recordCount = 0;
                try
                {

                    conn.ConnectionString = myConnectionString;// myConnectionString;
                    conn.Open();
                    appLogger.Info("Total " + SupCount + " Notifications records For Update");

                    Query = "INSERT INTO `Notifications`(`SUPPORTER_ID`, `SUPPORTER_NAME`, `CHILD_NAME`, `NEEDKEY`, `CHILD_IMAGE`, `MESSAGE_TITLE`, `MESSAGE_BODY`, `MESSAGE_TYPE`, `DESTINATION`, `POST_TITLE`, `SEND_NOTIFICATION`, `HERO`, `STATUS`, `DISPLAY_ORDER`, `IS_READ`, `CREATED_ON`, `UPDATED_ON`, `OA_ID`, `OA_BRAND_ID`, `USER_ID`, `CREATED_BY`, `UPDATED_BY`, `IS_DELETED`) VALUES " + sCommand + ";";//Message = 


                    MySqlCommand insertCommand = new MySqlCommand(Query, conn);
                    recordCount = insertCommand.ExecuteNonQuery();
                    appLogger.Info(recordCount + " Notifications records updated successfully");
                    conn.Close();
                    //SupCount -= 1000;
                    //startlimit += 1000;
                }
                catch (Exception e)
                {
                    conn.Close();
                    appLogger.Info("Error in inserting Notifications record in DB : " + e.Message);
                    appLogger.Info(e.Message);
                    appLogger.Info(e.InnerException);
                    appLogger.Info(e.StackTrace);
                }
                finally
                {
                    conn.Close();
                }

            }
            appLogger.Info(" Notifications updated successfully");
            return SupCount;
        }

        public int SyncBeneficiaryOrICPHold(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get BeneficiaryOrICPHold";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_CHILD.BeneficiaryOrICPHold.ID,SCBS_CHILD.BeneficiaryOrICPHold.BeneficiaryReinstatementReason,SCBS_CHILD.BeneficiaryOrICPHold.Beneficiary_GlobalID,SCBS_CHILD.BeneficiaryOrICPHold.Beneficiary_LocalID,SCBS_CHILD.BeneficiaryOrICPHold.Channel_Name,SCBS_CHILD.BeneficiaryOrICPHold.CreatedByAccount,SCBS_CHILD.BeneficiaryOrICPHold.CreatedTime,SCBS_CHILD.BeneficiaryOrICPHold.EstimatedNoMoneyYieldRate,SCBS_CHILD.BeneficiaryOrICPHold.HoldChannel,SCBS_CHILD.BeneficiaryOrICPHold.HoldEndDate,SCBS_CHILD.BeneficiaryOrICPHold.HoldID,SCBS_CHILD.BeneficiaryOrICPHold.HoldStatus,SCBS_CHILD.BeneficiaryOrICPHold.HoldType,SCBS_CHILD.BeneficiaryOrICPHold.NeedRNID,SCBS_CHILD.BeneficiaryOrICPHold.NotificationReason,SCBS_CHILD.BeneficiaryOrICPHold.PrimaryHoldOwner,SCBS_CHILD.BeneficiaryOrICPHold.ProcessEnquiry,SCBS_CHILD.BeneficiaryOrICPHold.ProjectRNID,SCBS_CHILD.BeneficiaryOrICPHold.ReleasedDate,SCBS_CHILD.BeneficiaryOrICPHold.ReservationID,SCBS_CHILD.BeneficiaryOrICPHold.ReservationType_Name,SCBS_CHILD.BeneficiaryOrICPHold.Reservation_RNID,SCBS_CHILD.BeneficiaryOrICPHold.ResourceOrder,SCBS_CHILD.BeneficiaryOrICPHold.SecondaryHoldOwner,SCBS_CHILD.BeneficiaryOrICPHold.SourceCode,SCBS_CHILD.BeneficiaryOrICPHold.SupporterGroupRNID,SCBS_CHILD.BeneficiaryOrICPHold.UpdatedByAccount,SCBS_CHILD.BeneficiaryOrICPHold.UpdatedTime FROM SCBS_CHILD.BeneficiaryOrICPHold";

                    string updatedRecordsFilter = " WHERE SCBS_CHILD.BeneficiaryOrICPHold.CreatedTime >= '" + fromTime + "' OR SCBS_CHILD.BeneficiaryOrICPHold.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CHILD.BeneficiaryOrICPHold.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " BeneficiaryOrICPHold records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO BeneficiaryOrICPHold");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("BeneficiaryOrICPHold_ID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " BeneficiaryOrICPHold records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncBeneficiaryOrICPHold - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncCPPathwayActivity(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get CPPathway Activity";

                do
                {
                    count = 0;

                    string Query = "SELECT CPPathway.Activity.ID,CPPathway.Activity.AVAvailable,CPPathway.Activity.ActivityChurchContact,CPPathway.Activity.ActivityDate,CPPathway.Activity.ActivityDateBooked,CPPathway.Activity.ActivityLeader,CPPathway.Activity.ActivityNotes,CPPathway.Activity.ActivityType,CPPathway.Activity.ActivityVolunteer,CPPathway.Activity.ActivityYear,CPPathway.Activity.Activity_Assigned,CPPathway.Activity.Activity_ExpectedSponsorships,CPPathway.Activity.AdHocFollowUpDateCompleted,CPPathway.Activity.AdHocFollowUpDateDue,CPPathway.Activity.AdHocFollowUp_Assigned,CPPathway.Activity.AdHocFollowUp_ChchContact,CPPathway.Activity.AdHocFollowUp_Notes,CPPathway.Activity.AdHocFollowUp_Reminder,CPPathway.Activity.AutomateScheduledFollowUp,CPPathway.Activity.CExpBangtailsRequired,CPPathway.Activity.CExpChildRequirements,CPPathway.Activity.CExpConfirmed,CPPathway.Activity.CExpPendingApproval,CPPathway.Activity.CExpProfilesRequired,CPPathway.Activity.CExpSourceCode,CPPathway.Activity.ChildRequirements,CPPathway.Activity.ChildRequirementsNew,CPPathway.Activity.ChildrenOnHoldFrom,CPPathway.Activity.CompassionExperience,CPPathway.Activity.ContactEmail,CPPathway.Activity.CreatedByAccount,CPPathway.Activity.CreatedTime,CPPathway.Activity.NumBangtailsRequired,CPPathway.Activity.NumLeafletsRequired,CPPathway.Activity.NumProfilesRequired,CPPathway.Activity.OrderProfilesOn,CPPathway.Activity.Organisation,CPPathway.Activity.Outcome,CPPathway.Activity.PostActivity_Attendance,CPPathway.Activity.PostActivity_Comments,CPPathway.Activity.PostActivity_Monies,CPPathway.Activity.PostActivity_NoMonies,CPPathway.Activity.PostActivity_TotalSponsorships,CPPathway.Activity.PresentationRequired,CPPathway.Activity.PresentationSupplied,CPPathway.Activity.PresentationTime,CPPathway.Activity.PresentationType,CPPathway.Activity.ProfileExpiry,CPPathway.Activity.ScheduledFollowUp1_Assigned,CPPathway.Activity.ScheduledFollowUp1_ChchContact,CPPathway.Activity.ScheduledFollowUp1_Completed,CPPathway.Activity.ScheduledFollowUp1_Notes,CPPathway.Activity.ScheduledFollowUp1_Reminder,CPPathway.Activity.ScheduledFollowUp2_Assigned,CPPathway.Activity.ScheduledFollowUp2_ChchContact,CPPathway.Activity.ScheduledFollowUp2_Completed,CPPathway.Activity.ScheduledFollowUp2_Notes,CPPathway.Activity.ScheduledFollowUp2_Reminder,CPPathway.Activity.ScheduledFollowUp3_Assigned,CPPathway.Activity.ScheduledFollowUp3_ChchContact,CPPathway.Activity.ScheduledFollowUp3_Completed,CPPathway.Activity.ScheduledFollowUp3_Notes,CPPathway.Activity.ScheduledFollowUp3_Reminder,CPPathway.Activity.ScheduledFollowUp4_Assigned,CPPathway.Activity.ScheduledFollowUp4_ChchContact,CPPathway.Activity.ScheduledFollowUp4_Completed,CPPathway.Activity.ScheduledFollowUp4_Notes,CPPathway.Activity.ScheduledFollowUp4_Reminder,CPPathway.Activity.ServiceTime,CPPathway.Activity.SourceCode,CPPathway.Activity.UpdatedByAccount,CPPathway.Activity.UpdatedTime,CPPathway.Activity.VolunteerHours,CPPathway.Activity.WebSMSCampaignStart,CPPathway.Activity.WebSMSCampaignEnd,CPPathway.Activity.WebSMSRequired as WebSMSProfilesRequired FROM CPPathway.Activity ";

                    string updatedRecordsFilter = " WHERE CPPathway.Activity.CreatedTime >= '" + fromTime + "' OR CPPathway.Activity.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY CPPathway.Activity.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    // //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "∧", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " CPPathwayActivity records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO CPPathwayActivity");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('∧'))
                            {
                                if (data == "ID")
                                    Columns.Add("ActivityID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('∧').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " CPPathwayActivity records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in sync CPPathwayActivity - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncResourceOrder(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get ResourceOrder";

                do
                {
                    count = 0;

                    string Query = "select SCBS_RM.ResourceOrder.ID,SCBS_RM.ResourceOrder.AmountRaised,SCBS_RM.ResourceOrder.AssignedTo,SCBS_RM.ResourceOrder.BudgetHolder,SCBS_RM.ResourceOrder.CPPActivity,SCBS_RM.ResourceOrder.CampaignCode,SCBS_RM.ResourceOrder.CompassionExperience,SCBS_RM.ResourceOrder.ConId,SCBS_RM.ResourceOrder.Country,SCBS_RM.ResourceOrder.CourierDeliveryStatus,SCBS_RM.ResourceOrder.CourierPodDateTime,SCBS_RM.ResourceOrder.CourierPodSignature,SCBS_RM.ResourceOrder.CourierScheduledDelivery,SCBS_RM.ResourceOrder.CreatedByAccount,SCBS_RM.ResourceOrder.CreatedTime,SCBS_RM.ResourceOrder.DespatchedForExternalDelivery,SCBS_RM.ResourceOrder.DespatchedForInternalDelivery,SCBS_RM.ResourceOrder.DisableAutoEmail,SCBS_RM.ResourceOrder.Enquiry,SCBS_RM.ResourceOrder.Event,SCBS_RM.ResourceOrder.EventCompleted,SCBS_RM.ResourceOrder.EventDate,SCBS_RM.ResourceOrder.Event_Manager,SCBS_RM.ResourceOrder.Event_Region,SCBS_RM.ResourceOrder.ExtReqDeliveryDate,SCBS_RM.ResourceOrder.ExtReqDeliveryTime,SCBS_RM.ResourceOrder.ExternalActDeliveryMethod,SCBS_RM.ResourceOrder.ExternalDelivered,SCBS_RM.ResourceOrder.ExternalDeliveryStatus,SCBS_RM.ResourceOrder.ExternalReqDeliveryMethod,SCBS_RM.ResourceOrder.ExternalTrackingNumber,SCBS_RM.ResourceOrder.ExtraValues,SCBS_RM.ResourceOrder.HoursTaken,SCBS_RM.ResourceOrder.Import_check,SCBS_RM.ResourceOrder.IntReqDeliveryDate,SCBS_RM.ResourceOrder.InternalActDeliveryMethod,SCBS_RM.ResourceOrder.InternalDelivered,SCBS_RM.ResourceOrder.InternalDeliveryStatus,SCBS_RM.ResourceOrder.InternalReqDeliveryMethod,SCBS_RM.ResourceOrder.InternalStatus,SCBS_RM.ResourceOrder.InternalTracking,SCBS_RM.ResourceOrder.InternalTrackingNumber,SCBS_RM.ResourceOrder.KinglineCreated,SCBS_RM.ResourceOrder.KingslineBookingDate,SCBS_RM.ResourceOrder.KingslineDeliveryType,SCBS_RM.ResourceOrder.KingslineFulfilled,SCBS_RM.ResourceOrder.KingslineID,SCBS_RM.ResourceOrder.KingslineReceived,SCBS_RM.ResourceOrder.KingslineTracking,SCBS_RM.ResourceOrder.MarketingCampaign,SCBS_RM.ResourceOrder.MarketingChannel,SCBS_RM.ResourceOrder.MarketingCode,SCBS_RM.ResourceOrder.MarketingManager,SCBS_RM.ResourceOrder.NumberAttended,SCBS_RM.ResourceOrder.OrderChecked,SCBS_RM.ResourceOrder.Org_IsCP,SCBS_RM.ResourceOrder.Org_Manager,SCBS_RM.ResourceOrder.Org_Region,SCBS_RM.ResourceOrder.Organisation,SCBS_RM.ResourceOrder.OwningTeam,SCBS_RM.ResourceOrder.Purpose,SCBS_RM.ResourceOrder.PurposeNotes,SCBS_RM.ResourceOrder.Region,SCBS_RM.ResourceOrder.ResolutionDate,SCBS_RM.ResourceOrder.RightNowTest121410,SCBS_RM.ResourceOrder.SubmittedForExternalDelivery,SCBS_RM.ResourceOrder.SubmittedForInternalDelivery,SCBS_RM.ResourceOrder.Supp_IsVolunteer,SCBS_RM.ResourceOrder.Supp_Manager,SCBS_RM.ResourceOrder.Supp_Region,SCBS_RM.ResourceOrder.Supporter,SCBS_RM.ResourceOrder.SupporterFeedback,SCBS_RM.ResourceOrder.UpdatedByAccount,SCBS_RM.ResourceOrder.UpdatedTime from SCBS_RM.ResourceOrder ";

                    string updatedRecordsFilter = " WHERE SCBS_RM.ResourceOrder.CreatedTime >= '" + fromTime + "' OR SCBS_RM.ResourceOrder.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_RM.ResourceOrder.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + "  ResourceOrder records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO ResourceOrder");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("ResourceOrderID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string New_row = row.Replace("'", " ");
                                List<string> values = New_row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " ResourceOrder records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in ResourceOrder - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncMiscEventTourDate(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get MiscEventTourDate";

                do
                {
                    count = 0;

                    string Query = "SELECT Misc.EventTourDate.ID,Misc.EventTourDate.ApproximateAudience,Misc.EventTourDate.Attendance,Misc.EventTourDate.ChildrenOnHoldFrom,Misc.EventTourDate.CreatedByAccount,Misc.EventTourDate.CreatedTime,Misc.EventTourDate.Date,Misc.EventTourDate.DonationsReceived,Misc.EventTourDate.Event,Misc.EventTourDate.EventManager,Misc.EventTourDate.ExpectedSponsorships,Misc.EventTourDate.NumberMonies,Misc.EventTourDate.NumberNoMonies,Misc.EventTourDate.NumberOfProfiles,Misc.EventTourDate.OrderOn,Misc.EventTourDate.OtherRequirements,Misc.EventTourDate.ProfileExpiryDate,Misc.EventTourDate.ReportedSponsorships,Misc.EventTourDate.SourceCode,Misc.EventTourDate.Status,Misc.EventTourDate.UpdatedByAccount,Misc.EventTourDate.UpdatedTime,Misc.EventTourDate.VenueAddress  FROM Misc.EventTourDate";

                    string updatedRecordsFilter = " WHERE Misc.EventTourDate.CreatedTime >= '" + fromTime + "' OR Misc.EventTourDate.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Misc.EventTourDate.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " MiscEventTourDate records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO MiscEventTourDate");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("EventTourDateID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " MiscEventTourDate records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncMiscEventTourDate - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncDonationDetails(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Donation Details";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_DON.DonationDetails.ID,SCBS_DON.DonationDetails.Amount,SCBS_DON.DonationDetails.CreatedByAccount,SCBS_DON.DonationDetails.CreatedTime,SCBS_DON.DonationDetails.Currency,SCBS_DON.DonationDetails.Donation,SCBS_DON.DonationDetails.DonationType,SCBS_DON.DonationDetails.Need,SCBS_DON.DonationDetails.UpdatedByAccount,SCBS_DON.DonationDetails.UpdatedTime  FROM SCBS_DON.DonationDetails";

                    string updatedRecordsFilter = " WHERE SCBS_DON.DonationDetails.CreatedTime >= '" + fromTime + "' OR SCBS_DON.DonationDetails.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_DON.DonationDetails.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " DonationDetails records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO DonationDetails");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("DonationID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " DonationDetails records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in DonationDetails - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncMarketingChannels(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get MarketingChannels";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_COMMON.MarketingChannels.ID,SCBS_COMMON.MarketingChannels.DisplayOrder  FROM SCBS_COMMON.MarketingChannels";


                    string orderBy = " ORDER BY SCBS_COMMON.MarketingChannels.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    //Query += updatedRecordsFilter + orderBy + limit;
                    Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " MarketingChannels records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO MarketingChannels");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("MarketingChannelsID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " MarketingChannels records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in MarketingChannels - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncMarketingManager(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get MarketingChannels";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_COMMON.MarketingManager.ID,SCBS_COMMON.MarketingManager.DisplayOrder  FROM SCBS_COMMON.MarketingManager";


                    string orderBy = " ORDER BY SCBS_COMMON.MarketingManager.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    //Query += updatedRecordsFilter + orderBy + limit;
                    Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " MarketingManager records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO MarketingManager");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("MarketingManagerID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " MarketingManager records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in MarketingManager - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncDonation(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Donation";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_DON.Donation.ID,SCBS_DON.Donation.Annual,SCBS_DON.Donation.AutoReceipt,SCBS_DON.Donation.CardPayment,SCBS_DON.Donation.CreatedByAccount,SCBS_DON.Donation.CreatedTime,SCBS_DON.Donation.DirectDebitPayment,SCBS_DON.Donation.PaymentCreateDate,SCBS_DON.Donation.Phone,SCBS_DON.Donation.Postal,SCBS_DON.Donation.Source,SCBS_DON.Donation.Supporter,SCBS_DON.Donation.SupporterGroup,SCBS_DON.Donation.TripID,SCBS_DON.Donation.UpdatedByAccount,SCBS_DON.Donation.UpdatedTime,SCBS_DON.Donation.WebUser,SCBS_DON.Donation.Website,SCBS_DON.Donation.WebsiteID FROM SCBS_DON.Donation";
                    string updatedRecordsFilter = " WHERE SCBS_DON.Donation.CreatedTime >= '" + fromTime + "' OR SCBS_DON.Donation.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_DON.Donation.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Donation records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Donation");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("DonationID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Donation records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Donation - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncDonationType(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get DonationType";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_DON.DonationType.ID,SCBS_DON.DonationType.Active,SCBS_DON.DonationType.CreatedByAccount,SCBS_DON.DonationType.CreatedTime,SCBS_DON.DonationType.DisplayOrder,SCBS_DON.DonationType.FundCompassDescription,SCBS_DON.DonationType.FundName,SCBS_DON.DonationType.Phone,SCBS_DON.DonationType.Postal,SCBS_DON.DonationType.RequiresNeed,SCBS_DON.DonationType.UpdatedByAccount,SCBS_DON.DonationType.UpdatedTime,SCBS_DON.DonationType.Website FROM SCBS_DON.DonationType";
                    string updatedRecordsFilter = " WHERE SCBS_DON.DonationType.CreatedTime >= '" + fromTime + "' OR SCBS_DON.DonationType.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_DON.DonationType.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " DonationType records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO DonationType");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("DonationTypeID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " DonationType records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in DonationType - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int Sync_incPerformance(string fromTime)
        {
            fromTime = DateTime.Today.AddDays(-1).AddHours(2).ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'");
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Inc_process";

                do
                {
                    count = 0;

                    string Query = "select Incident.ID,performancerecords.intervalAccount,performancerecords.staffGroup,performancerecords.intervalType,performancerecords.startTime,performancerecords.endTime,performancerecords.intervalTypeCount,performancerecords.isSolved,performancerecords.interface,performancerecords.relativeInterval,performancerecords.queue,performancerecords.sourceLevel1,performancerecords.sourceLevel2 from Incident";

                    string updatedRecordsFilter = " WHERE performancerecords.startTime >= '" + fromTime + "' OR performancerecords.endTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Incident.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Inc_performance records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SEPerformance");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("IncidentID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();

                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string singleRow = "";
                                string startTime = "";
                                string endTime = "";
                                List<string> values = row.Split('^').ToList();
                                string IncidentID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string intervalAccount = string.IsNullOrEmpty(values[1]) ? "0" : values[1];
                                string staffGroup = string.IsNullOrEmpty(values[2]) ? "0" : values[2];
                                string intervalType = string.IsNullOrEmpty(values[3]) ? "0" : values[3];
                                //string startTime = string.IsNullOrEmpty(values[4]) ? "" : Convert.ToDateTime(values[4]).ToString("yyyy-MM-dd hh:mm:ss");


                                // string endTime = string.IsNullOrEmpty(values[5]) ? "0000-00-00 00:00:00" : Convert.ToDateTime(values[5]).ToString("yyyy-MM-dd hh:mm:ss");
                                string intervalTypeCount = string.IsNullOrEmpty(values[6]) ? "0" : values[6];
                                string isSolved = string.IsNullOrEmpty(values[7]) ? "0" : values[7];
                                string interface_val = string.IsNullOrEmpty(values[8]) ? "0" : values[8];
                                string relativeInterval = string.IsNullOrEmpty(values[9]) ? "0" : values[9];
                                string queue = string.IsNullOrEmpty(values[10]) ? "0" : values[10];
                                string sourceLevel1 = string.IsNullOrEmpty(values[11]) ? "0" : values[11];
                                string sourceLevel2 = string.IsNullOrEmpty(values[12]) ? "0" : values[12];


                                if (!string.IsNullOrEmpty(values[4]))
                                {
                                    DateTime startTimedt = DateTime.ParseExact(values[4], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    startTime = startTimedt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    startTime = "0000-00-00 00:00:00";
                                }
                                if (!string.IsNullOrEmpty(values[5]))
                                {
                                    DateTime endTimedt = DateTime.ParseExact(values[5], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    endTime = endTimedt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    endTime = "0000-00-00 00:00:00";
                                }

                                singleRow = IncidentID + "," + intervalAccount + "," + staffGroup + "," + intervalType + ",'" + startTime + "','" + endTime + "'," + intervalTypeCount + "," + isSolved + "," + interface_val + "," + relativeInterval + "," + queue + "," + sourceLevel1 + "," + sourceLevel2;
                                //Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Inc_performance records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Inc_performance - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncVolunteerPathway(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get VolunteerPathway for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT VolPathway.VolunteerPathway.ID,VolPathway.VolunteerPathway.Supporter,VolPathway.VolunteerPathway.Role_ChurchSpeaker,VolPathway.VolunteerPathway.Role_Prayer,VolPathway.VolunteerPathway.Role_Present,VolPathway.VolunteerPathway.Role_RepresentAtChurch,VolPathway.VolunteerPathway.Role_ShareWithFriends,VolPathway.VolunteerPathway.Role_VolCompassionHouse,VolPathway.VolunteerPathway.Role_VolEvents,VolPathway.VolunteerPathway.AnnualSpeakingCapacity,VolPathway.VolunteerPathway.MaxTravelTime,VolPathway.VolunteerPathway.MaxTravelDistance_Miles FROM VolPathway.VolunteerPathway";
                    string updatedRecordsFilter = " WHERE VolPathway.VolunteerPathway.CreatedTime >= '" + fromTime + "' OR VolPathway.VolunteerPathway.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY VolPathway.VolunteerPathway.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " VolunteerPathway records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO VolunteerPathway");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("VolunteerPathwayID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                Rows.Add(string.Format("('{0}')", string.Join("','", values.Select(i => i.Replace("'", "''")))));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " VolunteerPathway records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in VolunteerPathway - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncEncounterAttendance(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get data for DWH Sync";

                do
                {
                    count = 0;


                    string Query = "SELECT Encounter.Attendance.ID,Encounter.Attendance.IsSupporter,Encounter.Attendance.Organisation,Encounter.Attendance.PassportCopyReceived,Encounter.Attendance.PhotoReceived,Encounter.Attendance.Status,Encounter.Attendance.Supporter,Encounter.Attendance.UpdatedByAccount,Encounter.Attendance.UpdatedTime,Encounter.Attendance.Visit,Encounter.Attendance.ApplicationFormReceived,Encounter.Attendance.ChildProtectionFormReceived,Encounter.Attendance.CreatedByAccount,Encounter.Attendance.CreatedTime,Encounter.Attendance.DBSReceived,Encounter.Attendance.EmergencyContactsReceived,Encounter.Attendance.ExpensesPaid,Encounter.Attendance.MetChild,Encounter.Attendance.Type,Encounter.Attendance.Need FROM Encounter.Attendance";

                    string updatedRecordsFilter = " WHERE Encounter.Attendance.CreatedTime >= '" + fromTime + "' OR Encounter.Attendance.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Encounter.Attendance.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Encounter Attendance records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO EncounterAttendance");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("AttendanceID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string AttendanceID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string IsSupporter = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Organisation = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string PassportCopyReceived = string.IsNullOrEmpty(values[3]) ? "null" : GetDateFromDateTime(values[3]);
                                string PhotoReceived = string.IsNullOrEmpty(values[4]) ? "null" : GetDateFromDateTime(values[4]);
                                string Status = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string Supporter = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string UpdatedTime = "";
                                string Visit = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string ApplicationFormReceived = string.IsNullOrEmpty(values[10]) ? "null" : GetDateFromDateTime(values[10]);
                                string ChildProtectionFormReceived = string.IsNullOrEmpty(values[11]) ? "null" : GetDateFromDateTime(values[11]);
                                string CreatedByAccount = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string CreatedTime = "";
                                string DBSReceived = string.IsNullOrEmpty(values[14]) ? "null" : GetDateFromDateTime(values[14]);
                                string EmergencyContactsReceived = string.IsNullOrEmpty(values[15]) ? "null" : GetDateFromDateTime(values[15]);
                                string ExpensesPaid = string.IsNullOrEmpty(values[16]) ? "null" : GetDateFromDateTime(values[16]);
                                string MetChild = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string Type = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string Need = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                if (!string.IsNullOrEmpty(values[13]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[13], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[8]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[8], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }

                                string singleRow = "" + AttendanceID + "," + IsSupporter + "," + Organisation + ",'" + PassportCopyReceived + "','" + PhotoReceived + "'," + Status + "," + Supporter + "," + UpdatedByAccount + ",'" + UpdatedTime + "'," + Visit + ",'" + ApplicationFormReceived + "','" + ChildProtectionFormReceived + "'," + CreatedByAccount + ",'" + CreatedTime + "','" + DBSReceived + "','" + EmergencyContactsReceived + "','" + ExpensesPaid + "'," + MetChild + "," + Type + "," + Need + "";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " EncounterAttendance records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncEncounterAttendance - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncEncounterVisit(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get data for DWH Sync";

                do
                {
                    count = 0;
                    string Query = "SELECT Encounter.Visit.ID,Encounter.Visit.Intervention,Encounter.Visit.MeetGraduates,Encounter.Visit.Need,Encounter.Visit.OfficeVisit,Encounter.Visit.Project,Encounter.Visit.ProjectDay,Encounter.Visit.ProposedVisitDate,Encounter.Visit.TripLeader,Encounter.Visit.TripPlanner,Encounter.Visit.Type,Encounter.Visit.UpdatedByAccount,Encounter.Visit.UpdatedTime,Encounter.Visit.VisitEndDate,Encounter.Visit.VisitName,Encounter.Visit.VisitStartDate,Encounter.Visit.AverageCostPerPerson,Encounter.Visit.ChildSurvival,Encounter.Visit.Church,Encounter.Visit.ConfirmedWithField,Encounter.Visit.Country,Encounter.Visit.CreatedByAccount,Encounter.Visit.CreatedTime,Encounter.Visit.FieldRequested,Encounter.Visit.FunDay,Encounter.Visit.DeliveryMethod FROM Encounter.Visit";

                    string updatedRecordsFilter = " WHERE Encounter.Visit.CreatedTime >= '" + fromTime + "' OR Encounter.Visit.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Encounter.Visit.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Encounter Visit records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO EncounterVisit");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("VisitID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string ID = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string Intervention = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string MeetGraduates = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string Need = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string OfficeVisit = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string Project = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string ProjectDay = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string ProposedVisitDate = string.IsNullOrEmpty(values[7]) ? "null" : GetDateFromDateTime(values[7]);
                                string TripLeader = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string TripPlanner = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string Type = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string UpdatedTime = "";//12
                                string VisitEndDate = string.IsNullOrEmpty(values[13]) ? "null" : GetDateFromDateTime(values[13]);
                                string VisitName = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string VisitStartDate = string.IsNullOrEmpty(values[15]) ? "null" : GetDateFromDateTime(values[15]);
                                string AverageCostPerPerson = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string ChildSurvival = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string Church = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string ConfirmedWithField = string.IsNullOrEmpty(values[19]) ? "null" : GetDateFromDateTime(values[19]);
                                string Country = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string CreatedByAccount = string.IsNullOrEmpty(values[21]) ? "null" : values[21];
                                string CreatedTime = "";//22
                                string FieldRequested = string.IsNullOrEmpty(values[23]) ? "null" : GetDateFromDateTime(values[23]);
                                string FunDay = string.IsNullOrEmpty(values[24]) ? "null" : values[24];
                                string DeliveryMethod = string.IsNullOrEmpty(values[25]) ? "null" : values[25];
                                if (!string.IsNullOrEmpty(values[22]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[22], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[12]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[12], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }

                                string singleRow = "" + ID + "," + Intervention + "," + MeetGraduates + "," + Need + "," + OfficeVisit + "," + Project + "," + ProjectDay + ",'" + ProposedVisitDate + "'," + TripLeader + "," + TripPlanner + "," + Type + "," + UpdatedByAccount + ",'" + UpdatedTime + "','" + VisitEndDate + "','" + VisitName.Replace("'", "") + "','" + VisitStartDate + "'," + AverageCostPerPerson + "," + ChildSurvival + "," + Church + ",'" + ConfirmedWithField + "'," + Country + "," + CreatedByAccount + ",'" + CreatedTime + "','" + FieldRequested + "'," + FunDay + "," + DeliveryMethod + "";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " EncounterVisit records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncEncounterVisit - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        public int SyncActionHistory(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get data for DWH Sync";

                do
                {
                    count = 0;


                    string Query = "SELECT Misc.ActionHistory.ID,Misc.ActionHistory.ProcessingID,Misc.ActionHistory.ActionID,Misc.ActionHistory.ActionName,Misc.ActionHistory.CallRating,Misc.ActionHistory.CancelledOnCall,Misc.ActionHistory.CellID,Misc.ActionHistory.CellName,Misc.ActionHistory.CommitmentID,Misc.ActionHistory.CreatedByAccount,Misc.ActionHistory.CreatedTime,Misc.ActionHistory.DateContacted,Misc.ActionHistory.DateSelected,Misc.ActionHistory.ModelDecile,Misc.ActionHistory.NoSubOnCall,Misc.ActionHistory.SupporterID,Misc.ActionHistory.UpdatedByAccount,Misc.ActionHistory.UpdatedTime FROM Misc.ActionHistory";

                    string updatedRecordsFilter = " WHERE Misc.ActionHistory.CreatedTime >= '" + fromTime + "' OR Misc.ActionHistory.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Misc.ActionHistory.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " ActionHistory records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO ActionHistory");
                        using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string ID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string ProcessingID = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string ActionID = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string ActionName = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string CallRating = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string CancelledOnCall = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string CellID = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string CellName = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string CommitmentID = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string CreatedByAccount = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string CreatedTime = "";//10
                                string DateContacted = string.IsNullOrEmpty(values[11]) ? "null" : GetDateFromDateTime(values[11]);
                                string DateSelected = string.IsNullOrEmpty(values[12]) ? "null" : GetDateFromDateTime(values[12]);
                                string ModelDecile = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string NoSubOnCall = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string SupporterID = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string UpdatedTime = "";//17
                                if (!string.IsNullOrEmpty(values[10]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[10], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[17]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[17], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }

                                string singleRow = "" + ID + "," + ProcessingID + ",'" + ActionID + "','" + ActionName + "'," + CallRating + "," + CancelledOnCall + "," + CellID + ",'" + CellName + "'," + CommitmentID + "," + CreatedByAccount + ",'" + CreatedTime + "','" + DateContacted + "','" + DateSelected + "'," + ModelDecile + "," + NoSubOnCall + "," + SupporterID + "," + UpdatedByAccount + ",'" + UpdatedTime + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " ActionHistory records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncActionHistory  - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }

        #endregion

        #region Sync DWH Landing Tables
        public int SyncNeed_Landing(string fromTime)
        {
            appLogger.Info("Process Started..");
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Need data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_CHILD.Need.ID, SCBS_CHILD.Need.Beneficiary_GlobalID, SCBS_CHILD.Need.Beneficiary_LocalID, SCBS_CHILD.Need.BirthDate, SCBS_CHILD.Need.ContinentName, SCBS_CHILD.Need.CorrespondenceLanguage, SCBS_CHILD.Need.CountryName, SCBS_CHILD.Need.CreatedTime, SCBS_CHILD.Need.EndDate, SCBS_CHILD.Need.Gender, SCBS_CHILD.Need.ICP_ID, SCBS_CHILD.Need.Latitude, SCBS_CHILD.Need.Longitude, SCBS_CHILD.Need.NeedKey, SCBS_CHILD.Need.NeedType, SCBS_CHILD.Need.PlannedCompDateChangeReason, SCBS_CHILD.Need.PlannedCompletionDate, SCBS_CHILD.Need.UpdatedTime,SCBS_CHILD.Need.NeedStatus,SCBS_CHILD.Need.ProgramDeliveryType FROM SCBS_CHILD.Need";
                    string updatedRecordsFilter = " WHERE SCBS_CHILD.Need.CreatedTime >= '" + fromTime + "' OR SCBS_CHILD.Need.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CHILD.Need.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Need records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Need");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("NeedID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string CreatedTime = "";
                                string UpdatedTime = "";
                                string FilterRow = row.Replace("'", "");
                                List<string> values = FilterRow.Split('^').ToList();
                                string NeedID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string Beneficiary_GlobalID = string.IsNullOrEmpty(values[1]) ? "0" : values[1];
                                string Beneficiary_LocalID = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string BirthDate = !string.IsNullOrEmpty(values[3]) ? GetDateFromDateTime(values[3]) : "null";
                                string ContinentName = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string CorrespondenceLanguage = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string CountryName = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                if (!string.IsNullOrEmpty(values[7]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[7], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }
                                string EndDate = !string.IsNullOrEmpty(values[8]) ? GetDateFromDateTime(values[8]) : "null";
                                string Gender = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string ICP_ID = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string Latitude = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string Longitude = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string NeedKey = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string NeedType = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string PlannedCompDateChangeReason = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string PlannedCompletionDate = !string.IsNullOrEmpty(values[16]) ? GetDateFromDateTime(values[16]) : "null";
                                string NeedStatus = string.IsNullOrEmpty(values[18]) ? "0" : values[18];
                                string ProgramDeliveryType = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                if (!string.IsNullOrEmpty(values[17]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[17], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                Longitude = (Longitude == "No" || Longitude == "No Value") ? "null" : Longitude;
                                Latitude = (Latitude == "No" || Latitude == "No Value") ? "null" : Latitude;

                                string singleRow = NeedID + ",'" + Beneficiary_GlobalID + "','" + Beneficiary_LocalID + "','" + BirthDate + "','" + ContinentName + "','" + CorrespondenceLanguage + "','" + CountryName + "','" + CreatedTime + "','" + EndDate + "'," + Gender + ",'" + ICP_ID + "'," + Latitude + "," + Longitude + ",'" + NeedKey + "'," + NeedType + ",'" + PlannedCompDateChangeReason + "','" + PlannedCompletionDate + "','" + UpdatedTime + "'," + NeedStatus + ",'" + ProgramDeliveryType + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Need records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncNeed - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
            appLogger.Info("Process Stopped..");
        }
        public int SyncVolunteerPathway_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get VolunteerPathway for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT VolPathway.VolunteerPathway.ID,VolPathway.VolunteerPathway.Supporter,VolPathway.VolunteerPathway.Role_ChurchSpeaker,VolPathway.VolunteerPathway.Role_Prayer,VolPathway.VolunteerPathway.Role_Present,VolPathway.VolunteerPathway.Role_RepresentAtChurch,VolPathway.VolunteerPathway.Role_ShareWithFriends,VolPathway.VolunteerPathway.Role_VolCompassionHouse,VolPathway.VolunteerPathway.Role_VolEvents,VolPathway.VolunteerPathway.AnnualSpeakingCapacity,VolPathway.VolunteerPathway.MaxTravelTime,VolPathway.VolunteerPathway.MaxTravelDistance_Miles FROM VolPathway.VolunteerPathway";
                    string updatedRecordsFilter = " WHERE VolPathway.VolunteerPathway.CreatedTime >= '" + fromTime + "' OR VolPathway.VolunteerPathway.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY VolPathway.VolunteerPathway.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " VolunteerPathway records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO VolunteerPathway");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("VolunteerPathwayID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                for (int i = 0; i < values.Count; i++)
                                {
                                    if (string.IsNullOrEmpty(values[i]))
                                    {
                                        values[i] = "null";
                                    }
                                }
                                Rows.Add(string.Format("({0})", string.Join(",", values)));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " VolunteerPathway records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in VolunteerPathway - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int Sync_incPerformance_Landing(string fromTime)
        {
            fromTime = DateTime.Today.AddDays(-1).AddHours(2).ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'");
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Inc_process data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "select Incident.ID,performancerecords.intervalAccount,performancerecords.staffGroup,performancerecords.intervalType,performancerecords.startTime,performancerecords.endTime,performancerecords.intervalTypeCount,performancerecords.isSolved,performancerecords.interface,performancerecords.relativeInterval,performancerecords.queue,performancerecords.sourceLevel1,performancerecords.sourceLevel2 from Incident";

                    string updatedRecordsFilter = " WHERE performancerecords.startTime >= '" + fromTime + "' OR performancerecords.endTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Incident.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Inc_performance records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SEPerformance");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("IncidentID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();

                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string singleRow = "";
                                string startTime = "";
                                string endTime = "";
                                List<string> values = row.Split('^').ToList();
                                string IncidentID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string intervalAccount = string.IsNullOrEmpty(values[1]) ? "0" : values[1];
                                string staffGroup = string.IsNullOrEmpty(values[2]) ? "0" : values[2];
                                string intervalType = string.IsNullOrEmpty(values[3]) ? "0" : values[3];
                                string intervalTypeCount = string.IsNullOrEmpty(values[6]) ? "0" : values[6];
                                string isSolved = string.IsNullOrEmpty(values[7]) ? "0" : values[7];
                                string interface_val = string.IsNullOrEmpty(values[8]) ? "0" : values[8];
                                string relativeInterval = string.IsNullOrEmpty(values[9]) ? "0" : values[9];
                                string queue = string.IsNullOrEmpty(values[10]) ? "0" : values[10];
                                string sourceLevel1 = string.IsNullOrEmpty(values[11]) ? "0" : values[11];
                                string sourceLevel2 = string.IsNullOrEmpty(values[12]) ? "0" : values[12];
                                if (!string.IsNullOrEmpty(values[4]))
                                {
                                    DateTime startTimedt = DateTime.ParseExact(values[4], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    startTime = startTimedt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    startTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[5]))
                                {
                                    DateTime endTimedt = DateTime.ParseExact(values[5], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    endTime = endTimedt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    endTime = "null";
                                }

                                singleRow = IncidentID + "," + intervalAccount + "," + staffGroup + "," + intervalType + ",'" + startTime + "','" + endTime + "'," + intervalTypeCount + "," + isSolved + "," + interface_val + "," + relativeInterval + "," + queue + "," + sourceLevel1 + "," + sourceLevel2;
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Inc_performance records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Inc_performance - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncSolicitCodes_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SolicitCodes  data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "select SCBS_SUP.SolicitCodes.ID, SCBS_SUP.SolicitCodes.CreatedTime, SCBS_SUP.SolicitCodes.CreatedByAccount, SCBS_SUP.SolicitCodes.UpdatedTime, SCBS_SUP.SolicitCodes.UpdatedByAccount, SCBS_SUP.SolicitCodes.ChannelEmail, SCBS_SUP.SolicitCodes.ChannelPhone, SCBS_SUP.SolicitCodes.ChannelPost, SCBS_SUP.SolicitCodes.ChannelSMS, SCBS_SUP.SolicitCodes.SCSubType, SCBS_SUP.SolicitCodes.SCType, SCBS_SUP.SolicitCodes.SolicitCode, SCBS_SUP.SolicitCodes.Source, SCBS_SUP.SolicitCodes.StartDate, SCBS_SUP.SolicitCodes.EndDate, SCBS_SUP.SolicitCodes.Supporter,SCBS_SUP.SolicitCodes.Parameter from SCBS_SUP.SolicitCodes";

                    string updatedRecordsFilter = " where SCBS_SUP.SolicitCodes.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.SolicitCodes.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.SolicitCodes.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SolicitCodes records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=0;INSERT INTO SolicitCodes");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("SolicitCodesID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string SolicitCodesID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string CreatedTime = "";
                                string CreatedByAccount = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string UpdatedTime = "";
                                string UpdatedByAccount = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string ChannelEmail = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string ChannelPhone = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string ChannelPost = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string ChannelSMS = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string SCSubType = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string SCType = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string SolicitCode = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string Source = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string StartDate = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string EndDate = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string Supporter = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string Parameter = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                if (!string.IsNullOrEmpty(values[1]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[1], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[3]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[3], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                string singleRow = SolicitCodesID + ",'" + CreatedTime + "'," + CreatedByAccount + ",'" + UpdatedTime + "'," + UpdatedByAccount + "," + ChannelEmail + "," + ChannelPhone + "," + ChannelPost + "," + ChannelSMS + "," + SCSubType + "," + SCType + "," + SolicitCode + "," + Source + ",'" + StartDate + "','" + EndDate + "'," + Supporter + ",'" + Parameter + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SolicitCodes records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SolicitCodes - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncCampaigns_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Campaigns  data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "select SCBS_COMMON.Campaigns.ID, SCBS_COMMON.Campaigns.UpdatedByAccount, SCBS_COMMON.Campaigns.UpdatedTime, SCBS_COMMON.Campaigns.CreatedByAccount, SCBS_COMMON.Campaigns.CreatedTime, SCBS_COMMON.Campaigns.Campaign, SCBS_COMMON.Campaigns.LegacyCampaign, SCBS_COMMON.Campaigns.Region, SCBS_COMMON.Campaigns.Manager, SCBS_COMMON.Campaigns.Channel, SCBS_COMMON.Campaigns.AssignByPostcode, SCBS_COMMON.Campaigns.AssignByEvent, SCBS_COMMON.Campaigns.Event, SCBS_COMMON.Campaigns.Organisation, SCBS_COMMON.Campaigns.Active, SCBS_COMMON.Campaigns.CompassionExperience,SCBS_COMMON.Campaigns.CPPActivity,SCBS_COMMON.Campaigns.EventTourDate from SCBS_COMMON.Campaigns";

                    string updatedRecordsFilter = " where SCBS_COMMON.Campaigns.CreatedTime >= '" + fromTime + "' OR SCBS_COMMON.Campaigns.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_COMMON.Campaigns.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Campaigns records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Campaigns");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("CampaignsID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string CampaignsID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string CreatedTime = "";//4
                                string UpdatedByAccount = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string UpdatedTime = "";//2
                                string CreatedByAccount = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string Campaign = string.IsNullOrEmpty(values[5]) ? "null" : values[5].Replace(",", "");
                                if (!string.IsNullOrEmpty(Campaign))
                                {
                                    Campaign = Campaign.Replace("'", "");
                                }
                                string LegacyCampaign = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string Region = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string Manager = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string Channel = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string AssignByPostcode = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string AssignByEvent = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string Event = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string Organisation = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string Active = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string CompassionExperience = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string CPPActivity = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string EventTourDate = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                if (!string.IsNullOrEmpty(values[4]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[4], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[2]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[2], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                string singleRow = CampaignsID + "," + UpdatedByAccount + ",'" + UpdatedTime + "'," + CreatedByAccount + ",'" + CreatedTime + "','" + Campaign + "'," + LegacyCampaign + "," + Region + "," + Manager + "," + Channel + "," + AssignByPostcode + "," + AssignByEvent + "," + Event + "," + Organisation + "," + Active + "," + CompassionExperience + "," + CPPActivity + "," + EventTourDate;
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");
                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Campaigns records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Campaigns - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncDonationDetails_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Donation Details data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_DON.DonationDetails.ID,SCBS_DON.DonationDetails.Amount,SCBS_DON.DonationDetails.CreatedByAccount,SCBS_DON.DonationDetails.CreatedTime,SCBS_DON.DonationDetails.Currency,SCBS_DON.DonationDetails.Donation,SCBS_DON.DonationDetails.DonationType,SCBS_DON.DonationDetails.Need,SCBS_DON.DonationDetails.UpdatedByAccount,SCBS_DON.DonationDetails.UpdatedTime  FROM SCBS_DON.DonationDetails";

                    string updatedRecordsFilter = " WHERE SCBS_DON.DonationDetails.CreatedTime >= '" + fromTime + "' OR SCBS_DON.DonationDetails.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_DON.DonationDetails.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " DonationDetails records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO DonationDetails");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("DonationID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string DonationID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string Amount = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string CreatedTime = "";
                                string CreatedByAccount = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string UpdatedTime = "";
                                string Currency = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string Donation = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string DonationType = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string Need = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                if (!string.IsNullOrEmpty(values[3]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[3], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[9]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[9], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                string singleRow = DonationID + "," + Amount + "," + CreatedByAccount + ",'" + CreatedTime + "'," + Currency + "," + Donation + "," + DonationType + "," + Need + "," + UpdatedByAccount + ",'" + UpdatedTime + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " DonationDetails records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in DonationDetails - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncSupporters_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Supporters  data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "select contact.ID,contact.CustomFields.c.advocate_status,contact.CustomFields.c.country, contact.CustomFields.c.appfirstloggedin, contact.CustomFields.c.applastloggedin, contact.CustomFields.c.appfirstloggedin_android, contact.CustomFields.c.applastloggedin_android, contact.CustomFields.c.postcode_area,contact.CustomFields.c.postcode_district, contact.CustomFields.c.add_mailingcountrydrop, contact.CreatedTime, contact.UpdatedTime,contact.CustomFields.c.status,contact.CustomFields.c.digitalchoice,contact.CustomFields.c.blackbaudid,contact.CustomFields.c.ambassador as Ambassador,contact.CustomFields.c.vip as VIP,contact.CustomFields.c.prospect_status as ProspectStatus,contact.CustomFields.c.trusteestatus as TrusteeStatus,contact.CustomFields.c.church_contact_role as ChurchContactRole,contact.CustomFields.c.cr_church_role as ChurchRole,contact.organization as Organisation from contact";

                    string updatedRecordsFilter = " where contact.CreatedTime >= '" + fromTime + "' OR contact.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY contact.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Supporters  records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO Supporters ");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                {
                                    Columns.Add("SupporterID");
                                }
                                else if (data == "country")
                                {
                                    Columns.Add("Compassion_office");
                                }
                                else if (data == "add_mailingcountrydrop")
                                {
                                    Columns.Add("Country");
                                }
                                else
                                {
                                    Columns.Add(data);
                                }

                            }

                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string SupporterID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string advocate_status = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Compassion_office = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string appfirstloggedin = "";
                                string applastloggedin = "";
                                string appfirstloggedin_android = "";
                                string applastloggedin_android = "";
                                string postcode_area = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string postcode_district = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string Country = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string CreatedTime = "";
                                string UpdatedTime = "";
                                string status = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string digitalchoice = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string blackbaudid = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string Ambassador = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string VIP = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string ProspectStatus = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string TrusteeStatus = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string ChurchContactRole = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string ChurchRole = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string Organisation = string.IsNullOrEmpty(values[21]) ? "null" : values[21];

                                if (!string.IsNullOrEmpty(values[3]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[3], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    appfirstloggedin = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    appfirstloggedin = "null";
                                }
                                if (!string.IsNullOrEmpty(values[4]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[4], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    applastloggedin = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    applastloggedin = "null";
                                }
                                if (!string.IsNullOrEmpty(values[5]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[5], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    appfirstloggedin_android = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    appfirstloggedin_android = "null";
                                }
                                if (!string.IsNullOrEmpty(values[6]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[6], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    applastloggedin_android = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    applastloggedin_android = "null";
                                }
                                if (!string.IsNullOrEmpty(values[10]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[10], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[11]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[11], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                string singleRow = SupporterID + "," + advocate_status + "," + Compassion_office + ",'" + appfirstloggedin + "','" + applastloggedin + "','" + appfirstloggedin_android + "','" + applastloggedin_android + "','" + postcode_area + "','" + postcode_district + "'," + Country + ",'" + CreatedTime + "','" + UpdatedTime + "'," + status + "," + digitalchoice + ",'" + blackbaudid + "'," + Ambassador + "," + VIP + "," + ProspectStatus + "," + TrusteeStatus + "," + ChurchContactRole + "," + ChurchRole + "," + Organisation + "";

                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Supporters records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Supporters - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncDonationType_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get DonationType data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_DON.DonationType.ID,SCBS_DON.DonationType.Active,SCBS_DON.DonationType.CreatedByAccount,SCBS_DON.DonationType.CreatedTime,SCBS_DON.DonationType.DisplayOrder,SCBS_DON.DonationType.FundCompassDescription,SCBS_DON.DonationType.FundName,SCBS_DON.DonationType.Phone,SCBS_DON.DonationType.Postal,SCBS_DON.DonationType.RequiresNeed,SCBS_DON.DonationType.UpdatedByAccount,SCBS_DON.DonationType.UpdatedTime,SCBS_DON.DonationType.Website FROM SCBS_DON.DonationType";
                    string updatedRecordsFilter = " WHERE SCBS_DON.DonationType.CreatedTime >= '" + fromTime + "' OR SCBS_DON.DonationType.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_DON.DonationType.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " DonationType records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO DonationType");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("DonationTypeID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string DonationTypeID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string Active = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string CreatedByAccount = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string CreatedTime = "";
                                string DisplayOrder = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string FundCompassDescription = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                if (!string.IsNullOrEmpty(FundCompassDescription))
                                {
                                    FundCompassDescription = FundCompassDescription.Replace(",", "");
                                    FundCompassDescription = FundCompassDescription.Replace("'", "");
                                }
                                string FundName = string.IsNullOrEmpty(values[6]) ? "" : values[6];
                                if (!string.IsNullOrEmpty(FundName))
                                {
                                    FundName = FundName.Replace(",", "");
                                    FundName = FundName.Replace("'", "");
                                }
                                string Phone = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string Postal = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string RequiresNeed = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string UpdatedTime = "";
                                string Website = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                if (!string.IsNullOrEmpty(values[3]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[3], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[11]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[11], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                string singleRow = DonationTypeID + "," + Active + "," + CreatedByAccount + ",'" + CreatedTime + "'," + DisplayOrder + ",'" + FundCompassDescription + "','" + FundName + "'," + Phone + "," + Postal + "," + RequiresNeed + "," + UpdatedByAccount + ",'" + UpdatedTime + "'," + Website;
                                Rows.Add(string.Format("({0})", singleRow));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " DonationType records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in DonationType - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncOrganisations_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Organisations  data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "select organization.ID,organization.Name,organization.CustomFields.c.denomination as Denomination,organization.CustomFields.c.relationship_status as RelationshipStatus,organization.CustomFields.c.church_size as ChurchSize,organization.CustomFields.standard.RegionalManager as Manager,organization.CustomFields.c.status as Status,organization.CustomFields.c.country as Country,organization.CustomFields.c.orgtype as OrgType,organization.CustomFields.c.organisationtype as OrganisationType,organization.CustomFields.c.cptype as CPType,organization.CustomFields.c.postcodearea as PostcodeArea,organization.CustomFields.c.add_mailingcountrydrop as MailingCountry,organization.CreatedTime, organization.UpdatedTime,organization.CustomFields.standard.Region,organization.CustomFields.c.add_postal_code from organization";

                    string updatedRecordsFilter = " where organization.CreatedTime >= '" + fromTime + "' OR organization.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY organization.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " organization ...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Organisation ");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                {
                                    Columns.Add("OrganisationID");
                                }
                                else
                                {
                                    Columns.Add(data);
                                }
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string OrganisationID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string Name = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                if (!string.IsNullOrEmpty(Name))
                                {
                                    Name = Name.Replace("'", @"\'");
                                }
                                string Denomination = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string RelationshipStatus = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string ChurchSize = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string Manager = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string Status = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string Country = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string OrganisationType = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string OrgType = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string CPType = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string PostcodeArea = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string MailingCountry = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string CreatedTime = "";
                                string UpdatedTime = "";
                                string Region = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string add_postal_code = string.IsNullOrEmpty(values[16]) ? "null" : values[16];

                                byte[] bytes = Encoding.ASCII.GetBytes(add_postal_code);
                                byte[] filterBytes = new byte[10];
                                if (bytes.Length > 7)
                                {
                                    for (int i = 0; i < bytes.Length; i++)
                                    {
                                        if (bytes[i] != 63)
                                        {
                                            filterBytes[i] = bytes[i];
                                        }
                                    }
                                    add_postal_code = Encoding.Default.GetString(filterBytes.Select(s => s).Where(k => k != 0).ToArray());
                                }

                                if (!string.IsNullOrEmpty(values[13]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[13], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[14]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[14], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                string singleRow = OrganisationID + ",'" + Name + "'," + Denomination + "," + RelationshipStatus + "," + ChurchSize + "," + Manager + "," + Status + "," + Country + "," + OrganisationType + "," + OrgType + "," + CPType + ",'" + PostcodeArea + "'," + MailingCountry + ",'" + CreatedTime + "','" + UpdatedTime + "'," + Region + ",'" + add_postal_code + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Organisations records updated successfully");
                        }

                        // For next set of data
                        Total += count;

                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Organisations - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncMarketingManager_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get MarketingChannels data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_COMMON.MarketingManager.ID,SCBS_COMMON.MarketingManager.DisplayOrder  FROM SCBS_COMMON.MarketingManager";


                    string orderBy = " ORDER BY SCBS_COMMON.MarketingManager.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    //Query += updatedRecordsFilter + orderBy + limit;
                    Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " MarketingManager records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO MarketingManager");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("MarketingManagerID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string MarketingManagerID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string DisplayOrder = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string singleRow = MarketingManagerID + "," + DisplayOrder;
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " MarketingManager records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in MarketingManager - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncMarketingChannels_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get MarketingChannels data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_COMMON.MarketingChannels.ID,SCBS_COMMON.MarketingChannels.DisplayOrder  FROM SCBS_COMMON.MarketingChannels";


                    string orderBy = " ORDER BY SCBS_COMMON.MarketingChannels.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    //Query += updatedRecordsFilter + orderBy + limit;
                    Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " MarketingChannels records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO MarketingChannels");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("MarketingChannelsID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string MarketingManagerID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string DisplayOrder = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string singleRow = MarketingManagerID + "," + DisplayOrder;
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " MarketingChannels records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in MarketingChannels - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncSupporterGroupLinks_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SupporterGroupLinks DWH Sync";

                do
                {
                    count = 0;

                    string Query = "select SCBS_SUP.SupporterGroupLinks.ID, SCBS_SUP.SupporterGroupLinks.OrganisationID, SCBS_SUP.SupporterGroupLinks.SupporterGroupID, SCBS_SUP.SupporterGroupLinks.SupporterID, SCBS_SUP.SupporterGroupLinks.Active, SCBS_SUP.SupporterGroupLinks.CreatedTime, SCBS_SUP.SupporterGroupLinks.CreatedByAccount, SCBS_SUP.SupporterGroupLinks.UpdatedTime, SCBS_SUP.SupporterGroupLinks.UpdatedByAccount, SCBS_SUP.SupporterGroupLinks.SupporterGroupHistory from SCBS_SUP.SupporterGroupLinks";

                    string updatedRecordsFilter = " where SCBS_SUP.SupporterGroupLinks.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.SupporterGroupLinks.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.SupporterGroupLinks.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SupporterGroupLinks records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SupporterGroupLinks");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("SupporterGroupLinksID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string SupporterGroupLinksID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string OrganisationID = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string SupporterGroupID = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string SupporterID = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string Active = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string CreatedTime = "";
                                string CreatedByAccount = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string UpdatedTime = "";
                                string UpdatedByAccount = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string SupporterGroupHistory = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                if (!string.IsNullOrEmpty(values[5]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[5], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[7]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[7], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                string singleRow = SupporterGroupLinksID + "," + OrganisationID + "," + SupporterGroupID + "," + SupporterID + "," + Active + ",'" + CreatedTime + "'," + CreatedByAccount + ",'" + UpdatedTime + "'," + UpdatedByAccount + "," + SupporterGroupHistory;
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SupporterGroupLinks records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SupporterGroupLinks - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncThreads_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            int iCount = 0;


            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Thread  data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT Incident.threads.account as Account,Incident.threads.channel Channel,Incident.threads.contact as Contact,Incident.threads.createdTime as CreatedTime,Incident.threads.displayOrder as DisplayOrder,Incident.threads.entryType as EntryType,Incident.threads.id as ThreadID,Incident.ID as IncidentID FROM Incident";

                    string updatedRecordsFilter = " where Incident.CreatedTime >= '" + fromTime + "' OR Incident.UpdatedTime >= '" + fromTime + "' AND Incident.threads.id is not null ";
                    string orderBy = " ORDER BY Incident.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Thread records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO Threads");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                iCount++;
                                List<string> values = row.Split('^').ToList();
                                string Account = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string Channel = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Contact = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string CreatedTime = "";
                                string DisplayOrder = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string EntryType = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string ThreadID = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string IncidentID = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                if (ThreadID != "null")
                                {
                                    if (!string.IsNullOrEmpty(values[3]))
                                    {
                                        DateTime dt = DateTime.ParseExact(values[3], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                        CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                        //2013-10-05T14:44:01Z
                                    }
                                    else
                                    {
                                        CreatedTime = "null";
                                    }
                                    string singleRow = Account + "," + Channel + "," + Contact + ",'" + CreatedTime + "'," + DisplayOrder + "," + EntryType + "," + ThreadID + "," + IncidentID;
                                    Rows.Add(string.Format("({0})", singleRow));
                                }
                            }
                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");
                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Threads records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Threads - FromTime : " + fromTime + ", StartLimit : " + startLimit + iCount);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncSupporterGroup_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SupporterGroup data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "select SCBS_SUP.SupporterGroup.ID, SCBS_SUP.SupporterGroup.StartDate, SCBS_SUP.SupporterGroup.Status, SCBS_SUP.SupporterGroup.CompassConID, SCBS_SUP.SupporterGroup.CompassionOffice, SCBS_SUP.SupporterGroup.CreatedTime, SCBS_SUP.SupporterGroup.UpdatedTime, SCBS_SUP.SupporterGroup.Type, SCBS_SUP.SupporterGroup.CreatedByAccount, SCBS_SUP.SupporterGroup.UpdatedByAccount, SCBS_SUP.SupporterGroup.PrioritySupporterId, SCBS_SUP.SupporterGroup.BlackBaudConstituentId, SCBS_SUP.SupporterGroup.GlobalID,SCBS_SUP.SupporterGroup.SubChildren, SCBS_SUP.SupporterGroup.SubPreferences,SCBS_SUP.SupporterGroup.DigitalChoice from SCBS_SUP.SupporterGroup";

                    string updatedRecordsFilter = " WHERE SCBS_SUP.SupporterGroup.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.SupporterGroup.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.SupporterGroup.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SupporterGroup records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SupporterGroup");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("SupporterGroupID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string FilterRow = row.Replace("'", "");
                                List<string> values = FilterRow.Split('^').ToList();
                                string SupporterGroupID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string StartDate = !string.IsNullOrEmpty(values[1]) ? GetDateFromDateTime(values[1]) : "null";
                                string Status = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string CompassConID = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string CompassionOffice = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string CreatedTime = "";
                                string UpdatedTime = "";
                                string Type = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string CreatedByAccount = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string PrioritySupporterId = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string BlackBaudConstituentId = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string GlobalID = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string SubChildren = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string SubPreferences = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string DigitalChoice = string.IsNullOrEmpty(values[15]) ? "null" : values[15];

                                if (!string.IsNullOrEmpty(values[5]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[5], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[6]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[6], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }

                                string singleRow = SupporterGroupID + ",'" + StartDate + "'," + Status + "," + CompassConID + "," + CompassionOffice + ",'" + CreatedTime + "','" + UpdatedTime + "'," + Type + "," + CreatedByAccount + "," + UpdatedByAccount + "," + PrioritySupporterId + ",'" + BlackBaudConstituentId + "','" + GlobalID + "'," + SubChildren + ",'" + SubPreferences + "'," + DigitalChoice;
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SupporterGroup records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncSupporterGroup - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncSupporterEnquiries_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            string updatingStartPoint = null;
            string updatingEndPoint = null;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SupporterEnquiries data for DWH Sync";

                do
                {
                    count = 0;
                    string Query = "SELECT Incident.ID,Incident.Queue,Incident.Mailbox,Incident.Category,Incident.Disposition,Incident.CustomFields.c.ContactAttemptsMade,Incident.CustomFields.c.FeedbackRating,Incident.CreatedTime,Incident.UpdatedTime,Incident.ClosedTime,Incident.CreatedByAccount,Incident.CustomFields.c.CampaignFormType,Incident.CustomFields.c.due_date as ResolutionDueDate,Incident.Product as MinistryAspect,Incident.CustomFields.c.sourcechannel,Incident.CustomFields.c.source,Incident.source.level1 as source_lvl1,Incident.source.id as source_lvl2,Incident.CustomFields.c.whitemail as whitemail,Incident.PrimaryContact.contact as PrimaryContact,Incident.assignedTo.account as AssignedTo,Incident.statusWithType.status.id as Status_id,Incident.statusWithType.status.lookupName as Status,Incident.CustomFields.c.InternalSubject,Incident.lookupname as Reference,Incident.customFields.c.sponsorchildref as ChildReference,Incident.customFields.c.rcworkitem as RingCentralWorkItem,Incident.customFields.c.rcworkitemskill as RingCentralWorkItemSkill,Incident.customFields.c.rcworkitemstatus as RingCentralWorkItemStatus,Incident.customFields.c.rcworkitemid as RingCentralWorkItemID,Incident.customFields.c.rcoriginalenquiryref as RingCentralOriginalEnquiryReference,Incident.customFields.c.rcenquiryiteration as RingCentralEnquiryIteration,Incident.statusWithType.statusType.id as StatusType,Incident.customFields.c.resolvestarted as ResolveStarted,Incident.customFields.c.resolveended as ResolveEnded,Incident.customFields.c.setupstarted as SetupStarted,Incident.customFields.c.setupended as SetupEnded,Incident.customFields.c.supportertype as SupporterType,Incident.customFields.c.needid as NeedID FROM Incident";

                    string updatedRecordsFilter = " WHERE Incident.CreatedTime >= '" + fromTime + "' OR Incident.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Incident.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;
                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;
                    byte[] byteArray;
                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;
                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SupporterEnquiries records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SupporterEnquiries");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("IncidentID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string IncidentID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string Queue = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Mailbox = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string Category = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string Disposition = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string ContactAttemptsMade = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string FeedbackRating = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string CreatedTime = "";//7
                                string UpdatedTime = "";//8
                                string ClosedTime = "";//9
                                string CreatedByAccount = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string CampaignFormType = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string ResolutionDueDate = !string.IsNullOrEmpty(values[12]) ? GetDateFromDateTime(values[12]) : "null";
                                string MinistryAspect = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string sourcechannel = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string source = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string source_lvl1 = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string source_lvl2 = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string whitemail = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string PrimaryContact = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string AssignedTo = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string Status_id = string.IsNullOrEmpty(values[21]) ? "null" : values[21];
                                string Status = string.IsNullOrEmpty(values[22]) ? "null" : values[22];
                                string InternalSubject = string.IsNullOrEmpty(values[23]) ? "null" : values[23].Replace(",", "");
                                string Reference = string.IsNullOrEmpty(values[24]) ? "null" : values[24];
                                string ChildReference = string.IsNullOrEmpty(values[25]) ? "null" : values[25];
                                string RingCentralWorkItem = string.IsNullOrEmpty(values[26]) ? "null" : values[26];
                                string RingCentralWorkItemSkill = string.IsNullOrEmpty(values[27]) ? "null" : values[27];
                                string RingCentralWorkItemStatus = string.IsNullOrEmpty(values[28]) ? "null" : values[28];
                                string RingCentralWorkItemID = string.IsNullOrEmpty(values[29]) ? "null" : values[29];
                                string RingCentralOriginalEnquiryReference = string.IsNullOrEmpty(values[30]) ? "null" : values[30];
                                string RingCentralEnquiryIteration = string.IsNullOrEmpty(values[31]) ? "null" : values[31];
                                string StatusType = string.IsNullOrEmpty(values[32]) ? "null" : values[32];
                                string ResolveStarted = "";
                                string ResolveEnded = "";
                                string SetupStarted = "";
                                string SetupEnded = "";
                                string SupporterType = string.IsNullOrEmpty(values[37]) ? "null" : values[37];
                                string NeedID = string.IsNullOrEmpty(values[38]) ? "null" : values[38];

                                if (!string.IsNullOrEmpty(values[33]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[33], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ResolveStarted = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ResolveStarted = "null";
                                }

                                if (!string.IsNullOrEmpty(values[34]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[34], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ResolveEnded = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ResolveEnded = "null";
                                }

                                if (!string.IsNullOrEmpty(values[35]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[35], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    SetupStarted = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    SetupStarted = "null";
                                }

                                if (!string.IsNullOrEmpty(values[36]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[36], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    SetupEnded = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    SetupEnded = "null";
                                }
                                if (!string.IsNullOrEmpty(values[7]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[7], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[8]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[8], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[9]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[9], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ClosedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ClosedTime = "null";
                                }
                                string singleRow = IncidentID + "," + Queue + "," + Mailbox + "," + Category + "," + Disposition + "," + ContactAttemptsMade + "," + FeedbackRating + ",'" + CreatedTime + "','" + UpdatedTime + "','" + ClosedTime + "'," + CreatedByAccount + "," + CampaignFormType + ",'" + ResolutionDueDate + "'," + MinistryAspect + "," + sourcechannel + ",'" + source + "'," + source_lvl1 + "," + source_lvl2 + "," + whitemail + "," + PrimaryContact + "," + AssignedTo + "," + Status_id + ",'" + Status + "','" + InternalSubject.Replace("'", "") + "','" + Reference + "','" + ChildReference + "'," + RingCentralWorkItem + "," + RingCentralWorkItemSkill + "," + RingCentralWorkItemStatus + ",'" + RingCentralWorkItemID + "','" + RingCentralOriginalEnquiryReference + "'," + RingCentralEnquiryIteration + "," + StatusType + ",'" + ResolveStarted + "','" + ResolveEnded + "','" + SetupStarted + "','" + SetupEnded + "'," + SupporterType + "," + NeedID;
                                Rows.Add(string.Format("({0})", singleRow));

                            }
                            updatingStartPoint = getDateTimeWhichProcessUpdatingInDB(Rows[0]);
                            updatingEndPoint = getDateTimeWhichProcessUpdatingInDB(Rows[count - 1]);
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");
                            sCommand.Replace("''s", "s");
                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SupporterEnquiries records updated successfully Between " + updatingStartPoint + " to " + updatingEndPoint);
                            updatingStartPoint = null;
                            updatingEndPoint = null;
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SupporterEnquiries - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                SendSMS("Error in SupporterEnquiries for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncEventTourDate_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get MiscEventTourDate data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT Misc.EventTourDate.ID,Misc.EventTourDate.ApproximateAudience,Misc.EventTourDate.Attendance,Misc.EventTourDate.ChildrenOnHoldFrom,Misc.EventTourDate.CreatedByAccount,Misc.EventTourDate.CreatedTime,Misc.EventTourDate.Date,Misc.EventTourDate.DonationsReceived,Misc.EventTourDate.Event,Misc.EventTourDate.EventManager,Misc.EventTourDate.ExpectedSponsorships,Misc.EventTourDate.NumberMonies,Misc.EventTourDate.NumberNoMonies,Misc.EventTourDate.NumberOfProfiles,Misc.EventTourDate.OrderOn,Misc.EventTourDate.OtherRequirements,Misc.EventTourDate.ProfileExpiryDate,Misc.EventTourDate.ReportedSponsorships,Misc.EventTourDate.SourceCode,Misc.EventTourDate.Status,Misc.EventTourDate.UpdatedByAccount,Misc.EventTourDate.UpdatedTime,Misc.EventTourDate.VenueAddress,Misc.EventTourDate.Type,Misc.EventTourDate.Organisation,Misc.EventTourDate.VenuePostcode,Misc.EventTourDate.Region  FROM Misc.EventTourDate";

                    string updatedRecordsFilter = " WHERE Misc.EventTourDate.CreatedTime >= '" + fromTime + "' OR Misc.EventTourDate.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Misc.EventTourDate.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;
                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " MiscEventTourDate records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO MiscEventTourDate");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("EventTourDateID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();

                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string singleRow = "";
                                string FilterRow = row.Replace("'", "");
                                List<string> values = FilterRow.Split('^').ToList();
                                string EventTourDateID = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string ApproximateAudience = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Attendance = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string ChildrenOnHoldFrom = !string.IsNullOrEmpty(values[3]) ? GetDateFromDateTime(values[3]) : "null";
                                string CreatedByAccount = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string CreatedTime = "";//5
                                string Date = !string.IsNullOrEmpty(values[6]) ? GetDateFromDateTime(values[6]) : "null";
                                string DonationsReceived = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string Event = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string EventManager = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string ExpectedSponsorships = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string NumberMonies = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string NumberNoMonies = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string NumberOfProfiles = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string OrderOn = !string.IsNullOrEmpty(values[14]) ? GetDateFromDateTime(values[14]) : "null";
                                string OtherRequirements = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string ProfileExpiryDate = !string.IsNullOrEmpty(values[16]) ? GetDateFromDateTime(values[16]) : "null";
                                string ReportedSponsorships = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string SourceCode = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string Status = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string UpdatedTime = "";//21
                                string VenueAddress = string.IsNullOrEmpty(values[22]) ? "null" : values[22];
                                string Type = string.IsNullOrEmpty(values[23]) ? "null" : values[23];
                                string Organisation = string.IsNullOrEmpty(values[24]) ? "null" : values[24];
                                string VenuePostcode = string.IsNullOrEmpty(values[25]) ? "null" : values[25];
                                string Region = string.IsNullOrEmpty(values[26]) ? "null" : values[26];

                                if (!string.IsNullOrEmpty(values[5]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[5], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[21]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[21], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                singleRow = EventTourDateID + "," + ApproximateAudience + "," + Attendance + ",'" + ChildrenOnHoldFrom + "'," + CreatedByAccount + ",'" + CreatedTime + "','" + Date + "'," + DonationsReceived + "," + Event + "," + EventManager + "," + ExpectedSponsorships + "," + NumberMonies + "," + NumberNoMonies + "," + NumberOfProfiles + ",'" + OrderOn + "','" + OtherRequirements + "','" + ProfileExpiryDate + "'," + ReportedSponsorships + ",'" + SourceCode + "'," + Status + "," + UpdatedByAccount + ",'" + UpdatedTime + "','" + VenueAddress + "'," + Type + "," + Organisation + ",'" + VenuePostcode + "'," + Region;
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " MiscEventTourDate records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncMiscEventTourDate - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncSchedChildDepartures_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SchedChildDepartures";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_CM.SchedChildDepartures.ID,SCBS_CM.SchedChildDepartures.BeneficiaryLifecycleEvent_ID,SCBS_CM.SchedChildDepartures.Beneficiary_GlobalID,SCBS_CM.SchedChildDepartures.DeathCategory,SCBS_CM.SchedChildDepartures.DeathSubcategory,SCBS_CM.SchedChildDepartures.Duplicate,SCBS_CM.SchedChildDepartures.EffectiveDate,SCBS_CM.SchedChildDepartures.FinalLetterSentToSponsor,SCBS_CM.SchedChildDepartures.Need,SCBS_CM.SchedChildDepartures.ReasonForRequest,SCBS_CM.SchedChildDepartures.RecordType,SCBS_CM.SchedChildDepartures.Reinstated,SCBS_CM.SchedChildDepartures.ScheduledDepartureDate,SCBS_CM.SchedChildDepartures.CreatedTime From SCBS_CM.SchedChildDepartures";

                    string updatedRecordsFilter = " WHERE SCBS_CM.SchedChildDepartures.CreatedTime >= '" + fromTime + "' OR SCBS_CM.SchedChildDepartures.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CM.SchedChildDepartures.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SchedChildDepartures records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO SchedChildDepartures");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("SchedChildDeparturesID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string SchedChildDeparturesID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string BeneficiaryLifecycleEvent_ID = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Beneficiary_GlobalID = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string DeathCategory = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string DeathSubcategory = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string Duplicate = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string EffectiveDate = !string.IsNullOrEmpty(values[6]) ? GetDateFromDateTime(values[6]) : "null";
                                string FinalLetterSentToSponsor = !string.IsNullOrEmpty(values[7]) ? GetDateFromDateTime(values[7]) : "null";
                                string Need = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string ReasonForRequest = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string RecordType = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string Reinstated = "";//11
                                string ScheduledDepartureDate = !string.IsNullOrEmpty(values[12]) ? GetDateFromDateTime(values[12]) : "null";
                                string CreatedTime = "";//13

                                if (!string.IsNullOrEmpty(values[11]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[11], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    Reinstated = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    Reinstated = "null";
                                }

                                if (!string.IsNullOrEmpty(values[13]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[13], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }
                                string singleRow = SchedChildDeparturesID + ",'" + BeneficiaryLifecycleEvent_ID + "','" + Beneficiary_GlobalID + "','" + DeathCategory + "','" + DeathSubcategory + "'," + Duplicate + ",'" + EffectiveDate + "','" + FinalLetterSentToSponsor + "'," + Need + ",'" + ReasonForRequest + "','" + RecordType + "','" + Reinstated + "','" + ScheduledDepartureDate + "','" + CreatedTime + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SchedChildDepartures records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncSchedChildDepartures - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncS2BCommunication_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get S2BCommunication data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_SUP.S2BCommunication.ID,SCBS_SUP.S2BCommunication.BatchID,SCBS_SUP.S2BCommunication.CommKitID,SCBS_SUP.S2BCommunication.CommunicationUpdateReceived,SCBS_SUP.S2BCommunication.CreatedByAccount,SCBS_SUP.S2BCommunication.CreatedTime,SCBS_SUP.S2BCommunication.DateProcessed,SCBS_SUP.S2BCommunication.DateScanned,SCBS_SUP.S2BCommunication.DocumentType,SCBS_SUP.S2BCommunication.FileURL,SCBS_SUP.S2BCommunication.ItemNotScannedEligable,SCBS_SUP.S2BCommunication.ItemNotScannedNotEligable,SCBS_SUP.S2BCommunication.MarkedForRework,SCBS_SUP.S2BCommunication.Need,SCBS_SUP.S2BCommunication.ReasonForRework,SCBS_SUP.S2BCommunication.ReworkComments,SCBS_SUP.S2BCommunication.SBCGlobalStatus,SCBS_SUP.S2BCommunication.Source,SCBS_SUP.S2BCommunication.SourceID,SCBS_SUP.S2BCommunication.SupporterGroup,SCBS_SUP.S2BCommunication.Type,SCBS_SUP.S2BCommunication.UpdatedByAccount,SCBS_SUP.S2BCommunication.UpdatedTime,SCBS_SUP.S2BCommunication.TemplateID,SCBS_SUP.S2BCommunication.TemplateName FROM SCBS_SUP.S2BCommunication";

                    string updatedRecordsFilter = " WHERE SCBS_SUP.S2BCommunication.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.S2BCommunication.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.S2BCommunication.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " S2BCommunication records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO S2BCommunication");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("S2BCommunicationID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string S2BCommunicationID = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string BatchID = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string CommKitID = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string CommunicationUpdateReceived = "";//3
                                string CreatedByAccount = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string CreatedTime = "";//5
                                string DateProcessed = !string.IsNullOrEmpty(values[6]) ? GetDateFromDateTime(values[6]) : "null";
                                string DateScanned = !string.IsNullOrEmpty(values[7]) ? GetDateFromDateTime(values[7]) : "null";
                                string DocumentType = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string FileURL = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string ItemNotScannedEligable = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string ItemNotScannedNotEligable = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string MarkedForRework = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string Need = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string ReasonForRework = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string ReworkComments = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string SBCGlobalStatus = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string Source = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string SourceID = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string SupporterGroup = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string Type = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[21]) ? "null" : values[21];
                                string UpdatedTime = "";//22
                                string TemplateID = string.IsNullOrEmpty(values[23]) ? "null" : values[23];
                                string TemplateName = string.IsNullOrEmpty(values[24]) ? "null" : values[24];


                                if (!string.IsNullOrEmpty(values[3]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[3], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CommunicationUpdateReceived = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CommunicationUpdateReceived = "null";
                                }

                                if (!string.IsNullOrEmpty(values[5]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[5], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[22]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[22], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }

                                string singleRow = S2BCommunicationID + ",'" + BatchID + "','" + CommKitID + "','" + CommunicationUpdateReceived + "'," + CreatedByAccount + ",'" + CreatedTime + "','" + DateProcessed + "','" + DateScanned + "','" + DocumentType + "','" + FileURL + "'," + ItemNotScannedEligable + "," + ItemNotScannedNotEligable + "," + MarkedForRework + "," + Need + ",'" + ReasonForRework + "','" + ReworkComments + "'," + SBCGlobalStatus + "," + Source + "," + SourceID + "," + SupporterGroup + "," + Type + "," + UpdatedByAccount + ",'" + UpdatedTime + "'," + TemplateID + ",'" + TemplateName + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " S2BCommunication records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in S2BCommunication - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncCommitment_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Need data for DWH Sync";

                do
                {
                    count = 0;


                    string Query = "SELECT SCBS_CM.Commitment.ID, SCBS_CM.Commitment.AcceptDate, SCBS_CM.Commitment.ApplicationCode, SCBS_CM.Commitment.BlackbaudID, SCBS_CM.Commitment.Campaign, SCBS_CM.Commitment.ChurchPartnership, SCBS_CM.Commitment.CompassionExperience, SCBS_CM.Commitment.CreatedTime, SCBS_CM.Commitment.DelinkReason, SCBS_CM.Commitment.DelinkType, SCBS_CM.Commitment.EndDate, SCBS_CM.Commitment.Event, SCBS_CM.Commitment.FirstFundedDate, SCBS_CM.Commitment.GlobalCommitmentID, SCBS_CM.Commitment.GlobalCorrespCommitmentID, SCBS_CM.Commitment.LinkReason, SCBS_CM.Commitment.LinkType, SCBS_CM.Commitment.LinkedToPartnership, SCBS_CM.Commitment.MarketingChannel, SCBS_CM.Commitment.MarketingCode, SCBS_CM.Commitment.Need, SCBS_CM.Commitment.Organisation, SCBS_CM.Commitment.ParentCommitmentID, SCBS_CM.Commitment.PaymentMethod, SCBS_CM.Commitment.RelationshipManager, SCBS_CM.Commitment.ResourceOrder, SCBS_CM.Commitment.Sequence, SCBS_CM.Commitment.SponsorshipAct, SCBS_CM.Commitment.SponsorshipPlus, SCBS_CM.Commitment.SponsorshipType, SCBS_CM.Commitment.StaffMemberName, SCBS_CM.Commitment.StartDate, SCBS_CM.Commitment.Supporter, SCBS_CM.Commitment.SupporterGroup, SCBS_CM.Commitment.Thread, SCBS_CM.Commitment.UpdatedByAccount, SCBS_CM.Commitment.UpdatedTime,SCBS_CM.Commitment.MarketingRegion,SCBS_CM.Commitment.SourceChannel,SCBS_CM.Commitment.CPPActivity FROM SCBS_CM.Commitment";

                    string updatedRecordsFilter = " WHERE SCBS_CM.Commitment.CreatedTime >= '" + fromTime + "' OR SCBS_CM.Commitment.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CM.Commitment.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Commitment records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO Commitments");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("CommitmentID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string CommitmentID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string AcceptDate = !string.IsNullOrEmpty(values[1]) ? GetStringInDateTimeFormat(values[1]) : "null";
                                string ApplicationCode = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string BlackbaudID = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string Campaign = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string ChurchPartnership = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string CompassionExperience = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string CreatedTime = "";//7
                                string DelinkReason = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string DelinkType = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string EndDate = !string.IsNullOrEmpty(values[10]) ? GetStringInDateTimeFormat(values[10]) : "null";
                                string Event = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string FirstFundedDate = !string.IsNullOrEmpty(values[12]) ? GetStringInDateTimeFormat(values[12]) : "null";
                                string GlobalCommitmentID = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string GlobalCorrespCommitmentID = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string LinkReason = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string LinkType = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string LinkedToPartnership = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string MarketingChannel = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string MarketingCode = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string Need = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string Organisation = string.IsNullOrEmpty(values[21]) ? "null" : values[21];
                                string ParentCommitmentID = string.IsNullOrEmpty(values[22]) ? "null" : values[22];
                                string PaymentMethod = string.IsNullOrEmpty(values[23]) ? "null" : values[23];
                                string RelationshipManager = string.IsNullOrEmpty(values[24]) ? "null" : values[24];
                                string ResourceOrder = string.IsNullOrEmpty(values[25]) ? "null" : values[25];
                                string Sequence = string.IsNullOrEmpty(values[26]) ? "null" : values[26];
                                string SponsorshipAct = string.IsNullOrEmpty(values[27]) ? "null" : values[27];
                                string SponsorshipPlus = string.IsNullOrEmpty(values[28]) ? "null" : values[28];
                                string SponsorshipType = string.IsNullOrEmpty(values[29]) ? "null" : values[29];
                                string StaffMemberName = string.IsNullOrEmpty(values[30]) ? "null" : values[30];
                                string StartDate = !string.IsNullOrEmpty(values[31]) ? GetStringInDateTimeFormat(values[31]) : "null";
                                string Supporter = string.IsNullOrEmpty(values[32]) ? "null" : values[32];
                                string SupporterGroup = string.IsNullOrEmpty(values[33]) ? "null" : values[33];
                                string Thread = string.IsNullOrEmpty(values[34]) ? "null" : values[34];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[35]) ? "null" : values[35];
                                string UpdatedTime = "";//36
                                string MarketingRegion = string.IsNullOrEmpty(values[37]) ? "null" : values[37];
                                string SourceChannel = string.IsNullOrEmpty(values[38]) ? "null" : values[38];
                                string CPPActivity = string.IsNullOrEmpty(values[39]) ? "null" : values[39];


                                if (!string.IsNullOrEmpty(values[7]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[7], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[36]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[36], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }

                                string singleRow = CommitmentID + ",'" + AcceptDate + "','" + ApplicationCode + "','" + BlackbaudID + "'," + Campaign + "," + ChurchPartnership + "," + CompassionExperience + ",'" + CreatedTime + "'," + DelinkReason + "," + DelinkType + ",'" + EndDate + "'," + Event + ",'" + FirstFundedDate + "','" + GlobalCommitmentID + "','" + GlobalCorrespCommitmentID + "'," + LinkReason + "," + LinkType + "," + LinkedToPartnership + "," + MarketingChannel + "," + MarketingCode + "," + Need + "," + Organisation + ",'" + ParentCommitmentID + "'," + PaymentMethod + "," + RelationshipManager + "," + ResourceOrder + "," + Sequence + "," + SponsorshipAct + "," + SponsorshipPlus + "," + SponsorshipType + ",'" + StaffMemberName + "','" + StartDate + "'," + Supporter + "," + SupporterGroup + "," + Thread + "," + UpdatedByAccount + ",'" + UpdatedTime + "'," + MarketingRegion + ",'" + SourceChannel + "'," + CPPActivity;
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Commitment records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncCommitment - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncCorrespondence_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Correspondence data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_SUP.Correspondence.ID,SCBS_SUP.Correspondence.CommKitID,SCBS_SUP.Correspondence.ConnectCreateStatus,SCBS_SUP.Correspondence.CreatedByAccount,SCBS_SUP.Correspondence.CreatedTime,SCBS_SUP.Correspondence.DateScanned,SCBS_SUP.Correspondence.DigitalLetterStatus,SCBS_SUP.Correspondence.Duplicate,SCBS_SUP.Correspondence.HoldLetter,SCBS_SUP.Correspondence.HoldLetterReason,SCBS_SUP.Correspondence.InternalLetterSent,SCBS_SUP.Correspondence.IsFinalLetter,SCBS_SUP.Correspondence.IsFinalLetterArchived,SCBS_SUP.Correspondence.IsOriginalLetterArchived,SCBS_SUP.Correspondence.IsOriginalMailed,SCBS_SUP.Correspondence.ItemNotScannedEligible,SCBS_SUP.Correspondence.ItemNotScannedNotEligible,SCBS_SUP.Correspondence.KingslineBatch,SCBS_SUP.Correspondence.KingslineDespatched,SCBS_SUP.Correspondence.KingslineDigitalLetterReceived,SCBS_SUP.Correspondence.KingslineDigitalURN,SCBS_SUP.Correspondence.KingslineLetterReceived,SCBS_SUP.Correspondence.KingslineScanned,SCBS_SUP.Correspondence.KingslineURN,SCBS_SUP.Correspondence.KingslineUpdated,SCBS_SUP.Correspondence.MarkedForRework,SCBS_SUP.Correspondence.Need,SCBS_SUP.Correspondence.NoAlertEmail,SCBS_SUP.Correspondence.NumberOfPages,SCBS_SUP.Correspondence.OldVersion,SCBS_SUP.Correspondence.OriginalLanguage,SCBS_SUP.Correspondence.PhysicalLetterStatus,SCBS_SUP.Correspondence.SBCTypes,SCBS_SUP.Correspondence.SupporterGroup,SCBS_SUP.Correspondence.Template,SCBS_SUP.Correspondence.TranslatedBy,SCBS_SUP.Correspondence.TranslationComplexity,SCBS_SUP.Correspondence.TranslationLanguage,SCBS_SUP.Correspondence.Type,SCBS_SUP.Correspondence.UpdatedByAccount,SCBS_SUP.Correspondence.UpdatedTime,SCBS_SUP.Correspondence.WrittenOnBeneficiaryBehalf FROM SCBS_SUP.Correspondence";

                    string updatedRecordsFilter = " WHERE SCBS_SUP.Correspondence.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.Correspondence.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.Correspondence.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Correspondence records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Correspondence");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("CorrespondenceID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string CorrespondenceID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string CommKitID = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string ConnectCreateStatus = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string CreatedByAccount = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string CreatedTime = "";//4
                                string DateScanned = !string.IsNullOrEmpty(values[5]) ? GetDateFromDateTime(values[5]) : "null";
                                string DigitalLetterStatus = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string Duplicate = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string HoldLetter = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string HoldLetterReason = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string InternalLetterSent = !string.IsNullOrEmpty(values[10]) ? GetDateFromDateTime(values[10]) : "null";
                                string IsFinalLetter = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string IsFinalLetterArchived = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string IsOriginalLetterArchived = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string IsOriginalMailed = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string ItemNotScannedEligible = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string ItemNotScannedNotEligible = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string KingslineBatch = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string KingslineDespatched = "";//18
                                string KingslineDigitalLetterReceived = "";//19
                                string KingslineDigitalURN = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string KingslineLetterReceived = "";//21
                                string KingslineScanned = "";//22
                                string KingslineURN = string.IsNullOrEmpty(values[23]) ? "null" : values[23];
                                string KingslineUpdated = "";//24
                                string MarkedForRework = string.IsNullOrEmpty(values[25]) ? "null" : values[25];
                                string Need = string.IsNullOrEmpty(values[26]) ? "null" : values[26];
                                string NoAlertEmail = string.IsNullOrEmpty(values[27]) ? "null" : values[27];
                                string NumberOfPages = string.IsNullOrEmpty(values[28]) ? "null" : values[28];
                                string OldVersion = string.IsNullOrEmpty(values[29]) ? "null" : values[29];
                                string OriginalLanguage = string.IsNullOrEmpty(values[30]) ? "null" : values[30];
                                string PhysicalLetterStatus = string.IsNullOrEmpty(values[31]) ? "null" : values[31];
                                string SBCTypes = string.IsNullOrEmpty(values[32]) ? "null" : values[32];
                                string SupporterGroup = string.IsNullOrEmpty(values[33]) ? "null" : values[33];
                                string Template = string.IsNullOrEmpty(values[34]) ? "null" : values[34];
                                string TranslatedBy = string.IsNullOrEmpty(values[35]) ? "null" : values[35];
                                string TranslationComplexity = string.IsNullOrEmpty(values[36]) ? "null" : values[36];
                                string TranslationLanguage = string.IsNullOrEmpty(values[37]) ? "null" : values[37];
                                string Type = string.IsNullOrEmpty(values[38]) ? "null" : values[38];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[39]) ? "null" : values[39];
                                string UpdatedTime = "";//40
                                string WrittenOnBeneficiaryBehalf = string.IsNullOrEmpty(values[41]) ? "null" : values[41];

                                if (!string.IsNullOrEmpty(values[4]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[4], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[40]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[40], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[18]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[18], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    KingslineDespatched = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    KingslineDespatched = "null";
                                }
                                if (!string.IsNullOrEmpty(values[19]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[19], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    KingslineDigitalLetterReceived = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    KingslineDigitalLetterReceived = "null";
                                }
                                if (!string.IsNullOrEmpty(values[21]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[21], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    KingslineLetterReceived = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    KingslineLetterReceived = "null";
                                }
                                if (!string.IsNullOrEmpty(values[22]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[22], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    KingslineScanned = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    KingslineScanned = "null";
                                }
                                if (!string.IsNullOrEmpty(values[24]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[24], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    KingslineUpdated = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    KingslineUpdated = "null";
                                }

                                string singleRow = CorrespondenceID + ",'" + CommKitID + "'," + ConnectCreateStatus + "," + CreatedByAccount + ",'" + CreatedTime + "','" + DateScanned + "'," + DigitalLetterStatus + "," + Duplicate + "," + HoldLetter + ",'" + HoldLetterReason + "','" + InternalLetterSent + "'," + IsFinalLetter + "," + IsFinalLetterArchived + "," + IsOriginalLetterArchived + "," + IsOriginalMailed + "," + ItemNotScannedEligible + "," + ItemNotScannedNotEligible + ",'" + KingslineBatch + "','" + KingslineDespatched + "','" + KingslineDigitalLetterReceived + "','" + KingslineDigitalURN + "','" + KingslineLetterReceived + "','" + KingslineScanned + "','" + KingslineURN + "','" + KingslineUpdated + "'," + MarkedForRework + "," + Need + "," + NoAlertEmail + ",'" + NumberOfPages + "'," + OldVersion + ",'" + OriginalLanguage + "'," + PhysicalLetterStatus + ",'" + SBCTypes + "'," + SupporterGroup + ",'" + Template + "','" + TranslatedBy + "','" + TranslationComplexity + "','" + TranslationLanguage + "'," + Type + "," + UpdatedByAccount + ",'" + UpdatedTime + "'," + WrittenOnBeneficiaryBehalf;

                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Correspondence records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncCorrespondence - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncResourceOrder_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;// 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get ResourceOrder data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "select SCBS_RM.ResourceOrder.ID,SCBS_RM.ResourceOrder.AmountRaised,SCBS_RM.ResourceOrder.AssignedTo,SCBS_RM.ResourceOrder.BudgetHolder,SCBS_RM.ResourceOrder.CPPActivity,SCBS_RM.ResourceOrder.CampaignCode,SCBS_RM.ResourceOrder.CompassionExperience,SCBS_RM.ResourceOrder.ConId,SCBS_RM.ResourceOrder.Country,SCBS_RM.ResourceOrder.CourierDeliveryStatus,SCBS_RM.ResourceOrder.CourierPodDateTime,SCBS_RM.ResourceOrder.CourierPodSignature,SCBS_RM.ResourceOrder.CourierScheduledDelivery,SCBS_RM.ResourceOrder.CreatedByAccount,SCBS_RM.ResourceOrder.CreatedTime,SCBS_RM.ResourceOrder.DespatchedForExternalDelivery,SCBS_RM.ResourceOrder.DespatchedForInternalDelivery,SCBS_RM.ResourceOrder.DisableAutoEmail,SCBS_RM.ResourceOrder.Enquiry,SCBS_RM.ResourceOrder.Event,SCBS_RM.ResourceOrder.EventCompleted,SCBS_RM.ResourceOrder.EventDate,SCBS_RM.ResourceOrder.Event_Manager,SCBS_RM.ResourceOrder.Event_Region,SCBS_RM.ResourceOrder.ExtReqDeliveryDate,SCBS_RM.ResourceOrder.ExtReqDeliveryTime,SCBS_RM.ResourceOrder.ExternalActDeliveryMethod,SCBS_RM.ResourceOrder.ExternalDelivered,SCBS_RM.ResourceOrder.ExternalDeliveryStatus,SCBS_RM.ResourceOrder.ExternalReqDeliveryMethod,SCBS_RM.ResourceOrder.ExternalTrackingNumber,SCBS_RM.ResourceOrder.ExtraValues,SCBS_RM.ResourceOrder.HoursTaken,SCBS_RM.ResourceOrder.Import_check,SCBS_RM.ResourceOrder.IntReqDeliveryDate,SCBS_RM.ResourceOrder.InternalActDeliveryMethod,SCBS_RM.ResourceOrder.InternalDelivered,SCBS_RM.ResourceOrder.InternalDeliveryStatus,SCBS_RM.ResourceOrder.InternalReqDeliveryMethod,SCBS_RM.ResourceOrder.InternalStatus,SCBS_RM.ResourceOrder.InternalTracking,SCBS_RM.ResourceOrder.InternalTrackingNumber,SCBS_RM.ResourceOrder.KinglineCreated,SCBS_RM.ResourceOrder.KingslineBookingDate,SCBS_RM.ResourceOrder.KingslineDeliveryType,SCBS_RM.ResourceOrder.KingslineFulfilled,SCBS_RM.ResourceOrder.KingslineID,SCBS_RM.ResourceOrder.KingslineReceived,SCBS_RM.ResourceOrder.KingslineTracking,SCBS_RM.ResourceOrder.MarketingCampaign,SCBS_RM.ResourceOrder.MarketingChannel,SCBS_RM.ResourceOrder.MarketingCode,SCBS_RM.ResourceOrder.MarketingManager,SCBS_RM.ResourceOrder.NumberAttended,SCBS_RM.ResourceOrder.OrderChecked,SCBS_RM.ResourceOrder.Org_IsCP,SCBS_RM.ResourceOrder.Org_Manager,SCBS_RM.ResourceOrder.Org_Region,SCBS_RM.ResourceOrder.Organisation,SCBS_RM.ResourceOrder.OwningTeam,SCBS_RM.ResourceOrder.Purpose,SCBS_RM.ResourceOrder.PurposeNotes,SCBS_RM.ResourceOrder.Region,SCBS_RM.ResourceOrder.ResolutionDate,SCBS_RM.ResourceOrder.RightNowTest121410,SCBS_RM.ResourceOrder.SubmittedForExternalDelivery,SCBS_RM.ResourceOrder.SubmittedForInternalDelivery,SCBS_RM.ResourceOrder.Supp_IsVolunteer,SCBS_RM.ResourceOrder.Supp_Manager,SCBS_RM.ResourceOrder.Supp_Region,SCBS_RM.ResourceOrder.Supporter,SCBS_RM.ResourceOrder.SupporterFeedback,SCBS_RM.ResourceOrder.UpdatedByAccount,SCBS_RM.ResourceOrder.UpdatedTime,SCBS_RM.ResourceOrder.EventTourDate from SCBS_RM.ResourceOrder ";

                    string updatedRecordsFilter = " WHERE SCBS_RM.ResourceOrder.CreatedTime >= '" + fromTime + "' OR SCBS_RM.ResourceOrder.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_RM.ResourceOrder.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + "  ResourceOrder records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO ResourceOrder");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("ResourceOrderID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string New_row = row.Replace("'", "");
                                List<string> values = New_row.Split('^').ToList();
                                string ResourceOrderID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string AmountRaised = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string AssignedTo = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string BudgetHolder = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string CPPActivity = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string CampaignCode = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string CompassionExperience = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string ConId = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string Country = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string CourierDeliveryStatus = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string CourierPodDateTime = "";//10
                                string CourierPodSignature = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string CourierScheduledDelivery = "";//12
                                string CreatedByAccount = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string CreatedTime = "";//14
                                string DespatchedForExternalDelivery = "";//15
                                string DespatchedForInternalDelivery = "";//16
                                string DisableAutoEmail = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string Enquiry = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string Event = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string EventCompleted = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string EventDate = !string.IsNullOrEmpty(values[21]) ? GetDateFromDateTime(values[21]) : "null";
                                string Event_Manager = string.IsNullOrEmpty(values[22]) ? "null" : values[22];
                                string Event_Region = string.IsNullOrEmpty(values[23]) ? "null" : values[23];
                                string ExtReqDeliveryDate = !string.IsNullOrEmpty(values[24]) ? GetDateFromDateTime(values[24]) : "null";
                                string ExtReqDeliveryTime = string.IsNullOrEmpty(values[25]) ? "null" : values[25];
                                string ExternalActDeliveryMethod = string.IsNullOrEmpty(values[26]) ? "null" : values[26];
                                string ExternalDelivered = "";//27
                                string ExternalDeliveryStatus = string.IsNullOrEmpty(values[28]) ? "null" : values[28];
                                string ExternalReqDeliveryMethod = string.IsNullOrEmpty(values[29]) ? "null" : values[29];
                                string ExternalTrackingNumber = string.IsNullOrEmpty(values[30]) ? "null" : values[30];
                                string ExtraValues = string.IsNullOrEmpty(values[31]) ? "null" : values[31];
                                string HoursTaken = string.IsNullOrEmpty(values[32]) ? "null" : values[32];
                                string Import_check = string.IsNullOrEmpty(values[33]) ? "null" : values[33];
                                string IntReqDeliveryDate = !string.IsNullOrEmpty(values[34]) ? GetDateFromDateTime(values[34]) : "null";
                                string InternalActDeliveryMethod = string.IsNullOrEmpty(values[35]) ? "null" : values[35];
                                string InternalDelivered = "";//36
                                string InternalDeliveryStatus = string.IsNullOrEmpty(values[37]) ? "null" : values[37];
                                string InternalReqDeliveryMethod = string.IsNullOrEmpty(values[38]) ? "null" : values[38];
                                string InternalStatus = string.IsNullOrEmpty(values[39]) ? "null" : values[39];
                                string InternalTracking = string.IsNullOrEmpty(values[40]) ? "null" : values[40];
                                string InternalTrackingNumber = string.IsNullOrEmpty(values[41]) ? "null" : values[41];
                                string KinglineCreated = "";//42
                                string KingslineBookingDate = "";//43
                                string KingslineDeliveryType = string.IsNullOrEmpty(values[44]) ? "null" : values[44];
                                string KingslineFulfilled = "";//45
                                string KingslineID = string.IsNullOrEmpty(values[46]) ? "null" : values[46];
                                string KingslineReceived = "";//47
                                string KingslineTracking = string.IsNullOrEmpty(values[48]) ? "null" : values[48];
                                string MarketingCampaign = string.IsNullOrEmpty(values[49]) ? "null" : values[49];
                                string MarketingChannel = string.IsNullOrEmpty(values[50]) ? "null" : values[50];
                                string MarketingCode = string.IsNullOrEmpty(values[51]) ? "null" : values[51];
                                string MarketingManager = string.IsNullOrEmpty(values[52]) ? "null" : values[52];
                                string NumberAttended = string.IsNullOrEmpty(values[53]) ? "null" : values[53];
                                string OrderChecked = string.IsNullOrEmpty(values[54]) ? "null" : values[54];
                                string Org_IsCP = string.IsNullOrEmpty(values[55]) ? "null" : values[55];
                                string Org_Manager = string.IsNullOrEmpty(values[56]) ? "null" : values[56];
                                string Org_Region = string.IsNullOrEmpty(values[57]) ? "null" : values[57];
                                string Organisation = string.IsNullOrEmpty(values[58]) ? "null" : values[58];
                                string OwningTeam = string.IsNullOrEmpty(values[59]) ? "null" : values[59];
                                string Purpose = string.IsNullOrEmpty(values[60]) ? "null" : values[60];
                                string PurposeNotes = string.IsNullOrEmpty(values[61]) ? "null" : values[61];
                                string Region = string.IsNullOrEmpty(values[62]) ? "null" : values[62];
                                string ResolutionDate = "";//63
                                string RightNowTest121410 = !string.IsNullOrEmpty(values[64]) ? GetDateFromDateTime(values[64]) : "null";
                                string SubmittedForExternalDelivery = "";//65
                                string SubmittedForInternalDelivery = "";//66
                                string Supp_IsVolunteer = string.IsNullOrEmpty(values[67]) ? "null" : values[67];
                                string Supp_Manager = string.IsNullOrEmpty(values[68]) ? "null" : values[68];
                                string Supp_Region = string.IsNullOrEmpty(values[69]) ? "null" : values[69];
                                string Supporter = string.IsNullOrEmpty(values[70]) ? "null" : values[70];
                                string SupporterFeedback = string.IsNullOrEmpty(values[71]) ? "null" : values[71];

                                //string Task = string.IsNullOrEmpty(values[72]) ? "null" : values[72];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[72]) ? "null" : values[72];
                                string UpdatedTime = "";//73
                                string EventTourDate = string.IsNullOrEmpty(values[74]) ? "null" : values[74];

                                if (!string.IsNullOrEmpty(values[14]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[14], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[73]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[73], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[10]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[10], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CourierPodDateTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CourierPodDateTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[12]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[12], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CourierScheduledDelivery = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CourierScheduledDelivery = "null";
                                }
                                if (!string.IsNullOrEmpty(values[15]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[15], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    DespatchedForExternalDelivery = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    DespatchedForExternalDelivery = "null";
                                }
                                if (!string.IsNullOrEmpty(values[16]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[16], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    DespatchedForInternalDelivery = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    DespatchedForInternalDelivery = "null";
                                }
                                if (!string.IsNullOrEmpty(values[27]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[27], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ExternalDelivered = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ExternalDelivered = "null";
                                }
                                if (!string.IsNullOrEmpty(values[42]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[42], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    KinglineCreated = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    KinglineCreated = "null";
                                }
                                if (!string.IsNullOrEmpty(values[43]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[43], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    KingslineBookingDate = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    KingslineBookingDate = "null";
                                }
                                if (!string.IsNullOrEmpty(values[45]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[45], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    KingslineFulfilled = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    KingslineFulfilled = "null";
                                }
                                if (!string.IsNullOrEmpty(values[47]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[47], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    KingslineReceived = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    KingslineReceived = "null";
                                }
                                if (!string.IsNullOrEmpty(values[63]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[63], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ResolutionDate = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ResolutionDate = "null";
                                }
                                if (!string.IsNullOrEmpty(values[65]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[65], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    SubmittedForExternalDelivery = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    SubmittedForExternalDelivery = "null";
                                }
                                if (!string.IsNullOrEmpty(values[66]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[66], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    SubmittedForInternalDelivery = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    SubmittedForInternalDelivery = "null";
                                }
                                if (!string.IsNullOrEmpty(values[36]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[36], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    InternalDelivered = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    InternalDelivered = "null";
                                }

                                string singleRow = ResourceOrderID + "," + AmountRaised + "," + AssignedTo + "," + BudgetHolder + "," + CPPActivity + "," + CampaignCode + "," + CompassionExperience + ",'" + ConId + "','" + Country + "','" + CourierDeliveryStatus + "','" + CourierPodDateTime + "','" + CourierPodSignature + "','" + CourierScheduledDelivery + "'," + CreatedByAccount + ",'" + CreatedTime + "','" + DespatchedForExternalDelivery + "','" + DespatchedForInternalDelivery + "'," + DisableAutoEmail + "," + Enquiry + "," + Event + "," + EventCompleted + ",'" + EventDate + "'," + Event_Manager + "," + Event_Region + ",'" + ExtReqDeliveryDate + "'," + ExtReqDeliveryTime + "," + ExternalActDeliveryMethod + ",'" + ExternalDelivered + "'," + ExternalDeliveryStatus + "," + ExternalReqDeliveryMethod + ",'" + ExternalTrackingNumber + "','" + ExtraValues + "'," + HoursTaken + "," + Import_check + ",'" + IntReqDeliveryDate + "'," + InternalActDeliveryMethod + ",'" + InternalDelivered + "'," + InternalDeliveryStatus + "," + InternalReqDeliveryMethod + "," + InternalStatus + ",'" + InternalTracking + "','" + InternalTrackingNumber + "','" + KinglineCreated + "','" + KingslineBookingDate + "','" + KingslineDeliveryType + "','" + KingslineFulfilled + "'," + KingslineID + ",'" + KingslineReceived + "','" + KingslineTracking + "'," + MarketingCampaign + "," + MarketingChannel + "," + MarketingCode + "," + MarketingManager + "," + NumberAttended + "," + OrderChecked + "," + Org_IsCP + "," + Org_Manager + "," + Org_Region + "," + Organisation + "," + OwningTeam + "," + Purpose + ",'" + PurposeNotes + "'," + Region + ",'" + ResolutionDate + "','" + RightNowTest121410 + "','" + SubmittedForExternalDelivery + "','" + SubmittedForInternalDelivery + "'," + Supp_IsVolunteer + "," + Supp_Manager + "," + Supp_Region + "," + Supporter + ",'" + SupporterFeedback + "'," + UpdatedByAccount + ",'" + UpdatedTime + "'," + EventTourDate;
                                Rows.Add(string.Format("({0})", singleRow));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " ResourceOrder records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in ResourceOrder - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncEvents_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Events  data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "select opportunity.ID,opportunity.Name,opportunity.Organization,opportunity.CustomFields.c.eventstatus,opportunity.CustomFields.c.eventstart,opportunity.CustomFields.c.eventend,opportunity.AssignedToAccount,opportunity.CustomFields.c.budgetholder,opportunity.Territory,opportunity.UpdatedTime,opportunity.CreatedTime,opportunity.CustomFields.standard.Region,opportunity.Customfields.SCBS_COMMON.Manager.name as Manager,opportunity.StageWithStrategy.stage as StageWithStrategy,opportunity.PrimaryContact.contact as Supporter,opportunity.customFields.Standard.ExpectedSponsorships,opportunity.customFields.Standard.ReportedSponsorships ,opportunity.customFields.c.agreement_expectedaudiencesize,opportunity.customFields.c.feedback_audiencesize,opportunity.customFields.c.feedback_whatworked,opportunity.customFields.c.feedback_whatdidnotwork,opportunity.customFields.Standard.TourEvent,opportunity.customFields.Standard.ProfileExpiry,opportunity.customFields.Standard.TotalSponsorships,opportunity.customFields.Standard.NumProfilesRequired,opportunity.customFields.Standard.ChildrenOnHoldFrom,opportunity.customFields.Standard.OrderOn,opportunity.customFields.Standard.WebSMSCampaignStart,opportunity.customFields.Standard.WebSMSCampaignEnd,opportunity.customFields.Standard.WebSMSProfilesRequired,opportunity.customFields.Standard.ChildRequirements,opportunity.customFields.Standard.EventManager,opportunity.customFields.Standard.ProjectManager,opportunity.customFields.Standard.RelationshipManager,opportunity.customFields.Standard.EventType,opportunity.customFields.Standard.NumBangtailsRequired,opportunity.customFields.Standard.CompassionExperience,opportunity.customFields.Standard.VolunteersRequired,opportunity.customFields.Standard.VolunteersTotalRequested AS VolunteersRequested,opportunity.customFields.Standard.VolunteersConfirmed from opportunity";

                    string updatedRecordsFilter = " where opportunity.CreatedTime >= '" + fromTime + "' OR opportunity.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY opportunity.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Events ...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Events ");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                {
                                    Columns.Add("EventID");
                                }
                                else if (data == "eventstatus")
                                {
                                    Columns.Add("Status");
                                }
                                else if (data == "eventstart")
                                {
                                    Columns.Add("EventStart");
                                }
                                else if (data == "eventend")
                                {
                                    Columns.Add("EventEnd");
                                }
                                else if (data == "AssignedToAccount")
                                {
                                    Columns.Add("Assigned");
                                }
                                else if (data == "budgetholder")
                                {
                                    Columns.Add("BudgetHolder");
                                }
                                else
                                {
                                    Columns.Add(data);
                                }
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string FilterRow = row.Replace("'", "");
                                List<string> values = FilterRow.Split('^').ToList();
                                string EventID = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string Name = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Organization = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string Status = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string EventStart = "";//4
                                string EventEnd = "";//5
                                string Assigned = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string BudgetHolder = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string Territory = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string UpdatedTime = "";//9
                                string CreatedTime = "";//10
                                string Region = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string Manager = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string StageWithStrategy = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string Supporter = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string ExpectedSponsorships = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string ReportedSponsorships = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string agreement_expectedaudiencesize = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string feedback_audiencesize = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string feedback_whatworked = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string feedback_whatdidnotwork = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string TourEvent = string.IsNullOrEmpty(values[21]) ? "null" : values[21];
                                string ProfileExpiry = !string.IsNullOrEmpty(values[22]) ? GetDateFromDateTime(values[22]) : "null";
                                string TotalSponsorships = string.IsNullOrEmpty(values[23]) ? "null" : values[23];
                                string NumProfilesRequired = string.IsNullOrEmpty(values[24]) ? "null" : values[24];
                                string ChildrenOnHoldFrom = !string.IsNullOrEmpty(values[25]) ? GetDateFromDateTime(values[25]) : "null";
                                string OrderOn = !string.IsNullOrEmpty(values[26]) ? GetDateFromDateTime(values[26]) : "null";
                                string WebSMSCampaignStart = !string.IsNullOrEmpty(values[27]) ? GetDateFromDateTime(values[27]) : "null";
                                string WebSMSCampaignEnd = !string.IsNullOrEmpty(values[28]) ? GetDateFromDateTime(values[28]) : "null";
                                string WebSMSProfilesRequired = string.IsNullOrEmpty(values[29]) ? "null" : values[29];
                                string ChildRequirements = string.IsNullOrEmpty(values[30]) ? "null" : values[30];
                                string EventManager = string.IsNullOrEmpty(values[31]) ? "null" : values[31];
                                string ProjectManager = string.IsNullOrEmpty(values[32]) ? "null" : values[32];
                                string RelationshipManager = string.IsNullOrEmpty(values[33]) ? "null" : values[33];
                                string EventType = string.IsNullOrEmpty(values[34]) ? "null" : values[34];
                                string NumBangtailsRequired = string.IsNullOrEmpty(values[35]) ? "null" : values[35];
                                string CompassionExperience = string.IsNullOrEmpty(values[36]) ? "null" : values[36];
                                string VolunteersRequired = string.IsNullOrEmpty(values[37]) ? "null" : values[37];
                                string VolunteersRequested = string.IsNullOrEmpty(values[38]) ? "null" : values[38];
                                string VolunteersConfirmed = string.IsNullOrEmpty(values[39]) ? "null" : values[39];
                                if (!string.IsNullOrEmpty(values[10]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[10], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[9]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[9], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[4]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[4], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    EventStart = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    EventStart = "null";
                                }
                                if (!string.IsNullOrEmpty(values[5]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[5], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    EventEnd = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    EventEnd = "null";
                                }

                                string singleRow = EventID + ",'" + Name + "'," + Organization + "," + Status + ",'" + EventStart + "','" + EventEnd + "'," + Assigned + "," + BudgetHolder + "," + Territory + ",'" + UpdatedTime + "','" + CreatedTime + "'," + Region + ",'" + Manager + "'," + StageWithStrategy + "," + Supporter + "," + ExpectedSponsorships + "," + ReportedSponsorships + "," + agreement_expectedaudiencesize + "," + feedback_audiencesize + ",'" + feedback_whatworked + "','" + feedback_whatdidnotwork + "'," + TourEvent + ",'" + ProfileExpiry + "'," + TotalSponsorships + "," + NumProfilesRequired + ",'" + ChildrenOnHoldFrom + "','" + OrderOn + "','" + WebSMSCampaignStart + "','" + WebSMSCampaignEnd + "'," + WebSMSProfilesRequired + ",'" + ChildRequirements + "'," + EventManager + "," + ProjectManager + "," + RelationshipManager + "," + EventType + "," + NumBangtailsRequired + "," + CompassionExperience + "," + VolunteersRequired + "," + VolunteersRequested + "," + VolunteersConfirmed + "";
                                Rows.Add(string.Format("({0})", singleRow));

                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Events records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Events - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncDonation_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Donation data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_DON.Donation.ID,SCBS_DON.Donation.Annual,SCBS_DON.Donation.AutoReceipt,SCBS_DON.Donation.CardPayment,SCBS_DON.Donation.CreatedByAccount,SCBS_DON.Donation.CreatedTime,SCBS_DON.Donation.DirectDebitPayment,SCBS_DON.Donation.PaymentCreateDate,SCBS_DON.Donation.Phone,SCBS_DON.Donation.Postal,SCBS_DON.Donation.Source,SCBS_DON.Donation.Supporter,SCBS_DON.Donation.SupporterGroup,SCBS_DON.Donation.TripID,SCBS_DON.Donation.UpdatedByAccount,SCBS_DON.Donation.UpdatedTime,SCBS_DON.Donation.WebUser,SCBS_DON.Donation.Website,SCBS_DON.Donation.WebsiteID FROM SCBS_DON.Donation";
                    string updatedRecordsFilter = " WHERE SCBS_DON.Donation.CreatedTime >= '" + fromTime + "' OR SCBS_DON.Donation.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_DON.Donation.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Donation records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Donation");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("DonationID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string DonationID = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string Annual = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string AutoReceipt = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string CardPayment = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string CreatedByAccount = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string CreatedTime = "";//5
                                string DirectDebitPayment = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string PaymentCreateDate = "";//7
                                string Phone = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string Postal = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string Source = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string Supporter = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string SupporterGroup = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string TripID = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string UpdatedTime = "";//15
                                string WebUser = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string Website = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string WebsiteID = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                if (!string.IsNullOrEmpty(values[5]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[5], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[15]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[15], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[7]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[7], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    PaymentCreateDate = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    PaymentCreateDate = "null";
                                }


                                string singleRow = DonationID + "," + Annual + "," + AutoReceipt + "," + CardPayment + "," + CreatedByAccount + ",'" + CreatedTime + "'," + DirectDebitPayment + ",'" + PaymentCreateDate + "'," + Phone + "," + Postal + "," + Source + "," + Supporter + "," + SupporterGroup + "," + TripID + "," + UpdatedByAccount + ",'" + UpdatedTime + "'," + WebUser + "," + Website + ",'" + WebsiteID + "'";

                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " Donation records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in Donation - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncCPPathwayActivity_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get CPPathway Activity data for DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT CPPathway.Activity.ID,CPPathway.Activity.AVAvailable,CPPathway.Activity.ActivityChurchContact,CPPathway.Activity.ActivityDate,CPPathway.Activity.ActivityDateBooked,CPPathway.Activity.ActivityLeader,CPPathway.Activity.ActivityNotes,CPPathway.Activity.ActivityType,CPPathway.Activity.ActivityVolunteer,CPPathway.Activity.ActivityYear,CPPathway.Activity.Activity_Assigned,CPPathway.Activity.Activity_ExpectedSponsorships,CPPathway.Activity.AdHocFollowUpDateCompleted,CPPathway.Activity.AdHocFollowUpDateDue,CPPathway.Activity.AdHocFollowUp_Assigned,CPPathway.Activity.AdHocFollowUp_ChchContact,CPPathway.Activity.AdHocFollowUp_Notes,CPPathway.Activity.AdHocFollowUp_Reminder,CPPathway.Activity.AutomateScheduledFollowUp,CPPathway.Activity.CExpBangtailsRequired,CPPathway.Activity.CExpChildRequirements,CPPathway.Activity.CExpConfirmed,CPPathway.Activity.CExpPendingApproval,CPPathway.Activity.CExpProfilesRequired,CPPathway.Activity.CExpSourceCode,CPPathway.Activity.ChildRequirements,CPPathway.Activity.ChildRequirementsNew,CPPathway.Activity.ChildrenOnHoldFrom,CPPathway.Activity.CompassionExperience,CPPathway.Activity.ContactEmail,CPPathway.Activity.CreatedByAccount,CPPathway.Activity.CreatedTime,CPPathway.Activity.NumBangtailsRequired,CPPathway.Activity.NumLeafletsRequired,CPPathway.Activity.NumProfilesRequired,CPPathway.Activity.OrderProfilesOn,CPPathway.Activity.Organisation,CPPathway.Activity.Outcome,CPPathway.Activity.PostActivity_Attendance,CPPathway.Activity.PostActivity_Comments,CPPathway.Activity.PostActivity_Monies,CPPathway.Activity.PostActivity_NoMonies,CPPathway.Activity.PostActivity_TotalSponsorships,CPPathway.Activity.PresentationRequired,CPPathway.Activity.PresentationSupplied,CPPathway.Activity.PresentationTime,CPPathway.Activity.PresentationType,CPPathway.Activity.ProfileExpiry,CPPathway.Activity.ScheduledFollowUp1_Assigned,CPPathway.Activity.ScheduledFollowUp1_ChchContact,CPPathway.Activity.ScheduledFollowUp1_Completed,CPPathway.Activity.ScheduledFollowUp1_Notes,CPPathway.Activity.ScheduledFollowUp1_Reminder,CPPathway.Activity.ScheduledFollowUp2_Assigned,CPPathway.Activity.ScheduledFollowUp2_ChchContact,CPPathway.Activity.ScheduledFollowUp2_Completed,CPPathway.Activity.ScheduledFollowUp2_Notes,CPPathway.Activity.ScheduledFollowUp2_Reminder,CPPathway.Activity.ScheduledFollowUp3_Assigned,CPPathway.Activity.ScheduledFollowUp3_ChchContact,CPPathway.Activity.ScheduledFollowUp3_Completed,CPPathway.Activity.ScheduledFollowUp3_Notes,CPPathway.Activity.ScheduledFollowUp3_Reminder,CPPathway.Activity.ScheduledFollowUp4_Assigned,CPPathway.Activity.ScheduledFollowUp4_ChchContact,CPPathway.Activity.ScheduledFollowUp4_Completed,CPPathway.Activity.ScheduledFollowUp4_Notes,CPPathway.Activity.ScheduledFollowUp4_Reminder,CPPathway.Activity.ServiceTime,CPPathway.Activity.SourceCode,CPPathway.Activity.UpdatedByAccount,CPPathway.Activity.UpdatedTime,CPPathway.Activity.VolunteerHours,CPPathway.Activity.WebSMSCampaignStart,CPPathway.Activity.WebSMSCampaignEnd,CPPathway.Activity.WebSMSRequired as WebSMSProfilesRequired,CPPathway.Activity.Outcome_UpdatedTime as OutcomeDateUpdated,CPPathway.Activity.Carols,CPPathway.Activity.Ambassador,CPPathway.Activity.AmbassadorAgreed FROM CPPathway.Activity ";

                    string updatedRecordsFilter = " WHERE CPPathway.Activity.CreatedTime >= '" + fromTime + "' OR CPPathway.Activity.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY CPPathway.Activity.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    //Query += updatedRecordsFilter + orderBy + limit;
                    Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "∧", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " CPPathwayActivity records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO CPPathwayActivity");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('∧'))
                            {
                                if (data == "ID")
                                    Columns.Add("ActivityID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string FilterRow = row.Replace("'", "");
                                List<string> values = FilterRow.Split('∧').ToList();
                                string ActivityID = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string AVAvailable = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string ActivityChurchContact = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string ActivityDate = "";//3
                                string ActivityDateBooked = "";//4
                                string ActivityLeader = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string ActivityNotes = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string ActivityType = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string ActivityVolunteer = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string ActivityYear = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string Activity_Assigned = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string Activity_ExpectedSponsorships = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string AdHocFollowUpDateCompleted = !string.IsNullOrEmpty(values[12]) ? GetDateFromDateTime(values[12]) : "null";
                                string AdHocFollowUpDateDue = !string.IsNullOrEmpty(values[13]) ? GetDateFromDateTime(values[13]) : "null";
                                string AdHocFollowUp_Assigned = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string AdHocFollowUp_ChchContact = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string AdHocFollowUp_Notes = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string AdHocFollowUp_Reminder = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string AutomateScheduledFollowUp = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string CExpBangtailsRequired = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string CExpChildRequirements = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string CExpConfirmed = "";//21
                                string CExpPendingApproval = string.IsNullOrEmpty(values[22]) ? "null" : values[22];
                                string CExpProfilesRequired = string.IsNullOrEmpty(values[23]) ? "null" : values[23];
                                string CExpSourceCode = string.IsNullOrEmpty(values[24]) ? "null" : values[24];
                                string ChildRequirements = string.IsNullOrEmpty(values[25]) ? "null" : values[25];
                                string ChildRequirementsNew = string.IsNullOrEmpty(values[26]) ? "null" : values[26];
                                string ChildrenOnHoldFrom = !string.IsNullOrEmpty(values[27]) ? GetDateFromDateTime(values[27]) : "null";
                                string CompassionExperience = string.IsNullOrEmpty(values[28]) ? "null" : values[28];
                                string ContactEmail = string.IsNullOrEmpty(values[29]) ? "null" : values[29];
                                string CreatedByAccount = string.IsNullOrEmpty(values[30]) ? "null" : values[30];
                                string CreatedTime = "";//31
                                string NumBangtailsRequired = string.IsNullOrEmpty(values[32]) ? "null" : values[32];
                                string NumLeafletsRequired = string.IsNullOrEmpty(values[33]) ? "null" : values[33];
                                string NumProfilesRequired = string.IsNullOrEmpty(values[34]) ? "null" : values[34];
                                string OrderProfilesOn = !string.IsNullOrEmpty(values[35]) ? GetDateFromDateTime(values[35]) : "null";
                                string Organisation = string.IsNullOrEmpty(values[36]) ? "null" : values[36];
                                string Outcome = string.IsNullOrEmpty(values[37]) ? "null" : values[37];
                                string PostActivity_Attendance = string.IsNullOrEmpty(values[38]) ? "null" : values[38];
                                string PostActivity_Comments = string.IsNullOrEmpty(values[39]) ? "null" : values[39];
                                string PostActivity_Monies = string.IsNullOrEmpty(values[40]) ? "null" : values[40];
                                string PostActivity_NoMonies = string.IsNullOrEmpty(values[41]) ? "null" : values[41];
                                string PostActivity_TotalSponsorships = string.IsNullOrEmpty(values[42]) ? "null" : values[42];
                                string PresentationRequired = string.IsNullOrEmpty(values[43]) ? "null" : values[43];
                                string PresentationSupplied = string.IsNullOrEmpty(values[44]) ? "null" : values[44];
                                string PresentationTime = string.IsNullOrEmpty(values[45]) ? "null" : values[45];
                                string PresentationType = string.IsNullOrEmpty(values[46]) ? "null" : values[46];
                                string ProfileExpiry = !string.IsNullOrEmpty(values[47]) ? GetDateFromDateTime(values[47]) : "null";

                                //string ReclassifyOrg = string.IsNullOrEmpty(values[48]) ? "null" : values[48];

                                string ScheduledFollowUp1_Assigned = string.IsNullOrEmpty(values[48]) ? "null" : values[48];
                                string ScheduledFollowUp1_ChchContact = string.IsNullOrEmpty(values[49]) ? "null" : values[49];
                                string ScheduledFollowUp1_Completed = !string.IsNullOrEmpty(values[50]) ? GetDateFromDateTime(values[50]) : "null";
                                string ScheduledFollowUp1_Notes = string.IsNullOrEmpty(values[51]) ? "null" : values[51];
                                string ScheduledFollowUp1_Reminder = string.IsNullOrEmpty(values[52]) ? "null" : values[52];
                                string ScheduledFollowUp2_Assigned = string.IsNullOrEmpty(values[53]) ? "null" : values[53];
                                string ScheduledFollowUp2_ChchContact = string.IsNullOrEmpty(values[54]) ? "null" : values[54];
                                string ScheduledFollowUp2_Completed = !string.IsNullOrEmpty(values[55]) ? GetDateFromDateTime(values[55]) : "null";
                                string ScheduledFollowUp2_Notes = string.IsNullOrEmpty(values[56]) ? "null" : values[56];
                                string ScheduledFollowUp2_Reminder = string.IsNullOrEmpty(values[57]) ? "null" : values[57];
                                string ScheduledFollowUp3_Assigned = string.IsNullOrEmpty(values[58]) ? "null" : values[58];
                                string ScheduledFollowUp3_ChchContact = string.IsNullOrEmpty(values[59]) ? "null" : values[59];
                                string ScheduledFollowUp3_Completed = !string.IsNullOrEmpty(values[60]) ? GetDateFromDateTime(values[60]) : "null";
                                string ScheduledFollowUp3_Notes = string.IsNullOrEmpty(values[61]) ? "null" : values[61];
                                string ScheduledFollowUp3_Reminder = string.IsNullOrEmpty(values[62]) ? "null" : values[62];
                                string ScheduledFollowUp4_Assigned = string.IsNullOrEmpty(values[63]) ? "null" : values[63];
                                string ScheduledFollowUp4_ChchContact = string.IsNullOrEmpty(values[64]) ? "null" : values[64];
                                string ScheduledFollowUp4_Completed = !string.IsNullOrEmpty(values[65]) ? GetDateFromDateTime(values[65]) : "null";
                                string ScheduledFollowUp4_Notes = string.IsNullOrEmpty(values[66]) ? "null" : values[66];
                                string ScheduledFollowUp4_Reminder = string.IsNullOrEmpty(values[67]) ? "null" : values[67];
                                string ServiceTime = string.IsNullOrEmpty(values[68]) ? "null" : values[68];
                                string SourceCode = string.IsNullOrEmpty(values[69]) ? "null" : values[69];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[70]) ? "null" : values[70];
                                string UpdatedTime = "";//71
                                string VolunteerHours = string.IsNullOrEmpty(values[72]) ? "null" : values[72];
                                string WebSMSCampaignStart = !string.IsNullOrEmpty(values[73]) ? GetDateFromDateTime(values[73]) : "null";
                                string WebSMSCampaignEnd = !string.IsNullOrEmpty(values[74]) ? GetDateFromDateTime(values[74]) : "null";
                                string WebSMSProfilesRequired = string.IsNullOrEmpty(values[75]) ? "null" : values[75];
                                string OutcomeDateUpdated = "";
                                string Carols = string.IsNullOrEmpty(values[77]) ? "null" : values[77];
                                string Ambassador = string.IsNullOrEmpty(values[78]) ? "null" : values[78];
                                string AmbassadorAgreed = string.IsNullOrEmpty(values[79]) ? "null" : values[79];

                                if (!string.IsNullOrEmpty(values[76]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[76], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    OutcomeDateUpdated = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    OutcomeDateUpdated = "null";
                                }


                                if (!string.IsNullOrEmpty(values[31]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[31], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[71]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[71], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[3]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[3], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ActivityDate = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ActivityDate = "null";
                                }
                                if (!string.IsNullOrEmpty(values[4]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[4], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ActivityDateBooked = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ActivityDateBooked = "null";
                                }
                                if (!string.IsNullOrEmpty(values[21]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[21], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CExpConfirmed = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CExpConfirmed = "null";
                                }

                                string singleRow = ActivityID + "," + AVAvailable + "," + ActivityChurchContact + ",'" + ActivityDate + "','" + ActivityDateBooked + "'," + ActivityLeader + ",'" + ActivityNotes + "'," + ActivityType + "," + ActivityVolunteer + "," + ActivityYear + "," + Activity_Assigned + "," + Activity_ExpectedSponsorships + ",'" + AdHocFollowUpDateCompleted + "','" + AdHocFollowUpDateDue + "'," + AdHocFollowUp_Assigned + "," + AdHocFollowUp_ChchContact + ",'" + AdHocFollowUp_Notes + "'," + AdHocFollowUp_Reminder + "," + AutomateScheduledFollowUp + "," + CExpBangtailsRequired + ",'" + CExpChildRequirements + "','" + CExpConfirmed + "'," + CExpPendingApproval + "," + CExpProfilesRequired + ",'" + CExpSourceCode + "','" + ChildRequirements + "','" + ChildRequirementsNew + "','" + ChildrenOnHoldFrom + "'," + CompassionExperience + ",'" + ContactEmail + "'," + CreatedByAccount + ",'" + CreatedTime + "'," + NumBangtailsRequired + "," + NumLeafletsRequired + "," + NumProfilesRequired + ",'" + OrderProfilesOn + "'," + Organisation + "," + Outcome + "," + PostActivity_Attendance + ",'" + PostActivity_Comments + "'," + PostActivity_Monies + "," + PostActivity_NoMonies + "," + PostActivity_TotalSponsorships + "," + PresentationRequired + "," + PresentationSupplied + ",'" + PresentationTime + "'," + PresentationType + ",'" + ProfileExpiry + "'," + ScheduledFollowUp1_Assigned + "," + ScheduledFollowUp1_ChchContact + ",'" + ScheduledFollowUp1_Completed + "','" + ScheduledFollowUp1_Notes + "'," + ScheduledFollowUp1_Reminder + "," + ScheduledFollowUp2_Assigned + "," + ScheduledFollowUp2_ChchContact + ",'" + ScheduledFollowUp2_Completed + "','" + ScheduledFollowUp2_Notes + "'," + ScheduledFollowUp2_Reminder + "," + ScheduledFollowUp3_Assigned + "," + ScheduledFollowUp3_ChchContact + ",'" + ScheduledFollowUp3_Completed + "','" + ScheduledFollowUp3_Notes + "'," + ScheduledFollowUp3_Reminder + "," + ScheduledFollowUp4_Assigned + "," + ScheduledFollowUp4_ChchContact + ",'" + ScheduledFollowUp4_Completed + "','" + ScheduledFollowUp4_Notes + "'," + ScheduledFollowUp4_Reminder + ",'" + ServiceTime + "','" + SourceCode + "'," + UpdatedByAccount + ",'" + UpdatedTime + "'," + VolunteerHours + ",'" + WebSMSCampaignStart + "','" + WebSMSCampaignEnd + "'," + WebSMSProfilesRequired + ",'" + OutcomeDateUpdated + "'," + Carols + "," + Ambassador + "," + AmbassadorAgreed + "";
                                Rows.Add(string.Format("({0})", singleRow));


                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " CPPathwayActivity records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in sync SyncCPPathwayActivity_Landing - FromTime : " + fromTime + ", StartLimit : " + startLimit);

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncBeneficiaryOrICPHold_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get BeneficiaryOrICPHold";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_CHILD.BeneficiaryOrICPHold.ID,SCBS_CHILD.BeneficiaryOrICPHold.BeneficiaryReinstatementReason,SCBS_CHILD.BeneficiaryOrICPHold.Beneficiary_GlobalID,SCBS_CHILD.BeneficiaryOrICPHold.Beneficiary_LocalID,SCBS_CHILD.BeneficiaryOrICPHold.Channel_Name,SCBS_CHILD.BeneficiaryOrICPHold.CreatedByAccount,SCBS_CHILD.BeneficiaryOrICPHold.CreatedTime,SCBS_CHILD.BeneficiaryOrICPHold.EstimatedNoMoneyYieldRate,SCBS_CHILD.BeneficiaryOrICPHold.HoldChannel,SCBS_CHILD.BeneficiaryOrICPHold.HoldEndDate,SCBS_CHILD.BeneficiaryOrICPHold.HoldID,SCBS_CHILD.BeneficiaryOrICPHold.HoldStatus,SCBS_CHILD.BeneficiaryOrICPHold.HoldType,SCBS_CHILD.BeneficiaryOrICPHold.NeedRNID,SCBS_CHILD.BeneficiaryOrICPHold.NotificationReason,SCBS_CHILD.BeneficiaryOrICPHold.PrimaryHoldOwner,SCBS_CHILD.BeneficiaryOrICPHold.ProcessEnquiry,SCBS_CHILD.BeneficiaryOrICPHold.ProjectRNID,SCBS_CHILD.BeneficiaryOrICPHold.ReleasedDate,SCBS_CHILD.BeneficiaryOrICPHold.ReservationID,SCBS_CHILD.BeneficiaryOrICPHold.ReservationType_Name,SCBS_CHILD.BeneficiaryOrICPHold.Reservation_RNID,SCBS_CHILD.BeneficiaryOrICPHold.ResourceOrder,SCBS_CHILD.BeneficiaryOrICPHold.SecondaryHoldOwner,SCBS_CHILD.BeneficiaryOrICPHold.SourceCode,SCBS_CHILD.BeneficiaryOrICPHold.SupporterGroupRNID,SCBS_CHILD.BeneficiaryOrICPHold.UpdatedByAccount,SCBS_CHILD.BeneficiaryOrICPHold.UpdatedTime FROM SCBS_CHILD.BeneficiaryOrICPHold";

                    string updatedRecordsFilter = " WHERE SCBS_CHILD.BeneficiaryOrICPHold.CreatedTime >= '" + fromTime + "' OR SCBS_CHILD.BeneficiaryOrICPHold.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CHILD.BeneficiaryOrICPHold.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " BeneficiaryOrICPHold records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO BeneficiaryOrICPHold");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("BeneficiaryOrICPHold_ID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string FilterRow = row.Replace("'", "");
                                List<string> values = FilterRow.Split('^').ToList();
                                string BeneficiaryOrICPHold_ID = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string BeneficiaryReinstatementReason = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Beneficiary_GlobalID = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string Beneficiary_LocalID = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string Channel_Name = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string CreatedByAccount = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string CreatedTime = "";//6
                                string EstimatedNoMoneyYieldRate = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string HoldChannel = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string HoldEndDate = "";//9
                                string HoldID = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string HoldStatus = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string HoldType = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string NeedRNID = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string NotificationReason = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string PrimaryHoldOwner = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string ProcessEnquiry = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string ProjectRNID = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string ReleasedDate = "";//18
                                string ReservationID = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string ReservationType_Name = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string Reservation_RNID = string.IsNullOrEmpty(values[21]) ? "null" : values[21];
                                string ResourceOrder = string.IsNullOrEmpty(values[22]) ? "null" : values[22];
                                string SecondaryHoldOwner = string.IsNullOrEmpty(values[23]) ? "null" : values[23];
                                string SourceCode = string.IsNullOrEmpty(values[24]) ? "null" : values[24];
                                string SupporterGroupRNID = string.IsNullOrEmpty(values[25]) ? "null" : values[25];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[26]) ? "null" : values[26];
                                string UpdatedTime = "";//27

                                if (!string.IsNullOrEmpty(values[6]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[6], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[27]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[27], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[18]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[18], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    ReleasedDate = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    ReleasedDate = "null";
                                }
                                if (!string.IsNullOrEmpty(values[9]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[9], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    HoldEndDate = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    HoldEndDate = "null";
                                }


                                string singleRow = BeneficiaryOrICPHold_ID + ",'" + BeneficiaryReinstatementReason + "','" + Beneficiary_GlobalID + "','" + Beneficiary_LocalID + "','" + Channel_Name + "'," + CreatedByAccount + ",'" + CreatedTime + "','" + EstimatedNoMoneyYieldRate + "'," + HoldChannel + ",'" + HoldEndDate + "','" + HoldID + "'," + HoldStatus + "," + HoldType + ",'" + NeedRNID + "','" + NotificationReason + "','" + PrimaryHoldOwner + "'," + ProcessEnquiry + "," + ProjectRNID + ",'" + ReleasedDate + "','" + ReservationID + "','" + ReservationType_Name + "'," + Reservation_RNID + "," + ResourceOrder + ",'" + SecondaryHoldOwner + "','" + SourceCode + "'," + SupporterGroupRNID + "," + UpdatedByAccount + ",'" + UpdatedTime + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " BeneficiaryOrICPHold records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncBeneficiaryOrICPHold - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncDelinkType(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get DelinkType ";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_CM.DelinkType.ID,SCBS_CM.DelinkType.Code,SCBS_CM.DelinkType.Description FROM SCBS_CM.DelinkType ";

                    string updatedRecordsFilter = " WHERE SCBS_CM.DelinkType.CreatedTime >= '" + fromTime + "' OR SCBS_CM.DelinkType.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CM.DelinkType.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;
                    //Query += updatedRecordsFilter + orderBy + limit;
                    Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " DelinkType records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO DelinkType");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();

                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string singleRow = "";
                                List<string> values = row.Split('^').ToList();
                                string ID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string Code = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Description = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                singleRow = ID + ",'" + Code + "','" + Description + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " DelinkType records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in DelinkType - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncLinkType(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get LinkType";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_CM.LinkType.ID,SCBS_CM.LinkType.Code,SCBS_CM.LinkType.Description FROM SCBS_CM.LinkType";

                    string updatedRecordsFilter = " WHERE SCBS_CM.LinkType.CreatedTime >= '" + fromTime + "' OR SCBS_CM.LinkType.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CM.LinkType.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;
                    //Query += updatedRecordsFilter + orderBy + limit;
                    Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " LinkType records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO LinkType");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();

                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string singleRow = "";
                                List<string> values = row.Split('^').ToList();
                                string ID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string Code = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Description = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                singleRow = ID + ",'" + Code + "','" + Description + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " LinkType records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in LinkType - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncBeneficiaryTransfer_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get BeneficiaryTransfer DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_CM.BeneficiaryTransfer.ID,SCBS_CM.BeneficiaryTransfer.ReasonForRequest,SCBS_CM.BeneficiaryTransfer.RecordType,SCBS_CM.BeneficiaryTransfer.ExpectedArrivalDate,SCBS_CM.BeneficiaryTransfer.NewBeneficiaryLocalNumber,SCBS_CM.BeneficiaryTransfer.NewICPID,SCBS_CM.BeneficiaryTransfer.NewICPName,SCBS_CM.BeneficiaryTransfer.OtherReasonForTransfer,SCBS_CM.BeneficiaryTransfer.BeneficiaryTransitionType,SCBS_CM.BeneficiaryTransfer.Beneficiary_GlobalID,SCBS_CM.BeneficiaryTransfer.Beneficiary_LocalID,SCBS_CM.BeneficiaryTransfer.CurrentICP,SCBS_CM.BeneficiaryTransfer.NewProgram,SCBS_CM.BeneficiaryTransfer.PreviouslyActiveProgram,SCBS_CM.BeneficiaryTransfer.BeneficiaryExit_EffectiveDate,SCBS_CM.BeneficiaryTransfer.BeneficiaryExit_ReasonForExit,SCBS_CM.BeneficiaryTransfer.TransferType,SCBS_CM.BeneficiaryTransfer.Need,SCBS_CM.BeneficiaryTransfer.BeneficiaryLifecycleEvent_ID,SCBS_CM.BeneficiaryTransfer.BenReinstatement_OtherReason,SCBS_CM.BeneficiaryTransfer.Duplicate,SCBS_CM.BeneficiaryTransfer.CreatedTime,SCBS_CM.BeneficiaryTransfer.CreatedByAccount,SCBS_CM.BeneficiaryTransfer.UpdatedTime,SCBS_CM.BeneficiaryTransfer.UpdatedByAccount FROM SCBS_CM.BeneficiaryTransfer";

                    string updatedRecordsFilter = " where SCBS_CM.BeneficiaryTransfer.CreatedTime >= '" + fromTime + "' OR SCBS_CM.BeneficiaryTransfer.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CM.BeneficiaryTransfer.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " BeneficiaryTransfer records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO BeneficiaryTransfer");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("BeneficiaryTransferID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Replace("'", "").Split('^').ToList();
                                string BeneficiaryTransferID = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string ReasonForRequest = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string RecordType = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string ExpectedArrivalDate = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string NewBeneficiaryLocalNumber = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string NewICPID = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string NewICPName = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string OtherReasonForTransfer = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string BeneficiaryTransitionType = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string Beneficiary_GlobalID = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string Beneficiary_LocaID = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string CurrentICP = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string NewProgram = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string PreviouslyActiveProgram = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string BeneficiaryExit_EffectiveDate = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string BeneficiaryExit_ReasonForExit = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string TransferType = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string Need = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string BeneficiaryLifecycleEvent_ID = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string BenReinstatement_OtherReason = string.IsNullOrEmpty(values[19]) ? "null" : values[19];
                                string Duplicate = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string CreatedTime = "";//21
                                string CreatedByAccount = string.IsNullOrEmpty(values[22]) ? "null" : values[22];
                                string UpdatedTime = "";//23
                                string UpdatedByAccount = string.IsNullOrEmpty(values[24]) ? "null" : values[24];

                                if (!string.IsNullOrEmpty(values[21]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[21], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }
                                if (!string.IsNullOrEmpty(values[23]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[23], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }
                                string singleRow = "" + BeneficiaryTransferID + ",'" + ReasonForRequest + "','" + RecordType + "','" + ExpectedArrivalDate + "','" + NewBeneficiaryLocalNumber + "','" + NewICPID + "','" + NewICPName + "','" + OtherReasonForTransfer + "','" + BeneficiaryTransitionType + "','" + Beneficiary_GlobalID + "','" + Beneficiary_LocaID + "','" + CurrentICP + "','" + NewProgram + "','" + PreviouslyActiveProgram + "','" + BeneficiaryExit_EffectiveDate + "','" + BeneficiaryExit_ReasonForExit + "','" + TransferType + "'," + Need + ",'" + BeneficiaryLifecycleEvent_ID + "','" + BenReinstatement_OtherReason + "'," + Duplicate + ",'" + CreatedTime + "'," + CreatedByAccount + ",'" + UpdatedTime + "'," + UpdatedByAccount + "";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " BeneficiaryTransfer records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in BeneficiaryTransfer - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncEncounterAttendance_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get data for DWH Sync";

                do
                {
                    count = 0;


                    string Query = "SELECT Encounter.Attendance.ID,Encounter.Attendance.IsSupporter,Encounter.Attendance.Organisation,Encounter.Attendance.PassportCopyReceived,Encounter.Attendance.PhotoReceived,Encounter.Attendance.Status,Encounter.Attendance.Supporter,Encounter.Attendance.UpdatedByAccount,Encounter.Attendance.UpdatedTime,Encounter.Attendance.Visit,Encounter.Attendance.ApplicationFormReceived,Encounter.Attendance.ChildProtectionFormReceived,Encounter.Attendance.CreatedByAccount,Encounter.Attendance.CreatedTime,Encounter.Attendance.DBSReceived,Encounter.Attendance.EmergencyContactsReceived,Encounter.Attendance.ExpensesPaid,Encounter.Attendance.MetChild,Encounter.Attendance.Type,Encounter.Attendance.Need FROM Encounter.Attendance";

                    string updatedRecordsFilter = " WHERE Encounter.Attendance.CreatedTime >= '" + fromTime + "' OR Encounter.Attendance.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Encounter.Attendance.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Encounter Attendance records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO EncounterAttendance");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("AttendanceID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string AttendanceID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string IsSupporter = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Organisation = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string PassportCopyReceived = string.IsNullOrEmpty(values[3]) ? "null" : GetDateFromDateTime(values[3]);
                                string PhotoReceived = string.IsNullOrEmpty(values[4]) ? "null" : GetDateFromDateTime(values[4]);
                                string Status = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string Supporter = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string UpdatedTime = "";
                                string Visit = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string ApplicationFormReceived = string.IsNullOrEmpty(values[10]) ? "null" : GetDateFromDateTime(values[10]);
                                string ChildProtectionFormReceived = string.IsNullOrEmpty(values[11]) ? "null" : GetDateFromDateTime(values[11]);
                                string CreatedByAccount = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string CreatedTime = "";
                                string DBSReceived = string.IsNullOrEmpty(values[14]) ? "null" : GetDateFromDateTime(values[14]);
                                string EmergencyContactsReceived = string.IsNullOrEmpty(values[15]) ? "null" : GetDateFromDateTime(values[15]);
                                string ExpensesPaid = string.IsNullOrEmpty(values[16]) ? "null" : GetDateFromDateTime(values[16]);
                                string MetChild = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string Type = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string Need = string.IsNullOrEmpty(values[19]) ? "null" : values[19];

                                if (!string.IsNullOrEmpty(values[13]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[13], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[8]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[8], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }

                                string singleRow = "" + AttendanceID + "," + IsSupporter + "," + Organisation + ",'" + PassportCopyReceived + "','" + PhotoReceived + "'," + Status + "," + Supporter + "," + UpdatedByAccount + ",'" + UpdatedTime + "'," + Visit + ",'" + ApplicationFormReceived + "','" + ChildProtectionFormReceived + "'," + CreatedByAccount + ",'" + CreatedTime + "','" + DBSReceived + "','" + EmergencyContactsReceived + "','" + ExpensesPaid + "'," + MetChild + "," + Type + "," + Need + "";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " EncounterAttendance records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncEncounterAttendance - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncEncounterVisit_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get data for DWH Sync";

                do
                {
                    count = 0;
                    string Query = "SELECT Encounter.Visit.ID,Encounter.Visit.Intervention,Encounter.Visit.MeetGraduates,Encounter.Visit.Need,Encounter.Visit.OfficeVisit,Encounter.Visit.Project,Encounter.Visit.ProjectDay,Encounter.Visit.ProposedVisitDate,Encounter.Visit.TripLeader,Encounter.Visit.TripPlanner,Encounter.Visit.Type,Encounter.Visit.UpdatedByAccount,Encounter.Visit.UpdatedTime,Encounter.Visit.VisitEndDate,Encounter.Visit.VisitName,Encounter.Visit.VisitStartDate,Encounter.Visit.AverageCostPerPerson,Encounter.Visit.ChildSurvival,Encounter.Visit.Church,Encounter.Visit.ConfirmedWithField,Encounter.Visit.Country,Encounter.Visit.CreatedByAccount,Encounter.Visit.CreatedTime,Encounter.Visit.FieldRequested,Encounter.Visit.FunDay,Encounter.Visit.DeliveryMethod FROM Encounter.Visit";

                    string updatedRecordsFilter = " WHERE Encounter.Visit.CreatedTime >= '" + fromTime + "' OR Encounter.Visit.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Encounter.Visit.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " Encounter Visit records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO EncounterVisit");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                if (data == "ID")
                                    Columns.Add("VisitID");
                                else
                                    Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string ID = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string Intervention = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string MeetGraduates = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string Need = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string OfficeVisit = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string Project = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string ProjectDay = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string ProposedVisitDate = string.IsNullOrEmpty(values[7]) ? "null" : GetDateFromDateTime(values[7]);
                                string TripLeader = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string TripPlanner = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string Type = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string UpdatedTime = "";//12
                                string VisitEndDate = string.IsNullOrEmpty(values[13]) ? "null" : GetDateFromDateTime(values[13]);
                                string VisitName = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string VisitStartDate = string.IsNullOrEmpty(values[15]) ? "null" : GetDateFromDateTime(values[15]);
                                string AverageCostPerPerson = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string ChildSurvival = string.IsNullOrEmpty(values[17]) ? "null" : values[17];
                                string Church = string.IsNullOrEmpty(values[18]) ? "null" : values[18];
                                string ConfirmedWithField = string.IsNullOrEmpty(values[19]) ? "null" : GetDateFromDateTime(values[19]);
                                string Country = string.IsNullOrEmpty(values[20]) ? "null" : values[20];
                                string CreatedByAccount = string.IsNullOrEmpty(values[21]) ? "null" : values[21];
                                string CreatedTime = "";//22
                                string FieldRequested = string.IsNullOrEmpty(values[23]) ? "null" : GetDateFromDateTime(values[23]);
                                string FunDay = string.IsNullOrEmpty(values[24]) ? "null" : values[24];
                                string DeliveryMethod = string.IsNullOrEmpty(values[25]) ? "null" : values[25];

                                if (!string.IsNullOrEmpty(values[22]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[22], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[12]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[12], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }

                                string singleRow = "" + ID + "," + Intervention + "," + MeetGraduates + "," + Need + "," + OfficeVisit + "," + Project + "," + ProjectDay + ",'" + ProposedVisitDate + "'," + TripLeader + "," + TripPlanner + "," + Type + "," + UpdatedByAccount + ",'" + UpdatedTime + "','" + VisitEndDate + "','" + VisitName.Replace("'", "") + "','" + VisitStartDate + "'," + AverageCostPerPerson + "," + ChildSurvival + "," + Church + ",'" + ConfirmedWithField + "'," + Country + "," + CreatedByAccount + ",'" + CreatedTime + "','" + FieldRequested + "'," + FunDay + "," + DeliveryMethod + "";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " EncounterVisit records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncEncounterVisit - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncActionHistory_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get data for DWH Sync";

                do
                {
                    count = 0;


                    string Query = "SELECT Misc.ActionHistory.ID,Misc.ActionHistory.ProcessingID,Misc.ActionHistory.ActionID,Misc.ActionHistory.ActionName,Misc.ActionHistory.CallRating,Misc.ActionHistory.CancelledOnCall,Misc.ActionHistory.CellID,Misc.ActionHistory.CellName,Misc.ActionHistory.CommitmentID,Misc.ActionHistory.CreatedByAccount,Misc.ActionHistory.CreatedTime,Misc.ActionHistory.DateContacted,Misc.ActionHistory.DateSelected,Misc.ActionHistory.ModelDecile,Misc.ActionHistory.NoSubOnCall,Misc.ActionHistory.SupporterID,Misc.ActionHistory.UpdatedByAccount,Misc.ActionHistory.UpdatedTime FROM Misc.ActionHistory";

                    string updatedRecordsFilter = " WHERE Misc.ActionHistory.CreatedTime >= '" + fromTime + "' OR Misc.ActionHistory.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Misc.ActionHistory.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " ActionHistory records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO ActionHistory");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                List<string> values = row.Split('^').ToList();
                                string ID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string ProcessingID = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string ActionID = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string ActionName = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string CallRating = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string CancelledOnCall = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                string CellID = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string CellName = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string CommitmentID = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string CreatedByAccount = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string CreatedTime = "";//10
                                string DateContacted = string.IsNullOrEmpty(values[11]) ? "null" : GetDateFromDateTime(values[11]);
                                string DateSelected = string.IsNullOrEmpty(values[12]) ? "null" : GetDateFromDateTime(values[12]);
                                string ModelDecile = string.IsNullOrEmpty(values[13]) ? "null" : values[13];
                                string NoSubOnCall = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string SupporterID = string.IsNullOrEmpty(values[15]) ? "null" : values[15];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[16]) ? "null" : values[16];
                                string UpdatedTime = "";//17
                                if (!string.IsNullOrEmpty(values[10]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[10], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[17]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[17], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }

                                string singleRow = "" + ID + "," + ProcessingID + ",'" + ActionID + "','" + ActionName + "'," + CallRating + "," + CancelledOnCall + "," + CellID + ",'" + CellName + "'," + CommitmentID + "," + CreatedByAccount + ",'" + CreatedTime + "','" + DateContacted + "','" + DateSelected + "'," + ModelDecile + "," + NoSubOnCall + "," + SupporterID + "," + UpdatedByAccount + ",'" + UpdatedTime + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " ActionHistory records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncActionHistory  - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncProjectOrgLinks_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get ProjectOrgLinks";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_SUP.ProjectOrgLinks.ID as ProjectOrgLinkID,SCBS_SUP.ProjectOrgLinks.Active,SCBS_SUP.ProjectOrgLinks.Organisation,SCBS_SUP.ProjectOrgLinks.Project,SCBS_SUP.ProjectOrgLinks.CreatedTime,SCBS_SUP.ProjectOrgLinks.UpdatedTime,SCBS_SUP.ProjectOrgLinks.CreatedByAccount,SCBS_SUP.ProjectOrgLinks.UpdatedByAccount FROM SCBS_SUP.ProjectOrgLinks";

                    string updatedRecordsFilter = " WHERE SCBS_SUP.ProjectOrgLinks.CreatedTime >= '" + fromTime + "' OR SCBS_SUP.ProjectOrgLinks.UpdatedTime >= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_SUP.ProjectOrgLinks.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;
                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " ProjectOrgLinks records...");
                        StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=OFF;INSERT INTO ProjectOrgLinks");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();

                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string singleRow = "";
                                List<string> values = row.Split('^').ToList();
                                string ProjectOrgLinkID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string Active = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Organisation = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string Project = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string CreatedTime = "";
                                string UpdatedTime = "";
                                string CreatedByAccount = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string UpdatedByAccount = string.IsNullOrEmpty(values[7]) ? "null" : values[7];

                                if (!string.IsNullOrEmpty(values[4]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[4], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[5]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[5], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }


                                singleRow = ProjectOrgLinkID + "," + Active + "," + Organisation + "," + Project + ",'" + CreatedTime + "','" + UpdatedTime + "'," + CreatedByAccount + "," + UpdatedByAccount + "";

                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " ProjectOrgLinks records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in ProjectOrgLinks - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public int SyncProject_Landing(string fromTime)
        {
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;

            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get SyncProject";

                do
                {
                    count = 0;

                    string Query = "SELECT SCBS_CHILD.Project.ID as ProjectID,SCBS_CHILD.Project.Cluster,SCBS_CHILD.Project.Name,SCBS_CHILD.Project.ProjectKey FROM SCBS_CHILD.Project";

                    string updatedRecordsFilter = " WHERE SCBS_CHILD.Project.CreatedTime >= '" + fromTime + "' OR SCBS_CHILD.Project.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY SCBS_CHILD.Project.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;
                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " SyncProject records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO Project");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();

                            foreach (string row in results.CSVTables[0].Rows)
                            {
                                string singleRow = "";
                                string FilterRow = row.Replace("'", "");
                                List<string> values = FilterRow.Split('^').ToList();
                                string ProjectID = string.IsNullOrEmpty(values[0]) ? "0" : values[0];
                                string Cluster = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string Name = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string ProjectKey = string.IsNullOrEmpty(values[3]) ? "null" : values[3];

                                singleRow = ProjectID + ",'" + Cluster + "','" + Name + "','" + ProjectKey + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " SyncProject records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncProject - FromTime : " + fromTime + ", StartLimit : " + startLimit);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return Total;
        }
        public string GetDateFromDateTime(string Date)
        {
            DateTime dt = DateTime.Parse(Date);
            string actualDate = dt.ToString("yyyy-MM-dd");
            return actualDate;
        }
        public string GetStringInDateTimeFormat(string Date)
        {
            DateTime dt = DateTime.Parse(Date);
            string actualDate = dt.ToString("yyyy-MM-dd HH:mm:ss");
            return actualDate;
        }

        #region sync Org Status
        public List<OrganizationData> GetRNLastUpdatedOrgDetails(string fromTime)
        {
            //string fromTime = DateTime.Today.AddDays(-3).AddHours(2).ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'");
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            List<OrganizationData> OrgList = new List<OrganizationData>();
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Organisations  data for DWH Sync";
                count = 0;
                do
                {
                    string Query = "select organization.ID,organization.CustomFields.c.relationship_status as RelationshipStatus from organization ";

                    string updatedRecordsFilter = " where (organization.CreatedTime >= '" + fromTime + "' OR organization.UpdatedTime >= '" + fromTime + "') ";
                    string RelationShipStatus = " AND organization.CustomFields.c.relationship_status IS NOT NULL ";
                    string orderBy = " ORDER BY organization.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + RelationShipStatus + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;
                    if (count > 0)
                    {

                        foreach (string row in results.CSVTables[0].Rows)
                        {
                            OrganizationData Orgdata = new OrganizationData();
                            string[] values = row.Split('^');
                            Orgdata.OrgID = values[0];
                            Orgdata.OrgRelationshipStatus = string.IsNullOrEmpty(values[1]) ? null : values[1];
                            OrgList.Add(Orgdata);
                        }
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }
                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in GetLastUpdatedOrgDetails :" + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
            return OrgList;
        }
        public int SyncOrgStatusOperation(string fromTime)
        {
            List<OrganizationData> RNOrgData = new List<OrganizationData>();
            RNOrgData = GetRNLastUpdatedOrgDetails(fromTime);
            foreach (OrganizationData Orgdata in RNOrgData)
            {
                // compare all data with db table
                OrganizationData CompObj = CheckIfOrgRecordExist(Orgdata);
                if (!string.IsNullOrEmpty(CompObj.OrgID))
                {
                    // Found Records in Table
                    Orgdata.OldOrgRelationshipStatus = CompObj.OrgRelationshipStatus;
                    Orgdata.ID = CompObj.ID;
                    appLogger.Info("Comparing Database OrgRelationshipStatus :" + CompObj.OrgRelationshipStatus + " with RN OrgRelationshipStatus: " + Orgdata.OrgRelationshipStatus + " for OrgID: " + Orgdata.OrgID);
                    if (CompObj.OrgRelationshipStatus != Orgdata.OrgRelationshipStatus)// Means OrgRelationshipStatus is change
                    {
                        CreateOrUpdateOrgRowInDB(Orgdata, "UpdateAndInsert");
                    }
                }
                else
                {
                    //Record is not found in table
                    CreateOrUpdateOrgRowInDB(Orgdata, "Insert");
                }

            }
            return RNOrgData.Count();
        }//Main
        public OrganizationData CheckIfOrgRecordExist(OrganizationData Orgdata)
        {
            OrganizationData OrgObj = new OrganizationData();
            try
            {
                string Query = "SELECT * FROM OrganisationRelationshipStatusDetails WHERE OrgID=" + Orgdata.OrgID + " order by Start desc";
                using (MySqlConnection conn = new MySqlConnection(myDWH_LandingConnectionString))
                {
                    conn.Open();
                    MySqlCommand mycmd = new MySqlCommand(Query, conn);
                    MySqlDataReader dataReader;
                    dataReader = mycmd.ExecuteReader();
                    if (dataReader.Read())
                    {
                        OrgObj.ID = Convert.ToInt64(dataReader["ID"]);
                        OrgObj.OrgID = dataReader["OrgID"].ToString();
                        OrgObj.OrgRelationshipStatus = dataReader["RelationshipStatus"].ToString();
                    }
                }

            }
            catch (Exception e)
            {
                appLogger.Error("Error in CheckIfOrgRecordExist: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
            return OrgObj;
        }
        public void CreateOrUpdateOrgRowInDB(OrganizationData Orgdata, string Type)
        {
            appLogger.Info("OrganisationRelationshipStatusDetails with Type: " + Type + " for OrganizationID: " + Orgdata.OrgID);

            try
            {
                if (Type == "Insert")
                {
                    // Insert
                    string InsertQuery = "INSERT INTO OrganisationRelationshipStatusDetails(OrgID,RelationshipStatus,Start) VALUES(" + Orgdata.OrgID + "," + Orgdata.OrgRelationshipStatus + ",CURDATE());";
                    appLogger.Info("Executing InsertQuery :" + InsertQuery);

                    using (MySqlConnection conn = new MySqlConnection(myDWH_LandingConnectionString))
                    {
                        conn.Open();
                        MySqlCommand mycmd = new MySqlCommand(InsertQuery, conn);
                        mycmd.ExecuteNonQuery();
                        appLogger.Info("InsertQuery Data updated for Type:  " + Type + " with OrgID: " + Orgdata.OrgID);
                    }
                }
                else
                {
                    // Update
                    string UpdateQuery = "UPDATE OrganisationRelationshipStatusDetails SET End= CURDATE() WHERE ID =" + Orgdata.ID + ";";
                    appLogger.Info("Executing UpdateQuery :" + UpdateQuery);

                    using (MySqlConnection conn = new MySqlConnection(myDWH_LandingConnectionString))
                    {
                        conn.Open();
                        MySqlCommand mycmd = new MySqlCommand(UpdateQuery, conn);
                        mycmd.ExecuteNonQuery();
                        appLogger.Info("UpdateQuery Data updated for Type: " + Type + " with OrgID: " + Orgdata.OrgID + " & RecordID:" + Orgdata.ID);
                    }
                    string InsertQuery2 = "INSERT INTO OrganisationRelationshipStatusDetails (OrgID,RelationshipStatus,Start) VALUES(" + Orgdata.OrgID + "," + Orgdata.OrgRelationshipStatus + ", CURDATE());";
                    appLogger.Info("Executing InsertQuery2 :" + UpdateQuery);
                    using (MySqlConnection conn = new MySqlConnection(myDWH_LandingConnectionString))
                    {
                        conn.Open();
                        MySqlCommand mycmd = new MySqlCommand(InsertQuery2, conn);
                        mycmd.ExecuteNonQuery();
                        appLogger.Info("InsertQuery2 Data updated for Type: " + Type + " with OrgID: " + Orgdata.OrgID);
                    }
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in CreateOrUpdateOrgRowInDB: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
        }
        #endregion

        #endregion

        public string getDateTimeWhichProcessUpdatingInDB(string Parse)
        {
            string RemoveSingleQuote = Parse.Replace("'", "");
            string RemoveCircularBrackets = RemoveSingleQuote.Replace("(", "");
            string RemoveCircularBrackets1 = RemoveCircularBrackets.Replace(")", "");
            List<string> Col = new List<string>();
            foreach (string data in RemoveCircularBrackets1.Split(','))
            {
                Col.Add(data);
            }
            return Col[8];
        }

        public int GetLastIDNotifications()
        {
            int total = 0;
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {

                conn.ConnectionString = myConnectionString;
                conn.Open();

                string Query = "Select Max(ID) as ID from Notifications ";
                MySqlCommand MyCommand2 = new MySqlCommand(Query, conn);
                MySqlDataReader dr;
                dr = MyCommand2.ExecuteReader();
                while (dr.Read())
                {
                    total = Convert.ToInt32(dr.GetString("ID"));
                }
                conn.Close();
            }
            catch (Exception e)
            {
                conn.Close();
                appLogger.Error("Error in getting total records for Notifications record in DB : " + e.Message);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return total;
        }

        public int GetLastIDtificationsCount(int LastID)
        {
            int total = 0;
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {
                conn = new MySql.Data.MySqlClient.MySqlConnection();
                conn.ConnectionString = myAppConnectionString;
                conn.Open();

                string Query = "Select count(ID) as ID from notifications where ID > " + LastID;
                MySqlCommand MyCommand2 = new MySqlCommand(Query, conn);
                MySqlDataReader dr;
                dr = MyCommand2.ExecuteReader();
                while (dr.Read())
                {
                    total = Convert.ToInt32(dr.GetString("ID"));
                }
                conn.Close();
            }
            catch (Exception e)
            {
                conn.Close();
                appLogger.Error("Error in getting total records for Notifications record in DB : " + e.Message);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return total;
        }

        public StringBuilder GetInsertStringNotificationsNew(int LastID)
        {
            StringBuilder sCommand = new StringBuilder();
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {

                conn.ConnectionString = myAppConnectionString;
                List<string> rows = new List<string>();

                //someString.Trim(charsToTrim);
                conn.Open();
                string mystring = null;
                string Query = "Select * from notifications where ID > " + LastID;
                MySqlCommand MyCommand2 = new MySqlCommand(Query, conn);
                MySqlDataReader dr;
                dr = MyCommand2.ExecuteReader();
                string supporterName = "";
                string ChildName = "";

                while (dr.Read())
                {
                    if (!string.IsNullOrEmpty(dr["SUPPORTER_NAME"].ToString()))
                    {
                        supporterName = dr["SUPPORTER_NAME"].ToString().Replace("'", " ");
                    }
                    if (!string.IsNullOrEmpty(dr["CHILD_NAME"].ToString()))
                    {
                        ChildName = dr["CHILD_NAME"].ToString().Replace("'", " ");
                    }
                    mystring = "('" + dr["SUPPORTER_ID"].ToString() + "','" + supporterName + "','" + ChildName + "','" + dr["NEEDKEY"].ToString() + "','" + dr["CHILD_IMAGE"].ToString() + "','" + dr["MESSAGE_TITLE"].ToString().Replace("'", "") + "','" + dr["MESSAGE_BODY"].ToString().Replace("'", "") + "','" + dr["MESSAGE_TYPE"].ToString().Replace("'", "") + "','" + dr["DESTINATION"].ToString() + "','" + dr["POST_TITLE"].ToString() + "','" + dr["SEND_NOTIFICATION"].ToString() + "','" + dr["HERO"].ToString() + "','" + dr["STATUS"].ToString() + "','" + dr["DISPLAY_ORDER"].ToString() + "','" + dr["IS_READ"].ToString() + "','" + dr["CREATED_ON"].ToString() + "','" + dr["UPDATED_ON"].ToString() + "','" + dr["OA_ID"].ToString() + "','" + dr["OA_BRAND_ID"].ToString() + "','" + dr["USER_ID"].ToString() + "','" + dr["CREATED_BY"].ToString() + "','" + dr["UPDATED_BY"].ToString() + "','" + dr["IS_DELETED"].ToString() + "')";
                    rows.Add(mystring);//,'"+(dr["Message"]).ToString().Trim('\'') + "'

                }

                sCommand.Append(string.Join(",", rows));

                conn.Close();
            }
            catch (Exception e)
            {
                conn.Close();
                appLogger.Error("Error in Sync Notification from database" + LastID + ": + Last Inserted ID");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }

            return sCommand;
        }

        public void insertLastUpdatedInDbLanding(string tbl_name, int count)
        {
            MySql.Data.MySqlClient.MySqlConnection conn;
            string Query = "";
            long recordId = 0;
            try
            {
                conn = new MySql.Data.MySqlClient.MySqlConnection();
                conn.ConnectionString = myDWH_LandingConnectionString;
                conn.Open();


                Query = "INSERT INTO dwh_landing.`LastUpdated`(`LastUpdated`,Source,Table_Name,RecordSync) VALUES ('" + DateTime.Now + "','RN','" + tbl_name + "'," + count + ")";


                MySqlCommand insertCommand = new MySqlCommand(Query, conn);
                MySqlDataReader dataReader;
                dataReader = insertCommand.ExecuteReader();     // Here our query will be executed and data saved into the database.
                recordId = insertCommand.LastInsertedId;
                //appLogger.Info("Error in inserting LastUpdated Record in DB : " + recordId);
                conn.Close();
            }
            catch (Exception e)
            {
                appLogger.Error("Error in inserting record in DB : " + e.Message);
                appLogger.Error(e.StackTrace);

            }

        }

        public void insertLastUpdatedInDb(string tbl_name, int count)
        {
            MySql.Data.MySqlClient.MySqlConnection conn;
            string Query = "";
            long recordId = 0;
            try
            {
                conn = new MySql.Data.MySqlClient.MySqlConnection();
                conn.ConnectionString = myConnectionString;
                conn.Open();


                Query = "INSERT INTO DWH.`LastUpdated`(`LastUpdated`,Table_Name,RecordSync) VALUES ('" + DateTime.Now + "','" + tbl_name + "'," + count + ")";


                MySqlCommand insertCommand = new MySqlCommand(Query, conn);
                MySqlDataReader dataReader;
                dataReader = insertCommand.ExecuteReader();     // Here our query will be executed and data saved into the database.
                recordId = insertCommand.LastInsertedId;
                //appLogger.Info("Error in inserting LastUpdated Record in DB : " + recordId);
                conn.Close();
            }
            catch (Exception e)
            {
                appLogger.Error("Error in inserting record in DB : " + e.Message);
                appLogger.Error(e.StackTrace);

            }

        }

        public string getRNTableCountByApi(string tableName)
        {
            string content = null;
            string Total = null;
            try
            {
                var restClient = new RestClient("https://my.compassionuk.org/services/rest/connect/v1.4/queryResults/?query=SELECT count(ID) FROM " + tableName); // live
                var tokenRequest = new RestRequest(Method.GET);

                tokenRequest.AddHeader("OSvC-CREST-Application-Context", "This is a valid request for account.");
                tokenRequest.AddHeader("Authorization", "Basic R0NfMjpHMzNjb24wNDA0");
                IRestResponse tokenResponse = restClient.Execute(tokenRequest);

                content = tokenResponse.Content; // raw content as string
                Rootobject tokenResponseContent = RestSharp.SimpleJson.DeserializeObject<Rootobject>(content);

                if (tokenResponseContent.items.Count() > 0)
                {
                    Total = tokenResponseContent.items[0].rows[0][0].ToString();
                }

            }
            catch (Exception e)
            {

                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
                if (content != null)
                {
                    appLogger.Error("Response content : " + content);
                }
            }
            return Total;
        }

        public int getDWHTableCount(string Table_name)
        {
            int total = 0;
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {
                conn = new MySql.Data.MySqlClient.MySqlConnection();
                conn.ConnectionString = myConnectionString;
                conn.Open();

                string Query = "Select Count(*) as ID from " + Table_name;
                MySqlCommand MyCommand2 = new MySqlCommand(Query, conn);
                MySqlDataReader dr;
                dr = MyCommand2.ExecuteReader();
                while (dr.Read())
                {
                    total = Convert.ToInt32(dr.GetString("ID"));
                }
                conn.Close();
            }
            catch (Exception e)
            {
                appLogger.Error("Error in getting total records for Demo record in DB : " + e.Message);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
                conn.Close();
            }
            return total;
        }

        public void InsertRNandDWHAllCount(string Table_Name, string RN, string DWH)
        {
            MySql.Data.MySqlClient.MySqlConnection conn = new MySql.Data.MySqlClient.MySqlConnection();
            string Query = "";
            try
            {
                conn.ConnectionString = myConnectionString;
                conn.Open();
                Query = "INSERT INTO `RNvsDWH` (TableName,RN,DWH) values ('" + Table_Name + "','" + RN + "','" + DWH + "');";
                MySqlCommand insertCommand = new MySqlCommand(Query, conn);
                MySqlDataReader dataReader;
                dataReader = insertCommand.ExecuteReader();     // Here our query will be executed and data saved into the database.
                conn.Close();
            }
            catch (Exception e)
            {
                conn.Close();
                appLogger.Info("Error in this query : " + Query);
                appLogger.Error("Error in inserting record in DB : " + e.Message);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                conn.Close();
            }
        }

        public void InsertRNandDWHLandingAllCount(string Table_Name, string RN, string DWH_Landing)
        {
            MySql.Data.MySqlClient.MySqlConnection conn = new MySql.Data.MySqlClient.MySqlConnection();
            string Query = "";
            try
            {
                conn.ConnectionString = myDWH_LandingConnectionString;
                conn.Open();
                Query = "INSERT INTO `RNvsDWH_Landing` (TableName,RN,DWH_Landing) values ('" + Table_Name + "','" + RN + "','" + DWH_Landing + "');";
                MySqlCommand insertCommand = new MySqlCommand(Query, conn);
                MySqlDataReader dataReader;
                dataReader = insertCommand.ExecuteReader();     // Here our query will be executed and data saved into the database.
                conn.Close();
            }
            catch (Exception e)
            {
                conn.Close();
                appLogger.Info("Error in this query : " + Query);
                appLogger.Error("Error in inserting record in DB : " + e.Message);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                conn.Close();
            }
        }

        public void InsertRNandDWHCount()
        {
            //All Table
            //{"Notifications","oneview_comapp"},
            string DWH = null;
            string RN = null;

            for (int i = 0; i < Table_Name.Length / 2; i++)
            {
                string NameInDWH = Table_Name[i, 0];
                string NameInRN = Table_Name[i, 1];
                DWH = getDWHTableCount(NameInDWH).ToString();
                /*  if (NameInRN == "SCBS_SUP.Correspondence;")
                  {
                      RN = "";
                  }
                  else
                  {

                  }*/
                RN = getRNTableCountByApi(NameInRN);

                InsertRNandDWHAllCount(NameInDWH, RN, DWH);
                appLogger.Info("Table " + NameInDWH + " RN " + RN + " DWH " + DWH);
            }


            /* DWH = getDWHTableCount("Need").ToString();
             RN = getRNTableCountByApi("SCBS_CHILD.Need;");
             InsertRNandDWHAllCount("", RN, DWH);*/
        }

        public string SendSMS(string message)
        {
            string username = "0Zalbb", password = "sfHba7", to = "447909821016", originator = "Compassion";
            message = Truncate(message, 600);//Maximum amount of characters for One text credit is 612 which is One SMS
            StringBuilder sb = new StringBuilder();
            byte[] buf = new byte[1024];
            string url = "https://api.textmarketer.co.uk/gateway/" +
                "?username=" + username + "&password=" + password + "&option=xml" +
                "&to=" + to + "&message=" + HttpUtility.UrlEncode(message) +
                "&orig=" + HttpUtility.UrlEncode(originator);
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            Stream resStream = response.GetResponseStream();
            string tempString = null;
            int count = 0;
            do
            {
                count = resStream.Read(buf, 0, buf.Length);
                if (count != 0)
                {
                    tempString = Encoding.ASCII.GetString(buf, 0, count);
                    sb.Append(tempString);
                }
            }
            while (count > 0);
            return sb.ToString();
        }

        public string Truncate(string value, int maxChars)
        {
            return value.Length <= maxChars ? value : value.Substring(0, maxChars) + "...";
        }

        public void InsertRNandDWH_landingCount()
        {
            //All Table
            //{"Notifications","oneview_comapp"},
            string DWH = null;
            string RN = null;
            string DWH_Landing = null;
            for (int i = 0; i < Table_Name.Length / 2; i++)
            {
                string NameInDWH = Table_Name[i, 0];
                string NameInRN = Table_Name[i, 1];
                DWH_Landing = getDWH_LandingTableCount(NameInDWH).ToString();
                RN = getRNTableCountByApi(NameInRN);

                if (NameInDWH != "SupporterEnquiries")
                {
                    InsertRNandDWHLandingAllCount(NameInDWH, RN, DWH_Landing);
                }
                appLogger.Info("Table " + NameInDWH + " RN " + RN + " DWH " + DWH + " DWH_Landing " + DWH_Landing);
            }
        }

        public int getDWH_LandingTableCount(string Table_name)
        {
            int total = 0;
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            conn.ConnectionString = myDWH_LandingConnectionString;
            try
            {
                conn.Open();
                string Query = "Select Count(*) as ID from " + Table_name;
                MySqlCommand MyCommand2 = new MySqlCommand(Query, conn);
                MySqlDataReader dr;
                dr = MyCommand2.ExecuteReader();
                while (dr.Read())
                {
                    total = Convert.ToInt32(dr.GetString("ID"));
                }
                conn.Close();
            }
            catch (Exception e)
            {
                conn.Close();
                appLogger.Error("Error in getting total records for Demo record in DB : " + e.Message);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                conn.Close();
            }
            return total;
        }

        public int SyncOrganisationAsks(string fromTime)
        {
            appLogger.Info("Process Started..");
            int pageSize = 2000;
            int count = 0;
            long startLimit = 0;
            int Total = 0;
            try
            {
                ClientInfoHeader clientInfoHeader = new ClientInfoHeader();
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                clientInfoHeader.AppID = "Get Prospect Ask DWH Sync";

                do
                {
                    count = 0;

                    string Query = "SELECT Prospect.Ask.ID,Prospect.Ask.Organisation,Prospect.Ask.RegionalManager,Prospect.Ask.InterventionID,Prospect.Ask.ChildSurvivalID,Prospect.Ask.InterventionName,Prospect.Ask.InterventionType,Prospect.Ask.AskAmount,Prospect.Ask.AcceptedAmount,Prospect.Ask.Status,Prospect.Ask.AskDate,Prospect.Ask.ResponseDate,Prospect.Ask.CreatedByAccount AS CreatedBy,Prospect.Ask.CreatedTime,Prospect.Ask.UpdatedByAccount AS UpdatedBy,Prospect.Ask.UpdatedTime FROM Prospect.Ask";


                    string updatedRecordsFilter = " WHERE Prospect.Ask.CreatedTime >= '" + fromTime + "' OR Prospect.Ask.UpdatedTime>= '" + fromTime + "'";
                    string orderBy = " ORDER BY Prospect.Ask.ID ASC";
                    string limit = " LIMIT " + startLimit + ", " + pageSize;

                    Query += updatedRecordsFilter + orderBy + limit;
                    //Query += orderBy + limit;

                    byte[] byteArray;

                    CSVTableSet results = null;
                    head = rightNowSyncPortClient.QueryCSV(clientInfoHeader, api, Query, pageSize, "^", false, true, out results, out byteArray);
                    count = results.CSVTables[0].Rows.Length;

                    if (count > 0)
                    {
                        appLogger.Info("Updating " + count + " OrganisationAsks records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO OrganisationAsks");
                        using (MySqlConnection mConnection = new MySqlConnection(myDWH_LandingConnectionString))
                        {
                            List<string> Columns = new List<string>();
                            foreach (string data in results.CSVTables[0].Columns.Split('^'))
                            {
                                Columns.Add(data);
                            }
                            sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                            List<string> Rows = new List<string>();
                            foreach (string row in results.CSVTables[0].Rows)
                            {

                                List<string> values = row.Split('^').ToList();
                                string ID = string.IsNullOrEmpty(values[0]) ? "null" : values[0];
                                string Organisation = string.IsNullOrEmpty(values[1]) ? "null" : values[1];
                                string RegionalManager = string.IsNullOrEmpty(values[2]) ? "null" : values[2];
                                string InterventionID = string.IsNullOrEmpty(values[3]) ? "null" : values[3];
                                string ChildSurvivalID = string.IsNullOrEmpty(values[4]) ? "null" : values[4];
                                string InterventionName = string.IsNullOrEmpty(values[5]) ? "null" : values[5];
                                if (!string.IsNullOrEmpty(InterventionName))
                                {
                                    InterventionName = InterventionName.Replace("\"\"\"", "");
                                    InterventionName = InterventionName.Replace("\"\"", "");
                                    InterventionName = InterventionName.Replace("\"", "");
                                    InterventionName = InterventionName.Replace("'", @"\'");
                                }



                                string InterventionType = string.IsNullOrEmpty(values[6]) ? "null" : values[6];
                                string AskAmount = string.IsNullOrEmpty(values[7]) ? "null" : values[7];
                                string AcceptedAmount = string.IsNullOrEmpty(values[8]) ? "null" : values[8];
                                string Status = string.IsNullOrEmpty(values[9]) ? "null" : values[9];
                                string AskDate = string.IsNullOrEmpty(values[10]) ? "null" : values[10];
                                string ResponseDate = string.IsNullOrEmpty(values[11]) ? "null" : values[11];
                                string CreatedBy = string.IsNullOrEmpty(values[12]) ? "null" : values[12];
                                string CreatedTime = null;
                                string UpdatedBy = string.IsNullOrEmpty(values[14]) ? "null" : values[14];
                                string UpdatedTime = null;


                                if (!string.IsNullOrEmpty(values[13]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[13], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    CreatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    CreatedTime = "null";
                                }

                                if (!string.IsNullOrEmpty(values[15]))
                                {
                                    DateTime dt = DateTime.ParseExact(values[15], "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                                    UpdatedTime = dt.ToString("yyyy-MM-dd HH:mm:ss");
                                }
                                else
                                {
                                    UpdatedTime = "null";
                                }


                                string singleRow = ID + "," + Organisation + "," + RegionalManager + ",'" + RegionalManager + "','" + ChildSurvivalID + "','" + InterventionName + "'," + InterventionType + "," + AskAmount + "," + AcceptedAmount + "," + Status + ",'" + AskDate + "','" + ResponseDate + "'," + CreatedBy + ",'" + CreatedTime + "'," + UpdatedBy + ",'" + UpdatedTime + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));

                            sCommand.Append(" ON DUPLICATE KEY UPDATE ");
                            List<string> UpdatePart = new List<string>();
                            foreach (string Column in Columns)
                            {
                                UpdatePart.Add(string.Format("{0} = VALUES({0})", Column));
                            }
                            sCommand.Append(string.Join(",", UpdatePart));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(";");

                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            appLogger.Info(count + " OrganisationAsks records updated successfully");
                        }

                        // For next set of data
                        Total += count;
                        startLimit += pageSize;
                    }
                    else
                    {
                        break;
                    }

                } while (true);
            }
            catch (Exception e)
            {
                appLogger.Error("Error in SyncOrganisationAsks: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
            return Total;
        }

        public List<string> GetAllBBLookupIds(long SupGrpId)
        {
            List<string> lookupIds = new List<string>();
            try
            {
                ClientInfoHeader info = new ClientInfoHeader();
                info.AppID = "get commitment data";
                APIAccessResponseHeaderType head = new APIAccessResponseHeaderType();
                string query = $"SELECT SGL.SupporterGroupID.BlackBaudConstituentId,contact.customFields.c.blackbaudid FROM SCBS_SUP.SupporterGroupLinks SGL  INNER JOIN SGL.SupporterID contact WHERE SGL.SupporterGroupID ={SupGrpId}";

                byte[] byteArray;

                CSVTableSet result;
                head = rightNowSyncPortClient.QueryCSV(info, api, query, 100000, "^", false, true, out result, out byteArray);

                if (result.CSVTables.Length > 0)
                {
                    if (result.CSVTables[0].Rows.Length > 0)
                    {
                        foreach (string row in result.CSVTables[0].Rows)
                        {
                            string[] values = row.Split('^');
                            lookupIds.Add(values[0]);
                            lookupIds.Add(values[1]);
                        }
                    }
                    else
                    {
                        appLogger.Info("No lookupIds row record found");
                    }
                }
                else
                {
                    appLogger.Info("No lookupIds record found");
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in GetAllBBLookupIds: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
            return lookupIds.Distinct().ToList();
        }

    }
}
