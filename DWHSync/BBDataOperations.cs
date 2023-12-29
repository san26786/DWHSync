using System.Linq;
using System.Web;
using log4net;
using MySql.Data.MySqlClient;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
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
using System.Data;
using System.Threading;
using System.Xml.Linq;
using System.Xml;
using DWHSync.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using Polly;
using System.Configuration;

namespace DWHSync
{
    public class BBDataOperations
    {
        protected static readonly ILog appLogger = LogManager.GetLogger(typeof(BBDataOperations));
        private string myConnectionStringoneview = "server=62.138.4.114;uid=oneview_RN;" + "pwd=Geecon0404;database=oneview_rn_backup; convert zero datetime=True;AutoEnlist=false";
        private string myConnectionStringDWH = "server=172.23.161.18;uid=dwh_user;" + "pwd=Geecon0404;database=DWH; convert zero datetime=True;AutoEnlist=false;Connection Lifetime=0;Min Pool Size=0;Max Pool Size=100;Pooling=true;";
        private string myConnectionString = "server=172.23.161.18;uid=dwh_landing_user;" + "pwd=Comp@ss!on@1952;database=dwh_landing; convert zero datetime=True;AutoEnlist=false;Connection Lifetime=0;Min Pool Size=0;Max Pool Size=100;Pooling=true;";
        private string mycukdevco_tcpt4ConnectionString = "server=172.23.161.30;uid=cuk_tcpt4;" + "pwd=Geecon0404;database=cuk_tcpt4_log; convert zero datetime=True;AutoEnlist=false";
        private static DBModel dbObj;
        private static long rowCounter;
        private Blackbaud.AppFx.WebAPI.ServiceProxy.AppFxWebService _service;
        private string BbDatabase = string.Empty;
        private string ApplicationName = System.Reflection.Assembly.GetCallingAssembly().FullName;

        private string URI = "https://s01p2bisweb003.blackbaud.net/64156_06c7d203-38a5-43a4-9fec-9c744931be66/appfxwebservice.asmx";

        private string UID = "esbuser64156";
        private string PWD = "Esbuswe@123";

        Policy retryPolicy = Policy
                      .Handle<Exception>(ex => ex.Message.Contains("The request failed with HTTP status 502: Bad Gateway"))
                      .Retry(3, onRetry: (exception, retryCounter) =>
                      {
                          // Add logic to be executed before each retry, such as logging
                          appLogger.Info("Retrying...");
                      });

        public BBDataOperations()
        {
            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            // Logging
            log4net.Config.XmlConfigurator.Configure();
            _service = new AppFxWebService();
            _service.Url = ConfigurationManager.AppSettings["BB_URI"];
            _service.Credentials = new System.Net.NetworkCredential(ConfigurationManager.AppSettings["BB_UID"], ConfigurationManager.AppSettings["BB_PWD"], "");

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
        }
        private Blackbaud.AppFx.WebAPI.ServiceProxy.ClientAppInfoHeader GetRequestHeader()
        {
            var header = new Blackbaud.AppFx.WebAPI.ServiceProxy.ClientAppInfoHeader();
            header.ClientAppName =
            header.REDatabaseToUse = BbDatabase;
            return header;
        }

        public void ExecuteParallel(params Action[] tasks)
        {
            // Initialize the reset events to keep track of completed threads
            ManualResetEvent[] resetEvents = new ManualResetEvent[tasks.Length];

            // Launch each method in it's own thread
            for (int i = 0; i < tasks.Length; i++)
            {
                resetEvents[i] = new ManualResetEvent(false);
                ThreadPool.QueueUserWorkItem(new WaitCallback((object index) =>
                {
                    int taskIndex = (int)index;

                    // Execute the method
                    tasks[taskIndex]();

                    // Tell the calling thread that we're done
                    resetEvents[taskIndex].Set();
                }), i);
            }

            // Wait for all threads to execute
            WaitHandle.WaitAll(resetEvents);
        }
        public void Run()
        {
            BBDataOperations bb = new BBDataOperations();
            // Sync BB Table Batch 1
            bb.ExecuteParallel(() =>
            {
                SycnConstituentHouseholdBelongedTo();
                SyncConstituentTaxDeclarations();
                SycnRevenueRecurringGiftsPayments();

                //Deleted Tables From DB DWH_Landing
                /*
                SycnConstituent();
                SycnConstituentSpouse();
                SyncConstituentRelationships();
                SycnConstituentProspects();
                SyncConstituentHouseholdMembers();
                SyncConstituentHouseholdDissolution();
                SycnRevenueAppeal();
                SycnConstituentPrimaryOrg();
                SycnConstituentAlias();
                SycnConstituentOrgType();
                SycnConstituentAlternateID();
                SycnConstituentPrimaryContact();
                SyncConstituentGroupMembers();
                SyncConstituentGroupsBelongedTo();
                SycnRevenuePledgeInstallments();
                SycnRecurringGiftRateChangeStatus();
                */
            });
            //Sync BB Table Batch 2
            bb.ExecuteParallel(() =>
            {
                //Deleted Tables From DB DWH_Landing
                /*
                SycnSponsorships();
                SycnSponsorshipsFinancialSponsor();
                SycnSponsorshipsSponsor();
                SycnSponsorshipsOpportunity();
                SycnRevenueRecurringGiftsReducedPayments();
                SycnRevenueRecurringGiftsSponsorships();
                SycnLetterGiftInteractions();
                SycnRevenueRecurringGift();
                SycnRevenuePaymentAllocations();
                SycnRevenuePledges();
                SycnRevenueRecurringGiftsInstallments();
                SyncRevenuePaymentOdata();
                SycnConstituentConstituencies();
                SycnRevenuePledgePayments();
                */
            });
        }

        public void SycnConstituent()
        {
            string Table_Name = " Constituent ";
            appLogger.Info("Sync " + Table_Name + "Process Started...");
            int Total = 0;
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("27552563-53da-4cd6-8739-a5b87d6f6c82");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent records...");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=0;INSERT INTO `Constituent`(`LookupID`, `ConstituentType`, `IsGroupOrHousehold`, `IsIndividual`,`IsOrganization`, `IsPrimaryOrganisation`, `Inactive`, `InactiveDetails`, `DeceasedDate`, `DeceasedConfirmation`, `Deceased`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[13];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        if (!string.IsNullOrEmpty(rows[11]))
                        {
                            rows[11] = rows[11].Replace(",", " ");
                            rows[11] = rows[11].Replace("'", "");
                        }
                        if (!string.IsNullOrEmpty(rows[8]) && rows[8] != "00000000")
                        {
                            DateTime DeceasedDate = DateTime.ParseExact(rows[8], "yyyyMMdd", CultureInfo.InvariantCulture);
                            rows[8] = DeceasedDate.ToString("yyyy-MM-dd");
                        }
                        else
                        {
                            rows[8] = "null";
                        }

                        if (!string.IsNullOrEmpty(rows[3]))
                        {
                            rows[3] = rows[3] == "1" ? "True" : "False";
                        }

                        rows[11] = !string.IsNullOrEmpty(rows[11]) ? GetDateAddedValueInDateTimeFormat(rows[11]) : "null";
                        rows[12] = !string.IsNullOrEmpty(rows[12]) ? GetDateChangeValueInDateTimeFormat(rows[12]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), ConstituentType = VALUES(ConstituentType), IsGroupOrHousehold = VALUES(IsGroupOrHousehold), IsIndividual = VALUES(IsIndividual),IsOrganization= VALUES(IsOrganization), IsPrimaryOrganisation = VALUES(IsPrimaryOrganisation), Inactive = VALUES(Inactive), InactiveDetails = VALUES(InactiveDetails), DeceasedDate = VALUES(DeceasedDate), DeceasedConfirmation = VALUES(DeceasedConfirmation), Deceased = VALUES(Deceased), DateAdded = VALUES(DateAdded), DateChanged = VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'null'", "null");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent records Found To Sync in Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Constituent");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Constituent records Sync Process Ended");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnConstituentOrgType()
        {
            string Table_Name = " Constituent Org Type ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("fa11118e-4c7f-4151-a0ea-a58309b763b8");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Org Type records...");

                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentOrgType`(`LookupID`, `RecordID`, `OrgType`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[5];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateAddedValueInDateTimeFormat(rows[3]) : "null";
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateChangeValueInDateTimeFormat(rows[4]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID), OrgType = VALUES(OrgType), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Org Type records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Org Type records Found To Update in Database...");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Constituent Org Type");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync Constituent Org Type Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnConstituentAlias()
        {
            string Table_Name = " Constituent Alias ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("356dd56e-8565-425a-a3d4-4b3b1f31477b");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Alias records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentAlias`(`LookupID`, `AliasID`, `AliasType`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[5];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateAddedValueInDateTimeFormat(rows[3]) : "null";
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateChangeValueInDateTimeFormat(rows[4]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), AliasID = VALUES(AliasID), AliasType = VALUES(AliasType), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Alias records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Alias Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Constituent Alias");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync Constituent Alias Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnConstituentAlternateID()
        {
            string Table_Name = " Constituent Alternate ID ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");

            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            int RowCount = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("dc3ee14f-c17d-4a2a-9066-b91932c6fb3d");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Alternate ID records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentAlternateID`(`LookupID`, `RecordID`, `AlternateLookupID`, `AlternateIDType`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[6];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        RowCount = i;
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateAddedValueInDateTimeFormat(rows[4]) : "null";
                        rows[5] = !string.IsNullOrEmpty(rows[5]) ? GetDateChangeValueInDateTimeFormat(rows[5]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID), AlternateLookupID = VALUES(AlternateLookupID), AlternateIDType = VALUES(AlternateIDType), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;

                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Alternate ID records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Alternate ID Records Found To Insert Into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Constituent Alternate ID" + e.Message + " at Row " + RowCount);
                new RightNowDataOperations().SendSMS("Error in Sync Constituent Alternate ID for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync Constituent Alternate ID Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnConstituentConstituencies()
        {
            string Table_Name = " Constituent Constituencies ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("e30a24c5-9731-4edc-b232-e44c8d37028e");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Constituencies records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentConstituencies`(`LookupID`, `RecordID`, `ConstituentConstituency`, `ConstituencyIsActive`, `DateFrom`, `DateTo`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[8];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[5] = !string.IsNullOrEmpty(rows[5]) ? GetDateFromDateTime(rows[5]) : "null";
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateFromDateTime(rows[4]) : "null";
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateAddedValueInDateTimeFormat(rows[6]) : "null";
                        rows[7] = !string.IsNullOrEmpty(rows[7]) ? GetDateChangeValueInDateTimeFormat(rows[7]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }

                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID), ConstituentConstituency = VALUES(ConstituentConstituency), ConstituencyIsActive = VALUES(ConstituencyIsActive),DateFrom = VALUES(DateFrom),DateTo = VALUES(DateTo), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply; Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Constituencies records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Constituencies Records Found To Insert Into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Constituent Constituencies");
                new RightNowDataOperations().SendSMS("Error in Sync Constituent Constituencies for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Constituent Constituencies Sync Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnConstituentPrimaryContact()
        {
            string Table_Name = " Constituent Primary Contact ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("c1bbbfb2-81c4-43d9-8a63-ee7ae0c69549");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Primary Contact records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentPrimaryContact`(`LookupID`, `PrimaryContactLookupID`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[4];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[2] = !string.IsNullOrEmpty(rows[2]) ? GetDateAddedValueInDateTimeFormat(rows[2]) : "null";
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateChangeValueInDateTimeFormat(rows[3]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }

                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), PrimaryContactLookupID = VALUES(PrimaryContactLookupID), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Primary Contact records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Primary Contact Found To Insert Into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Constituent Primary Contact");
                new RightNowDataOperations().SendSMS("Error in Sync Constituent Primary Contact for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Constituent Primary Contact Sync Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnConstituentPrimaryOrg()
        {
            string Table_Name = " Constituent Primary Org ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("e42d1af2-f41b-4ea8-8d88-7dbe592d5f58");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Primary Org records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentPrimaryOrg`(`LookupID`, `PrimaryOrganisationLookupID`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[4];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[2] = !string.IsNullOrEmpty(rows[2]) ? GetDateAddedValueInDateTimeFormat(rows[2]) : "null";
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateChangeValueInDateTimeFormat(rows[3]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }

                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), PrimaryOrganisationLookupID = VALUES(PrimaryOrganisationLookupID), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Primary Org records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Primary Org Records Found To Insert in Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Constituent Primary Org");
                new RightNowDataOperations().SendSMS("Error in Sync  Constituent Primary Org for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();

            }
            appLogger.Info("Constituent Primary Org Sync Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SyncConstituentHouseholdMembers()
        {
            string Table_Name = " Constituent Household Members ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");

            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("0e728f10-bd21-4ad8-8fab-da236334cd7b");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Household Members records");

                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentHouseholdMembers`(`LookupID`, `RecordID`, `IsFormerMember`, `IsPrimaryContact`, `HouseholdMemberLookupID`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[7];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        if (!string.IsNullOrEmpty(rows[2]))
                        {
                            rows[2] = rows[2] == "1" ? "True" : "False";
                        }
                        rows[5] = !string.IsNullOrEmpty(rows[5]) ? GetDateAddedValueInDateTimeFormat(rows[5]) : "null";
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateChangeValueInDateTimeFormat(rows[6]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }

                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID),IsFormerMember = VALUES(IsFormerMember),IsPrimaryContact = VALUES(IsPrimaryContact),HouseholdMemberLookupID = VALUES(HouseholdMemberLookupID), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Household Members records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Household Members Records Found To Insert in Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting  Constituent Household Members");
                new RightNowDataOperations().SendSMS("Error in Sync Constituent Household Members for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();

            }
            appLogger.Info("Constituent Household Members Sync Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SyncConstituentHouseholdDissolution()
        {
            string Table_Name = " Constituent Household Dissolution ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("9c6e03cf-7b35-4885-b483-f31ee834e211");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Household Dissolution records");

                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentHouseholdDissolution`(`LookupID`, `RecordID`, `Dissolved`, `DissolvedDate`, `DissolvedReason`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[7];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;

                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateFromDateTime(rows[3]) : "null";
                        rows[5] = !string.IsNullOrEmpty(rows[5]) ? GetDateAddedValueInDateTimeFormat(rows[5]) : "null";
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateChangeValueInDateTimeFormat(rows[6]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }

                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID),Dissolved = VALUES(Dissolved),DissolvedDate = VALUES(DissolvedDate),DissolvedReason = VALUES(DissolvedReason), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Household Dissolution records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Household Dissolution Records Found To Insert in Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting  Constituent Household Dissolution");
                new RightNowDataOperations().SendSMS("Error in Sync Household Dissolution for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();

            }
            appLogger.Info("Constituent Household Dissolution Sync Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SyncConstituentGroupMembers()
        {
            string Table_Name = " Constituent Group Members ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            int Total = 0;
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("d91ef2d5-746d-4a80-a70e-f8d4b62e4b6e");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Group Members records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentGroupMembers`(`LookupID`, `RecordID`, `IsCurrentMember`, `IsFormerMember`, `IsPrimaryContact`, `GroupMemberLookupID`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[8];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateAddedValueInDateTimeFormat(rows[6]) : "null";
                        rows[7] = !string.IsNullOrEmpty(rows[7]) ? GetDateChangeValueInDateTimeFormat(rows[7]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }

                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID),IsCurrentMember = VALUES(IsCurrentMember),IsFormerMember = VALUES(IsFormerMember),IsPrimaryContact = VALUES(IsPrimaryContact),GroupMemberLookupID =VALUES(GroupMemberLookupID),DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Group Members records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Group Members Records Found To Insert in Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting  Constituent Group Members");
                new RightNowDataOperations().SendSMS("Error in Sync Constituent Group Members for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();

            }
            appLogger.Info("Constituent Group Members Sync Process Ended...");
            InsertTableCount(Table_Name, Total);
        }

        //done here
        public void SyncConstituentGroupsBelongedTo()
        {
            string Table_Name = " Constituent Group BelongedTo ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            int Total = 0;
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("521d3516-44bc-47d8-a84d-f5b6192b3223");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Group BelongedTo records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentGroupsBelongedTo`(`LookupID`, `RecordID`, `IsCurrent`, `IsFormer`, `IsPrimaryContact`, `GroupLookupID`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[7];
                    string[] alteredRows = new string[7];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateAddedValueInDateTimeFormat(rows[6]) : "null";
                        rows[7] = !string.IsNullOrEmpty(rows[7]) ? GetDateChangeValueInDateTimeFormat(rows[7]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }

                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID),IsCurrent = VALUES(IsCurrent),IsFormer = VALUES(IsFormer),IsPrimaryContact = VALUES(IsPrimaryContact),GroupLookupID =VALUES(GroupLookupID),DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Group BelongedTo records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Group BelongedTo Records Found To Insert in Database");
                }

            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting  Constituent Group BelongedTo");
                new RightNowDataOperations().SendSMS("Error in Sync Constituent Group BelongedTo for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Constituent Group BelongedTo Sync Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnConstituentProspects()
        {
            string Table_Name = " Constituent Prospects ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");

            int Total = 0;
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();

            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("3219ef91-8942-4cc7-9c77-6eef643d9ca7");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Prospects records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentProspects`(`LookupID`, `RecordID`, `IsActive`, `ProspectManagerEndDate`, `ProspectManagerStartDate`, `ProspectStatus`, `ProspectManager`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[9];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateFromDateTime(rows[3]) : "null";
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateFromDateTime(rows[4]) : "null";
                        rows[7] = !string.IsNullOrEmpty(rows[7]) ? GetDateAddedValueInDateTimeFormat(rows[7]) : "null";
                        rows[8] = !string.IsNullOrEmpty(rows[8]) ? GetDateChangeValueInDateTimeFormat(rows[8]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }

                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID), IsActive = VALUES(IsActive), ProspectManagerEndDate = VALUES(ProspectManagerEndDate),ProspectManagerStartDate = VALUES(ProspectManagerStartDate),ProspectStatus = VALUES(ProspectStatus),ProspectManager = VALUES(ProspectManager), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Prospects records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Prospects Records Found To Insert Into Database");
                }

            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Constituent Prospects");
                new RightNowDataOperations().SendSMS("Error in Sync Constituent Prospects for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Constituent Prospects Sync Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SyncConstituentRelationships()
        {
            string Table_Name = " Constituent Relationships ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("bd9e1f20-f785-4e56-b8d1-9bc724476ced");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Relationships records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentRelationships`(`LookupID`, `RecordID`, `RelatedLookupID`, `RelationshipType`, `ReciprocalRelationshipType`, `ContactType`, `RelationshipStartDate`, `RelationshipEndDate`, `IsContact`, `IsPrimaryBusiness`, `IsPrimaryContact`, `IsSpouse`, `OrgWillMatchContributions`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[15];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateFromDateTime(rows[6]) : "null";
                        rows[7] = !string.IsNullOrEmpty(rows[7]) ? GetDateFromDateTime(rows[7]) : "null";
                        rows[13] = !string.IsNullOrEmpty(rows[13]) ? GetDateAddedValueInDateTimeFormat(rows[13]) : "null";
                        rows[14] = !string.IsNullOrEmpty(rows[14]) ? GetDateChangeValueInDateTimeFormat(rows[14]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }

                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID),RelatedLookupID = VALUES(RelatedLookupID),RelationshipType = VALUES(RelationshipType),ReciprocalRelationshipType = VALUES(ReciprocalRelationshipType),ContactType =VALUES(ContactType),RelationshipStartDate =VALUES(RelationshipStartDate),RelationshipEndDate =VALUES(RelationshipEndDate),IsContact =VALUES(IsContact),IsPrimaryBusiness =VALUES(IsPrimaryBusiness),IsPrimaryContact =VALUES(IsPrimaryContact),IsSpouse =VALUES(IsSpouse),OrgWillMatchContributions =VALUES(OrgWillMatchContributions),DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Relationships records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Relationships Records Found To Insert in Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting  Constituent Relationships");
                new RightNowDataOperations().SendSMS("Error in Sync Constituent Relationships for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();

            }
            appLogger.Info("Constituent Relationships Sync Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnConstituentSpouse()
        {
            string Table_Name = " Constituent Spouse ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("b2e7a601-4dc5-4f1a-b326-77273afea9d7");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Spouse records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentSpouse`(`LookupID`, `SpouseLookupID`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[4];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[2] = !string.IsNullOrEmpty(rows[2]) ? GetDateAddedValueInDateTimeFormat(rows[2]) : "null";
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateChangeValueInDateTimeFormat(rows[3]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), SpouseLookupID = VALUES(SpouseLookupID), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Spouse records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Spouse Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Constituent Spouse");
                new RightNowDataOperations().SendSMS("Error in Sync  Constituent Spouse for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync Constituent Spouse Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SyncConstituentTaxDeclarations()
        {
            string Table_Name = " Constituent Tax Declarations ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");

            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;

            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("ff799194-6039-4d54-a43d-c1c6430869b2");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent Tax Declarations records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ConstituentTaxDeclarations`(`LookupID`, `RecordID`, `DeclarationStarts`, `DeclarationEnds`, `DeclarationIndicator`, `DeclarationMade`, `DeclarationSource`, `PaysTax`, `ScannedDocumentExists`, `UseR68Address`, `UseR68Alias`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[13];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        if (!string.IsNullOrEmpty(rows[9]))
                        {
                            rows[9] = rows[9] == "1" ? "True" : "False";
                        }
                        if (!string.IsNullOrEmpty(rows[10]))
                        {
                            rows[10] = rows[10] == "1" ? "True" : "False";
                        }
                        rows[2] = !string.IsNullOrEmpty(rows[2]) ? GetDateFromDateTime(rows[2]) : "null";
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateFromDateTime(rows[3]) : "null";
                        rows[5] = !string.IsNullOrEmpty(rows[5]) ? GetDateFromDateTime(rows[5]) : "null";
                        rows[11] = !string.IsNullOrEmpty(rows[11]) ? GetDateAddedValueInDateTimeFormat(rows[11]) : "null";
                        rows[12] = !string.IsNullOrEmpty(rows[12]) ? GetDateChangeValueInDateTimeFormat(rows[12]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }

                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID),DeclarationStarts = VALUES(DeclarationStarts),DeclarationEnds = VALUES(DeclarationEnds),DeclarationIndicator = VALUES(DeclarationIndicator),DeclarationMade =VALUES(DeclarationMade),DeclarationSource =VALUES(DeclarationSource),PaysTax =VALUES(PaysTax),ScannedDocumentExists =VALUES(ScannedDocumentExists),UseR68Address =VALUES(UseR68Address),UseR68Alias =VALUES(UseR68Alias),DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'null'", "null");//To Insert Null value in date column if date is set empty
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Constituent Tax Declarations records updated successfully");
                }
                else
                {
                    appLogger.Info("No Constituent Tax Declarations Records Found To Insert in Database");
                }

            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Constituent Tax Declarations");
                new RightNowDataOperations().SendSMS("Error in Sync Constituent Tax Declarations for dwh_landing");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();

            }
            appLogger.Info("Constituent Tax Declarations Sync Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnSponsorships()
        {
            string Table_Name = " Sponsorships ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");

            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("e14d3866-0415-4119-b465-1d5a176ff6fc");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Sponsorships records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `Sponsorships`(`CommitmentID`, `RecordID`, `StartDate`, `EndDate`, `Status`, `GiftSponsorship`, `LastAction`, `LastActionReason`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[10];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[2] = !string.IsNullOrEmpty(rows[2]) ? GetDateFromDateTime(rows[2]) : "null";
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateFromDateTime(rows[3]) : "null";
                        rows[8] = !string.IsNullOrEmpty(rows[8]) ? GetDateAddedValueInDateTimeFormat(rows[8]) : "null";
                        rows[9] = !string.IsNullOrEmpty(rows[9]) ? GetDateChangeValueInDateTimeFormat(rows[9]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE CommitmentID = VALUES(CommitmentID), RecordID = VALUES(RecordID), StartDate = VALUES(StartDate), EndDate = VALUES(EndDate), GiftSponsorship = VALUES(GiftSponsorship), LastAction = VALUES(LastAction), LastActionReason = VALUES(LastActionReason), Status = VALUES(Status), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'null'", "null");//To Insert Null value in date column if date is set empty
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Sponsorships records updated successfully");
                }
                else
                {
                    appLogger.Info("No Sponsorships Records Found To Insert into Database");
                }

            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Sponsorships");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync Sponsorships Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnSponsorshipsFinancialSponsor()
        {
            string Table_Name = " Sponsorships Financial Sponsor ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");

            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {

                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("9b1db1b1-fdfd-43ca-9fe6-855d5641bc9f");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Sponsorships Financial Sponsor records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `SponsorshipsFinancialSponsor`(`CommitmentID`, `RecordID`, `LookupID`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[5];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateAddedValueInDateTimeFormat(rows[3]) : "null";
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateChangeValueInDateTimeFormat(rows[4]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE CommitmentID = VALUES(CommitmentID), RecordID = VALUES(RecordID),LookupID = VALUES(LookupID), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    string str = sCommand.ToString();
                    //sCommand.Replace("'s", "s");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Sponsorships Financial Sponsor records updated successfully");
                }
                else
                {
                    appLogger.Info("No Sponsorships Financial Sponsor Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Sponsorships Financial Sponsor");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync Sponsorships Financial Sponsor Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnSponsorshipsOpportunity()
        {
            string Table_Name = " Sponsorships Opportunity ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");

            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("cc8658cc-3706-44db-8296-df491e5467a6");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Sponsorships Opportunity records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `SponsorshipsOpportunity`(`CommitmentID`, `SponsorshipRecordID`, `Eligibility`, `BeneficiaryLocalID`, `StartDate`, `EndDate`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[8];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateFromDateTime(rows[4]) : "null";
                        rows[5] = !string.IsNullOrEmpty(rows[5]) ? GetDateFromDateTime(rows[5]) : "null";
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateAddedValueInDateTimeFormat(rows[6]) : "null";
                        rows[7] = !string.IsNullOrEmpty(rows[7]) ? GetDateChangeValueInDateTimeFormat(rows[7]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE CommitmentID = VALUES(CommitmentID),SponsorshipRecordID = VALUES(SponsorshipRecordID),Eligibility = VALUES(Eligibility),BeneficiaryLocalID = VALUES(BeneficiaryLocalID),StartDate = VALUES(StartDate), EndDate = VALUES(EndDate), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    sCommand.Replace("'null'", "null");//To Insert Null value in date column if date is set empty
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Sponsorships Opportunity records updated successfully");
                }
                else
                {
                    appLogger.Info("No Sponsorships Opportunity Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Sponsorships Opportunity");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync Sponsorships Opportunity Spouse Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnSponsorshipsSponsor()
        {
            string Table_Name = " Sponsorships Sponsor ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("28c5b316-ad85-4438-81d1-0286a23c3154");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `SponsorshipsSponsor`(`CommitmentID`, `RecordID`, `LookupID`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[5];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateAddedValueInDateTimeFormat(rows[3]) : "null";
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateChangeValueInDateTimeFormat(rows[4]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE CommitmentID = VALUES(CommitmentID), RecordID = VALUES(RecordID), LookupID = VALUES(LookupID), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRecurringGiftRateChangeStatus()
        {
            string Table_Name = " Recurring Gift Rate Change Status ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("c20062ec-18c2-4be5-9739-3b76606774c9");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenueRecurringGiftsRateChangeStatus`(`RevenueID`, `RecordID`, `Value`, `StartDate`, `EndDate`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[7];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateFromDateTime(rows[3]) : "null";
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateFromDateTime(rows[4]) : "null";
                        rows[5] = !string.IsNullOrEmpty(rows[5]) ? GetDateAddedValueInDateTimeFormat(rows[5]) : "null";
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateChangeValueInDateTimeFormat(rows[6]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE RevenueID = VALUES(RevenueID), RecordID = VALUES(RecordID), Value = VALUES(Value), StartDate = VALUES(StartDate), EndDate = VALUES(EndDate), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }

            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnConstituentHouseholdBelongedTo()
        {
            string Table_Name = " Constituent Household Belonged To ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("1b6bac29-d5da-413a-b729-e1f211a9a36d");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = retryPolicy.Execute(() => _service.DataListLoad(req));
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("SET FOREIGN_KEY_CHECKS=0;INSERT INTO `ConstituentHouseholdBelongedTo`(`LookupID`, `RecordID`, `IsCurrent`, `IsPrimaryContact`, `HouseholdLookupID`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[7];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        if (!string.IsNullOrEmpty(rows[2]))
                        {
                            rows[2] = rows[2] == "1" ? "True" : "False";
                        }
                        rows[5] = !string.IsNullOrEmpty(rows[5]) ? GetDateAddedValueInDateTimeFormat(rows[5]) : "null";
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateChangeValueInDateTimeFormat(rows[6]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE LookupID = VALUES(LookupID), RecordID = VALUES(RecordID), IsCurrent = VALUES(IsCurrent), IsPrimaryContact = VALUES(IsPrimaryContact), HouseholdLookupID = VALUES(HouseholdLookupID), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRevenueAppeal()
        {
            string Table_Name = " Revenue Appeal ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("5d414b39-cfb8-4380-9257-45f3260e6dd1");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenueAppeal`(`AppealName`, `RecordID`, `StartDate`, `EndDate`, `AppealCategory`, `AppealDescription`, `AppealGoal`, `AppealIsActive`, `AppealReportCode`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[11];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        if (!string.IsNullOrEmpty(rows[0]))
                        {
                            rows[0] = rows[0].Replace(",", "");
                            rows[0] = rows[0].Replace("'", "");
                        }
                        if (!string.IsNullOrEmpty(rows[5]))
                        {
                            rows[5] = rows[5].Replace(",", "");
                            rows[5] = rows[5].Replace("'", "");
                        }
                        rows[2] = !string.IsNullOrEmpty(rows[2]) ? GetDateFromDateTime(rows[2]) : "null";
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateFromDateTime(rows[3]) : "null";
                        rows[9] = !string.IsNullOrEmpty(rows[9]) ? GetDateFromDateTime(rows[9]) : "null";
                        rows[10] = !string.IsNullOrEmpty(rows[10]) ? GetDateChangeValueInDateTimeFormat(rows[10]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE AppealName = VALUES(AppealName), RecordID = VALUES(RecordID), StartDate = VALUES(StartDate), EndDate = VALUES(EndDate), AppealCategory = VALUES(AppealCategory),AppealDescription = VALUES(AppealDescription),AppealGoal = VALUES(AppealGoal),AppealIsActive = VALUES(AppealIsActive),AppealReportCode = VALUES(AppealReportCode), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Replace("'null'", "null");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRevenuePledgePayments()
        {
            string Table_Name = " Revenue Pledge Payments ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("41cd1a32-e2f1-416e-9875-6b3c0f8e1a09");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenuePledgePayments`(`RevenueID`,`AllocatedAmount`, `PaymentRevenueID`, `DateAdded`, `DateChanged`,`RecordID`,BaseCurrency,TransactionCurrency) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[8];
                    string[] alteredRows = new string[8];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        Array.Copy(rows, alteredRows, 8);
                        alteredRows[3] = !string.IsNullOrEmpty(alteredRows[3]) ? GetDateFromDateTime(alteredRows[3]) : "null";
                        alteredRows[4] = !string.IsNullOrEmpty(alteredRows[4]) ? GetDateAddedValueInDateTimeFormat(alteredRows[4]) : "null";

                        string singleRow = string.Join("','", alteredRows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE  RevenueID = VALUES(RevenueID), AllocatedAmount = VALUES(AllocatedAmount), PaymentRevenueID = VALUES(PaymentRevenueID), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged),RecordID = VALUES(RecordID),BaseCurrency = Value(BaseCurrency),TransactionCurrency = Value(TransactionCurrency)");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRevenuePledgeInstallments()
        {
            string Table_Name = " Revenue Pledge Installments ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("ee683e8d-2117-409a-958b-37e21fac6f8e");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenuePledgeInstallments`(`RevenueID`, `RecordID`, `Amount`, `AmountOrgCurrency`, `AmountTransactionCurrency`, `AmountApplied`, `Balance`, `BalanceOrgCurrency`, `BalanceTransactionCurrency`, `Date`, `ReceiptAmount`, `ReceiptAmountOrgCurrency`, `ReceiptAmountTransactionCurrency`, `WriteOffAmount`, `DateAdded`, `DateChanged`,`BaseCurrency`,`TransactionCurrency`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[18];
                    string[] alteredRows = new string[18];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        Array.Copy(rows, alteredRows, 18);//this done because 2 extra hidden value coming in rows
                        alteredRows[9] = !string.IsNullOrEmpty(alteredRows[9]) ? GetDateFromDateTime(alteredRows[9]) : "null";
                        alteredRows[14] = !string.IsNullOrEmpty(alteredRows[14]) ? GetDateFromDateTime(alteredRows[14]) : "null";
                        alteredRows[15] = !string.IsNullOrEmpty(alteredRows[15]) ? GetDateAddedValueInDateTimeFormat(alteredRows[15]) : "null";

                        string singleRow = string.Join("','", alteredRows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE  RevenueID = VALUES(RevenueID),RecordID = VALUES(RecordID),Amount = VALUES(Amount),AmountOrgCurrency = VALUES(AmountOrgCurrency),AmountTransactionCurrency = VALUES(AmountTransactionCurrency),AmountApplied = VALUES(AmountApplied),Balance = VALUES(Balance),BalanceOrgCurrency = VALUES(BalanceOrgCurrency),BalanceTransactionCurrency = VALUES(BalanceTransactionCurrency),Date = VALUES(Date),ReceiptAmount = VALUES(ReceiptAmount),ReceiptAmountOrgCurrency = VALUES(ReceiptAmountOrgCurrency),ReceiptAmountTransactionCurrency = VALUES(ReceiptAmountTransactionCurrency),WriteOffAmount = VALUES(WriteOffAmount), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged),BaseCurrency = Value(BaseCurrency),TransactionCurrency = Value(TransactionCurrency)");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRevenueRecurringGiftsPayments()
        {
            string Table_Name = " Revenue Recurring Gifts Payments ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("d7a04acd-cbb8-4001-9735-295a5d0fefab");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenueRecurringGiftsPayments`(`RevenueID`, `RecordID`, `InstallmentDate`, `PaymentAllocatedAmount`, `PaymentRevenueID`, `DateAdded`, `DateChanged`,`BaseCurrency`,`TransactionCurrency`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[13];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[2] = !string.IsNullOrEmpty(rows[2]) ? GetDateFromDateTime(rows[2]) : "null";
                        rows[5] = !string.IsNullOrEmpty(rows[5]) ? GetDateFromDateTime(rows[5]) : "null";
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateChangeValueInDateTimeFormat(rows[6]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE RevenueID = VALUES(RevenueID), RecordID = VALUES(RecordID), InstallmentDate = VALUES(InstallmentDate), PaymentAllocatedAmount = VALUES(PaymentAllocatedAmount), PaymentRevenueID = VALUES(PaymentRevenueID), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged),BaseCurrency= VALUES(BaseCurrency),TransactionCurrency= VALUES(TransactionCurrency)");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRevenueRecurringGiftsReducedPayments()
        {
            string Table_Name = " Revenue Recurring Gifts Reduced Payments ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("6e4dac5f-d4d7-4322-a2d0-c7f46628b512");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenueRecurringGiftsReducedPayments`(`RevenueID`, `RecordID`, `Value`, `StartDate`, `EndDate`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[7];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateFromDateTime(rows[3]) : "null";
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateFromDateTime(rows[4]) : "null";
                        rows[5] = !string.IsNullOrEmpty(rows[5]) ? GetDateAddedValueInDateTimeFormat(rows[5]) : "null";
                        rows[6] = !string.IsNullOrEmpty(rows[6]) ? GetDateChangeValueInDateTimeFormat(rows[6]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE RevenueID = VALUES(RevenueID), RecordID = VALUES(RecordID), Value = VALUES(Value), StartDate = VALUES(StartDate), EndDate = VALUES(EndDate), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Replace("'null'", "null");//To Insert Null value in date column if date is set empty
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRevenueRecurringGiftsSponsorships()
        {
            string Table_Name = " Revenue Recurring Gifts Sponsorships ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("00efb8c0-ec51-4733-abb2-97ede7247cc7");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenueRecurringGiftsSponsorships`(`RevenueID`, `RecordID`, `CommitmentID`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[5];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateAddedValueInDateTimeFormat(rows[3]) : "null";
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateChangeValueInDateTimeFormat(rows[4]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE RevenueID = VALUES(RevenueID), RecordID = VALUES(RecordID), CommitmentID = VALUES(CommitmentID), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged)");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRevenuePaymentAllocations()
        {
            string Table_Name = " Revenue Payment Allocations ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("d1aa86af-000b-49a7-883e-deece4a152d0");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenuePaymentAllocations`(`RevenueAllocationID`, `RevenueID`, `PaymentDate`, `AllocatedAmount`, `AllocatedAmountOrgCurrency`, `AllocatedAmountTransactionCurrency`, `Application`, `DesignationName`, `RevenueCategory`, `Type`, `GiftRecipient`, `DateAdded`, `DateChanged`,BaseCurrency,TransactionCurrency) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[15];
                    string[] alteredRows = new string[15];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        Array.Copy(rows, alteredRows, 15);//this done because 2 extra hidden value coming in rows
                        alteredRows[2] = !string.IsNullOrEmpty(alteredRows[2]) ? GetDateFromDateTime(alteredRows[2]) : "null";
                        alteredRows[11] = !string.IsNullOrEmpty(alteredRows[11]) ? GetDateFromDateTime(alteredRows[11]) : "null";
                        alteredRows[12] = !string.IsNullOrEmpty(alteredRows[12]) ? GetDateAddedValueInDateTimeFormat(alteredRows[12]) : "null";
                        alteredRows[7] = alteredRows[7].Replace("'", "");

                        string singleRow = string.Join("','", alteredRows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE  RevenueAllocationID = VALUES(RevenueAllocationID),RevenueID = VALUES(RevenueID),PaymentDate = VALUES(PaymentDate),AllocatedAmount = VALUES(AllocatedAmount),AllocatedAmountOrgCurrency = VALUES(AllocatedAmountOrgCurrency),AllocatedAmountTransactionCurrency = VALUES(AllocatedAmountTransactionCurrency),Application = VALUES(Application),DesignationName = VALUES(DesignationName),RevenueCategory = VALUES(RevenueCategory),Type = VALUES(Type),GiftRecipient = VALUES(GiftRecipient), DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged),BaseCurrency = Value(BaseCurrency),TransactionCurrency = Value(TransactionCurrency)");
                    sCommand.Replace("'null'", "null");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRevenueRecurringGiftsInstallments()
        {
            string Table_Name = "  Revenue Recurring Gifts Installments ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("c7ec5c2e-8704-4787-aca6-22f6d87695a9");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenueRecurringGiftsInstallments`(`RevenueID`, `RecordID`, `InstallmentAgedDays`, `InstallmentAgedMonths`, `InstallmentAgedYears`, `InstallmentAmount`, `InstallmentBalance`, `InstallmentDate`, `InstallmentDirectDebitRejections`, `InstallmentPaid`, `InstallmentWrittenOff`, `WriteOffAmount`, `WriteOffAmountOrgCurrency`, `WriteOffAmountTransactionCurrency`, `WriteOffDate`, `WriteOffReason`, `WriteOffType`, `DateAdded`, `DateChanged`,`BaseCurrency`,`TransactionCurrency`,`WriteOffBaseCurrency`,`WriteOffTransactionCurrency`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[23];
                    string[] alteredRows = new string[23];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        Array.Copy(rows, alteredRows, 23);//this done because 2 extra hidden value coming in rows
                        alteredRows[5] = !string.IsNullOrEmpty(alteredRows[5]) ? alteredRows[5] : "null";
                        alteredRows[6] = !string.IsNullOrEmpty(alteredRows[6]) ? alteredRows[6] : "null";
                        alteredRows[13] = !string.IsNullOrEmpty(alteredRows[13]) ? alteredRows[13] : "null";
                        alteredRows[12] = !string.IsNullOrEmpty(alteredRows[12]) ? alteredRows[12] : "null";
                        alteredRows[11] = !string.IsNullOrEmpty(alteredRows[11]) ? alteredRows[11] : "null";
                        alteredRows[10] = !string.IsNullOrEmpty(alteredRows[10]) ? alteredRows[10] : "null";
                        alteredRows[9] = !string.IsNullOrEmpty(alteredRows[9]) ? alteredRows[9] : "null";
                        alteredRows[7] = !string.IsNullOrEmpty(alteredRows[7]) ? GetDateFromDateTime(alteredRows[7]) : "null";
                        alteredRows[14] = !string.IsNullOrEmpty(alteredRows[14]) ? GetDateFromDateTime(alteredRows[14]) : "null";

                        alteredRows[17] = !string.IsNullOrEmpty(alteredRows[17]) ? GetDateFromDateTime(alteredRows[17]) : "null";
                        alteredRows[18] = !string.IsNullOrEmpty(alteredRows[18]) ? GetDateAddedValueInDateTimeFormat(alteredRows[18]) : "null";

                        string singleRow = string.Join("','", alteredRows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE  RevenueID = VALUES(RevenueID),RecordID = VALUES(RecordID),InstallmentAgedDays = VALUES(InstallmentAgedDays),InstallmentAmount = VALUES(InstallmentAmount),InstallmentBalance = VALUES(InstallmentBalance),InstallmentDate = VALUES(InstallmentDate),InstallmentDirectDebitRejections = VALUES(InstallmentDirectDebitRejections),InstallmentPaid = VALUES(InstallmentPaid),InstallmentWrittenOff = VALUES(InstallmentWrittenOff),WriteOffAmount = VALUES(WriteOffAmount),InstallmentDirectDebitRejections = VALUES(InstallmentDirectDebitRejections), InstallmentPaid = VALUES(InstallmentPaid),InstallmentWrittenOff = VALUES(InstallmentWrittenOff),WriteOffAmount = VALUES(WriteOffAmount),WriteOffAmountOrgCurrency = VALUES(WriteOffAmountOrgCurrency),WriteOffAmountTransactionCurrency = VALUES(WriteOffAmountTransactionCurrency),WriteOffDate = VALUES(WriteOffDate),WriteOffReason = VALUES(WriteOffReason),WriteOffType = VALUES(WriteOffType),DateAdded = VALUES(DateAdded),DateChanged= VALUES(DateChanged),BaseCurrency= VALUES(BaseCurrency),TransactionCurrency= VALUES(TransactionCurrency),WriteOffBaseCurrency= VALUES(WriteOffBaseCurrency),WriteOffTransactionCurrency= VALUES(WriteOffTransactionCurrency)");
                    sCommand.Append(";");
                    sCommand.Replace("'null'", "null");
                    sCommand.Replace("'t", "");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }

        public void SycnRevenuePayments()
        {
            string Table_Name = "  Revenue Payments ";
            int Count = 0;
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("62d72d84-31d7-428a-9bbb-e443f746b0b5");
                req.IncludeMetaData = true;
                /*var dfi = new DataFormItem();
                req.Parameters = dfi;*/
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenuePayments`(`RevenueID`, `LookupID`, `Amount`, `AmountOrgCurrency`, `AmountTransactionCurrency`, `Date`, `DesignationList`, `GLPostDate`, `GLPostProces`, `GLPostStatus`, `InboundChannel`, `OriginalPaymentAmount`, `OriginalPaymentAmountOrgCurrency`, `OriginalPaymentAmountTransactionCurrency`, `PaymentMethod`, `OtherMethod`, `PledgeGrantAwardBalance`, `ReceiptAmount`, `AdvanceNoticeSentDat`, `DirectDebitReferenceDate`, `DirectDebitReferenceNumber`, `SendInstruction`, `SendInstructionType`, `SetUpInstructionDate`, `NewInstructionDate`, `CancelInstructionDate`, `DDISource`, `GAClaimableTaxAmount`, `GAClaimableTaxAmountOrgCurrency`, `GAClaimableTaxAmountTransactionCurrency`, `GAGrossAmount`, `GAGrossAmountOrgCurrency`, `GAGrossAmountTransactionCurrency`, `GAPotentialGrossAmount`, `GAPotentialGrossAmountOrgCurrency`, `GAPotentialGrossAmountTransactionCurrency`, `ReceivedTaxClaimAmount`, `ReceivedTaxClaimAmountOrgCurrency`, `ReceivedTaxClaimAmountTransactionCurrency`, `CalcGAClaimableTaxAmount`, `CalcGAClaimableTaxAmountOrgCurrency`, `CalcGAClaimableTaxAmountTransactionCurrency`, `CalcGAGrossAmount`, `CalcGAGrossAmountOrgCurrency`, `CalcGAGrossAmountTransactionCurrency`, `CalcGAPotentialGrossAmount`, `CalcGAPotentialGrossAmountOrgCurrency`, `CalcGAPotentialGrossAmountTransactionCurrency`, `CalcGAReceivedTaxClaimAmount`, `CalcGAReceivedTaxClaimAmountOrgCurrency`, `CalcGAReceivedTaxClaimAmountTransactionCurrency`, `DateAdded`, `DateChanged`, `AppealName`,BaseCurrency,TransactionCurrency) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[56];
                    string[] alteredRows = new string[56];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        Count = i;
                        Array.Copy(rows, alteredRows, 56);//this done because 6 extra hidden value coming in rows
                        alteredRows[2] = !string.IsNullOrEmpty(alteredRows[2]) ? alteredRows[2] : "null";
                        alteredRows[3] = !string.IsNullOrEmpty(alteredRows[3]) ? alteredRows[3] : "null";
                        alteredRows[4] = !string.IsNullOrEmpty(alteredRows[4]) ? alteredRows[4] : "null";
                        if (!string.IsNullOrEmpty(alteredRows[6]))
                        {
                            alteredRows[6] = alteredRows[6].Replace("'", "");
                        }
                        alteredRows[11] = !string.IsNullOrEmpty(alteredRows[11]) ? alteredRows[11] : "null";
                        alteredRows[12] = !string.IsNullOrEmpty(alteredRows[12]) ? alteredRows[12] : "null";
                        alteredRows[13] = !string.IsNullOrEmpty(alteredRows[13]) ? alteredRows[13] : "null";
                        alteredRows[16] = !string.IsNullOrEmpty(alteredRows[16]) ? alteredRows[16] : "null";
                        alteredRows[17] = !string.IsNullOrEmpty(alteredRows[17]) ? alteredRows[17] : "null";
                        alteredRows[27] = !string.IsNullOrEmpty(alteredRows[27]) ? alteredRows[27] : "null";
                        alteredRows[28] = !string.IsNullOrEmpty(alteredRows[28]) ? alteredRows[28] : "null";
                        alteredRows[29] = !string.IsNullOrEmpty(alteredRows[29]) ? alteredRows[29] : "null";
                        alteredRows[30] = !string.IsNullOrEmpty(alteredRows[30]) ? alteredRows[30] : "null";
                        alteredRows[31] = !string.IsNullOrEmpty(alteredRows[31]) ? alteredRows[31] : "null";
                        alteredRows[32] = !string.IsNullOrEmpty(alteredRows[32]) ? alteredRows[32] : "null";
                        alteredRows[33] = !string.IsNullOrEmpty(alteredRows[33]) ? alteredRows[33] : "null";
                        alteredRows[34] = !string.IsNullOrEmpty(alteredRows[34]) ? alteredRows[34] : "null";
                        alteredRows[35] = !string.IsNullOrEmpty(alteredRows[35]) ? alteredRows[35] : "null";
                        alteredRows[36] = !string.IsNullOrEmpty(alteredRows[36]) ? alteredRows[36] : "null";
                        alteredRows[37] = !string.IsNullOrEmpty(alteredRows[37]) ? alteredRows[37] : "null";
                        alteredRows[38] = !string.IsNullOrEmpty(alteredRows[38]) ? alteredRows[38] : "null";
                        alteredRows[39] = !string.IsNullOrEmpty(alteredRows[39]) ? alteredRows[39] : "null";
                        alteredRows[40] = !string.IsNullOrEmpty(alteredRows[40]) ? alteredRows[40] : "null";
                        alteredRows[41] = !string.IsNullOrEmpty(alteredRows[41]) ? alteredRows[41] : "null";
                        alteredRows[42] = !string.IsNullOrEmpty(alteredRows[42]) ? alteredRows[42] : "null";
                        alteredRows[43] = !string.IsNullOrEmpty(alteredRows[43]) ? alteredRows[43] : "null";
                        alteredRows[44] = !string.IsNullOrEmpty(alteredRows[44]) ? alteredRows[44] : "null";
                        alteredRows[45] = !string.IsNullOrEmpty(alteredRows[45]) ? alteredRows[45] : "null";
                        alteredRows[46] = !string.IsNullOrEmpty(alteredRows[46]) ? alteredRows[46] : "null";
                        alteredRows[47] = !string.IsNullOrEmpty(alteredRows[47]) ? alteredRows[47] : "null";
                        alteredRows[48] = !string.IsNullOrEmpty(alteredRows[48]) ? alteredRows[48] : "null";
                        alteredRows[49] = !string.IsNullOrEmpty(alteredRows[49]) ? alteredRows[49] : "null";
                        alteredRows[50] = !string.IsNullOrEmpty(alteredRows[50]) ? alteredRows[50] : "null";
                        alteredRows[5] = !string.IsNullOrEmpty(alteredRows[5]) ? GetDateFromDateTime(alteredRows[5]) : "null";
                        alteredRows[7] = !string.IsNullOrEmpty(alteredRows[7]) ? GetDateFromDateTime(alteredRows[7]) : "null";
                        alteredRows[18] = !string.IsNullOrEmpty(alteredRows[18]) ? GetDateFromDateTime(alteredRows[18]) : "null";
                        //alteredRows[19] = !string.IsNullOrEmpty(alteredRows[19]) ? GetDateFromDateTime(alteredRows[19]) : "null";
                        if (!string.IsNullOrEmpty(alteredRows[19]) && alteredRows[19] != "00000000")
                        {
                            DateTime DeceasedDate = DateTime.ParseExact(alteredRows[19], "yyyyMMdd", CultureInfo.InvariantCulture);
                            alteredRows[19] = DeceasedDate.ToString("yyyy-MM-dd");
                        }
                        else
                        {
                            alteredRows[19] = "null";
                        }

                        alteredRows[23] = !string.IsNullOrEmpty(alteredRows[23]) ? GetDateFromDateTime(alteredRows[23]) : "null";
                        alteredRows[24] = !string.IsNullOrEmpty(alteredRows[24]) ? GetDateFromDateTime(alteredRows[24]) : "null";
                        alteredRows[25] = !string.IsNullOrEmpty(alteredRows[25]) ? GetDateFromDateTime(alteredRows[25]) : "null";
                        alteredRows[51] = !string.IsNullOrEmpty(alteredRows[51]) ? GetDateFromDateTime(alteredRows[51]) : "null";
                        alteredRows[52] = !string.IsNullOrEmpty(alteredRows[52]) ? GetDateAddedValueInDateTimeFormat(alteredRows[52]) : "null";
                        if (!string.IsNullOrEmpty(alteredRows[53]))
                        {
                            alteredRows[53] = alteredRows[53].Replace("'", "");
                        }
                        string singleRow = string.Join("','", alteredRows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE  RevenueID= Values(RevenueID),LookupID= Values(LookupID),Amount= Values(Amount),AmountOrgCurrency= Values(AmountOrgCurrency),AmountTransactionCurrency= Values(AmountTransactionCurrency),Date= Values(Date),DesignationList= Values(DesignationList),GLPostDate= Values(GLPostDate),GLPostProces= Values(GLPostProces),GLPostStatus= Values(GLPostStatus),InboundChannel= Values(InboundChannel),OriginalPaymentAmount= Values(OriginalPaymentAmount),OriginalPaymentAmountOrgCurrency= Values(OriginalPaymentAmountOrgCurrency),OriginalPaymentAmountTransactionCurrency= Values(OriginalPaymentAmountTransactionCurrency),PaymentMethod= Values(PaymentMethod),OtherMethod= Values(OtherMethod),PledgeGrantAwardBalance= Values(PledgeGrantAwardBalance),ReceiptAmount= Values(ReceiptAmount),AdvanceNoticeSentDat= Values(AdvanceNoticeSentDat),DirectDebitReferenceDate= Values(DirectDebitReferenceDate),DirectDebitReferenceNumber= Values(DirectDebitReferenceNumber),SendInstruction= Values(SendInstruction),SendInstructionType= Values(SendInstructionType),SetUpInstructionDate= Values(SetUpInstructionDate),NewInstructionDate= Values(NewInstructionDate),CancelInstructionDate= Values(CancelInstructionDate),DDISource= Values(DDISource),GAClaimableTaxAmount= Values(GAClaimableTaxAmount),GAClaimableTaxAmountOrgCurrency= Values(GAClaimableTaxAmountOrgCurrency),GAClaimableTaxAmountTransactionCurrency= Values(GAClaimableTaxAmountTransactionCurrency),GAGrossAmount= Values(GAGrossAmount),GAGrossAmountOrgCurrency= Values(GAGrossAmountOrgCurrency),GAGrossAmountTransactionCurrency= Values(GAGrossAmountTransactionCurrency),GAPotentialGrossAmount= Values(GAPotentialGrossAmount),GAPotentialGrossAmountOrgCurrency= Values(GAPotentialGrossAmountOrgCurrency),GAPotentialGrossAmountTransactionCurrency= Values(GAPotentialGrossAmountTransactionCurrency),ReceivedTaxClaimAmount= Values(ReceivedTaxClaimAmount),ReceivedTaxClaimAmountOrgCurrency= Values(ReceivedTaxClaimAmountOrgCurrency),ReceivedTaxClaimAmountTransactionCurrency= Values(ReceivedTaxClaimAmountTransactionCurrency),CalcGAClaimableTaxAmount= Values(CalcGAClaimableTaxAmount),CalcGAClaimableTaxAmountOrgCurrency= Values(CalcGAClaimableTaxAmountOrgCurrency),CalcGAClaimableTaxAmountTransactionCurrency= Values(CalcGAClaimableTaxAmountTransactionCurrency),CalcGAGrossAmount= Values(CalcGAGrossAmount),CalcGAGrossAmountOrgCurrency= Values(CalcGAGrossAmountOrgCurrency),CalcGAGrossAmountTransactionCurrency= Values(CalcGAGrossAmountTransactionCurrency),CalcGAPotentialGrossAmount= Values(CalcGAPotentialGrossAmount),CalcGAPotentialGrossAmountOrgCurrency= Values(CalcGAPotentialGrossAmountOrgCurrency),CalcGAPotentialGrossAmountTransactionCurrency= Values(CalcGAPotentialGrossAmountTransactionCurrency),CalcGAReceivedTaxClaimAmount= Values(CalcGAReceivedTaxClaimAmount),CalcGAReceivedTaxClaimAmountOrgCurrency= Values(CalcGAReceivedTaxClaimAmountOrgCurrency),CalcGAReceivedTaxClaimAmountTransactionCurrency= Values(CalcGAReceivedTaxClaimAmountTransactionCurrency),DateAdded= Values(DateAdded),DateChanged= Values(DateChanged),AppealName= Values(AppealName),BaseCurrency = Value(BaseCurrency),TransactionCurrency = Value(TransactionCurrency)");
                    sCommand.Append(";");
                    sCommand.Replace("'t", "");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name + " in Row " + Count);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnLetterGiftInteractions()
        {
            string Table_Name = "  Letter Gift Interactions ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("1a9e0954-bd84-4df5-9340-0281ca65c120");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `LetterGiftInteractions`(`InteractionID`, `Date`, `LookupID`, `Category`, `Comment`, `Completed`, `ContactMethod`, `Event`, `EventName`, `ExpectedDate`, `FundingRequestStepStage`, `InteractionLookupID`, `IsContactReport`, `IsInteraction`, `Location`, `PlanType`, `ProspectPlanStepStage`, `Status`, `Subcategory`, `Summary`, `DateAdded`, `DateChanged`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[22];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[1] = !string.IsNullOrEmpty(rows[1]) ? GetDateFromDateTime(rows[1]) : "null";
                        rows[9] = !string.IsNullOrEmpty(rows[9]) ? GetDateFromDateTime(rows[9]) : "null";
                        rows[20] = !string.IsNullOrEmpty(rows[20]) ? GetDateFromDateTime(rows[20]) : "null";
                        rows[21] = !string.IsNullOrEmpty(rows[21]) ? GetDateAddedValueInDateTimeFormat(rows[21]) : "null";
                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE InteractionID=Value(InteractionID),Date=Value(Date),LookupID=Value(LookupID),Category=Value(Category),Comment=Value(Comment),Completed=Value(Completed),ContactMethod=Value(ContactMethod),Event=Value(Event),EventName=Value(EventName),ExpectedDate=Value(ExpectedDate),FundingRequestStepStage=Value(FundingRequestStepStage),InteractionLookupID=Value(InteractionLookupID),IsContactReport=Value(IsContactReport),IsInteraction=Value(IsInteraction),Location=Value(Location),PlanType=Value(PlanType),ProspectPlanStepStage=Value(ProspectPlanStepStage),Status=Value(Status),Subcategory=Value(Subcategory),Summary=Value(Summary),DateAdded=Value(DateAdded),DateChanged=Value(DateChanged)");
                    sCommand.Replace("'null'", "null");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRevenueRecurringGift()
        {
            string Table_Name = "  Revenue Recurring Gift ";
            int Count = 0;
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("c4c8b23c-dcc4-4134-8bcb-275aa75793b5");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenueRecurringGift`(`RevenueID`, `LookupID`, `Date`, `Amount`, `AmountOrganisationCurrency`, `AmountTransactionCurrency`, `RecurringGiftStatus`, `PaymentMethod`, `InstallmentsStartDate`, `InstallmentsEndDate`, `InstallmentFrequency`, `RecurringGiftNextTransactionDate`, `LatestInstallmentPaymentDate`, `LatestInstallmentPaymentAmount`, `DesignationList`, `InboundChannel`, `AccountSystem`, `PledgeGrantAwardBalanceTransactionCurrency`, `PledgeGrantAwardBalanceOrganisationCurrency`, `AdvanceNoticeSentDate`, `DirectDebitReferenceDate`, `DirectDebitReferenceNumber`, `SendInstruction`, `SendInstructionType`, `NewInstructionDate`, `SetUpInstructionDate`, `CancelInstructionDate`, `DDISource`, `SendPledgeReminder`, `DateAdded`, `DateChanged`,`BaseCurrency`,`TransactionCurrency`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[33];
                    string[] alteredRows = new string[33];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        Count = i;
                        Array.Copy(rows, alteredRows, 33);//this done because 3 extra hidden value coming in rows

                        alteredRows[3] = !string.IsNullOrEmpty(alteredRows[3]) ? alteredRows[3] : "null";
                        alteredRows[4] = !string.IsNullOrEmpty(alteredRows[4]) ? alteredRows[4] : "null";
                        alteredRows[5] = !string.IsNullOrEmpty(alteredRows[5]) ? alteredRows[5] : "null";
                        alteredRows[13] = !string.IsNullOrEmpty(alteredRows[13]) ? alteredRows[13] : "null";
                        alteredRows[18] = !string.IsNullOrEmpty(alteredRows[18]) ? alteredRows[18] : "null";
                        alteredRows[17] = !string.IsNullOrEmpty(alteredRows[17]) ? alteredRows[17] : "null";

                        alteredRows[2] = !string.IsNullOrEmpty(alteredRows[2]) ? GetDateFromDateTime(alteredRows[2]) : "null";
                        alteredRows[8] = !string.IsNullOrEmpty(alteredRows[8]) ? GetDateFromDateTime(alteredRows[8]) : "null";
                        alteredRows[9] = !string.IsNullOrEmpty(alteredRows[9]) ? GetDateFromDateTime(alteredRows[9]) : "null";
                        alteredRows[12] = !string.IsNullOrEmpty(alteredRows[12]) ? GetDateFromDateTime(alteredRows[12]) : "null";
                        alteredRows[11] = !string.IsNullOrEmpty(alteredRows[11]) ? GetDateFromDateTime(alteredRows[11]) : "null";
                        alteredRows[19] = !string.IsNullOrEmpty(alteredRows[19]) ? GetDateFromDateTime(alteredRows[19]) : "null";
                        // alteredRows[20] = !string.IsNullOrEmpty(alteredRows[20]) ? GetDateFromDateTime(alteredRows[20]) : "null";
                        alteredRows[24] = !string.IsNullOrEmpty(alteredRows[24]) ? GetDateFromDateTime(alteredRows[24]) : "null";
                        alteredRows[25] = !string.IsNullOrEmpty(alteredRows[25]) ? GetDateFromDateTime(alteredRows[25]) : "null";
                        alteredRows[26] = !string.IsNullOrEmpty(alteredRows[26]) ? GetDateFromDateTime(alteredRows[26]) : "null";
                        alteredRows[29] = !string.IsNullOrEmpty(alteredRows[29]) ? GetDateFromDateTime(alteredRows[29]) : "null";
                        if (!string.IsNullOrEmpty(alteredRows[20]) && alteredRows[20] != "00000000")
                        {
                            DateTime DeceasedDate = DateTime.ParseExact(alteredRows[20], "yyyyMMdd", CultureInfo.InvariantCulture);
                            alteredRows[20] = DeceasedDate.ToString("yyyy-MM-dd");
                        }
                        else
                        {
                            alteredRows[20] = "null";
                        }
                        alteredRows[30] = !string.IsNullOrEmpty(alteredRows[30]) ? GetDateAddedValueInDateTimeFormat(alteredRows[30]) : "null";

                        string singleRow = string.Join("','", alteredRows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE RevenueID = Value(RevenueID),LookupID = Value(LookupID),Date = Value(Date),Amount = Value(Amount),AmountOrganisationCurrency = Value(AmountOrganisationCurrency),AmountTransactionCurrency = Value(AmountTransactionCurrency),RecurringGiftStatus = Value(RecurringGiftStatus),PaymentMethod = Value(PaymentMethod),InstallmentsStartDate = Value(InstallmentsStartDate),InstallmentsEndDate = Value(InstallmentsEndDate),InstallmentFrequency = Value(InstallmentFrequency),RecurringGiftNextTransactionDate = Value(RecurringGiftNextTransactionDate),LatestInstallmentPaymentDate = Value(LatestInstallmentPaymentDate),LatestInstallmentPaymentAmount = Value(LatestInstallmentPaymentAmount),DesignationList = Value(DesignationList),InboundChannel = Value(InboundChannel),AccountSystem = Value(AccountSystem),PledgeGrantAwardBalanceTransactionCurrency = Value(PledgeGrantAwardBalanceTransactionCurrency),PledgeGrantAwardBalanceOrganisationCurrency = Value(PledgeGrantAwardBalanceOrganisationCurrency),AdvanceNoticeSentDate = Value(AdvanceNoticeSentDate),DirectDebitReferenceDate = Value(DirectDebitReferenceDate),DirectDebitReferenceNumber = Value(DirectDebitReferenceNumber),SendInstruction = Value(SendInstruction),SendInstructionType = Value(SendInstructionType),NewInstructionDate = Value(NewInstructionDate),SetUpInstructionDate = Value(SetUpInstructionDate),CancelInstructionDate = Value(CancelInstructionDate),DDISource = Value(DDISource),SendPledgeReminder = Value(SendPledgeReminder),DateAdded = Value(DateAdded),DateChanged = Value(DateChanged),BaseCurrency = Value(BaseCurrency),TransactionCurrency = Value(TransactionCurrency)");
                    sCommand.Replace("'null'", "null");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name + " in Row " + Count);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void SycnRevenuePledges()
        {
            string Table_Name = "  Revenue Pledges ";
            int Count = 0;
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("d9489b46-b6b4-4420-8c3d-4ebc660ad36e");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenuePledges`(`RevenueID`, `LookupID`, `AccountSystem`, `Amount`, `AmountOrgCurrency`, `AmountTransactionCurrency`, `Date`, `DesignationList`, `GLPostDate`, `GLPostProcess`, `GLPostStatus`, `InboundChannel`, `InstallmentFrequency`, `InstallmentEndDate`, `InstallmentStartDate`, `IsPending`, `LatestPaymentAmount`, `LatestPaymentDate`, `LatestPaymentAmountOrgCurrency`, `LatestPaymentAmountTransactionCurrency`, `NumberOfInstallments`, `OriginalPledgeAmount`, `OriginalPledgeAmountOrgCurrency`, `OriginalPledgeAmountTransactionCurrency`, `PaymentMethod`, `PledgeGrantAwardBalance`, `PledgeGrantAwardBalanceOrgCurrency`, `PledgeGrantAwardBalanceTransactionCurrency`, `PledgeGrantAwardFirstInstallmentDue`, `PledgeGrantAwardLastInstallmentAmount`, `PledgeGrantAwardLastInstallmentAmountOrgCurrency`, `PledgeGrantAwardLastInstallmentAmountTransactionCurrency`, `PledgeGrantAwardLastInstallmentDue`, `PledgeGrantAwardNextInstallmentDue`, `AdvanceNoticeSentDate`, `DirectDebitReferenceDate`, `DirectDebitReferenceNumber`, `SendInstruction`, `SendInstructionType`, `SetUpInstructionDate`, `NewInstructionDate`, `CancelInstructionDate`, `DDISource`, `DateAdded`, `DateChanged`,BaseCurrency,TransactionCurrency) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[47];
                    string[] alteredRows = new string[47];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        Count = i;
                        Array.Copy(rows, alteredRows, 47);//this done because 3 extra hidden value coming in rows

                        alteredRows[3] = !string.IsNullOrEmpty(alteredRows[3]) ? alteredRows[3] : "null";
                        alteredRows[4] = !string.IsNullOrEmpty(alteredRows[4]) ? alteredRows[4] : "null";
                        alteredRows[5] = !string.IsNullOrEmpty(alteredRows[5]) ? alteredRows[5] : "null";
                        alteredRows[16] = !string.IsNullOrEmpty(alteredRows[16]) ? alteredRows[16] : "null";
                        alteredRows[18] = !string.IsNullOrEmpty(alteredRows[18]) ? alteredRows[18] : "null";
                        alteredRows[19] = !string.IsNullOrEmpty(alteredRows[19]) ? alteredRows[19] : "null";
                        alteredRows[23] = !string.IsNullOrEmpty(alteredRows[23]) ? alteredRows[23] : "null";
                        alteredRows[21] = !string.IsNullOrEmpty(alteredRows[21]) ? alteredRows[21] : "null";
                        alteredRows[22] = !string.IsNullOrEmpty(alteredRows[22]) ? alteredRows[22] : "null";
                        alteredRows[27] = !string.IsNullOrEmpty(alteredRows[27]) ? alteredRows[27] : "null";
                        alteredRows[26] = !string.IsNullOrEmpty(alteredRows[26]) ? alteredRows[26] : "null";
                        alteredRows[25] = !string.IsNullOrEmpty(alteredRows[25]) ? alteredRows[25] : "null";
                        alteredRows[29] = !string.IsNullOrEmpty(alteredRows[29]) ? alteredRows[29] : "null";
                        alteredRows[30] = !string.IsNullOrEmpty(alteredRows[30]) ? alteredRows[30] : "null";
                        alteredRows[31] = !string.IsNullOrEmpty(alteredRows[31]) ? alteredRows[31] : "null";

                        alteredRows[6] = !string.IsNullOrEmpty(alteredRows[6]) ? GetDateFromDateTime(alteredRows[6]) : "null";
                        alteredRows[8] = !string.IsNullOrEmpty(alteredRows[8]) ? GetDateFromDateTime(alteredRows[8]) : "null";
                        alteredRows[13] = !string.IsNullOrEmpty(alteredRows[13]) ? GetDateFromDateTime(alteredRows[13]) : "null";
                        alteredRows[14] = !string.IsNullOrEmpty(alteredRows[14]) ? GetDateFromDateTime(alteredRows[14]) : "null";
                        alteredRows[17] = !string.IsNullOrEmpty(alteredRows[17]) ? GetDateFromDateTime(alteredRows[17]) : "null";
                        alteredRows[28] = !string.IsNullOrEmpty(alteredRows[28]) ? GetDateFromDateTime(alteredRows[28]) : "null";
                        alteredRows[32] = !string.IsNullOrEmpty(alteredRows[32]) ? GetDateFromDateTime(alteredRows[32]) : "null";
                        alteredRows[33] = !string.IsNullOrEmpty(alteredRows[33]) ? GetDateFromDateTime(alteredRows[33]) : "null";
                        alteredRows[34] = !string.IsNullOrEmpty(alteredRows[34]) ? GetDateFromDateTime(alteredRows[34]) : "null";
                        //alteredRows[35] = !string.IsNullOrEmpty(alteredRows[35]) ? GetDateFromDateTime(alteredRows[35]) : "null";
                        alteredRows[39] = !string.IsNullOrEmpty(alteredRows[39]) ? GetDateFromDateTime(alteredRows[39]) : "null";
                        alteredRows[40] = !string.IsNullOrEmpty(alteredRows[40]) ? GetDateFromDateTime(alteredRows[40]) : "null";
                        alteredRows[41] = !string.IsNullOrEmpty(alteredRows[41]) ? GetDateFromDateTime(alteredRows[41]) : "null";

                        if (!string.IsNullOrEmpty(alteredRows[35]) && alteredRows[35] != "00000000")
                        {
                            DateTime DeceasedDate = DateTime.ParseExact(alteredRows[35], "yyyyMMdd", CultureInfo.InvariantCulture);
                            alteredRows[35] = DeceasedDate.ToString("yyyy-MM-dd");
                        }
                        else
                        {
                            alteredRows[35] = "null";
                        }
                        alteredRows[43] = !string.IsNullOrEmpty(alteredRows[43]) ? GetDateAddedValueInDateTimeFormat(alteredRows[43]) : "null";
                        alteredRows[44] = !string.IsNullOrEmpty(alteredRows[44]) ? GetDateAddedValueInDateTimeFormat(alteredRows[44]) : "null";

                        string singleRow = string.Join("','", alteredRows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE RevenueID = Value(RevenueID),LookupID = Value(LookupID),AccountSystem = Value(AccountSystem),Amount = Value(Amount),AmountOrgCurrency = Value(AmountOrgCurrency),AmountTransactionCurrency = Value(AmountTransactionCurrency),Date = Value(Date),DesignationList = Value(DesignationList),GLPostDate = Value(GLPostDate),GLPostProcess = Value(GLPostProcess),GLPostStatus = Value(GLPostStatus),InboundChannel = Value(InboundChannel),InstallmentFrequency = Value(InstallmentFrequency),InstallmentEndDate = Value(InstallmentEndDate),InstallmentStartDate = Value(InstallmentStartDate),IsPending = Value(IsPending),LatestPaymentAmount = Value(LatestPaymentAmount),LatestPaymentDate = Value(LatestPaymentDate),LatestPaymentAmountOrgCurrency = Value(LatestPaymentAmountOrgCurrency),LatestPaymentAmountTransactionCurrency = Value(LatestPaymentAmountTransactionCurrency),NumberOfInstallments = Value(NumberOfInstallments),OriginalPledgeAmount = Value(OriginalPledgeAmount),OriginalPledgeAmountOrgCurrency = Value(OriginalPledgeAmountOrgCurrency),OriginalPledgeAmountTransactionCurrency = Value(OriginalPledgeAmountTransactionCurrency),PaymentMethod = Value(PaymentMethod),PledgeGrantAwardBalance = Value(PledgeGrantAwardBalance),PledgeGrantAwardBalanceOrgCurrency = Value(PledgeGrantAwardBalanceOrgCurrency),PledgeGrantAwardBalanceTransactionCurrency = Value(PledgeGrantAwardBalanceTransactionCurrency),PledgeGrantAwardFirstInstallmentDue = Value(PledgeGrantAwardFirstInstallmentDue),PledgeGrantAwardLastInstallmentAmount = Value(PledgeGrantAwardLastInstallmentAmount),PledgeGrantAwardLastInstallmentAmountOrgCurrency = Value(PledgeGrantAwardLastInstallmentAmountOrgCurrency),PledgeGrantAwardLastInstallmentAmountTransactionCurrency = Value(PledgeGrantAwardLastInstallmentAmountTransactionCurrency),PledgeGrantAwardLastInstallmentDue = Value(PledgeGrantAwardLastInstallmentDue),PledgeGrantAwardNextInstallmentDue = Value(PledgeGrantAwardNextInstallmentDue),AdvanceNoticeSentDate = Value(AdvanceNoticeSentDate),DirectDebitReferenceDate = Value(DirectDebitReferenceDate),DirectDebitReferenceNumber = Value(DirectDebitReferenceNumber),SendInstruction = Value(SendInstruction),SendInstructionType = Value(SendInstructionType),SetUpInstructionDate = Value(SetUpInstructionDate),NewInstructionDate = Value(NewInstructionDate),CancelInstructionDate = Value(CancelInstructionDate),DDISource = Value(DDISource),DateAdded = Value(DateAdded),DateChanged = Value(DateChanged),BaseCurrency = Value(BaseCurrency),TransactionCurrency = Value(TransactionCurrency)");
                    sCommand.Append(";");
                    string sql = sCommand.ToString();
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name + " in Row " + Count);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            InsertTableCount(Table_Name, Total);
        }

        public RevenuePayment GetPaymentFromOData()
        {
            string content = "";
            RevenuePayment payobj = new RevenuePayment();
            try
            {
                var client = new RestClient("https://crm64156p.sky.blackbaud.com/64156p/ODataQuery.ashx?databasename=64156p&AdHocQueryID=455606aa-2c30-410c-b782-a6788bf6063e");//O&I - DWH - Payments

                client.Authenticator = new HttpBasicAuthenticator(@"S27\" + ConfigurationManager.AppSettings["BB_UID"], ConfigurationManager.AppSettings["BB_PWD"]);
                //client.Timeout = 300000;
                var request = new RestRequest(Method.GET);
                System.Net.ServicePointManager.Expect100Continue = false;
                IRestResponse response = client.Execute(request);
                content = response.Content;
                payobj = SimpleJson.DeserializeObject<RevenuePayment>(content);
                return payobj;
            }
            catch (Exception ex)
            {
                appLogger.Error("Error in fetching Revenue Payment from Odata " + ex.Message);
                appLogger.Error(ex.InnerException);
                appLogger.Error(ex.StackTrace);
                if (content != null)
                {
                    appLogger.Error("Response content : " + content);
                }
                return null;
            }
        }
        public void SyncRevenuePaymentOdata()
        {
            string Table_Name = "Revenue payment";
            int Total = 0;
            appLogger.Info("Revenue payment Sync Started...");
            int rowCounter = 0;
            try
            {
                string command = "SET FOREIGN_KEY_CHECKS=0;INSERT INTO `RevenuePayments`(`RevenueID`,`LookupID`,`Amount`,`AmountOrgCurrency`,`AmountTransactionCurrency`,`Date`,`DesignationList`,`GLPostDate`,`GLPostProces`,`GLPostStatus`,`InboundChannel`,`OriginalPaymentAmount`,`OriginalPaymentAmountOrgCurrency`,`OriginalPaymentAmountTransactionCurrency`,`PaymentMethod`,`OtherMethod`,`PledgeGrantAwardBalance`,`ReceiptAmount`,`AdvanceNoticeSentDat`,`DirectDebitReferenceDate`,`DirectDebitReferenceNumber`,`SendInstruction`,`SendInstructionType`,`SetUpInstructionDate`,`NewInstructionDate`,`CancelInstructionDate`,`DDISource`,`GAClaimableTaxAmount`,`GAClaimableTaxAmountOrgCurrency`,`GAClaimableTaxAmountTransactionCurrency`,`GAGrossAmount`,`GAGrossAmountOrgCurrency`,`GAGrossAmountTransactionCurrency`,`GAPotentialGrossAmount`,`GAPotentialGrossAmountOrgCurrency`,`GAPotentialGrossAmountTransactionCurrency`,`ReceivedTaxClaimAmount`,`ReceivedTaxClaimAmountOrgCurrency`,`ReceivedTaxClaimAmountTransactionCurrency`,`CalcGAClaimableTaxAmount`,`CalcGAClaimableTaxAmountOrgCurrency`,`CalcGAClaimableTaxAmountTransactionCurrency`,`CalcGAGrossAmount`,`CalcGAGrossAmountOrgCurrency`,`CalcGAGrossAmountTransactionCurrency`,`CalcGAPotentialGrossAmount`,`CalcGAPotentialGrossAmountOrgCurrency`,`CalcGAPotentialGrossAmountTransactionCurrency`,`CalcGAReceivedTaxClaimAmount`,`CalcGAReceivedTaxClaimAmountOrgCurrency`,`CalcGAReceivedTaxClaimAmountTransactionCurrency`,`DateAdded`,`DateChanged`,`AppealName`,`BaseCurrency`,`TransactionCurrency`) VALUES(?RevenueID,?LookupID,?Amount,?AmountOrgCurrency,?AmountTransactionCurrency,?Date,?DesignationList,?GLPostDate,?GLPostProces,?GLPostStatus,?InboundChannel,?OriginalPaymentAmount,?OriginalPaymentAmountOrgCurrency,?OriginalPaymentAmountTransactionCurrency,?PaymentMethod,?OtherMethod,?PledgeGrantAwardBalance,?ReceiptAmount,?AdvanceNoticeSentDat,?DirectDebitReferenceDate,?DirectDebitReferenceNumber,?SendInstruction,?SendInstructionType,?SetUpInstructionDate,?NewInstructionDate,?CancelInstructionDate,?DDISource,?GAClaimableTaxAmount,?GAClaimableTaxAmountOrgCurrency,?GAClaimableTaxAmountTransactionCurrency,?GAGrossAmount,?GAGrossAmountOrgCurrency,?GAGrossAmountTransactionCurrency,?GAPotentialGrossAmount,?GAPotentialGrossAmountOrgCurrency,?GAPotentialGrossAmountTransactionCurrency,?ReceivedTaxClaimAmount,?ReceivedTaxClaimAmountOrgCurrency,?ReceivedTaxClaimAmountTransactionCurrency,?CalcGAClaimableTaxAmount,?CalcGAClaimableTaxAmountOrgCurrency,?CalcGAClaimableTaxAmountTransactionCurrency,?CalcGAGrossAmount,?CalcGAGrossAmountOrgCurrency,?CalcGAGrossAmountTransactionCurrency,?CalcGAPotentialGrossAmount,?CalcGAPotentialGrossAmountOrgCurrency,?CalcGAPotentialGrossAmountTransactionCurrency,?CalcGAReceivedTaxClaimAmount,?CalcGAReceivedTaxClaimAmountOrgCurrency,?CalcGAReceivedTaxClaimAmountTransactionCurrency,?DateAdded,?DateChanged,?AppealName,?BaseCurrency,?TransactionCurrency) ON DUPLICATE KEY UPDATE RevenueID= Values(RevenueID),LookupID= Values(LookupID),Amount= Values(Amount),AmountOrgCurrency= Values(AmountOrgCurrency),AmountTransactionCurrency= Values(AmountTransactionCurrency),Date= Values(Date),DesignationList= Values(DesignationList),GLPostDate= Values(GLPostDate),GLPostProces= Values(GLPostProces),GLPostStatus= Values(GLPostStatus),InboundChannel= Values(InboundChannel),OriginalPaymentAmount= Values(OriginalPaymentAmount),OriginalPaymentAmountOrgCurrency= Values(OriginalPaymentAmountOrgCurrency),OriginalPaymentAmountTransactionCurrency= Values(OriginalPaymentAmountTransactionCurrency),PaymentMethod= Values(PaymentMethod),OtherMethod= Values(OtherMethod),PledgeGrantAwardBalance= Values(PledgeGrantAwardBalance),ReceiptAmount= Values(ReceiptAmount),AdvanceNoticeSentDat= Values(AdvanceNoticeSentDat),DirectDebitReferenceDate= Values(DirectDebitReferenceDate),DirectDebitReferenceNumber= Values(DirectDebitReferenceNumber),SendInstruction= Values(SendInstruction),SendInstructionType= Values(SendInstructionType),SetUpInstructionDate= Values(SetUpInstructionDate),NewInstructionDate= Values(NewInstructionDate),CancelInstructionDate= Values(CancelInstructionDate),DDISource= Values(DDISource),GAClaimableTaxAmount= Values(GAClaimableTaxAmount),GAClaimableTaxAmountOrgCurrency= Values(GAClaimableTaxAmountOrgCurrency),GAClaimableTaxAmountTransactionCurrency= Values(GAClaimableTaxAmountTransactionCurrency),GAGrossAmount= Values(GAGrossAmount),GAGrossAmountOrgCurrency= Values(GAGrossAmountOrgCurrency),GAGrossAmountTransactionCurrency= Values(GAGrossAmountTransactionCurrency),GAPotentialGrossAmount= Values(GAPotentialGrossAmount),GAPotentialGrossAmountOrgCurrency= Values(GAPotentialGrossAmountOrgCurrency),GAPotentialGrossAmountTransactionCurrency= Values(GAPotentialGrossAmountTransactionCurrency),ReceivedTaxClaimAmount= Values(ReceivedTaxClaimAmount),ReceivedTaxClaimAmountOrgCurrency= Values(ReceivedTaxClaimAmountOrgCurrency),ReceivedTaxClaimAmountTransactionCurrency= Values(ReceivedTaxClaimAmountTransactionCurrency),CalcGAClaimableTaxAmount= Values(CalcGAClaimableTaxAmount),CalcGAClaimableTaxAmountOrgCurrency= Values(CalcGAClaimableTaxAmountOrgCurrency),CalcGAClaimableTaxAmountTransactionCurrency= Values(CalcGAClaimableTaxAmountTransactionCurrency),CalcGAGrossAmount= Values(CalcGAGrossAmount),CalcGAGrossAmountOrgCurrency= Values(CalcGAGrossAmountOrgCurrency),CalcGAGrossAmountTransactionCurrency= Values(CalcGAGrossAmountTransactionCurrency),CalcGAPotentialGrossAmount= Values(CalcGAPotentialGrossAmount),CalcGAPotentialGrossAmountOrgCurrency= Values(CalcGAPotentialGrossAmountOrgCurrency),CalcGAPotentialGrossAmountTransactionCurrency= Values(CalcGAPotentialGrossAmountTransactionCurrency),CalcGAReceivedTaxClaimAmount= Values(CalcGAReceivedTaxClaimAmount),CalcGAReceivedTaxClaimAmountOrgCurrency= Values(CalcGAReceivedTaxClaimAmountOrgCurrency),CalcGAReceivedTaxClaimAmountTransactionCurrency= Values(CalcGAReceivedTaxClaimAmountTransactionCurrency),DateAdded= Values(DateAdded),DateChanged= Values(DateChanged),AppealName= Values(AppealName),BaseCurrency = Values(BaseCurrency),TransactionCurrency = Values(TransactionCurrency)";

                RevenuePayment records = GetPaymentFromOData();
                rowCounter = 0;
                if (records != null && records.value.Count() > 0)
                {
                    appLogger.Info("Total: " + records.value.Count() + " Found To Sync in DB");
                    foreach (Value row in records.value)
                    {
                        try
                        {
                            dbObj = createFromCsvRow(row);
                            // DB Upsert
                            using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                            using (MySqlCommand myCmd = new MySqlCommand(command, mConnection))
                            {
                                mConnection.Open();
                                myCmd.Parameters.Add("?RevenueID", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.RevenueID) ? dbObj.RevenueID : null;
                                myCmd.Parameters.Add("?LookupID", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.LookupID) ? dbObj.LookupID : null;
                                myCmd.Parameters.Add("?Amount", MySqlDbType.Decimal).Value = dbObj.Amount;
                                myCmd.Parameters.Add("?AmountOrgCurrency", MySqlDbType.Decimal).Value = dbObj.AmountOrgCurrency;
                                myCmd.Parameters.Add("?AmountTransactionCurrency", MySqlDbType.Decimal).Value = dbObj.AmountTransactionCurrency;
                                myCmd.Parameters.Add("?Date", MySqlDbType.DateTime).Value = dbObj.Date;
                                myCmd.Parameters.Add("?DesignationList", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.DesignationList) ? dbObj.DesignationList : null;
                                myCmd.Parameters.Add("?GLPostDate", MySqlDbType.DateTime).Value = dbObj.GLPostDate;
                                myCmd.Parameters.Add("?GLPostProces", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.GLPostProces) ? dbObj.GLPostProces : null;
                                myCmd.Parameters.Add("?GLPostStatus", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.GLPostStatus) ? dbObj.GLPostStatus : null;
                                myCmd.Parameters.Add("?InboundChannel", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.InboundChannel) ? dbObj.InboundChannel : null;
                                myCmd.Parameters.Add("?OriginalPaymentAmount", MySqlDbType.Decimal).Value = dbObj.OriginalPaymentAmount;
                                myCmd.Parameters.Add("?OriginalPaymentAmountOrgCurrency", MySqlDbType.Decimal).Value = dbObj.OriginalPaymentAmountOrgCurrency;
                                myCmd.Parameters.Add("?OriginalPaymentAmountTransactionCurrency", MySqlDbType.Decimal).Value = dbObj.OriginalPaymentAmountTransactionCurrency;
                                myCmd.Parameters.Add("?PaymentMethod", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.PaymentMethod) ? dbObj.PaymentMethod : null;
                                myCmd.Parameters.Add("?OtherMethod", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.OtherMethod) ? dbObj.OtherMethod : null;
                                myCmd.Parameters.Add("?PledgeGrantAwardBalance", MySqlDbType.Decimal).Value = dbObj.PledgeGrantAwardBalance;
                                myCmd.Parameters.Add("?ReceiptAmount", MySqlDbType.Decimal).Value = dbObj.ReceiptAmount;
                                myCmd.Parameters.Add("?AdvanceNoticeSentDat", MySqlDbType.DateTime).Value = dbObj.AdvanceNoticeSentDat;
                                myCmd.Parameters.Add("?DirectDebitReferenceDate", MySqlDbType.DateTime).Value = dbObj.DirectDebitReferenceDate;
                                myCmd.Parameters.Add("?DirectDebitReferenceNumber", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.DirectDebitReferenceNumber) ? dbObj.DirectDebitReferenceNumber : null;
                                myCmd.Parameters.Add("?SendInstruction", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.SendInstruction) ? dbObj.SendInstruction : null;
                                myCmd.Parameters.Add("?SendInstructionType", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.SendInstructionType) ? dbObj.SendInstructionType : null;
                                myCmd.Parameters.Add("?SetUpInstructionDate", MySqlDbType.DateTime).Value = dbObj.SetUpInstructionDate;
                                myCmd.Parameters.Add("?NewInstructionDate", MySqlDbType.DateTime).Value = dbObj.NewInstructionDate;
                                myCmd.Parameters.Add("?CancelInstructionDate", MySqlDbType.DateTime).Value = dbObj.CancelInstructionDate;
                                myCmd.Parameters.Add("?DDISource", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.DDISource) ? dbObj.DDISource : null;
                                myCmd.Parameters.Add("?GAClaimableTaxAmount", MySqlDbType.Decimal).Value = dbObj.GAClaimableTaxAmount;
                                myCmd.Parameters.Add("?GAClaimableTaxAmountOrgCurrency", MySqlDbType.Decimal).Value = dbObj.GAClaimableTaxAmountOrgCurrency;
                                myCmd.Parameters.Add("?GAClaimableTaxAmountTransactionCurrency", MySqlDbType.Decimal).Value = dbObj.GAClaimableTaxAmountTransactionCurrency;
                                myCmd.Parameters.Add("?GAGrossAmount", MySqlDbType.Decimal).Value = dbObj.GAGrossAmount;
                                myCmd.Parameters.Add("?GAGrossAmountOrgCurrency", MySqlDbType.Decimal).Value = dbObj.GAGrossAmountOrgCurrency;
                                myCmd.Parameters.Add("?GAGrossAmountTransactionCurrency", MySqlDbType.Decimal).Value = dbObj.GAGrossAmountTransactionCurrency;
                                myCmd.Parameters.Add("?GAPotentialGrossAmount", MySqlDbType.Decimal).Value = dbObj.GAPotentialGrossAmount;
                                myCmd.Parameters.Add("?GAPotentialGrossAmountOrgCurrency", MySqlDbType.Decimal).Value = dbObj.GAPotentialGrossAmountOrgCurrency;
                                myCmd.Parameters.Add("?GAPotentialGrossAmountTransactionCurrency", MySqlDbType.Decimal).Value = dbObj.GAPotentialGrossAmountTransactionCurrency;
                                myCmd.Parameters.Add("?ReceivedTaxClaimAmount", MySqlDbType.Decimal).Value = dbObj.ReceivedTaxClaimAmount;
                                myCmd.Parameters.Add("?ReceivedTaxClaimAmountOrgCurrency", MySqlDbType.Decimal).Value = dbObj.ReceivedTaxClaimAmountOrgCurrency;
                                myCmd.Parameters.Add("?ReceivedTaxClaimAmountTransactionCurrency", MySqlDbType.Decimal).Value = dbObj.ReceivedTaxClaimAmountTransactionCurrency;
                                myCmd.Parameters.Add("?CalcGAClaimableTaxAmount", MySqlDbType.Decimal).Value = dbObj.CalcGAClaimableTaxAmount;
                                myCmd.Parameters.Add("?CalcGAClaimableTaxAmountOrgCurrency", MySqlDbType.Decimal).Value = dbObj.CalcGAClaimableTaxAmountOrgCurrency;
                                myCmd.Parameters.Add("?CalcGAClaimableTaxAmountTransactionCurrency", MySqlDbType.Decimal).Value = dbObj.CalcGAClaimableTaxAmountTransactionCurrency;
                                myCmd.Parameters.Add("?CalcGAGrossAmount", MySqlDbType.Decimal).Value = dbObj.CalcGAGrossAmount;
                                myCmd.Parameters.Add("?CalcGAGrossAmountOrgCurrency", MySqlDbType.Decimal).Value = dbObj.CalcGAGrossAmountOrgCurrency;
                                myCmd.Parameters.Add("?CalcGAGrossAmountTransactionCurrency", MySqlDbType.Decimal).Value = dbObj.CalcGAGrossAmountTransactionCurrency;
                                myCmd.Parameters.Add("?CalcGAPotentialGrossAmount", MySqlDbType.Decimal).Value = dbObj.CalcGAPotentialGrossAmount;
                                myCmd.Parameters.Add("?CalcGAPotentialGrossAmountOrgCurrency", MySqlDbType.Decimal).Value = dbObj.CalcGAPotentialGrossAmountOrgCurrency;
                                myCmd.Parameters.Add("?CalcGAPotentialGrossAmountTransactionCurrency", MySqlDbType.Decimal).Value = dbObj.CalcGAPotentialGrossAmountTransactionCurrency;
                                myCmd.Parameters.Add("?CalcGAReceivedTaxClaimAmount", MySqlDbType.Decimal).Value = dbObj.CalcGAReceivedTaxClaimAmount;
                                myCmd.Parameters.Add("?CalcGAReceivedTaxClaimAmountOrgCurrency", MySqlDbType.Decimal).Value = dbObj.CalcGAReceivedTaxClaimAmountOrgCurrency;
                                myCmd.Parameters.Add("?CalcGAReceivedTaxClaimAmountTransactionCurrency", MySqlDbType.Decimal).Value = dbObj.CalcGAReceivedTaxClaimAmountTransactionCurrency;
                                myCmd.Parameters.Add("?DateAdded", MySqlDbType.DateTime).Value = dbObj.DateAdded;
                                myCmd.Parameters.Add("?DateChanged", MySqlDbType.DateTime).Value = dbObj.DateChanged;
                                myCmd.Parameters.Add("?AppealName", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.AppealName) ? dbObj.AppealName : null;
                                myCmd.Parameters.Add("?BaseCurrency", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.BaseCurrency) ? dbObj.BaseCurrency : null;
                                myCmd.Parameters.Add("?TransactionCurrency", MySqlDbType.VarChar).Value = !string.IsNullOrWhiteSpace(dbObj.TransactionCurrency) ? dbObj.TransactionCurrency : null;
                                myCmd.ExecuteNonQuery();
                                //myCmd.CommandType = CommandType.Text;
                            }
                            Total = rowCounter++;
                            if (rowCounter % 1000 == 0)
                            {
                                appLogger.Info(rowCounter + " records inserted successfully");
                            }
                        }
                        catch (Exception e)
                        {
                            appLogger.Error("Error for row " + rowCounter + ", RevenueID " + dbObj.RevenueID);
                            appLogger.Error("Error Message - " + e.Message);
                            appLogger.Error("Stack Trace - " + e.StackTrace);
                        }
                    }
                    if (rowCounter < 1000)
                    {
                        appLogger.Info(rowCounter + " records inserted successfully.");
                    }
                }
                else
                {
                    appLogger.Info("No Records found for Sync Revenue Payment");
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Main - Error in sync revenue payment from Odata: " + e.Message);
                appLogger.Error("Main - Error for row " + rowCounter);
                appLogger.Error("Main - Error Message - " + e.Message);
                appLogger.Error("Main - Stack Trace - " + e.StackTrace);
            }
            appLogger.Info("Revenue Payment Sync Ended...");
            InsertTableCount(Table_Name, Total);
        }
        public void InsertRevenuePaymentData()
        {
            string Table_Name = " Single values for Revenue Payments ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            //fetch Revenue ids
            RevenuePayment rows = new RevenuePayment();
            rows = GetPaymentFromOData();
            appLogger.Info("Total Revenue payment records found from Odata to insert :" + rows.value.Count());
            //Insert into payment
            foreach (Value data in rows.value)
            {
                //Insert Single row
                InsertSingleRowRevenuePayments(data.RevenueID);
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
        }
        public void InsertSingleRowRevenuePayments(string Revenueid)
        {
            int Count = 0;
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {

                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("7cb848c9-8696-4183-826e-92242e355509");
                req.IncludeMetaData = true;
                var fvSet = new DataFormFieldValueSet();
                fvSet.Add("REVENUEID", Revenueid);
                var dfi = new DataFormItem();
                dfi.Values = fvSet;
                req.Parameters = dfi;
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " records with revenueId" + Revenueid);
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `RevenuePayments_16022021`(`RevenueID`, `LookupID`, `Amount`, `AmountOrgCurrency`, `AmountTransactionCurrency`, `Date`, `DesignationList`, `GLPostDate`, `GLPostProces`, `GLPostStatus`, `InboundChannel`, `OriginalPaymentAmount`, `OriginalPaymentAmountOrgCurrency`, `OriginalPaymentAmountTransactionCurrency`, `PaymentMethod`, `OtherMethod`, `PledgeGrantAwardBalance`, `ReceiptAmount`, `AdvanceNoticeSentDat`, `DirectDebitReferenceDate`, `DirectDebitReferenceNumber`, `SendInstruction`, `SendInstructionType`, `SetUpInstructionDate`, `NewInstructionDate`, `CancelInstructionDate`, `DDISource`, `GAClaimableTaxAmount`, `GAClaimableTaxAmountOrgCurrency`, `GAClaimableTaxAmountTransactionCurrency`, `GAGrossAmount`, `GAGrossAmountOrgCurrency`, `GAGrossAmountTransactionCurrency`, `GAPotentialGrossAmount`, `GAPotentialGrossAmountOrgCurrency`, `GAPotentialGrossAmountTransactionCurrency`, `ReceivedTaxClaimAmount`, `ReceivedTaxClaimAmountOrgCurrency`, `ReceivedTaxClaimAmountTransactionCurrency`, `CalcGAClaimableTaxAmount`, `CalcGAClaimableTaxAmountOrgCurrency`, `CalcGAClaimableTaxAmountTransactionCurrency`, `CalcGAGrossAmount`, `CalcGAGrossAmountOrgCurrency`, `CalcGAGrossAmountTransactionCurrency`, `CalcGAPotentialGrossAmount`, `CalcGAPotentialGrossAmountOrgCurrency`, `CalcGAPotentialGrossAmountTransactionCurrency`, `CalcGAReceivedTaxClaimAmount`, `CalcGAReceivedTaxClaimAmountOrgCurrency`, `CalcGAReceivedTaxClaimAmountTransactionCurrency`, `DateAdded`, `DateChanged`, `AppealName`,BaseCurrency,TransactionCurrency) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[56];
                    string[] alteredRows = new string[56];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        Count = i;
                        Array.Copy(rows, alteredRows, 56);//this done because 6 extra hidden value coming in rows
                        alteredRows[2] = !string.IsNullOrEmpty(alteredRows[2]) ? alteredRows[2] : "null";
                        alteredRows[3] = !string.IsNullOrEmpty(alteredRows[3]) ? alteredRows[3] : "null";
                        alteredRows[4] = !string.IsNullOrEmpty(alteredRows[4]) ? alteredRows[4] : "null";
                        alteredRows[11] = !string.IsNullOrEmpty(alteredRows[11]) ? alteredRows[11] : "null";
                        alteredRows[12] = !string.IsNullOrEmpty(alteredRows[12]) ? alteredRows[12] : "null";
                        alteredRows[13] = !string.IsNullOrEmpty(alteredRows[13]) ? alteredRows[13] : "null";
                        alteredRows[16] = !string.IsNullOrEmpty(alteredRows[16]) ? alteredRows[16] : "null";
                        alteredRows[17] = !string.IsNullOrEmpty(alteredRows[17]) ? alteredRows[17] : "null";
                        alteredRows[27] = !string.IsNullOrEmpty(alteredRows[27]) ? alteredRows[27] : "null";
                        alteredRows[28] = !string.IsNullOrEmpty(alteredRows[28]) ? alteredRows[28] : "null";
                        alteredRows[29] = !string.IsNullOrEmpty(alteredRows[29]) ? alteredRows[29] : "null";
                        alteredRows[30] = !string.IsNullOrEmpty(alteredRows[30]) ? alteredRows[30] : "null";
                        alteredRows[31] = !string.IsNullOrEmpty(alteredRows[31]) ? alteredRows[31] : "null";
                        alteredRows[32] = !string.IsNullOrEmpty(alteredRows[32]) ? alteredRows[32] : "null";
                        alteredRows[33] = !string.IsNullOrEmpty(alteredRows[33]) ? alteredRows[33] : "null";
                        alteredRows[34] = !string.IsNullOrEmpty(alteredRows[34]) ? alteredRows[34] : "null";
                        alteredRows[35] = !string.IsNullOrEmpty(alteredRows[35]) ? alteredRows[35] : "null";
                        alteredRows[36] = !string.IsNullOrEmpty(alteredRows[36]) ? alteredRows[36] : "null";
                        alteredRows[37] = !string.IsNullOrEmpty(alteredRows[37]) ? alteredRows[37] : "null";
                        alteredRows[38] = !string.IsNullOrEmpty(alteredRows[38]) ? alteredRows[38] : "null";
                        alteredRows[39] = !string.IsNullOrEmpty(alteredRows[39]) ? alteredRows[39] : "null";
                        alteredRows[40] = !string.IsNullOrEmpty(alteredRows[40]) ? alteredRows[40] : "null";
                        alteredRows[41] = !string.IsNullOrEmpty(alteredRows[41]) ? alteredRows[41] : "null";
                        alteredRows[42] = !string.IsNullOrEmpty(alteredRows[42]) ? alteredRows[42] : "null";
                        alteredRows[43] = !string.IsNullOrEmpty(alteredRows[43]) ? alteredRows[43] : "null";
                        alteredRows[44] = !string.IsNullOrEmpty(alteredRows[44]) ? alteredRows[44] : "null";
                        alteredRows[45] = !string.IsNullOrEmpty(alteredRows[45]) ? alteredRows[45] : "null";
                        alteredRows[46] = !string.IsNullOrEmpty(alteredRows[46]) ? alteredRows[46] : "null";
                        alteredRows[47] = !string.IsNullOrEmpty(alteredRows[47]) ? alteredRows[47] : "null";
                        alteredRows[48] = !string.IsNullOrEmpty(alteredRows[48]) ? alteredRows[48] : "null";
                        alteredRows[49] = !string.IsNullOrEmpty(alteredRows[49]) ? alteredRows[49] : "null";
                        alteredRows[50] = !string.IsNullOrEmpty(alteredRows[50]) ? alteredRows[50] : "null";
                        alteredRows[5] = !string.IsNullOrEmpty(alteredRows[5]) ? GetDateFromDateTime(alteredRows[5]) : "null";
                        alteredRows[7] = !string.IsNullOrEmpty(alteredRows[7]) ? GetDateFromDateTime(alteredRows[7]) : "null";
                        alteredRows[18] = !string.IsNullOrEmpty(alteredRows[18]) ? GetDateFromDateTime(alteredRows[18]) : "null";
                        //alteredRows[19] = !string.IsNullOrEmpty(alteredRows[19]) ? GetDateFromDateTime(alteredRows[19]) : "null";
                        if (!string.IsNullOrEmpty(alteredRows[19]) && alteredRows[19] != "00000000")
                        {
                            DateTime DeceasedDate = DateTime.ParseExact(alteredRows[19], "yyyyMMdd", CultureInfo.InvariantCulture);
                            alteredRows[19] = DeceasedDate.ToString("yyyy-MM-dd");
                        }
                        else
                        {
                            alteredRows[19] = "null";
                        }

                        alteredRows[23] = !string.IsNullOrEmpty(alteredRows[23]) ? GetDateFromDateTime(alteredRows[23]) : "null";
                        alteredRows[24] = !string.IsNullOrEmpty(alteredRows[24]) ? GetDateFromDateTime(alteredRows[24]) : "null";
                        alteredRows[25] = !string.IsNullOrEmpty(alteredRows[25]) ? GetDateFromDateTime(alteredRows[25]) : "null";
                        alteredRows[51] = !string.IsNullOrEmpty(alteredRows[51]) ? GetDateFromDateTime(alteredRows[51]) : "null";
                        alteredRows[52] = !string.IsNullOrEmpty(alteredRows[52]) ? GetDateAddedValueInDateTimeFormat(alteredRows[52]) : "null";
                        if (!string.IsNullOrEmpty(alteredRows[53]))
                        {
                            alteredRows[53] = alteredRows[53].Replace("'", "");
                        }
                        string singleRow = string.Join("','", alteredRows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow)); sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE  RevenueID= Values(RevenueID),LookupID= Values(LookupID),Amount= Values(Amount),AmountOrgCurrency= Values(AmountOrgCurrency),AmountTransactionCurrency= Values(AmountTransactionCurrency),Date= Values(Date),DesignationList= Values(DesignationList),GLPostDate= Values(GLPostDate),GLPostProces= Values(GLPostProces),GLPostStatus= Values(GLPostStatus),InboundChannel= Values(InboundChannel),OriginalPaymentAmount= Values(OriginalPaymentAmount),OriginalPaymentAmountOrgCurrency= Values(OriginalPaymentAmountOrgCurrency),OriginalPaymentAmountTransactionCurrency= Values(OriginalPaymentAmountTransactionCurrency),PaymentMethod= Values(PaymentMethod),OtherMethod= Values(OtherMethod),PledgeGrantAwardBalance= Values(PledgeGrantAwardBalance),ReceiptAmount= Values(ReceiptAmount),AdvanceNoticeSentDat= Values(AdvanceNoticeSentDat),DirectDebitReferenceDate= Values(DirectDebitReferenceDate),DirectDebitReferenceNumber= Values(DirectDebitReferenceNumber),SendInstruction= Values(SendInstruction),SendInstructionType= Values(SendInstructionType),SetUpInstructionDate= Values(SetUpInstructionDate),NewInstructionDate= Values(NewInstructionDate),CancelInstructionDate= Values(CancelInstructionDate),DDISource= Values(DDISource),GAClaimableTaxAmount= Values(GAClaimableTaxAmount),GAClaimableTaxAmountOrgCurrency= Values(GAClaimableTaxAmountOrgCurrency),GAClaimableTaxAmountTransactionCurrency= Values(GAClaimableTaxAmountTransactionCurrency),GAGrossAmount= Values(GAGrossAmount),GAGrossAmountOrgCurrency= Values(GAGrossAmountOrgCurrency),GAGrossAmountTransactionCurrency= Values(GAGrossAmountTransactionCurrency),GAPotentialGrossAmount= Values(GAPotentialGrossAmount),GAPotentialGrossAmountOrgCurrency= Values(GAPotentialGrossAmountOrgCurrency),GAPotentialGrossAmountTransactionCurrency= Values(GAPotentialGrossAmountTransactionCurrency),ReceivedTaxClaimAmount= Values(ReceivedTaxClaimAmount),ReceivedTaxClaimAmountOrgCurrency= Values(ReceivedTaxClaimAmountOrgCurrency),ReceivedTaxClaimAmountTransactionCurrency= Values(ReceivedTaxClaimAmountTransactionCurrency),CalcGAClaimableTaxAmount= Values(CalcGAClaimableTaxAmount),CalcGAClaimableTaxAmountOrgCurrency= Values(CalcGAClaimableTaxAmountOrgCurrency),CalcGAClaimableTaxAmountTransactionCurrency= Values(CalcGAClaimableTaxAmountTransactionCurrency),CalcGAGrossAmount= Values(CalcGAGrossAmount),CalcGAGrossAmountOrgCurrency= Values(CalcGAGrossAmountOrgCurrency),CalcGAGrossAmountTransactionCurrency= Values(CalcGAGrossAmountTransactionCurrency),CalcGAPotentialGrossAmount= Values(CalcGAPotentialGrossAmount),CalcGAPotentialGrossAmountOrgCurrency= Values(CalcGAPotentialGrossAmountOrgCurrency),CalcGAPotentialGrossAmountTransactionCurrency= Values(CalcGAPotentialGrossAmountTransactionCurrency),CalcGAReceivedTaxClaimAmount= Values(CalcGAReceivedTaxClaimAmount),CalcGAReceivedTaxClaimAmountOrgCurrency= Values(CalcGAReceivedTaxClaimAmountOrgCurrency),CalcGAReceivedTaxClaimAmountTransactionCurrency= Values(CalcGAReceivedTaxClaimAmountTransactionCurrency),DateAdded= Values(DateAdded),DateChanged= Values(DateChanged),AppealName= Values(AppealName),BaseCurrency = Value(BaseCurrency),TransactionCurrency = Value(TransactionCurrency)");
                    sCommand.Append(";");
                    sCommand.Replace("'t", "");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting revenue id" + Revenueid + " in Row " + Count);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }

        }
        public void RevenuePaymentSmartQuerySearch()
        {
            try
            {
                List<string> revernues = new List<string>();
                revernues.Add("rev-25888163");
                revernues.Add("rev-25888399");
                revernues.Add("rev-25888373");

                XElement xmlElements = new XElement("REVENUEID", revernues.Select(i => new XElement("REVENUEID", i)));

                SmartQueryLoadRequest sqreq = new SmartQueryLoadRequest();
                sqreq.ClientAppInfo = GetRequestHeader();
                sqreq.SmartQueryID = new Guid("20c819f5-0403-48d7-a9d5-26566f664831"); // live Smart Query: O&I - DWH - Revenue Payments
                List<DataFormFieldValueSet> list = new List<DataFormFieldValueSet>();


                var fvSetInternal = new DataFormFieldValueSet();
                fvSetInternal.Add("VALUE", "rev-25888373");
                list.Add(fvSetInternal);

                //RevenueID
                var fvSet = new DataFormFieldValueSet();
                fvSet.Add("REVENUEID", list);
                var sqdfi = new DataFormItem();
                sqdfi.Values = fvSet;
                sqreq.Filter = sqdfi;
                var sqresult = _service.SmartQueryLoad(sqreq);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.InnerException);
                Console.WriteLine(ex.StackTrace);
            }
        }
        public void GetExampleOData()
        {
            string content = "";
            RevenuePayment payobj = new RevenuePayment();
            try
            {
                var client = new RestClient("https://s01p2bisweb003.blackbaud.net/64156_06c7d203-38a5-43a4-9fec-9c744931be66/ODataQuery.ashx?databasename=64156&AdHocQueryID=3a0b495a-07b1-4c76-a734-76ccea8b5843");//Ad-hoc Query: Copy of O&I - DWH - Payments for Geecon
                client.Authenticator = new HttpBasicAuthenticator(@"blackbaud\esbuser64156", "Esbuswe@123");
                //client.Timeout = 300000;
                var request = new RestRequest(Method.GET);
                System.Net.ServicePointManager.Expect100Continue = false;
                IRestResponse response = client.Execute(request);
                content = response.Content;
                //appLogger.Info("Content :"+content);
                payobj = SimpleJson.DeserializeObject<RevenuePayment>(content);
                //return payobj;
            }
            catch (Exception ex)
            {
                appLogger.Error("Error in fetching Revenue Payment from Odata " + ex.Message);
                appLogger.Error(ex.InnerException);
                appLogger.Error(ex.StackTrace);
                if (content != null)
                {
                    appLogger.Error("Response content : " + content);
                }
                //return null;
            }
        }
        public string SycnRevenuePaymentsBatch(string revenueId, DateTime dateChanged, int batchSize)
        {
            appLogger.Info("DateChanged - " + dateChanged.ToString("yyyy-MM-dd") + ", RevId - " + revenueId);

            string uptoRevenueId = "rev-" + (Convert.ToInt64(revenueId.Replace("rev-", "")) + batchSize).ToString();

            SmartQueryLoadRequest sqreq = new SmartQueryLoadRequest();
            sqreq.ClientAppInfo = GetRequestHeader();
            sqreq.SmartQueryID = new Guid("1e1b3668-3563-412e-bd07-0c0d3df95625");

            var fvSet = new DataFormFieldValueSet();
            fvSet.Add("REVENUEID_1", revenueId);
            fvSet.Add("REVENUEID_2", uptoRevenueId);
            fvSet.Add("DATECHANGED", dateChanged.ToString("yyyy-MM-dd"));

            var sqdfi = new DataFormItem();
            sqdfi.Values = fvSet;

            sqreq.Filter = sqdfi;
            sqreq.MaxRecords = batchSize;

            var sqresult = _service.SmartQueryLoad(sqreq);

            appLogger.Info("DateChanged - " + dateChanged.ToString("yyyy-MM-dd") + ", RevId - " + revenueId + ", Result rows count - " + sqresult.Output.Rows.Length);

            if (sqresult.Output.Rows.Length > 0)
            {
                return sqresult.Output.Rows[sqresult.Output.Rows.Length - 1].Values[0];
            }
            else
            {
                return null;
            }
        }



        #region Sync Blackbaud Reports
        public int SycnProspectResearchReport(string System)
        {
            appLogger.Info("Prospect Research Report records Sync Process Started :" + System);
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("cf34d866-9285-4c0e-b388-24701df8b251");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + " Constituent records...");
                    if (System == "DWH")
                    {
                        mConnection.ConnectionString = myConnectionStringDWH;
                    }
                    else
                    {
                        mConnection.ConnectionString = myConnectionString;
                    }

                    StringBuilder sCommand = new StringBuilder("INSERT INTO `ProspectResearchReport`(`BBID`, `ProspectName`, `RequestDate`, `DueDate`, `DateChanged`, `ResearcherName`, `ResearchType`, `Status`, `RequestedByName`, `Priority`, `RequestID`) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[11];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        rows[1] = rows[1].Replace("'", "");
                        rows[2] = !string.IsNullOrEmpty(rows[2]) ? GetDateAddedValueInDateTimeFormat(rows[2]) : "null";
                        rows[3] = !string.IsNullOrEmpty(rows[3]) ? GetDateFromDateTime(rows[3]) : "null";
                        rows[4] = !string.IsNullOrEmpty(rows[4]) ? GetDateChangeValueInDateTimeFormat(rows[4]) : "null";

                        string singleRow = string.Join("','", rows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow));
                    sCommand.Replace("'null'", "null");
                    sCommand.Append(" ON DUPLICATE KEY UPDATE BBID = VALUES(BBID), ProspectName = VALUES(ProspectName), RequestDate = VALUES(RequestDate), DueDate = VALUES(DueDate),DateChanged= VALUES(DateChanged), ResearcherName = VALUES(ResearcherName), ResearchType = VALUES(ResearchType), Status = VALUES(Status), RequestedByName = VALUES(RequestedByName), Priority = VALUES(Priority), RequestID = VALUES(RequestID)");
                    sCommand.Append(";");
                    sCommand.Replace("'null'", "null");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + " Prospect Research Report records updated successfully");
                }
                else
                {
                    appLogger.Info("No Prospect Research Report records Found To Sync in Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting Prospect Research Report");
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Prospect Research Report records Sync Process Ended");
            return Total;
        }
        public void RunProspectResearchReport()
        {
            appLogger.Info("Prospect Research Sync Process Started..");
            int Total_DWH_Landing = 0;
            int Total_DWH = 0;
            // truncate Research Report data
            trucateProspectResearchReport("DWH_Landing");
            trucateProspectResearchReport("DWH");
            //sync Data
            Total_DWH_Landing = SycnProspectResearchReport("DWH_Landing");
            Total_DWH = SycnProspectResearchReport("DWH");

            insertLastUpdatedInDbDWH("Prospect Research", Total_DWH);//dwh
            insertLastUpdatedInDbDWH_landing("Prospect Research", Total_DWH_Landing);//dwh_landing
            appLogger.Info("Prospect Research Sync Process Ended..");

        }
        private void trucateProspectResearchReport(string System)
        {
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            string Query = "";
            try
            {
                if (System == "DWH")
                {
                    conn.ConnectionString = myConnectionStringDWH;
                }
                else
                {
                    conn.ConnectionString = myConnectionString;
                }

                conn.Open();
                Query = "truncate table ProspectResearchReport";
                MySqlCommand updateCommand = new MySqlCommand(Query, conn);
                MySqlDataReader dataReader;
                dataReader = updateCommand.ExecuteReader();
                conn.Close();
                appLogger.Info("ProspectResearchReport Table Truncated successfully in " + System);
            }
            catch (Exception ex)
            {
                conn.Close();
                appLogger.Error("Error in Truncate ProspectResearchReport table in :" + System + " : " + ex.Message);
                appLogger.Error(ex.StackTrace);
            }
        }
        #endregion
        public void MajorDonorProcessInDWH()
        {
            appLogger.Info("Major Donor Sync Process Started..");
            //truncate table first
            int Total = 0;
            trucateMojorDonorTableInDWH();
            trucateMojorDonorTableInDWH_Landing();

            //insert new records
            SycnMajorDonor();// dwh_landing
            Total = SyncMajorDonorTable();
            if (Total > 0 )
            {
                //update total record Inserted in Table 
                insertLastUpdatedInDbDWH("MajorDonor", Total);
                insertLastUpdatedInDbDWH_landing("MajorDonor", Total);
            }else
            {
                appLogger.Info("Major Donor sync no record found to insert ");
            }

            appLogger.Info("Major Donor Sync Process Ended..");
        }
        private void trucateMojorDonorTableInDWH()
        {
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            string Query = "";
            try
            {
                conn.ConnectionString = myConnectionStringDWH;
                conn.Open();
                Query = "truncate table DWH.MajorDonor";
                MySqlCommand updateCommand = new MySqlCommand(Query, conn);
                MySqlDataReader dataReader;
                dataReader = updateCommand.ExecuteReader();
                conn.Close();
                appLogger.Info("MajorDonor DWH Table Truncated successfully.");
            }
            catch (Exception ex)
            {
                conn.Close();
                appLogger.Error("Error in Truncate MajorDonor table in DHW " + ex.Message);
                appLogger.Error(ex.StackTrace);
            }
        }
        private void trucateMojorDonorTableInDWH_Landing()
        {
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            string Query = "";
            try
            {
                conn.ConnectionString = myConnectionString;
                conn.Open();
                Query = "truncate table dwh_landing.MajorDonor";
                MySqlCommand updateCommand = new MySqlCommand(Query, conn);
                MySqlDataReader dataReader;
                dataReader = updateCommand.ExecuteReader();
                conn.Close();
                appLogger.Info("MajorDonor dwh_landing Table Truncated successfully.");
            }
            catch (Exception ex)
            {
                conn.Close();
                appLogger.Error("Error in Truncate MajorDonor table in dwh_landing " + ex.Message);
                appLogger.Error(ex.StackTrace);
            }
        }
        public int SyncMajorDonorTable()
        {
            string Table_Name = "  Major Donor ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            int counter = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("e1d8fc73-6000-41e3-a54a-e8bc755c80ce");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);

                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionStringDWH;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `MajorDonor`(`BBID`, `Name`, `Postcode`, `ProspectStatus`, `ProspectManager`, `ProspectManagerStartDate`, `ProspectManagerEndDate`, `PreviousProspectManager`, `PreviousProspectManagerStartDate`, `PreviousProspectManagerEndDate`, `StepDate`, `StepObjective`, `StepStage`, `StepStatus`, `AskDate`, `AskAmount`, `ResponceDate`, `AcceptedAmount`, `OpportunityStatus`, `Designation`, `InterventionName`, `InterventionType`, `Country`,`Constituencydateto`,`Associatedrevenue`,DateAddedAssociatedRevenue,AssociatedRevenueID) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[27];
                    string[] alteredRows = new string[27];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        Total = rows.Count();
                        Array.Copy(rows, alteredRows, 27);//this done because 3 extra hidden value coming in rows
                                                          //20,11 -> ',' ,1,3,4,7,8,9,10,11 - '
                        counter++;
                        alteredRows[20] = string.IsNullOrEmpty(alteredRows[20]) ? "null" : alteredRows[20].Replace("'s", "s").Replace(",", "");
                        alteredRows[11] = string.IsNullOrEmpty(alteredRows[11]) ? "null" : alteredRows[11].Replace("'s", "s").Replace(",", "");

                        alteredRows[1] = string.IsNullOrEmpty(alteredRows[1]) ? "null" : alteredRows[1].Replace("'s", "s").Replace("'", "");
                        alteredRows[3] = string.IsNullOrEmpty(alteredRows[3]) ? "null" : alteredRows[3].Replace("'s", "s").Replace("'", "");
                        alteredRows[4] = string.IsNullOrEmpty(alteredRows[4]) ? "null" : alteredRows[4].Replace("'s", "s").Replace("'", "");
                        alteredRows[7] = string.IsNullOrEmpty(alteredRows[7]) ? "null" : alteredRows[7].Replace("'s", "s").Replace("'", "");
                        alteredRows[11] = string.IsNullOrEmpty(alteredRows[11]) ? "null" : alteredRows[11].Replace("'s", "s").Replace("'", "");

                        alteredRows[15] = !string.IsNullOrEmpty(alteredRows[15]) ? alteredRows[15] : "null";
                        alteredRows[17] = !string.IsNullOrEmpty(alteredRows[17]) ? alteredRows[17] : "null";
                        alteredRows[24] = !string.IsNullOrEmpty(alteredRows[24]) ? alteredRows[24] : "null";


                        alteredRows[5] = !string.IsNullOrEmpty(alteredRows[5]) ? GetDateFromDateTime(alteredRows[5]) : "null";
                        alteredRows[6] = !string.IsNullOrEmpty(alteredRows[6]) ? GetDateFromDateTime(alteredRows[6]) : "null";
                        alteredRows[8] = !string.IsNullOrEmpty(alteredRows[8]) ? GetDateFromDateTime(alteredRows[8]) : "null";
                        alteredRows[9] = !string.IsNullOrEmpty(alteredRows[9]) ? GetDateFromDateTime(alteredRows[9]) : "null";
                        alteredRows[10] = !string.IsNullOrEmpty(alteredRows[10]) ? GetDateFromDateTime(alteredRows[10]) : "null";
                        alteredRows[14] = !string.IsNullOrEmpty(alteredRows[14]) ? GetDateFromDateTime(alteredRows[14]) : "null";
                        alteredRows[16] = !string.IsNullOrEmpty(alteredRows[16]) ? GetDateFromDateTime(alteredRows[16]) : "null";
                        alteredRows[23] = !string.IsNullOrEmpty(alteredRows[23]) ? GetDateFromDateTime(alteredRows[23]) : "null";
                        alteredRows[25] = !string.IsNullOrEmpty(alteredRows[25]) ? GetDateFromDateTime(alteredRows[25]) : "null";


                        string singleRow = string.Join("','", alteredRows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow));
                    sCommand.Replace("'null'", "null");
                    sCommand.Replace("@", "");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name + counter);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }
            appLogger.Info("Sync" + Table_Name + "Process Ended...");
            return Total;
        }
        public void SycnMajorDonor()
        {
            string Table_Name = "  Major Donor ";
            appLogger.Info("Sync" + Table_Name + "Process Started...");
            MySql.Data.MySqlClient.MySqlConnection mConnection;
            mConnection = new MySql.Data.MySqlClient.MySqlConnection();
            int Total = 0;
            int counter = 0;
            try
            {
                DataListLoadRequest req = new DataListLoadRequest();
                req.ClientAppInfo = GetRequestHeader();
                req.DataListID = new Guid("e1d8fc73-6000-41e3-a54a-e8bc755c80ce");
                req.IncludeMetaData = true;
                var dfi = new DataFormItem();
                req.Parameters = dfi;
                var result = _service.DataListLoad(req);
                if (result.TotalRowsInReply > 0)
                {
                    appLogger.Info("Updating " + result.TotalAvailableRows + Table_Name + " records");
                    mConnection.ConnectionString = myConnectionString;
                    StringBuilder sCommand = new StringBuilder("INSERT INTO `MajorDonor`(`BBID`, `Name`, `Postcode`, `ProspectStatus`, `ProspectManager`, `ProspectManagerStartDate`, `ProspectManagerEndDate`, `PreviousProspectManager`, `PreviousProspectManagerStartDate`, `PreviousProspectManagerEndDate`, `StepDate`, `StepObjective`, `StepStage`, `StepStatus`, `AskDate`, `AskAmount`, `ResponceDate`, `AcceptedAmount`, `OpportunityStatus`, `Designation`, `InterventionName`, `InterventionType`, `Country`,`Constituencydateto`,`Associatedrevenue`,DateAddedAssociatedRevenue,AssociatedRevenueID) VALUES ");
                    List<DataListResultRow> row = new List<DataListResultRow>();
                    List<string> stringRow = new List<string>();
                    string[] rows = new string[27];
                    string[] alteredRows = new string[27];
                    for (int i = 0; i < result.TotalAvailableRows; i++)
                    {
                        rows = result.Rows[i].Values;
                        Array.Copy(rows, alteredRows, 27);//this done because 3 extra hidden value coming in rows
                                                          //20,11 -> ',' ,1,3,4,7,8,9,10,11 - '
                        counter++;
                        alteredRows[20] = string.IsNullOrEmpty(alteredRows[20]) ? "null" : alteredRows[20].Replace("'s", "s").Replace(",", "");
                        alteredRows[11] = string.IsNullOrEmpty(alteredRows[11]) ? "null" : alteredRows[11].Replace("'s", "s").Replace(",", "");

                        alteredRows[1] = string.IsNullOrEmpty(alteredRows[1]) ? "null" : alteredRows[1].Replace("'s", "s").Replace("'", "");
                        alteredRows[3] = string.IsNullOrEmpty(alteredRows[3]) ? "null" : alteredRows[3].Replace("'s", "s").Replace("'", "");
                        alteredRows[4] = string.IsNullOrEmpty(alteredRows[4]) ? "null" : alteredRows[4].Replace("'s", "s").Replace("'", "");
                        alteredRows[7] = string.IsNullOrEmpty(alteredRows[7]) ? "null" : alteredRows[7].Replace("'s", "s").Replace("'", "");
                        alteredRows[11] = string.IsNullOrEmpty(alteredRows[11]) ? "null" : alteredRows[11].Replace("'s", "s").Replace("'", "");

                        alteredRows[15] = !string.IsNullOrEmpty(alteredRows[15]) ? alteredRows[15] : "null";
                        alteredRows[17] = !string.IsNullOrEmpty(alteredRows[17]) ? alteredRows[17] : "null";
                        alteredRows[24] = !string.IsNullOrEmpty(alteredRows[24]) ? alteredRows[24] : "null";


                        alteredRows[5] = !string.IsNullOrEmpty(alteredRows[5]) ? GetDateFromDateTime(alteredRows[5]) : "null";
                        alteredRows[6] = !string.IsNullOrEmpty(alteredRows[6]) ? GetDateFromDateTime(alteredRows[6]) : "null";
                        alteredRows[8] = !string.IsNullOrEmpty(alteredRows[8]) ? GetDateFromDateTime(alteredRows[8]) : "null";
                        alteredRows[9] = !string.IsNullOrEmpty(alteredRows[9]) ? GetDateFromDateTime(alteredRows[9]) : "null";
                        alteredRows[10] = !string.IsNullOrEmpty(alteredRows[10]) ? GetDateFromDateTime(alteredRows[10]) : "null";
                        alteredRows[14] = !string.IsNullOrEmpty(alteredRows[14]) ? GetDateFromDateTime(alteredRows[14]) : "null";
                        alteredRows[16] = !string.IsNullOrEmpty(alteredRows[16]) ? GetDateFromDateTime(alteredRows[16]) : "null";
                        alteredRows[23] = !string.IsNullOrEmpty(alteredRows[23]) ? GetDateFromDateTime(alteredRows[23]) : "null";
                        alteredRows[25] = !string.IsNullOrEmpty(alteredRows[25]) ? GetDateFromDateTime(alteredRows[25]) : "null";


                        string singleRow = string.Join("','", alteredRows);
                        stringRow.Add(string.Format("('{0}')", string.Join("','", singleRow)));
                    }
                    sCommand.Append(string.Join(",", stringRow));
                    sCommand.Replace("'null'", "null");
                    sCommand.Replace("@", "");
                    sCommand.Append(";");
                    mConnection.Open();
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery(); Total += result.TotalRowsInReply;
                    }
                    appLogger.Info(result.TotalAvailableRows + Table_Name + " records updated successfully");
                }
                else
                {
                    appLogger.Info("No " + Table_Name + " Records Found To Insert into Database");
                }
            }
            catch (Exception e)
            {
                mConnection.Close();
                appLogger.Error("Error in Inserting" + Table_Name + counter);
                appLogger.Error(e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                mConnection.Close();
            }


            appLogger.Info("Sync" + Table_Name + "Process Ended...");
        }
        public void insertLastUpdatedInDbDWH(string tbl_name, int count)
        {
            MySql.Data.MySqlClient.MySqlConnection conn;
            string Query = "";
            long recordId = 0;
            try
            {
                conn = new MySql.Data.MySqlClient.MySqlConnection();
                conn.ConnectionString = myConnectionStringDWH;
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

        public void insertLastUpdatedInDbDWH_landing(string tbl_name, int count)
        {
            MySql.Data.MySqlClient.MySqlConnection conn;
            string Query = "";
            long recordId = 0;
            try
            {
                conn = new MySql.Data.MySqlClient.MySqlConnection();
                conn.ConnectionString = myConnectionString;
                conn.Open();


                Query = "INSERT INTO `LastUpdated`(`LastUpdated`,Table_Name,RecordSync) VALUES ('" + DateTime.Now + "','" + tbl_name + "'," + count + ")";


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

        #region Helper Methods
        public static DBModel createFromCsvRow(Value row)
        {
            DBModel dbObj = new DBModel();
            dbObj.RevenueID = row.RevenueID;
            dbObj.LookupID = row.LookupID;
            dbObj.Amount = stringToDecimal(row.Amount);
            dbObj.AmountOrgCurrency = stringToDecimal(row.AmountOrgCurrency);
            dbObj.AmountTransactionCurrency = stringToDecimal(row.AmountTransactionCurrency);
            dbObj.Date = stringToDateTime(row.Date);
            dbObj.DesignationList = row.DesignationList;
            dbObj.GLPostDate = stringToDateTime(row.GLPostDate);
            dbObj.GLPostProces = row.GLPostProces;
            dbObj.GLPostStatus = row.GLPostStatus;
            dbObj.InboundChannel = row.InboundChannel;
            dbObj.OriginalPaymentAmount = stringToDecimal(row.OriginalPaymentAmount);
            dbObj.OriginalPaymentAmountOrgCurrency = stringToDecimal(row.OriginalPaymentAmountOrgCurrency);
            dbObj.OriginalPaymentAmountTransactionCurrency = stringToDecimal(row.OriginalPaymentAmountTransactionCurrency);
            dbObj.PaymentMethod = row.PaymentMethod;
            dbObj.OtherMethod = row.OtherMethod;
            dbObj.PledgeGrantAwardBalance = stringToDecimal(row.PledgeGrantAwardBalance);
            dbObj.ReceiptAmount = stringToDecimal(row.ReceiptAmount);
            dbObj.AdvanceNoticeSentDat = stringToDateTime(row.AdvanceNoticeSentDat);
            dbObj.DirectDebitReferenceDate = stringToDateTime(row.DirectDebitReferenceDate);
            dbObj.DirectDebitReferenceNumber = row.DirectDebitReferenceNumber;
            dbObj.SendInstruction = row.SendInstruction;
            dbObj.SendInstructionType = row.SendInstructionType;
            dbObj.SetUpInstructionDate = stringToDateTime(row.SetUpInstructionDate);
            dbObj.NewInstructionDate = stringToDateTime(row.NewInstructionDate);
            dbObj.CancelInstructionDate = stringToDateTime(row.CancelInstructionDate);
            dbObj.DDISource = row.DDISource;
            dbObj.GAClaimableTaxAmount = stringToDecimal(row.GAClaimableTaxAmount);
            dbObj.GAClaimableTaxAmountOrgCurrency = stringToDecimal(row.GAClaimableTaxAmountOrgCurrency);
            dbObj.GAClaimableTaxAmountTransactionCurrency = stringToDecimal(row.GAClaimableTaxAmountTransactionCurrency);
            dbObj.GAGrossAmount = stringToDecimal(row.GAGrossAmount);
            dbObj.GAGrossAmountOrgCurrency = stringToDecimal(row.GAGrossAmountOrgCurrency);
            dbObj.GAGrossAmountTransactionCurrency = stringToDecimal(row.GAGrossAmountTransactionCurrency);
            dbObj.GAPotentialGrossAmount = stringToDecimal(row.GAPotentialGrossAmount);
            dbObj.GAPotentialGrossAmountOrgCurrency = stringToDecimal(row.GAPotentialGrossAmountOrgCurrency);
            dbObj.GAPotentialGrossAmountTransactionCurrency = stringToDecimal(row.GAPotentialGrossAmountTransactionCurrency);
            dbObj.ReceivedTaxClaimAmount = stringToDecimal(row.ReceivedTaxClaimAmount);
            dbObj.ReceivedTaxClaimAmountOrgCurrency = stringToDecimal(row.ReceivedTaxClaimAmountOrgCurrency);
            dbObj.ReceivedTaxClaimAmountTransactionCurrency = stringToDecimal(row.ReceivedTaxClaimAmountTransactionCurrency);
            dbObj.CalcGAClaimableTaxAmount = stringToDecimal(row.CalcGAClaimableTaxAmount);
            dbObj.CalcGAClaimableTaxAmountOrgCurrency = stringToDecimal(row.CalcGAClaimableTaxAmountOrgCurrency);
            dbObj.CalcGAClaimableTaxAmountTransactionCurrency = stringToDecimal(row.CalcGAClaimableTaxAmountTransactionCurrency);
            dbObj.CalcGAGrossAmount = stringToDecimal(row.CalcGAGrossAmount);
            dbObj.CalcGAGrossAmountOrgCurrency = stringToDecimal(row.CalcGAGrossAmountOrgCurrency);
            dbObj.CalcGAGrossAmountTransactionCurrency = stringToDecimal(row.CalcGAGrossAmountTransactionCurrency);
            dbObj.CalcGAPotentialGrossAmount = stringToDecimal(row.CalcGAPotentialGrossAmount);
            dbObj.CalcGAPotentialGrossAmountOrgCurrency = stringToDecimal(row.CalcGAPotentialGrossAmountOrgCurrency);
            dbObj.CalcGAPotentialGrossAmountTransactionCurrency = stringToDecimal(row.CalcGAPotentialGrossAmountTransactionCurrency);
            dbObj.CalcGAReceivedTaxClaimAmount = stringToDecimal(row.CalcGAReceivedTaxClaimAmount);
            dbObj.CalcGAReceivedTaxClaimAmountOrgCurrency = stringToDecimal(row.CalcGAReceivedTaxClaimAmountOrgCurrency);
            dbObj.CalcGAReceivedTaxClaimAmountTransactionCurrency = stringToDecimal(row.CalcGAReceivedTaxClaimAmountTransactionCurrency);
            dbObj.DateAdded = stringToDateTime(row.DateAdded);
            dbObj.DateChanged = stringToDateTime(row.DateChanged);
            dbObj.AppealName = row.AppealName;
            dbObj.BaseCurrency = row.BaseCurrency;
            dbObj.TransactionCurrency = row.TransactionCurrency;

            return dbObj;
        }
        public static decimal stringToDecimal(string strVal)
        {
            // Example - £1,006.29 / €360.00
            if (!string.IsNullOrWhiteSpace(strVal))
            {
                strVal = strVal.Replace("£", "").Replace("€", "");
                return Convert.ToDecimal(strVal);
            }
            return 0;
        }
        public static DateTime? stringToDateTime(string strVal)
        {
            if (string.IsNullOrWhiteSpace(strVal) || strVal.Equals("0/0/2000"))
            {
                return null;
            }

            // Example - 3/26/2020 05:46 AM
            // Format - M/d/yyyy hh:mm tt
            try
            {
                DateTime? dateTime = DateTime.ParseExact(strVal, "M/d/yyyy hh:mm tt", CultureInfo.InvariantCulture);
                return dateTime;
            }
            catch (FormatException e1)
            {
                // Example - 3/16/2020
                // Format - M/d/yyyy
                try
                {
                    DateTime dateTime = DateTime.ParseExact(strVal, "M/d/yyyy", CultureInfo.InvariantCulture);
                    return dateTime;
                }
                catch (FormatException e2)
                {

                    try
                    {
                        DateTime dt = DateTime.Parse(strVal);
                        return dt;
                    }
                    catch (FormatException e3)
                    {
                        throw e3;
                    }

                }
            }
        }
        public void InsertTableCount(string tbl_name, int count)
        {
            MySql.Data.MySqlClient.MySqlConnection conn = new MySql.Data.MySqlClient.MySqlConnection(); ;
            string Query = "";
            long recordId = 0;
            try
            {
                conn.ConnectionString = myConnectionString;
                conn.Open();
                Query = "INSERT INTO dwh_landing.`LastUpdated`(`LastUpdated`,Source,Table_Name,RecordSync) VALUES ('" + DateTime.Now + "','BB','" + tbl_name.Trim() + "'," + count + ")";

                MySqlCommand insertCommand = new MySqlCommand(Query, conn);
                MySqlDataReader dataReader;
                dataReader = insertCommand.ExecuteReader();     // Here our query will be executed and data saved into the database.
                recordId = insertCommand.LastInsertedId;
                conn.Close();
            }
            catch (Exception e)
            {
                conn.Close();
                appLogger.Error("Error in inserting record in DB : " + e.Message);
                appLogger.Error(e.StackTrace);

            }
            finally
            {
                conn.Close();
            }

        }
        public string GetDateAddedValueInDateTimeFormat(string DataAdded)
        {
            string significantdigitsDA = ".";
            string DateAddedValue = DataAdded;
            string[] DateAddedStr = null;
            if (DateAddedValue.Contains("."))
            {
                DateAddedStr = DateAddedValue.Split('.');
                if (DateAddedStr[1].Length > 0)
                {
                    for (int j = 0; j < DateAddedStr[1].Length; j++)
                    {
                        significantdigitsDA += "f";
                    }
                }
            }
            else
            {
                significantdigitsDA = "";
            }
            DateTime DateAdded = DateTime.ParseExact(DataAdded, "yyyy-MM-ddTHH:mm:ss" + significantdigitsDA, CultureInfo.InvariantCulture);
            string DateAdd = DateAdded.ToString("yyyy-MM-dd HH:mm:ss");
            return DateAdd;
        }
        public string GetDateChangeValueInDateTimeFormat(string DateChange)
        {
            string significantdigitsDC = ".";
            string DateChangedValue = DateChange;
            string[] DateChangedStr = null;
            if (DateChangedValue.Contains("."))
            {
                DateChangedStr = DateChangedValue.Split('.');
                if (DateChangedStr[1].Length > 0)
                {
                    for (int j = 0; j < DateChangedStr[1].Length; j++)
                    {
                        significantdigitsDC += "f";
                    }
                }
            }
            else
            {
                significantdigitsDC = "";
            }
            DateTime DateChanged = DateTime.ParseExact(DateChange, "yyyy-MM-ddTHH:mm:ss" + significantdigitsDC, CultureInfo.InvariantCulture);
            string DC = DateChanged.ToString("yyyy-MM-dd HH:mm:ss");
            return DC;
        }
        public string GetDateTimeRNValueInDateTimeFormat(string DataAdded)
        {
            string significantdigitsDA = ".";
            string DateAddedValue = DataAdded;
            string[] DateAddedStr = null;
            if (DateAddedValue.Contains("."))
            {
                DateAddedStr = DateAddedValue.Split('.');
                if (DateAddedStr[1].Length > 0)
                {
                    for (int j = 1; j < DateAddedStr[1].Length; j++)
                    {
                        significantdigitsDA += "f";
                    }
                }
            }
            else
            {
                significantdigitsDA = "";
            }
            DateTime DateAdded = DateTime.ParseExact(DataAdded, "yyyy-MM-ddTHH:mm:ss" + significantdigitsDA + "Z", CultureInfo.InvariantCulture);
            string DateAdd = DateAdded.ToString("yyyy-MM-dd HH:mm:ss");
            return DateAdd;
        }
        public string GetDateFromDateTime(string Date)
        {
            DateTime dt = DateTime.Parse(Date);
            string actualDate = dt.ToString("yyyy-MM-dd");
            return actualDate;
        }
        #endregion

        public StepsReporting GetStepsReportingData()
        {
            string content = "";
            StepsReporting payobj = new StepsReporting();
            try
            {
                var client = new RestClient("https://crm64156p.sky.blackbaud.com/64156p/ODataQuery.ashx?databasename=64156p&AdHocQueryID=b7bae10e-6280-4907-a488-6239f81d69bc");// Steps Reporting
                client.Authenticator = new HttpBasicAuthenticator(@"S27\" + ConfigurationManager.AppSettings["BB_UID"], ConfigurationManager.AppSettings["BB_PWD"]);
                //client.Timeout = 300000;
                var request = new RestRequest(Method.GET);
                System.Net.ServicePointManager.Expect100Continue = false;
                IRestResponse response = client.Execute(request);
                content = response.Content;
                payobj = SimpleJson.DeserializeObject<StepsReporting>(content);
                return payobj;
            }
            catch (Exception ex)
            {
                appLogger.Error("Error in fetching GetPledgeData_1 from Odata " + ex.Message);
                appLogger.Error(ex.InnerException);
                appLogger.Error(ex.StackTrace);
                if (content != null)
                {
                    appLogger.Error("Response content : " + content);
                }
                return null;
            }
        }
        public int SyncStepsReporting(string Database)
        {
            appLogger.Info("Steps Reporting process started...");
            List<Value> allValues = new List<Value>();
            StepsReporting records = GetStepsReportingData();
            int SyncCount = 0;
            int PageSize = 2000;
            int Total = 0;
            int startLimit = 0;
            int Count = 2000;
            try
            {
                if (records.value.Count() > 0)
                {
                    Total = records.value.Count();
                    SyncCount = Total;
                    appLogger.Info("Total Record found to insert into DB:" + Total);
                    do
                    {
                        if (Total < 2000)
                        {
                            Count = Total;
                        }
                        List<Value> rows = records.value.ToList().GetRange(startLimit, Count);
                        appLogger.Info("Updating " + rows.Count + " Steps Reporting records...");
                        StringBuilder sCommand = new StringBuilder("INSERT INTO `StepsReporting`(`BBID`, `Name`, `Prospectstatus`, `Prospectmanager`, `Prospectmanagerstartdate`, `Prospectmanagerenddate`, `Stepdate`, `Stepobjective`, `Stepstage`, `Stepstatus`) VALUES ");
                        string ConnectionString = Database == "DWH_landing" ? myConnectionString : myConnectionStringDWH;
                        using (MySqlConnection mConnection = new MySqlConnection(ConnectionString))
                        {
                            List<string> Rows = new List<string>();
                            foreach (Value row in rows)
                            {
                                string BBID = !string.IsNullOrEmpty(row.BBID) ? row.BBID : "null";
                                string Name = !string.IsNullOrEmpty(row.Name) ? row.Name : "null";
                                string Prospectstatus = !string.IsNullOrEmpty(row.Prospectstatus) ? row.Prospectstatus : "null";
                                string Prospectmanager = !string.IsNullOrEmpty(row.Prospectmanager) ? row.Prospectmanager : "null";
                                string Prospectmanagerstartdate = !string.IsNullOrEmpty(row.Prospectmanagerstartdate) ? GetDateFromDateTime(row.Prospectmanagerstartdate) : "null";
                                string Prospectmanagerenddate = !string.IsNullOrEmpty(row.Prospectmanagerenddate) ? GetDateFromDateTime(row.Prospectmanagerenddate) : "null";
                                string Stepdate = !string.IsNullOrEmpty(row.Stepdate) ? GetDateFromDateTime(row.Stepdate) : "null";
                                string Stepobjective = !string.IsNullOrEmpty(row.Stepobjective) ? row.Stepobjective.Replace("'", "") : "null";
                                string Stepstage = !string.IsNullOrEmpty(row.Stepstage) ? row.Stepstage : "null";
                                string Stepstatus = !string.IsNullOrEmpty(row.Stepstatus) ? row.Stepstatus : "null";

                                string singleRow = "'" + BBID + "','" + Name.Replace("'", "") + "','" + Prospectstatus + "','" + Prospectmanager + "','" + Prospectmanagerstartdate + "','" + Prospectmanagerenddate + "','" + Stepdate + "','" + Stepobjective.Replace("@", "") + "','" + Stepstage + "','" + Stepstatus + "'";
                                Rows.Add(string.Format("({0})", singleRow));
                            }
                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Replace("'null'", "null");
                            sCommand.Append(" ON DUPLICATE KEY UPDATE BBID= Values(BBID),Name= Values(Name),Prospectstatus= Values(Prospectstatus),Prospectmanager= Values(Prospectmanager),Prospectmanagerstartdate= Values(Prospectmanagerstartdate),Prospectmanagerenddate= Values(Prospectmanagerenddate),Stepdate= Values(Stepdate),Stepobjective= Values(Stepobjective),Stepstage= Values(Stepstage),Stepstatus= Values(Stepstatus)");
                            sCommand.Append(";");
                            mConnection.Open();
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                            mConnection.Close();
                            appLogger.Info(rows.Count + " Steps Reporting records updated successfully");
                            appLogger.Info("Remaining record to insert:" + Total);
                        }
                        startLimit += PageSize;
                        Total -= PageSize;
                    } while (Total > 0);
                }
            }
            catch (Exception e)
            {
                SyncCount = 0;
                appLogger.Error("Error in sync Steps Reporting:" + e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            appLogger.Info("Steps Reporting ended...");
            return SyncCount;

        }

        private void trucateStepsReporting_DWH_Landing()
        {
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            string Query = "";
            try
            {
                conn.ConnectionString = myConnectionString;
                conn.Open();
                Query = "truncate table StepsReporting";
                MySqlCommand updateCommand = new MySqlCommand(Query, conn);
                MySqlDataReader dataReader;
                dataReader = updateCommand.ExecuteReader();
                conn.Close();
                appLogger.Info("StepsReporting Table Truncated successfully DWH_Landing.");
            }
            catch (Exception ex)
            {
                conn.Close();
                appLogger.Error("Error in Truncate StepsReporting table in DHW_landing " + ex.Message);
                appLogger.Error(ex.StackTrace);
            }
        }

        private void trucateStepsReporting_DWH()
        {
            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();
            string Query = "";
            try
            {
                conn.ConnectionString = myConnectionStringDWH;
                conn.Open();
                Query = "truncate table StepsReporting";
                MySqlCommand updateCommand = new MySqlCommand(Query, conn);
                MySqlDataReader dataReader;
                dataReader = updateCommand.ExecuteReader();
                conn.Close();
                appLogger.Info("StepsReporting Table Truncated successfully DWH.");
            }
            catch (Exception ex)
            {
                conn.Close();
                appLogger.Error("Error in Truncate StepsReporting table in DHW_landing " + ex.Message);
                appLogger.Error(ex.StackTrace);
            }
        }

        public void RunStepsReporting()
        {
            trucateStepsReporting_DWH_Landing();
            trucateStepsReporting_DWH();
            int DWH_Total = SyncStepsReporting("DWH");
            int Landing_Total = SyncStepsReporting("DWH_landing");
            insertLastUpdatedInDbDWH_landing("Steps Reporting", Landing_Total);
            insertLastUpdatedInDbDWH("Steps Reporting", DWH_Total);
        }




        #region IE122 - Flag Rate Change Notification Journey - Non-primary Supporter payer 

        public void SyncRateChangeRelatedDataInTables()
        {
            Stopwatch stopwatch = new Stopwatch();
            try
            {

                Stopwatch sw1 = new Stopwatch();
                sw1.Start();

                stopwatch.Start();
                TrucateTable("PayerRateChangeDetails");
                stopwatch.Stop();

                appLogger.Info($"Timetake to truncate data :{stopwatch.ElapsedMilliseconds}ms");

                stopwatch.Start();
                ImportPayerData();
                stopwatch.Stop();

                appLogger.Info($"Time taken to Sync Payer data :{stopwatch.ElapsedMilliseconds}ms");


                stopwatch.Start();
                TrucateTable("SupporterToSupporterGroupLinks");
                stopwatch.Stop();

                appLogger.Info($"Timetake to truncate data :{stopwatch.ElapsedMilliseconds}ms");

                stopwatch.Start();
                ImportSupporterToSupporterGroupLinksData();
                stopwatch.Stop();

                appLogger.Info($"Time taken to Sync SupporterToSupporterGroupLinks data :{stopwatch.ElapsedMilliseconds}ms");


                stopwatch.Start();
                TrucateTable("RgDetailsForRateChange");
                stopwatch.Stop();

                appLogger.Info($"Timetake to truncate data :{stopwatch.ElapsedMilliseconds}ms");

                stopwatch.Start();
                ImportRgDetailsForRateChangeData();
                stopwatch.Stop();

                appLogger.Info($"Time taken to Sync RgDetailsForRateChange data :{stopwatch.ElapsedMilliseconds}ms");



                stopwatch.Start();
                UpdatePrimarySupporterInPayer();
                stopwatch.Stop();
                appLogger.Info($"Time taken to update primary supporter data :{stopwatch.ElapsedMilliseconds}ms");


                stopwatch.Start();
                UpdateSupporterGroupInPayer();
                stopwatch.Stop();
                appLogger.Info($"Time taken to update supporter group data :{stopwatch.ElapsedMilliseconds}ms");


                stopwatch.Start();
                UpdateNonPrimarySupporterInPayer();
                stopwatch.Stop();
                appLogger.Info($"Time taken to update non primary supporter data :{stopwatch.ElapsedMilliseconds}ms");



                TrucateTable("RgOnDonationBreak");


                stopwatch.Start();
                ImportPayerDonationBreakData();
                stopwatch.Stop();

                appLogger.Info($"Time taken to ImportPayerDonationBreakData :{stopwatch.ElapsedMilliseconds}ms");


                TrucateTable("RgWithReducedRateAttribute");

                stopwatch.Start();
                ImportTemporaryReducedRateForRecurringGift();
                UpdateCompassionOfficeInRgRateChangeDetails();
                stopwatch.Stop();

                appLogger.Info($"Time taken to ImportTemporaryReducedRateForRecurringGift and UpdateCompassionOfficeInRgRateChangeDetails :{stopwatch.ElapsedMilliseconds}ms");

                stopwatch.Start();
                AddNonPrimaryPayerAttributesInBB();
                UpdateRateChangeAttributesInBB("Temporarily Excluded: Donation Break");
                UpdateRateChangeAttributesInBB("Temporarily Excluded: Reduced Rate");
                stopwatch.Stop();

                appLogger.Info($"Time taken to add all process attributes in BB:{stopwatch.ElapsedMilliseconds}ms");

                sw1.Stop();
                appLogger.Info($"Total time to sync data: {sw1.ElapsedMilliseconds}ms");





            }
            catch (Exception e)
            {
                appLogger.Error("Error in ImportDataInTables" + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
        }
        public void ImportPayerData()
        {
            try
            {
                string Query = @"Insert into dwh_landing.`PayerRateChangeDetails` (commitmentID, supporterGroup, sgBlackbaudID, psBlackbaudID, prioritySupporterID, 1_1_CompassionOffice, sgType, startDate, needKey, needGlobalID, NeedID, Need)
                                (SELECT c.commitmentID
                                    ,c.supporterGroup
                                    ,sg.blackbaudConstituentID AS sgBlackbaudID
                                    ,s.blackbaudID AS psBlackbaudID
                                    ,sg.prioritySupporterID
                                    ,sg.compassionOffice AS 1_1_CompassionOffice
                                    ,sg.type AS sgType
                                    ,DATE(c.startDate) AS startDate
                                    ,n.needKey
                                    ,n.beneficiary_GlobalID AS needGlobalID
                                    ,n.NeedID
                                    ,c.Need
                                FROM dwh_landing.Commitments c
                                LEFT JOIN dwh_landing.SupporterGroup sg
                                ON c.supporterGroup=sg.SupporterGroupID
                                LEFT JOIN dwh_landing.Supporters s
                                ON sg.prioritySupporterID=s.supporterID
                                INNER JOIN dwh_landing.Need n
                                        ON c.need=n.NeedID
                                WHERE c.sponsorshipAct=1169
                                    AND c.endDate IS NULL
                                    AND c.firstFundedDate IS NOT NULL)
                                    ;";//AND n.endDate IS NULL)

                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    cmd.ExecuteNonQuery();
                    conn.Close();
                    appLogger.Info("Data imported in PayerRateChangeDetails.");
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in ImportPayerData: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
        }
        public void TrucateTable(string tableName)
        {
            try
            {
                string Query = $"TRUNCATE TABLE {tableName};";
                MySqlConnection conn = new MySqlConnection(myConnectionString);
                conn.Open();
                MySqlCommand cmd = new MySqlCommand(Query, conn);
                cmd.CommandTimeout = 7200 * 2;
                cmd.ExecuteNonQuery();
                conn.Close();
                appLogger.Info($"{tableName} table data truncated.");
            }
            catch (Exception e)
            {
                appLogger.Error("Error in TrucateTable: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
        }
        public void ImportSupporterToSupporterGroupLinksData()
        {
            try
            {
                string Query = @"INSERT INTO dwh_landing.`SupporterToSupporterGroupLinks` 
                                (SELECT sg.supporterGroupID AS SupporterGroupID
                                      ,s.supporterID AS SupporterID
                                      ,s.blackbaudID AS SupporterBlackbaudID
                                FROM dwh_landing.SupporterGroup sg
                                INNER JOIN dwh_landing.SupporterGroupLinks sgl
                                        ON sgl.supporterGroupID=sg.supporterGroupID
                                INNER JOIN dwh_landing.Supporters s
		                                ON sgl.supporterID=s.supporterID
                                WHERE sgl.active=1)
                                ;";

                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {

                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    cmd.ExecuteNonQuery();
                    conn.Close();
                    appLogger.Info("Data imported in SupporterToSupporterGroupLinks.");
                }

            }
            catch (Exception e)
            {
                appLogger.Error("Error in ImportSupporterToSupporterGroupLinksData: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
        }
        public void ImportRgDetailsForRateChangeData()
        {
            appLogger.Info("ImportRgDetailsForRateChangeData is started...");
            List<ConstituentData> constituentDatas = new List<ConstituentData>();
            SqlConnection conn = new SqlConnection("Data Source=s27readonlyaccess.sky.blackbaud.com,51303;Initial Catalog=64156p;User ID=64156preadonly;Password=aZdr#4uMXW$BwE3!6jmVG#8Rdz;encrypt=true;");
            int pageSize = 2000;
            try
            {
                #region Get RG details from BB

                string Query = @"SELECT  c.lookupID AS constituentID
                                    ,c.constituentType AS constituentType
                                    ,cs.siteName AS constituentSite
                                    ,r.lookupID AS revenueID
                                    ,CASE
                                        WHEN LEN(cld.firstName)=9 THEN 
                                    CONCAT(SUBSTRING(cld.firstName,1,2),'0',SUBSTRING(cld.firstName,3,3),'0',
                                    SUBSTRING(cld.firstName, 6,4))
                                        ELSE cld.firstName
                                        END AS needKey
                                    ,r.status2 AS rgStatus
                                    ,r.date AS rgSetUpDate
                                    ,r.amount AS rgAmount
                                    ,cur.Name AS rgCurrency
                                    ,cur1.Name AS rgBaseCurrency
                                    ,r.frequency AS rgFrequency
                                   FROM v_query_revenue AS r
                                    INNER JOIN v_query_revenuesplit rs
                                            ON r.id=rs.revenueID
                                    INNER JOIN v_query_sponsorship spon
                                            ON spon.revenueSplitID=rs.ID
                                    LEFT JOIN v_query_sponsorshipOpportunity opp
                                            ON opp.id=spon.sponsorshipOpportunityID
                                    LEFT JOIN v_query_sponsorshipOpportunityChild cld
                                            ON opp.id=cld.id
                                    LEFT JOIN v_query_constituent c
                                            ON r.constituentID=c.ID
                                    LEFT JOIN v_query_constituentSite cs
                                            ON c.ID=cs.constituentID
                                    LEFT JOIN v_query_currency cur
                                            ON r.transactionCurrencyID=cur.ID
                                    LEFT JOIN v_query_currency cur1
                                            ON r.baseCurrencyID=cur1.ID
                                    WHERE r.transactionType='Recurring gift'
                                        AND rs.designationName IN('Child Support', 'Child Support - CIRL')
                                        AND r.status2 IN('Active', 'Held')
                                        AND spon.status LIKE 'Active%'";

                Stopwatch stopwatch = Stopwatch.StartNew();
                stopwatch.Start();
                conn.Open();
                SqlCommand cmd = new SqlCommand(Query, conn);
                cmd.CommandTimeout = 7200 * 2;
                SqlDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    ConstituentData data = new ConstituentData();
                    data.constituentID = reader["constituentID"].ToString();
                    data.constituentType = reader["constituentType"].ToString();
                    data.constituentSite = reader["constituentSite"].ToString();
                    data.revenueID = reader["revenueID"].ToString();
                    data.needKey = reader["needKey"].ToString();
                    data.rgStatus = reader["rgStatus"].ToString();
                    data.rgSetUpDate = reader["rgSetUpDate"].ToString();
                    data.rgAmount = Convert.ToDouble(reader["rgAmount"].ToString());
                    data.rgCurrency = reader["rgCurrency"].ToString();
                    data.rgBaseCurrency = reader["rgBaseCurrency"].ToString();
                    data.rgFrequency = reader["rgFrequency"].ToString();
                    constituentDatas.Add(data);
                }
                conn.Close();
                stopwatch.Stop();
                appLogger.Info("Time extractRecurringGiftsOnlyConstituent taken to get BB query responce: " + stopwatch.ElapsedMilliseconds + " ms");

                #endregion

                appLogger.Info($"RgDetails {constituentDatas.Count()} found to insert into DB.");
                if (constituentDatas.Count() > 0)
                {
                    while (constituentDatas.Any())
                    {
                        List<ConstituentData> insertList = new List<ConstituentData>();
                        insertList = constituentDatas.Take(pageSize).ToList();
                        constituentDatas = constituentDatas.Skip(pageSize).ToList();

                        if (insertList.Count() > 0)
                        {
                            StringBuilder sCommand = new StringBuilder("INSERT INTO RgDetailsForRateChange");
                            using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                            {
                                List<string> Columns = new List<string>() { "constituentID", "constituentType", "constituentSite", "revenueID", "needKey", "rgStatus", "rgSetUpDate", "rgAmount", "rgCurrency", "rgBaseCurrency", "rgFrequency" };

                                sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                                List<string> Rows = new List<string>();
                                foreach (ConstituentData row in insertList)
                                {
                                    Rows.Add(string.Format("({0})", $"'{row.constituentID}','{row.constituentType}','{row.constituentSite}','{row.revenueID}','{row.needKey}','{row.rgStatus}','{row.rgSetUpDate}',{row.rgAmount},'{row.rgCurrency}','{row.rgBaseCurrency}','{row.rgFrequency}'"));
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
                                appLogger.Info(constituentDatas.Count() + " RgDetailsForRateChange records updated successfully");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in ImportRgDetailsForRateChangeData: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
            finally
            {
                conn.Close();
            }
            appLogger.Info("ImportRgDetailsForRateChangeData is ended...");
        }
        public void UpdatePrimarySupporterInPayer()
        {
            try
            {
                string Query = @"UPDATE dwh_landing.`PayerRateChangeDetails`,
                                (
                                SELECT p.commitmentID as commitment_ID
	                                ,p.prioritySupporterID as Primary_SupporterID
	                                ,rg.revenueID as revenue_ID
	                                FROM dwh_landing.`PayerRateChangeDetails` p 
	                                INNER JOIN dwh_landing.RgDetailsForRateChange rg
	                                ON (rg.constituentID = p.psBlackbaudID AND rg.needKey = p.needKey)
                                ) as abc

                                SET MatchType = 'Primary Supporter'
                                ,PrimarySupporterID = abc.Primary_SupporterID
                                ,RevenueID = abc.revenue_ID
                                WHERE commitmentID = abc.commitment_ID;";

                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    cmd.ExecuteNonQuery();
                    conn.Close();
                    appLogger.Info("Primary Supporter updated in UpdatePrimarySupporterInPayer.");
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in UpdatePrimarySupporterInPayer: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
        }
        public void UpdateSupporterGroupInPayer()
        {
            try
            {
                string Query = @"UPDATE dwh_landing.`PayerRateChangeDetails`,
                                (
                                SELECT p.commitmentID as commitment_ID
	                                ,p.prioritySupporterID as Primary_SupporterID
	                                ,rg.revenueID as revenue_ID
	                                FROM dwh_landing.`PayerRateChangeDetails` p 
	                                INNER JOIN dwh_landing.RgDetailsForRateChange rg
	                                ON (rg.constituentID = p.sgBlackbaudID AND rg.needKey = p.needKey)
                                ) as abc

                                SET MatchType = 'Supporter Group'
                                ,PrimarySupporterID = abc.Primary_SupporterID
                                ,RevenueID = abc.revenue_ID
                                WHERE commitmentID = abc.commitment_ID AND (MatchType = '' OR MatchType IS NULL);";


                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    cmd.ExecuteNonQuery();
                    conn.Close();
                    appLogger.Info("Supporter Group updated in UpdateSupporterGroupInPayer.");
                }

            }
            catch (Exception e)
            {
                appLogger.Error("Error in UpdateSupporterGroupInPayer: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
        }
        public void UpdateNonPrimarySupporterInPayer()
        {
            try
            {
                string Query = @"UPDATE dwh_landing.`PayerRateChangeDetails`,
                                (select prtd.commitmentID AS commitment_ID
                                , rg.revenueID as Revenue_ID
                                , prtd.prioritySupporterID AS priority_SupporterID
                                 from dwh_landing.`PayerRateChangeDetails` prtd
                                INNER JOIN dwh_landing.SupporterToSupporterGroupLinks stsg
	                                ON prtd.supporterGroup = stsg.SupporterGroupID
                                INNER JOIN dwh_landing.RgDetailsForRateChange rg
		                                ON (rg.constituentID = stsg.SupporterBlackbaudID  AND prtd.needKey = rg.needKey)
                                where prtd.MatchType = '' OR prtd.MatchType IS NULL) as abc

                                SET MatchType = 'Non Primary Supporter'
                                ,PrimarySupporterID = abc.priority_SupporterID
                                ,RevenueID = abc.Revenue_ID
                                WHERE commitmentID = abc.commitment_ID AND (MatchType = '' OR MatchType IS NULL);";

                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    cmd.ExecuteNonQuery();
                    conn.Close();
                    appLogger.Info("Non Primary Supporter updated in UpdateNonPrimarySupporterInPayer.");
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in UpdateNonPrimarySupporterInPayer: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
        }
        #endregion

        #region IE120 Donation Break Data Import
        public void ImportPayerDonationBreakData()
        {
            List<string> revenueIds = new List<string>();
            int pageSize = 2000;
            SqlConnection bbConn = new SqlConnection("Data Source=s27readonlyaccess.sky.blackbaud.com,51303;Initial Catalog=64156p;User ID=64156preadonly;Password=aZdr#4uMXW$BwE3!6jmVG#8Rdz;encrypt=true;");
            try
            {
                #region Get Donation Break data from RODB

                string Query = @"SELECT  HeldRGs.revenueID
                                ,1 AS [1_5_currentDB]
                                FROM (
                                SELECT DISTINCT r.LOOKUPID REVENUEID,
                                        rgwo.WRITEOFFREASONCODE,
                                        rgi.DATE,
                                        ROW_NUMBER() OVER (PARTITION BY r.LOOKUPID ORDER BY rgi.DATE DESC) row_num
                                FROM V_QUERY_REVENUE as r
                                LEFT JOIN recurringGiftInstallment rgi
                                        ON r.ID=rgi.revenueID
                                LEFT JOIN USR_V_QUERY_BBE_RECURRINGGIFTINSTALLMENTWRITEOFF wo
                                ON rgi.ID = wo.RECURRINGGIFTINSTALLMENTID
                                LEFT JOIN V_QUERY_RECURRINGGIFTWRITEOFF rgwo
                                ON wo.WRITEOFFID = rgwo.ID
                                WHERE r.STATUS2 = 'Held'
                                    AND rgi.status != 'Expected'
                                    ) HeldRGs

                                WHERE row_num = 1 AND HeldRGs.WRITEOFFREASONCODE = 'Break - Donation break'";

                Stopwatch stopwatch = Stopwatch.StartNew();
                stopwatch.Start();
                bbConn.Open();
                SqlCommand cmd = new SqlCommand(Query, bbConn);
                cmd.CommandTimeout = 7200 * 2;
                SqlDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    revenueIds.Add(reader["revenueID"].ToString());
                }
                bbConn.Close();
                #endregion


                #region Import data in PayerOnDonationBreak


                appLogger.Info($"RgOnDonationBreak {revenueIds.Count()} found to insert into DB.");
                if (revenueIds.Count() > 0)
                {
                    while (revenueIds.Any())
                    {
                        List<string> insertList = new List<string>();
                        insertList = revenueIds.Take(pageSize).ToList();
                        revenueIds = revenueIds.Skip(pageSize).ToList();

                        if (insertList.Count() > 0)
                        {
                            StringBuilder sCommand = new StringBuilder("INSERT INTO RgOnDonationBreak");
                            using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                            {
                                List<string> Columns = new List<string>() { "RevenueID" };

                                sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                                List<string> Rows = new List<string>();
                                foreach (string revenueid in insertList)
                                {
                                    Rows.Add(string.Format("({0})", $"'{revenueid}'"));
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
                                appLogger.Info(insertList.Count() + " RgOnDonationBreak records updated successfully");
                            }
                        }
                    }
                }

                #endregion

            }
            catch (Exception e)
            {
                appLogger.Error("Error in ImportPayerDonationBreakData: " + e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            finally
            {
                bbConn.Close();
            }


        }
        #endregion

        #region IE121 Temporary reduced rate for recurring gift Data Import
        public void ImportTemporaryReducedRateForRecurringGift()
        {
            appLogger.Info("Sync TemporaryReducedRateForRecurringGift Started...");
            List<string> revenueIds = new List<string>();
            int pageSize = 2000;
            SqlConnection bbConn = new SqlConnection("Data Source=s27readonlyaccess.sky.blackbaud.com,51303;Initial Catalog=64156p;User ID=64156preadonly;Password=aZdr#4uMXW$BwE3!6jmVG#8Rdz;encrypt=true;");
            try
            {
                #region Get Temporary reduced rate for recurring gift from RODB

                string Query = @"SELECT  DISTINCT r.lookupID AS revenueID
                                       ,1 AS reducedRateAttribute
                                FROM v_query_revenue r
                                INNER JOIN v_query_attribute7B062F1B796A449691D97CE76A3C12BF rrdb
                                        ON r.id=rrdb.parentID
                                WHERE r.transactionType='Recurring gift'
                                ";
                /*
                 AND (rrdb.endDate>GETDATE()
 	                                   OR rrdb.endDate IS NULL)
                 */



                Stopwatch stopwatch = Stopwatch.StartNew();
                stopwatch.Start();
                bbConn.Open();
                SqlCommand cmd = new SqlCommand(Query, bbConn);
                cmd.CommandTimeout = 7200 * 2;
                SqlDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    revenueIds.Add(reader["revenueID"].ToString());
                }
                bbConn.Close();
                #endregion

                #region Import data in RgWithReducedRateAttribute


                appLogger.Info($"RgWithReducedRateAttribute {revenueIds.Count()} found to insert into DB.");
                if (revenueIds.Count() > 0)
                {
                    while (revenueIds.Any())
                    {
                        List<string> insertList = new List<string>();
                        insertList = revenueIds.Take(pageSize).ToList();
                        revenueIds = revenueIds.Skip(pageSize).ToList();

                        if (insertList.Count() > 0)
                        {
                            StringBuilder sCommand = new StringBuilder("INSERT INTO RgWithReducedRateAttribute");
                            using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
                            {
                                List<string> Columns = new List<string>() { "revenueID" };

                                sCommand.Append(string.Format(" ({0}) VALUES ", string.Join(",", Columns)));

                                List<string> Rows = new List<string>();
                                foreach (string revenueid in insertList)
                                {
                                    Rows.Add(string.Format("({0})", $"'{revenueid}'"));
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
                                appLogger.Info(insertList.Count() + " RgWithReducedRateAttribute records updated successfully");
                            }
                        }
                    }
                }

                #endregion

                appLogger.Info("Sync TemporaryReducedRateForRecurringGift ended...");

            }
            catch (Exception e)
            {
                appLogger.Error("Error in ImportTemporaryReducedRateForRecurringGift: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
            finally
            {
                bbConn.Close();
            }
        }
        public void UpdateCompassionOfficeInRgRateChangeDetails()
        {
            try
            {
                string Query = @"UPDATE dwh_landing.RgDetailsForRateChange,
                                (SELECT RevenueID AS Revenue_ID,1_1_CompassionOffice AS Compassion_Office FROM dwh_landing.PayerRateChangeDetails ) AS abc
                                SET 1_1_CompassionOffice = abc.Compassion_Office
                                WHERE revenueID = Revenue_ID;";

                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    cmd.ExecuteNonQuery();
                    conn.Close();
                    appLogger.Info("CompassionOffice fields updated in RgDetailsForRateChange.");
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in UpdateCompassionOfficeInRgRateChangeDetails: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
        }
        #endregion

        #region Update Non-Primary Supporter attributes in BB


        Dictionary<string, string> RateChangeAttributes = new Dictionary<string, string>() {

            {"Temporarily Excluded: Non-primary supporter payer","7f87b8de-7055-4bff-8067-354fc4697849"},
            {"Temporarily Excluded: Donation Break","0bf5c282-15e7-4275-b6fc-440bf833279d" },
            {"Temporarily Excluded: Reduced Rate","4d98234c-f77d-42ed-afbc-76e40952546b" },
        };

        public void UpdateRateChangeAttributesInBB(string attributeName)
        {
            List<string> revenueIds = new List<string>();
            try
            {
                switch (attributeName)
                {
                    case "Temporarily Excluded: Non-primary supporter payer":
                        revenueIds = GetAllNonPrimarySupporterRevenueIds();
                        if (revenueIds.Count() > 0)
                        {
                            AddRateChangeAttribute(revenueIds, attributeName);
                        }
                        break;
                    case "Temporarily Excluded: Donation Break":
                        revenueIds = GetAllDonationBreakRevenueIds();
                        if (revenueIds.Count() > 0)
                        {
                            AddRateChangeAttribute(revenueIds, attributeName);
                        }
                        break;
                    case "Temporarily Excluded: Reduced Rate":
                        revenueIds = GetAllReduceRateRevenueIds();
                        if (revenueIds.Count() > 0)
                        {
                            List<string> matchedRevenueIds = GetAllReduceRateRevenueIdsNeedToAddAttributesInBB(revenueIds);
                            if (matchedRevenueIds.Count > 0)
                            {
                                AddRateChangeAttribute(matchedRevenueIds, attributeName);
                            }

                        }
                        break;
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in UpdateRateChangeAttributesInBB: " + e.Message);
                appLogger.Error(e.StackTrace);
                appLogger.Error(e.InnerException);
            }
        }
       
        public List<string> GetAllDonationBreakRevenueIds()
        {
            List<string> revenueIds = new List<string>();
            try
            {
                string Query = @"select rg.RevenueID from dwh_landing.`PayerRateChangeDetails` prcd
                                    INNER JOIN dwh_landing.`RgOnDonationBreak` rg
	                                    ON prcd.RevenueID = rg.RevenueID;
                                    ;";
                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    MySqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        revenueIds.Add(reader["RevenueID"].ToString());
                    }
                    conn.Close();
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in GetAllDonationBreakRevenueIds: " + e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return revenueIds;
        }
        public List<string> GetAllReduceRateRevenueIds()
        {
            List<string> revenueIds = new List<string>();
            try
            {
                string Query = @"SELECT rr.revenueID FROM dwh_landing.RgDetailsForRateChange rg
                                INNER JOIN dwh_landing.RgWithReducedRateAttribute rr
	                                ON rg.revenueID = rr.revenueID
                                WHERE (1_1_CompassionOffice = '2' AND rgCurrency = 'Euro'
                                    AND ( (rgFrequency = 'Monthly' AND rgAmount < 30) 
	                                OR (rgFrequency = 'Bimonthly' AND rgAmount < 60)
                                    OR (rgFrequency = 'Quarterly' AND rgAmount < 90 )
                                    OR (rgFrequency = 'Semi-annually' AND rgAmount < 180)
                                    OR (rgFrequency = 'Annually' AND rgAmount < 360 )
                                    OR (rgFrequency = 'Biweekly' AND rgAmount < 13.84 )
                                    OR (rgFrequency = 'Weekly' AND rgAmount < 6.92 )
	                                    )
                                    )
                                    OR
                                    (1_1_CompassionOffice = '1' AND rgCurrency = 'UK Pound Sterling'
                                    AND ( (rgFrequency = 'Monthly' AND rgAmount < 28 ) 
	                                OR (rgFrequency = 'Bimonthly' AND rgAmount < 56 )
                                    OR (rgFrequency = 'Quarterly' AND rgAmount < 84  )
                                    OR (rgFrequency = 'Semi-annually' AND rgAmount < 168 )
                                    OR (rgFrequency = 'Annually' AND rgAmount < 336  )
                                    OR (rgFrequency = 'Biweekly' AND rgAmount < 12.92  )
                                    OR (rgFrequency = 'Weekly' AND rgAmount < 6.46  )
	                                    )
                                )";


                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    cmd.CommandTimeout = 7200 * 2;
                    MySqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        revenueIds.Add(reader["RevenueID"].ToString());
                    }
                    conn.Close();
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in GetAllReduceRateRevenueIds: " + e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return revenueIds;
        }
        public List<string> GetAllNonPrimarySupporterRevenueIds()
        {
            List<string> revenueIds = new List<string>();
            try
            {
                string Query = "SELECT RevenueID FROM dwh_landing.`PayerRateChangeDetails` WHERE MatchType = 'Non Primary Supporter';";
                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    MySqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        revenueIds.Add(reader["RevenueID"].ToString());
                    }
                    conn.Close();
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in GetAllNonPrimarySupporterRevenueIds: " + e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return revenueIds;
        }
        public List<string> GetAllReduceRateRevenueIdsNeedToAddAttributesInBB(List<string> RevenueIds)
        {
            List<string> revenueIds = new List<string>();
            try
            {
                string Query = @"SELECT RevenueID FROM dwh_landing.PayerRateChangeDetails 
                                WHERE RevenueID IN ('" + string.Join("','", RevenueIds) + "');";


                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    cmd.CommandTimeout = 7200 * 2;
                    MySqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        revenueIds.Add(reader["RevenueID"].ToString());
                    }
                    conn.Close();
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in GetAllReduceRateRevenueIds: " + e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return revenueIds;
        }


        public void AddRateChangeAttribute(List<string> revenueIds, string attributeName)
        {
            appLogger.Info($"AddRateChangeAttribute started for {attributeName}");
            string RevenueId = null;
            try
            {

                RateChangeService.RateChangeServicesClient rateChange = new RateChangeService.RateChangeServicesClient();
                appLogger.Info($"Total records found to add attribute in BB {revenueIds.Count()}");
                foreach (string revenueId in revenueIds)
                {
                    RevenueId = revenueId;
                    long RecordId = InsertRateChangeExceptionRecord(revenueId, attributeName);
                    rateChange.addRateChangeAttribute(attributeName, revenueId, RecordId);
                    appLogger.Info($"{attributeName} attribute added for revenue record {revenueId}");
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in AddRateChangeAttribute: " + e.Message + " with RevenueId: " + RevenueId + " and Type " + attributeName);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            appLogger.Info($"AddRateChangeAttribute ended for {attributeName}");
        }
        public long InsertRateChangeExceptionRecord(string RevenueId, string ProcessType)
        {
            long lastInsertedID = 0;
            try
            {
                string Query = $"INSERT INTO RateChangeExceptionAuditLog (`RevenueId`, `ProcessType`) VALUES ('{RevenueId}','{ProcessType}')";
                using (MySqlConnection conn = new MySqlConnection(mycukdevco_tcpt4ConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    cmd.ExecuteNonQuery();
                    lastInsertedID = cmd.LastInsertedId;
                    conn.Close();
                    appLogger.Info($"RateChangeExceptionAuditLog record inserted in DB with ID {lastInsertedID}");
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in InsertRateChangeExceptionRecord: " + e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return lastInsertedID;
        }

        #endregion

        #region Update Non-Primary Supporter attributes in BB V2

        private List<string> GetAllNonPrimarySupporterGroupIds()
        {
            List<string> SupporterGroupIds = new List<string>();
            try
            {
                string Query = "SELECT supporterGroup FROM dwh_landing.`PayerRateChangeDetails` WHERE MatchType = 'Non Primary Supporter';";
                //string Query = "SELECT supporterGroup FROM dwh_landing.PayerRateChangeDetails where prioritySupporterID = \"54768\";";


                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    MySqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        SupporterGroupIds.Add(reader["supporterGroup"].ToString());
                    }
                    conn.Close();
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in GetAllNonPrimarySupporterRevenueIds: " + e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return SupporterGroupIds;
        }
        private List<string> GetRevenueDetails(List<string> LookupIds)
        {
            List<string> RevenueIds = new List<string>();
            try
            {
                string Query = "SELECT revenueID FROM dwh_landing.RgDetailsForRateChange where constituentID IN ('" + string.Join("','", LookupIds) + "');";
                using (MySqlConnection conn = new MySqlConnection(myConnectionString))
                {
                    conn.Open();
                    MySqlCommand cmd = new MySqlCommand(Query, conn);
                    MySqlDataReader reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        RevenueIds.Add(reader["revenueID"].ToString());
                    }
                    conn.Close();
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in GetAllNonPrimarySupporterRevenueIds: " + e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
            return RevenueIds;
        }
        public void AddNonPrimaryPayerAttributesInBB()
        {
            try
            {

                List<string> SupporterGroupIds = GetAllNonPrimarySupporterGroupIds();

                foreach (string supgrpid in SupporterGroupIds)
                {
                    List<string> LookupIds = new RightNowDataOperations().GetAllBBLookupIds(Convert.ToInt64(supgrpid));
                    if (LookupIds.Count() > 0)
                    {
                        List<string> RevenueIds = GetRevenueDetails(LookupIds);
                        if (RevenueIds.Count() > 0)
                        {
                            AddRateChangeAttribute(RevenueIds, "Temporarily Excluded: Non-primary supporter payer");
                        }
                        else
                        {
                            appLogger.Info($"No RevenueId found for given LookupIds: {string.Join(",", LookupIds)}");
                        }
                    }
                    else
                    {
                        appLogger.Info($"No LookupIds found for given SupporterGroupId: {supgrpid}");
                    }
                }
            }
            catch (Exception e)
            {
                appLogger.Error("Error in CreateAttributesInBB: " + e.Message);
                appLogger.Error(e.InnerException);
                appLogger.Error(e.StackTrace);
            }
        }
        #endregion

        public void AddAttrCode()
        {
            AddNonPrimaryPayerAttributesInBB();
            UpdateRateChangeAttributesInBB("Temporarily Excluded: Donation Break");
            UpdateRateChangeAttributesInBB("Temporarily Excluded: Reduced Rate");
        }


    }
}
