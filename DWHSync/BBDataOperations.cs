using Blackbaud.AppFx.WebAPI.ServiceProxy;
using Blackbaud.AppFx.XmlTypes.DataForms;
using DWHSync.Model;
using log4net;
using Microsoft.Web.Administration;
using MySql.Data.MySqlClient;
using Polly;
using RestSharp;
using RestSharp.Authenticators;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

namespace DWHSync
{
    public class BBDataOperations
    {
        protected static readonly ILog appLogger = LogManager.GetLogger(typeof(BBDataOperations));
        private string myConnectionStringDWH = "";
        private string myConnectionString = "";
        private static DBModel dbObj;
        private static long rowCounter;
        private Blackbaud.AppFx.WebAPI.ServiceProxy.AppFxWebService _service;
        private string BbDatabase = string.Empty;
        private string ApplicationName = System.Reflection.Assembly.GetCallingAssembly().FullName;

        Policy retryPolicy = Policy
                      .Handle<Exception>(ex => ex.Message.Contains("The request failed with HTTP status 502: Bad Gateway"))
                      .Retry(3, onRetry: (exception, retryCounter) =>
                      {
                          // Add logic to be executed before each retry, such as logging
                          appLogger.Info("Retrying...");
                      });

        public BBDataOperations()
        {
            Dictionary<string, string> keyValue = new Dictionary<string, string>();

            using (ServerManager serverManager = new ServerManager())
            {
                Microsoft.Web.Administration.Configuration config = serverManager.GetWebConfiguration("Default Web Site", "/");
                Microsoft.Web.Administration.ConfigurationSection appSettingsSection = config.GetSection("appSettings");

                foreach (Microsoft.Web.Administration.ConfigurationElement element in appSettingsSection.GetCollection())
                {
                    string key = element["key"].ToString();
                    string value = element["value"].ToString();
                    keyValue.Add(key, value);
                }
            }

            myConnectionString = keyValue["DWHUserConnectionString"];
            myConnectionStringDWH = keyValue["DWHLandingConnectionString"];

            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            // Logging
            log4net.Config.XmlConfigurator.Configure();
            _service = new AppFxWebService();
            _service.Url = keyValue["BB_URI"];
            _service.Credentials = new System.Net.NetworkCredential(keyValue["BB_UID"], keyValue["BB_PWD"], "");

            var req = new Blackbaud.AppFx.WebAPI.ServiceProxy.GetAvailableREDatabasesRequest();
            req.ClientAppInfo = GetRequestHeader();
            var reply = retryPolicy.Execute(() => _service.GetAvailableREDatabases(req));
            BbDatabase = reply.Databases[0];

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
            if (Total > 0)
            {
                //update total record Inserted in Table 
                insertLastUpdatedInDbDWH("MajorDonor", Total);
                insertLastUpdatedInDbDWH_landing("MajorDonor", Total);
            }
            else
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



    }
}
