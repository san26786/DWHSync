using log4net;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.IO;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Configuration;
using System.Net;

namespace DWHSync
{
    public partial class DWHSyncService : ServiceBase
    {
        protected static readonly ILog appLogger = LogManager.GetLogger(typeof(DWHSyncService));
        private string myConnectionString = "server=62.138.4.114;uid=oneview_RN;" + "pwd=Geecon0404;database=oneview_rn_backup; convert zero datetime=True;AutoEnlist=false";


        // Timer
        System.Timers.Timer _timer;
        DateTime _scheduleTime;
        TimeSpan start;
        Thread Worker;
        AutoResetEvent StopRequest = new AutoResetEvent(false);

        // Constructor
        public DWHSyncService()
        {
            InitializeComponent();

            // Logging
            log4net.Config.XmlConfigurator.Configure();

            // Timer
            _timer = new System.Timers.Timer();

            _scheduleTime = DateTime.Today.AddDays(1).AddHours(2); // Schedule to run once a day at 2:00 a.m.
            start = new TimeSpan(2, 0, 0);

            //_scheduleTime = DateTime.Today.AddHours(15).AddMinutes(40); // Schedule to run once a day at 2:00 a.m.
            //start = new TimeSpan(15, 40, 0);

        }

        // Main Function
        public void DoWork()
        //public void DoWork(object sender, System.Timers.ElapsedEventArgs e)
        {
            try
            {
                appLogger.Info("Processing Started..");
                // Disable timer to stop auto trigerring on new threads
                _timer.Enabled = false;
                //SendEmail("hii testing");


                // Do work here...
                RightNowDataOperations rnObj = new RightNowDataOperations();
                //rnObj.SyncCDPayment();
                BBDataOperations bbObj = new BBDataOperations();
                string lastdate = DateTime.Today.AddDays(-3).AddHours(2).ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'");
                string fromTime = DateTime.Today.AddDays(-3).AddHours(2).ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'");





                // Run this code once every 10 seconds or stop right away if the service 
                // is stopped
                //if (StopRequest.WaitOne(10000)) return;


                #region [RightNow Operation]


                #region DWH Sync
                int SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncNeed(fromTime); //Need
                rnObj.insertLastUpdatedInDb("Need", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSupporters(fromTime);//Supporter
                rnObj.insertLastUpdatedInDb("Supporters", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSupporterGroup(fromTime);// Support Group
                rnObj.insertLastUpdatedInDb("SupporterGroup", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSupporterGroupLinks(fromTime);// Suporter GRoupLink
                rnObj.insertLastUpdatedInDb("SupporterGroupLinks", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncCommitment(fromTime);// Commitent
                rnObj.insertLastUpdatedInDb("Commitment", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSupporterEnquiries(fromTime);//Incidennt

                rnObj.insertLastUpdatedInDb("Incident", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSchedChildDepartures(fromTime);
                rnObj.insertLastUpdatedInDb("SchedChildDepartures", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncBeneficiaryOrICPHold(fromTime);
                rnObj.insertLastUpdatedInDb("BeneficiaryOrICPHold", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncS2BCommunication(fromTime);
                rnObj.insertLastUpdatedInDb("S2BCommunication", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncCorrespondence(fromTime);
                rnObj.insertLastUpdatedInDb("Correspondence", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncThreads(fromTime);
                rnObj.insertLastUpdatedInDb("Thread", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncCampaigns(fromTime);
                rnObj.insertLastUpdatedInDb("Campaigns", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SolicitCodes(fromTime);
                rnObj.insertLastUpdatedInDb("SolicitCodes", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncOrganisations(fromTime);
                rnObj.insertLastUpdatedInDb("Organisations", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncNotifications();
                rnObj.insertLastUpdatedInDb("Notifications", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncMiscEventTourDate(fromTime);
                rnObj.insertLastUpdatedInDb("MiscEventTourDate", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncCPPathwayActivity(fromTime);
                rnObj.insertLastUpdatedInDb("CPPathwayActivity", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncResourceOrder(fromTime);
                rnObj.insertLastUpdatedInDb("ResourceOrder", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncDonationDetails(fromTime);
                rnObj.insertLastUpdatedInDb("DonationDetails", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncDonation(fromTime);
                rnObj.insertLastUpdatedInDb("Donation", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncDonationType(fromTime);
                rnObj.insertLastUpdatedInDb("DonationType", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncMarketingChannels(fromTime);
                rnObj.insertLastUpdatedInDb("MarketingChannels", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncMarketingManager(fromTime);
                rnObj.insertLastUpdatedInDb("MarketingManager", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncEvents(fromTime);
                rnObj.insertLastUpdatedInDb("Events", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncEventTourDate(fromTime);
                rnObj.insertLastUpdatedInDb("EventTourDate", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.Sync_incPerformance(fromTime);
                rnObj.insertLastUpdatedInDb("Inc_performance", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncVolunteerPathway(fromTime);
                rnObj.insertLastUpdatedInDb("VolunteerPathway", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncEncounterVisit(fromTime);
                rnObj.insertLastUpdatedInDb("EncounterVisit", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncEncounterAttendance(fromTime);
                rnObj.insertLastUpdatedInDb("EncounterAttendance", SyncRecordCount);
                SyncRecordCount = 0;

                SyncRecordCount = rnObj.SyncActionHistory(fromTime);
                rnObj.insertLastUpdatedInDb("ActionHistory", SyncRecordCount);
                rnObj.InsertRNandDWHCount();
                #endregion

                #region DWH Landing Sync
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncNeed_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Need", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncCommitment_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Commitment", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSupporters_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Supporters", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSupporterGroup_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("SupporterGroup", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSupporterGroupLinks_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("SupporterGroupLinks", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSupporterEnquiries_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Incident", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSchedChildDepartures_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("SchedChildDepartures", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncBeneficiaryOrICPHold_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("BeneficiaryOrICPHold", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncS2BCommunication_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("S2BCommunication", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncCorrespondence_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Correspondence", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncThreads_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Thread", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncCampaigns_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Campaigns", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncSolicitCodes_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("SolicitCodes", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncOrganisations_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Organisations", SyncRecordCount);
                /*SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncNotificationDwh_landing();
                rnObj.insertLastUpdatedInDbLanding("Notifications", SyncRecordCount);*/
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncEventTourDate_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("MiscEventTourDate", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncCPPathwayActivity_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("CPPathwayActivity", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncResourceOrder_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("ResourceOrder", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncDonationDetails_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("DonationDetails", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncDonation_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Donation", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncDonationType_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("DonationType", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncMarketingChannels_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("MarketingChannels", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncMarketingManager_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("MarketingManager", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncEvents_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Events", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncDelinkType(fromTime);
                rnObj.insertLastUpdatedInDbLanding("DelinkType", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncLinkType(fromTime);
                rnObj.insertLastUpdatedInDbLanding("LinkType", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.Sync_incPerformance_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Inc_performance", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncVolunteerPathway_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("VolunteerPathway", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncEncounterVisit_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("EncounterVisit", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncEncounterAttendance_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("EncounterAttendance", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncActionHistory_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("ActionHistory", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncOrgStatusOperation(fromTime);
                rnObj.insertLastUpdatedInDbLanding("OrganisationRelationshipStatus", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncBeneficiaryTransfer_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("BeneficiaryTransfer", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncProjectOrgLinks_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("ProjectOrgLinks", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncProject_Landing(fromTime);
                rnObj.insertLastUpdatedInDbLanding("Project", SyncRecordCount);
                SyncRecordCount = 0;
                SyncRecordCount = rnObj.SyncOrganisationAsks(fromTime);
                rnObj.insertLastUpdatedInDbLanding("OrganisationAsks", SyncRecordCount);



                rnObj.InsertRNandDWH_landingCount();
                #endregion


                #endregion

                bbObj.RunProspectResearchReport();
                bbObj.MajorDonorProcessInDWH();
                bbObj.RunStepsReporting();
                bbObj.Run();

                appLogger.Info("Processing Stopped..");
                
            }
             catch (Exception ex)
             {
                 appLogger.Error(ex.Message);
                 appLogger.Info(new RightNowDataOperations().SendSMS("DHW Sync is failed due to :"+ex.Message));// send sms to sanjeev sir 
                 appLogger.Error(ex.InnerException);
                 appLogger.Error(ex.StackTrace);
             }
             finally
             {
                  _timer.Enabled = true;  // Reenable timer
                  _timer.Interval = DateTime.Today.AddDays(1).AddHours(2).Subtract(DateTime.Now).TotalSeconds * 1000;
             }

             //var webClient = new WebClient();
            // webClient.DownloadString("http://oneview.org.uk/dwh_checklist_cronjob.php");
             // If tick for the first time, reset next run to start at 2am next morning
             _timer.Enabled = true;  // Reenable timer
             _timer.Interval = DateTime.Today.AddDays(1).AddHours(2).Subtract(DateTime.Now).TotalSeconds * 1000;
         }

         // Send Email




         protected override void OnStart(string[] args)
         {
            // Start the worker thread
            // For first time, set amount of seconds between current time and schedule time

            _timer.Enabled = true;
            _timer.Interval = _scheduleTime.Subtract(DateTime.Now).TotalSeconds * 1000;
            //_timer.Elapsed += new System.Timers.ElapsedEventHandler(DoWork);

            /* Worker = new Thread(DoWork);
              Worker.Start();*/
        }

        protected override void OnStop()
        {
            StopRequest.Set();
            Worker.Join();
        }
    }
}
