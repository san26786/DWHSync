using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWHSync.Model
{
    public class IncidentandThreadModel
    {
        public string IncidentID { get; set; }
        public string PrimaryContact { get; set; }
        public string OtherContacts { get; set; }
        public string AssignedTo { get; set; }
        public string Queue { get; set; }
        public string Mailbox { get; set; }
        public string ResolutionDueDate { get; set; }
        public string MinistryAspect { get; set; }
        public string Category { get; set; }
        public string Disposition { get; set; }
        public string Status { get; set; }
        public string ContactAttemptsMade { get; set; }
        public string FeedbackRating { get; set; }
        public string CreatedTime { get; set; }
        public string UpdatedTime { get; set; }
        public string ClosedTime { get; set; }
        public string CreatedByAccount { get; set; }
        public string CampaignFormType { get; set; }
        public string InternalSubject { get; set; }
        public string Account { get; set; }
        public string Channel { get; set; }
        public string Contact { get; set; }
        public string ThreadCreatedTime { get; set; }
        public string DisplayOrder { get; set; }
        public string EntryType { get; set; }
        public string ThreadID { get; set; }
        public string IncidentIDForThread { get; set; }
        public string status_id { get; set; }
    }
    public class ThreadModel
    {
        public string ThreadID { get; set; }
        public string IncidentID { get; set; }
        public string Account { get; set; }
        public string Channel { get; set; }
        public string Contact { get; set; }
        public string CreatedTime { get; set; }
        public string DisplayOrder { get; set; }
        public string EntryType { get; set; }
        public string Text { get; set; }
    }
}
