using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWHSync.Model
{
    public class ConstituentData
    {
        public string constituentID { get; set; }
        public string constituentType { get; set; }
        public string constituentSite { get; set; }
        public string revenueID { get; set; }
        public string needKey { get; set; }
        public string rgStatus { get; set; }
        public string rgSetUpDate { get; set; }
        public double rgAmount { get; set; }
        public string rgCurrency { get; set; }
        public string rgBaseCurrency { get; set; }
        public string rgFrequency { get; set; }
    }
}
