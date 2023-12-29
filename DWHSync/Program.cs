using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace DWHSync
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
           //ServiceBase[] ServicesToRun;
           // ServicesToRun = new ServiceBase[]
           // {
           //     new DWHSyncService()
           // };
           // ServiceBase.Run(ServicesToRun);
            
            DWHSyncService dwhObj = new DWHSyncService();
            dwhObj.DoWork();
        }
    }
}
