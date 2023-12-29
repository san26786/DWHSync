using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWHSync.Model
{
    public class Rootobject
    {
        public Item[] items { get; set; }
        public Link[] links { get; set; }
        public string type { get; set; }
        public string title { get; set; }
        public int status { get; set; }
        public string detail { get; set; }
        public string instance { get; set; }
        public string oerrorCode { get; set; }
    }

    public class Item
    {
        public string tableName { get; set; }
        public int count { get; set; }
        public string[] columnNames { get; set; }
        public string[][] rows { get; set; }
    }

    public class Link
    {
        public string rel { get; set; }
        public string href { get; set; }
        public string mediaType { get; set; }
    }
}
