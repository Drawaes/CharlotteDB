using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    public static class StorageFile
    {
        public readonly static byte[] MagicHeader = Encoding.UTF8.GetBytes("CharlotteDBStart");
        public readonly static byte[] MagicTrailer = Encoding.UTF8.GetBytes("CharlotteDBEnd");
    }
}
