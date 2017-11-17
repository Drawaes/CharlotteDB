using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using CharlotteDB.Core.Keys;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    public static class StorageFile
    {
        public readonly static byte[] MagicHeader = Encoding.UTF8.GetBytes("CharlotteDBStart");
        public readonly static byte[] MagicTrailer = Encoding.UTF8.GetBytes("CharlotteDBEnd");
        


        //public Task<StorageFile<TComparer>> LoadFromFile<TComparer>(string fileName, TComparer comparer)
        //    where TComparer : IKeyComparer
        //{

        //}
    }
}
