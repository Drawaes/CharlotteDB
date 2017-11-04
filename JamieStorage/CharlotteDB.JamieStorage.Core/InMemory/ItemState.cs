using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.InMemory
{
    public enum ItemState :byte
    {
        Alive = 0,
        Deleted = 1,
        DeletedNew = 0
    }
}
