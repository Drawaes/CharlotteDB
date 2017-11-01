using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.Allocation
{
    public interface IAllocator
    {
        int NormalBufferSize { get; }
        Memory<byte> AllocateNormalBuffer();
        Memory<byte> AllocateLargebuffer(int size);
        void ReturnBuffer(Memory<byte> buffer);
    }
}
