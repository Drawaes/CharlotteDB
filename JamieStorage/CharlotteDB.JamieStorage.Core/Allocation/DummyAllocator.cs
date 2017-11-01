using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.Allocation
{
    public class DummyAllocator : IAllocator
    {
        private int _normalBufferSize;

        public DummyAllocator(int normalBufferSize) => _normalBufferSize = normalBufferSize;

        public int NormalBufferSize => _normalBufferSize;

        public Memory<byte> AllocateNormalBuffer() => new Memory<byte>(new byte[_normalBufferSize]);

        public Memory<byte> AllocateLargebuffer(int size) => new Memory<byte>(new byte[size]);

        public void ReturnBuffer(Memory<byte> buffer)
        {
            // Do nothing for now, there is no pooling just relying on the GC
        }
    }
}
