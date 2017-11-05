using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.Core.Allocation
{
    public class DummyAllocator : Allocator
    {
        private int _normalBufferSize;

        public DummyAllocator(int normalBufferSize) => _normalBufferSize = normalBufferSize;

        public override int NormalBufferSize => _normalBufferSize;

        public override OwnedMemory<byte> AllocateNormalBuffer() => new OwnedArray<byte>(new byte[_normalBufferSize]);

        public override OwnedMemory<byte> AllocateCustomBuffer(int size) => new OwnedArray<byte>(new byte[size]);

        public override void ReturnBuffer(OwnedMemory<byte> buffer)
        {
            // Do nothing for now, there is no pooling just relying on the GC
        }
    }
}
