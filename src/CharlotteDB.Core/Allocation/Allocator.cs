using System.Buffers;

namespace CharlotteDB.Core.Allocation
{
    public abstract class Allocator
    {
        public abstract int NormalBufferSize { get; }
        public abstract OwnedMemory<byte> AllocateNormalBuffer();
        public abstract OwnedMemory<byte> AllocateCustomBuffer(int size);
        public abstract void ReturnBuffer(OwnedMemory<byte> buffer);
    }
}
