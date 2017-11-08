using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.Core.Allocation
{
    public class QueuedAllocator : Allocator
    {
        private ConcurrentQueue<OwnedMemory<byte>> _memory = new ConcurrentQueue<OwnedMemory<byte>>();
        private int _bufferSize;

        public QueuedAllocator(int bufferSize) => _bufferSize = bufferSize;

        public override int NormalBufferSize => _bufferSize;

        public override OwnedMemory<byte> AllocateCustomBuffer(int size) => new OwnedArray<byte>(new byte[size]);
        
        public override OwnedMemory<byte> AllocateNormalBuffer()
        {
            if(_memory.TryDequeue(out var buffer))
            {
                return buffer;
            }
            return new OwnedArray<byte>(_bufferSize);
        }

        public override void ReturnBuffer(OwnedMemory<byte> buffer)
        {
            if(buffer.Length != _bufferSize)
            {
                throw new ArgumentOutOfRangeException(nameof(buffer.Length));
            }
            _memory.Enqueue(buffer);
        }
    }
}
