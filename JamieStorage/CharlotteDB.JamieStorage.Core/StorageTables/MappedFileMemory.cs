using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Threading;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    public unsafe class MappedFileMemory : OwnedMemory<byte>
    {
        private void* _ptr;
        private int _length;
        private bool _disposed;
        private MemoryMappedFile _mappedFile;
        private int _retainedCounter;

        public MappedFileMemory(void* ptr, int length, MemoryMappedFile memoryMappedFile)
        {
            _mappedFile = memoryMappedFile;
            _ptr = ptr;
            _length = length;
        }

        public override bool IsDisposed => _mappedFile.SafeMemoryMappedFileHandle.IsInvalid || _disposed;

        public override Span<byte> Span => new Span<byte>(_ptr, _length);

        public override int Length => _length;

        protected override bool IsRetained => throw new NotImplementedException();

        public override MemoryHandle Pin() => new MemoryHandle(this, _ptr);

        public override bool Release() => throw new NotImplementedException();

        public override void Retain() => Interlocked.Increment(ref _retainedCounter);

        protected override void Dispose(bool disposing) => _disposed = true;

        protected override bool TryGetArray(out ArraySegment<byte> arraySegment)
        {
            arraySegment = default;
            return false;
        }
    }
}
