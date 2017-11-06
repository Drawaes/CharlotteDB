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
        private byte* _ptr;
        private int _length;
        private MemoryMappedViewAccessor _memoryFile;
        private int _retainedCounter;

        public MappedFileMemory(long offset, int length, MemoryMappedFile memoryMappedFile)
        {
            _memoryFile = memoryMappedFile.CreateViewAccessor(0, length);
            _memoryFile.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
            
            _length = length;
        }

        public override bool IsDisposed => _memoryFile.SafeMemoryMappedViewHandle?.IsInvalid != false;

        public override Span<byte> Span => new Span<byte>(_ptr, _length);

        public override int Length => _length;

        protected override bool IsRetained => throw new NotImplementedException();

        public override MemoryHandle Pin() => new MemoryHandle(this, _ptr);

        public override bool Release() => throw new NotImplementedException();

        public override void Retain() => Interlocked.Increment(ref _retainedCounter);

        protected override void Dispose(bool disposing)
        {
            _memoryFile.SafeMemoryMappedViewHandle.ReleasePointer();
            _memoryFile?.Dispose();
        }

        protected override bool TryGetArray(out ArraySegment<byte> arraySegment)
        {
            arraySegment = default;
            return false;
        }
    }
}
