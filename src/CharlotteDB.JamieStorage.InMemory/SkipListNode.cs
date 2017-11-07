using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using CharlotteDB.Core;

namespace CharlotteDB.JamieStorage.InMemory
{
    internal ref struct SkipListNode
    {
        private Memory<byte> _bufferStart;
        private SkipNodeHeader _header;

        public SkipListNode(Memory<byte> buffer)
        {
            _bufferStart = buffer;
            _header = buffer.Span.Read<SkipNodeHeader>();
        }

        public Span<int> PointerTable => _bufferStart.Span.Slice(Unsafe.SizeOf<SkipNodeHeader>()).NonPortableCast<byte, int>().Slice(0, _header.Height);
        public Memory<byte> Key => _bufferStart.Slice(Unsafe.SizeOf<SkipNodeHeader>() + (_header.Height << 2), _header.KeyLength);
        public int DataPointer => _header.DataPointer;
        public ItemState State => _header.State;

        internal void Update(ItemState state, int dataPointer)
        {
            _header.State = state;
            _header.DataPointer = dataPointer;
            _bufferStart.Span.WriteAdvance(_header);
        }
    }
}
