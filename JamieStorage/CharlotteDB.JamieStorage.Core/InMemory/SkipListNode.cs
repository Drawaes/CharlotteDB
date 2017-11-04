using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core;

namespace CharlotteDB.JamieStorage.Core.InMemory
{
    public ref struct SkipListNode
    {
        private Memory<byte> _bufferStart;
        private ushort _height;
        private byte _state;
        private ushort _keyLength;
        private long _dataPointer;

        public SkipListNode(Memory<byte> buffer)
        {
            _bufferStart = buffer;
            var tempSpan = buffer.Span.ReadAdvance(out _height);
            tempSpan = tempSpan.ReadAdvance(out _keyLength);
            _dataPointer = tempSpan.Read<long>();
            _state = (byte)(_dataPointer & 0xFF);
            _dataPointer = _dataPointer >> 8;
        }

        public Span<uint> PointerTable => _bufferStart.Span.Slice(12).NonPortableCast<byte, uint>().Slice(0, _height);
        public Memory<byte> Key => _bufferStart.Slice(12 + (_height << 2), _keyLength);
        public long DataPointer => _dataPointer;
        public byte State => _state;

        internal void Update(byte state, long dataPointer) => _bufferStart.Slice(sizeof(uint)).Span.WriteAdvance(((dataPointer << 8) | state));
    }
}
