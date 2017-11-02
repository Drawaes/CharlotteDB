using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.InMemory
{
    public ref struct SkipListNode
    {
        private Span<byte> _bufferStart;
        private ushort _height;
        private byte _state;
        private ushort _keyLength;
        private long _dataPointer;

        public SkipListNode(Span<byte> buffer)
        {
            _bufferStart = buffer;
            var tempSpan = _bufferStart.Read(out uint header);
            _height = (ushort)(header & 0xFFFF);
            _keyLength = (ushort)((header >> 16) & 0xFFFF);
            tempSpan.Read(out _dataPointer);
            _state = (byte)(_dataPointer & 0xFF);
            _dataPointer = _dataPointer >> 8;
        }

        public Span<uint> PointerTable => _bufferStart.Slice(12).NonPortableCast<byte, uint>().Slice(0, _height);
        public Span<byte> Key => _bufferStart.Slice(12 + (_height << 2));
        public long DataPointer => _dataPointer;
        public byte State => _state;

        internal void Update(byte state, long dataPointer) => _bufferStart.Slice(sizeof(uint)).Write(((dataPointer << 8) | state));
    }
}
