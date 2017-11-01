using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.InMemory
{
    public ref struct SkipListNode
    {
        private Span<byte> _span;
        private int _length;
        private Span<byte> _key;
        private byte _state;
        private Memory<byte> _data;
        private Span<long> _pointers;
        private byte _height;
        private static readonly int _fixedHeaderSize = sizeof(int) + sizeof(byte) * 2 + Unsafe.SizeOf<Memory<byte>>() + sizeof(int); // Header + state + height + keyLengthPrefix

        public SkipListNode(Span<byte> input)
        {
            input = input.ReadAndAdvance(out _length);
            input = input.Slice(0, _length);
            _span = input;
            input = input.ReadAndAdvance(out _state);
            input = input.ReadAndAdvance(out _height);
            _pointers = input.Slice(0, (int)(_height << 3)).NonPortableCast<byte, long>();
            input = input.Slice(_height << 3);
            input = input.ReadAndAdvance(out _data);
            input = input.ReadAndAdvance(out int keyLength);
            _key = input.Slice(0, keyLength);
        }

        internal static int LengthRequired(Span<byte> key, byte height) => key.Length + _fixedHeaderSize + (height << 3); // Key + length prefix

        internal static SkipListNode Create(Span<byte> emptyBuffer, Span<byte> key, Memory<byte> data, byte height, byte state)
        {
            var node = new SkipListNode
            {
                _length = emptyBuffer.Length - sizeof(int),
                _state = state,
                _height = height,
                _data = data,
            };

            emptyBuffer = emptyBuffer.Write(node._length);
            node._span = emptyBuffer;
            emptyBuffer = emptyBuffer.Write(state);
            emptyBuffer = emptyBuffer.Write(height);
            var heightSize = height << 3;
            node._pointers = emptyBuffer.Slice(0, heightSize).NonPortableCast<byte, long>();
            emptyBuffer = emptyBuffer.Slice(height << 3);
            emptyBuffer = emptyBuffer.Write(data);
            emptyBuffer = emptyBuffer.Write(key.Length);
            key.CopyTo(emptyBuffer);
            node._key = emptyBuffer;
            return node;
        }

        public Span<byte> Key => _key;
        public Span<long> Pointers => _pointers;
        public Memory<byte> Data => _data;
        public int Length => _length;
    }
}
