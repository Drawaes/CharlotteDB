using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.InMemory
{
    internal ref struct SkipListNode
    {
        private Span<byte> _key;
        private long _data;
        private Span<long> _pointers;
        private byte _height;

        public SkipListNode(Span<byte> input)
        {
            input = input.Read(out int keyLength);
            _key = input.Slice(0, keyLength);
            input = input.Slice(keyLength);
            input = input.Read(out _data);
            input = input.Read(out _height);
            _pointers = input.Slice(0, _height << 3).NonPortableCast<byte, long>();
        }

        public Span<byte> Key => _key;
        public Span<long> Pointers => _pointers;
        public long Data => _data;
    }
}
