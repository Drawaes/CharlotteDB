using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;

namespace CharlotteDB.JamieStorage.InMemory
{
    public class InMemorySortedList : IInMemoryStore
    {
        private long _size;
        private SortedDictionary<Memory<byte>, StateObject> _storage;
        private Allocator _allocator;
        private int _bufferSize;
        private int _currentPointer;
        private List<OwnedMemory<byte>> _buffers;
        private SortedDictionary<Memory<byte>, StateObject>.Enumerator _enumerator;
        private bool _hasEnumerator = false;

        public int Count => _storage.Count;

        public long SpaceUsed => _size;

        public MemoryNode CurrentNode
        {
            get
            {
                var memNode = new MemoryNode()
                {
                    Key = _enumerator.Current.Key,
                    Data = _enumerator.Current.Value.Data,
                    State = _enumerator.Current.Value.State,
                };
                return memNode;
            }
        }
        public void Dispose() { }

        public Memory<byte> GetDataFromPointer(int pointer) => throw new NotImplementedException();

        private (Memory<byte> key, Memory<byte> data) AllocateNode(Span<byte> key, Span<byte> data)
        {
            var buffer = _currentPointer / _bufferSize;
            var bufferIndex = _currentPointer % _bufferSize;
            var size = key.Length + data.Length;
            if ((bufferIndex + size) >= (_bufferSize) || buffer >= _buffers.Count)
            {
                buffer = _buffers.Count;
                bufferIndex = 0;
                _currentPointer = buffer * _bufferSize;
                _buffers.Add(_allocator.AllocateNormalBuffer());
            }

            var mem = _buffers[buffer].Memory.Slice(bufferIndex, size);
            _currentPointer += size;

            var keyMem = mem.Slice(0, key.Length);
            var valMem = mem.Slice(key.Length);
            key.CopyTo(keyMem.Span);
            data.CopyTo(valMem.Span);

            _size += key.Length + data.Length;

            return (keyMem, valMem);
        }

        public void Insert(Span<byte> key, Span<byte> data)
        {
            var storedValue = AllocateNode(key, data);
            _storage[storedValue.key] = new StateObject() { Data = storedValue.data, State = ItemState.Alive };
        }

        public bool Next()
        {
            if (!_hasEnumerator)
            {
                _hasEnumerator = true;
                _enumerator = _storage.GetEnumerator();
            }
            return _enumerator.MoveNext();
        }

        public void Remove(Span<byte> key)
        {
            var storedValue = AllocateNode(key, default);
            _storage[storedValue.key] = new StateObject() { Data = default, State = ItemState.Deleted };
        }

        public void Reset() => _enumerator = _storage.GetEnumerator();

        public SearchResult TryFind(Span<byte> key, out Memory<byte> data)
        {
            var mem = new byte[key.Length];
            key.CopyTo(mem.AsSpan());
            if(_storage.TryGetValue(mem, out var value))
            {
                data = value.Data;
                if (value.State == ItemState.Alive)
                {
                    return SearchResult.Found;
                }
                else
                {
                    return SearchResult.Deleted;
                }
            }
            data = default;
            return SearchResult.NotFound;
        }

        public void UpdateState(ItemState state)
        {
            var item = _storage[_enumerator.Current.Key];
            item.State = state;
        }

        public void Init(Allocator allocator, IKeyComparer comparer)
        {
            _allocator = allocator;
            _bufferSize = _allocator.NormalBufferSize;
            _currentPointer = 0;
            _storage = new SortedDictionary<Memory<byte>, StateObject>(comparer);
            _buffers = new List<OwnedMemory<byte>>
            {
                _allocator.AllocateNormalBuffer()
            };
        }

        private class StateObject
        {
            public Memory<byte> Data;
            public ItemState State;
        }
    }
}
