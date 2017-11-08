using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;

namespace CharlotteDB.JamieStorage.InMemory
{
    public class SkipList2<TCompare> : IDisposable where TCompare : IKeyComparer
    {
        private const int MAXHEIGHT = 100;
        private const double LEVELPROBABILITY = 0.5;

        private List<OwnedMemory<byte>> _keyBuffers = new List<OwnedMemory<byte>>();
        private int _bufferSize;
        private int _currentKeyPointer;
        private byte _currentHeight;
        private int[] _headNode;
        private Random _random = new Random();
        private TCompare _comparer;
        private Allocator _allocator;

        public SkipList2(TCompare comparer, Allocator allocator)
        {
            _allocator = allocator;
            _comparer = comparer;
            _headNode = new int[MAXHEIGHT];
            for (var i = 0; i < _headNode.Length; i++)
            {
                _headNode[i] = -1;
            }
            _bufferSize = _allocator.NormalBufferSize;
        }

        private SkipListNode GetNodeForPointer(int pointerToItem)
        {
            var bufferId = pointerToItem / _bufferSize;
            var bufferIndex = pointerToItem % _bufferSize;
            var span = _keyBuffers[bufferId].Memory.Slice(bufferIndex);
            return new SkipListNode(span);
        }

        public void Dispose()
        {
        }
    }
}
