using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using CharlotteDB.Core;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;

namespace CharlotteDB.JamieStorage.InMemory
{
    public class SkipList2<TCompare> : IDisposable where TCompare : IKeyComparer
    {
        private const int MAXHEIGHT = 100;
        private const double LEVELPROBABILITY = 0.5;

        private List<OwnedMemory<byte>> _keyBuffers = new List<OwnedMemory<byte>>();
        private List<OwnedMemory<byte>> _dataBuffers = new List<OwnedMemory<byte>>();
        private int _bufferSize;
        private int _currentKeyPointer = 0;
        private int _currentDataPointer = 0;
        private byte _currentHeight;
        private int[] _headNode;
        private Random _random = new Random();
        private TCompare _comparer;
        private Allocator _allocator;
        private int _count;
        private int _iteratorNodePointer = -2;

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
            _keyBuffers.Add(_allocator.AllocateNormalBuffer());
            _dataBuffers.Add(_allocator.AllocateNormalBuffer());
        }

        public int Count => _count;
        public long SpaceUsed => _currentDataPointer + _currentKeyPointer;

        private SkipListNode GetNodeForPointer(int pointerToItem)
        {
            Debug.Assert(pointerToItem >= 0);
            var bufferId = pointerToItem / _bufferSize;
            var bufferIndex = pointerToItem % _bufferSize;
            var span = _keyBuffers[bufferId].Memory.Slice(bufferIndex);
            return new SkipListNode(span);
        }

        private SkipListNode AllocateNode(Span<byte> key, ushort height, int dataPointer, ItemState state, out int pointer)
        {
            var sizeNeeded = (height * sizeof(int)) + Unsafe.SizeOf<SkipNodeHeader>() + key.Length;

            var header = new SkipNodeHeader()
            {
                DataPointer = dataPointer,
                Height = height,
                KeyLength = (ushort)key.Length,
                State = state,
            };

            var buffer = AllocateBuffer(_keyBuffers, sizeNeeded, ref _currentKeyPointer);
            var keyBuffer = buffer.memory.Span.WriteAdvance(header);
            key.CopyTo(keyBuffer.Slice(height << 2));
            pointer = _currentKeyPointer;
            _currentKeyPointer += sizeNeeded;
            pointer = buffer.pointer;
            return new SkipListNode(buffer.memory);
        }

        private int StoreData(Span<byte> data)
        {
            var (mem, pointer) = AllocateBuffer(_dataBuffers, data.Length + sizeof(int), ref _currentDataPointer);
            var span = mem.Span.WriteAdvance(data.Length);
            data.CopyTo(span);
            return pointer;
        }

        public Memory<byte> GetDataFromPointer(int pointer)
        {
            var buffer = pointer / _bufferSize;
            var bufferIndex = pointer % _bufferSize;
            var memory = _dataBuffers[buffer].Memory.Slice(bufferIndex);
            var length = memory.Span.Read<int>();

            return memory.Slice(sizeof(int), length);
        }

        private (Memory<byte> memory, int pointer) AllocateBuffer(List<OwnedMemory<byte>> buffers, int size, ref int currentPointer)
        {
            var buffer = currentPointer / _bufferSize;
            var bufferIndex = currentPointer % _bufferSize;

            if ((bufferIndex + size) >= (_bufferSize) || buffer >= buffers.Count)
            {
                buffer = buffers.Count;
                bufferIndex = 0;
                currentPointer = buffer * _bufferSize;
                buffers.Add(_allocator.AllocateNormalBuffer());
            }

            var mem = buffers[buffer].Memory.Slice(bufferIndex, size);
            var pointer = currentPointer;
            currentPointer += size;
            return (mem, pointer);
        }

        public SearchResult TryFind(Span<byte> key, out Memory<byte> data)
        {
            var levels = _currentHeight;
            var currentPointerTable = (Span<int>)_headNode;
            for (var l = levels - 1; l >= 0;)
            {
                if (currentPointerTable[l] == -1)
                {
                    l--;
                    continue;
                }
                // we have a valid pointer to an item
                var nodeAtPointer = GetNodeForPointer(currentPointerTable[l]);
                // now we compare
                var compareResult = _comparer.Compare(key, nodeAtPointer.Key.Span);
                // if the key is > than we need to step into this next node
                // but not drop a level
                if (compareResult > 0)
                {
                    currentPointerTable = nodeAtPointer.PointerTable;
                    continue;
                }
                else if (compareResult == 0)
                {
                    if (nodeAtPointer.State == ItemState.Alive)
                    {
                        data = GetDataFromPointer(nodeAtPointer.DataPointer);
                        return SearchResult.Found;
                    }
                    else
                    {
                        data = default;
                        return SearchResult.Deleted;
                    }
                }
                else if (compareResult < 0)
                {
                    //We overshot so we will just spin down a level
                    l--;
                }
            }
            data = default;
            return SearchResult.NotFound;
        }

        public void Insert(Span<byte> key, Span<byte> data) => Insert(key, data, ItemState.Alive);

        public void Remove(Span<byte> key) => Insert(key, default, ItemState.Deleted);

        private unsafe void Insert(Span<byte> key, Span<byte> data, ItemState state)
        {
            var height = (ushort)GetHeight();
            Span<int> replacementNodes = stackalloc int[height];

            int dataPointer;
            if (state == ItemState.Alive)
            {
                dataPointer = StoreData(data);
            }
            else
            {
                dataPointer = 0;
            }

            var matchIndex = GetReplacementNodes(key, replacementNodes);
            if (matchIndex >= 0)
            {
                var node = GetNodeForPointer(matchIndex);
                node.Update(state, dataPointer);
                return;
            }

            var newNode = AllocateNode(key, height, dataPointer, state, out var newPointer);
            Debug.Assert(height >= 0);

            for (var i = 0; i < replacementNodes.Length; i++)
            {
                if (replacementNodes[i] == -1)
                {
                    _headNode[i] = newPointer;
                    newNode.PointerTable[i] = -1;
                }
                else
                {
                    var oldNode = GetNodeForPointer(replacementNodes[i]);
                    newNode.PointerTable[i] = oldNode.PointerTable[i];
                    oldNode.PointerTable[i] = newPointer;
                }
            }
            _count++;
        }

        public void Reset() => _iteratorNodePointer = -2;

        public void UpdateState(ItemState state)
        {
            Debug.Assert(_iteratorNodePointer > -1);
            var node = GetNodeForPointer(_iteratorNodePointer);
            node.Update(state, node.DataPointer);
        }

        public MemoryNode CurrentNode
        {
            get
            {
                var node = GetNodeForPointer(_iteratorNodePointer);
                var mem = new MemoryNode()
                {
                    Data = GetDataFromPointer(node.DataPointer),
                    Key = node.Key,
                    State = node.State
                };
                return mem;
            }
        }

        public bool Next()
        {
            if (_iteratorNodePointer == -2)
            {
                _iteratorNodePointer = _headNode[0];
                return true;
            }

            var skip = GetNodeForPointer(_iteratorNodePointer);
            if (skip.PointerTable[0] == -1)
            {
                return false;
            }

            _iteratorNodePointer = skip.PointerTable[0];
            return true;
        }

        private int GetReplacementNodes(Span<byte> key, Span<int> replacementNodes)
        {
            var currentPointers = (Span<int>)_headNode;
            var currentPointer = -1;
            for (var level = _currentHeight - 1; level >= 0;)
            {
                if (currentPointers[level] == -1)
                {
                    if (level < replacementNodes.Length)
                    {
                        replacementNodes[level] = currentPointer;
                    }
                    level--;
                    continue;
                }

                var nextNode = GetNodeForPointer(currentPointers[level]);
                var compare = _comparer.Compare(key, nextNode.Key.Span);
                switch (compare)
                {
                    case 0:
                        // Matches so we are going to update in place
                        return currentPointers[level];
                    case 1:
                        // Key is bigger need to step forward in the list
                        currentPointer = currentPointers[level];
                        currentPointers = nextNode.PointerTable;
                        break;
                    case -1:
                        // Key is smaller so we insert here and drop down
                        if (level < replacementNodes.Length)
                        {
                            replacementNodes[level] = currentPointer;
                        }
                        level--;
                        break;
                }
            }
            return -1;
        }

        /// <summary>
        /// Here we get the height for an inserted element we used "fixed dice" ensure that we don't 
        /// make levels more than 1 greater than the current.
        /// </summary>
        /// <param name="maxLevel"></param>
        /// <returns></returns>
        private int GetHeight()
        {
            var newHeight = 1;
            while ((_random.NextDouble() < LEVELPROBABILITY))
            {
                newHeight++;
            }
            if (newHeight > _currentHeight)
            {
                if (_currentHeight <= MAXHEIGHT)
                {
                    _currentHeight++;
                }
                return _currentHeight;
            }
            return newHeight;
        }

        public void Dispose()
        {
            foreach (var b in _dataBuffers)
            {
                _allocator.ReturnBuffer(b);
            }
            foreach (var b in _keyBuffers)
            {
                _allocator.ReturnBuffer(b);
            }
        }
    }
}
