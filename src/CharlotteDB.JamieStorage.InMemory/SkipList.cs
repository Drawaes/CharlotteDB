using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using CharlotteDB.Core;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;

namespace CharlotteDB.JamieStorage.InMemory
{
    public class SkipList<TCompare> where TCompare : IKeyComparer
    {
        private List<OwnedMemory<byte>> _keyBuffers = new List<OwnedMemory<byte>>();
        private List<OwnedMemory<byte>> _dataBuffers = new List<OwnedMemory<byte>>();
        private int _bufferSize;
        private int _currentKeyPointer;
        private int _currentDataPointer;
        private TCompare _comparer;
        private Allocator _allocator;
        private int _count;
        private int _height;
        private const double _heightProbability = 0.5;
        private Random _random;
        private uint _maxHeight;
        private int _bufferShift;
        private int _bufferMask;
        private int[] _headNode;
        private int _currentNodePointer = -1;

        public SkipList(int seed, TCompare comparer, Allocator allocator)
        {
            _comparer = comparer;
            _allocator = allocator;
            _bufferShift = (int)Math.Ceiling(Math.Log(allocator.NormalBufferSize, 2));
            _bufferSize = 1 << _bufferShift;
            _bufferMask = (1 << _bufferShift) - 1;
            _keyBuffers.Add(_allocator.AllocateNormalBuffer());
            _dataBuffers.Add(_allocator.AllocateNormalBuffer());
            _currentKeyPointer = 1;
            _currentDataPointer = 1;
            _random = new Random(seed);
            _maxHeight = byte.MaxValue;
            _headNode = new int[_maxHeight];
        }

        public SkipList(TCompare comparer, Allocator allocator) : this(Environment.TickCount, comparer, allocator)
        {
        }

        public long SpaceUsed => _currentDataPointer + _currentKeyPointer;
        public int Count => _count;

        public MemoryNode CurrentNode
        {
            get
            {
                var node = GetNodeForPointer(_currentNodePointer);
                var mem = new MemoryNode()
                {
                    Data = GetDataFromPointer(node.DataPointer),
                    Key = node.Key,
                    State = (ItemState)node.State
                };
                return mem;
            }
        }

        private SkipListNode GetNodeForPointer(int pointerToItem)
        {
            var bufferId = pointerToItem >> _bufferShift;
            var bufferIndex = pointerToItem & _bufferMask;
            var span = _keyBuffers[bufferId].Memory.Slice(bufferIndex);
            return new SkipListNode(span);
        }

        private Span<byte> AllocateMemory(List<OwnedMemory<byte>> buffers, int size, ref int currentPointer, out int startPointer)
        {
            var bufferStart = currentPointer >> _bufferShift;
            var bufferEnd = (currentPointer + size) >> _bufferShift;
            if (bufferStart != bufferEnd)
            {
                buffers.Add(_allocator.AllocateNormalBuffer());
                currentPointer = (buffers.Count - 1) * _bufferSize;
                return AllocateMemory(buffers, size, ref currentPointer, out startPointer);
            }

            var nodeSpan = buffers[bufferStart].Span.Slice((currentPointer & _bufferMask), size);
            startPointer = currentPointer;
            currentPointer += size;
            return nodeSpan;
        }

        private Span<int> AllocateNode(ushort height, Span<byte> key, int data, ItemState state, out int pointerStart)
        {
            var sizeNeeded = key.Length + Unsafe.SizeOf<SkipNodeHeader>() + (height << 2);

            var nodeSpan = AllocateMemory(_keyBuffers, sizeNeeded, ref _currentKeyPointer, out pointerStart);

            var header = new SkipNodeHeader()
            {
                DataPointer = data,
                Height = height,
                KeyLength = (ushort)key.Length,
                State = state,
            };
            nodeSpan = nodeSpan.WriteAdvance(header);
            var returnSpan = nodeSpan.NonPortableCast<byte, int>().Slice(0, height);
            nodeSpan = nodeSpan.Slice(height << 2);
            key.CopyTo(nodeSpan);
            return returnSpan;
        }

        private int StoreData(Span<byte> data)
        {
            var span = AllocateMemory(_dataBuffers, (data.Length + sizeof(int)), ref _currentDataPointer, out var startPointer);
            span = span.WriteAdvance(data.Length);
            data.CopyTo(span);
            return startPointer;
        }

        public Memory<byte> GetDataFromPointer(long pointer)
        {
            var buffer = pointer >> _bufferShift;
            var bufferIndex = pointer & _bufferMask;
            var memory = _dataBuffers[(int)buffer].Memory.Slice((int)bufferIndex);
            var length = memory.Span.Read<int>();

            return memory.Slice(sizeof(int), length);
        }

        public void Insert(Span<byte> key, Span<byte> data) => Insert(key, data, ItemState.Alive);

        public void Remove(Span<byte> key) => Insert(key, new Span<byte>(), ItemState.Deleted);

        private void Insert(Span<byte> key, Span<byte> data, ItemState state)
        {
            var height = GetHeight();
            var dataStore = StoreData(data);
            var pointerSpan = AllocateNode((byte)height, key, dataStore, state, out var pointerStart);
            var currentPointerList = (Span<int>)_headNode;
            for (var level = (_height - 1); level >= 0;)
            {
                var nextPointer = currentPointerList[level];
                if (nextPointer == 0)
                {

                    if (level < height)
                    {
                        currentPointerList[level] = pointerStart;
                        pointerSpan[level] = 0;
                    }

                    // End of the chain we need to drop down
                    level--;
                    continue;
                }

                // We have a next value so lets check if it is bigger
                var nextNode = GetNodeForPointer(nextPointer);
                var compareResult = _comparer.Compare(key, nextNode.Key.Span);
                if (compareResult == 0)
                {
                    nextNode.Update(state, dataStore);
                    return;
                }
                else if (compareResult > 0)
                {
                    // bigger than the next node lets step into the next node and not change level
                    currentPointerList = nextNode.PointerTable;
                }
                else
                {
                    if (level < height)
                    {
                        // We are inserting between the next node
                        currentPointerList[level] = pointerStart;
                        pointerSpan[level] = nextPointer;
                    }

                    // drop a level
                    level--;
                }
            }
            _count++;
        }

        public SearchResult TryFind(Span<byte> key, out Memory<byte> data)
        {
            var levels = _height;
            var currentPointerTable = (Span<int>)_headNode;
            for (var l = levels - 1; l >= 0;)
            {
                if (currentPointerTable[l] == 0)
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
                    if (nodeAtPointer.State == 0)
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

        /// <summary>
        /// Here we get the height for an inserted element we used "fixed dice" ensure that we don't 
        /// make levels more than 1 greater than the current.
        /// </summary>
        /// <param name="maxLevel"></param>
        /// <returns></returns>
        protected int GetHeight()
        {
            var newHeight = 1;
            while ((_random.NextDouble() < _heightProbability))
            {
                newHeight++;
            }
            if (newHeight > _height)
            {
                if (_height < _maxHeight)
                {
                    _height++;
                }
                return _height;
            }
            return newHeight;
        }

        public void Reset() => _currentNodePointer = -1;

        public void UpdateState(ItemState state)
        {
            var node = GetNodeForPointer(_currentNodePointer);
            node.Update(state, node.DataPointer);
        }

        public bool Next()
        {
            if (_currentNodePointer == -1)
            {
                _currentNodePointer = _headNode[0];
                return true;
            }

            var skip = GetNodeForPointer(_currentNodePointer);
            if (skip.PointerTable[0] == 0)
            {
                return false;
            }

            _currentNodePointer = skip.PointerTable[0];
            return true;
        }
    }
}
