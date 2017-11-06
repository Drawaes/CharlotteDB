using System;
using System.Buffers;
using System.Collections.Generic;
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
        private uint _currentKeyPointer;
        private uint _currentDataPointer;
        private TCompare _comparer;
        private Allocator _allocator;
        private int _count;
        private int _height;
        private const double _heightProbability = 0.5;
        private Random _random;
        private uint _maxHeight;
        private int _bufferShift;
        private uint _bufferMask;
        private uint[] _headNode;
        private long _currentNodePointer = -1;

        public SkipList(int seed, TCompare comparer, Allocator allocator)
        {
            _comparer = comparer;
            _allocator = allocator;
            _bufferShift = (int)Math.Ceiling(Math.Log(allocator.NormalBufferSize, 2));
            _bufferSize = 1 << _bufferShift;
            _bufferMask = (uint)(1 << _bufferShift) - 1;
            _keyBuffers.Add(_allocator.AllocateNormalBuffer());
            _dataBuffers.Add(_allocator.AllocateNormalBuffer());

            _random = new Random(seed);
            _maxHeight = byte.MaxValue;
            _headNode = new uint[_maxHeight];
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
                var node = GetNodeForPointer((uint)_currentNodePointer);
                var mem = new MemoryNode()
                {
                    Data = GetDataFromPointer(node.DataPointer),
                    Key = node.Key,
                    State = (ItemState)node.State
                };
                return mem;
            }
        }

        private SkipListNode GetNodeForPointer(uint pointerToItem)
        {
            var bufferId = pointerToItem >> _bufferShift;
            var bufferIndex = pointerToItem & _bufferMask;
            var span = _keyBuffers[(int)bufferId].Memory.Slice((int)bufferIndex);
            return new SkipListNode(span);
        }

        private Span<byte> AllocateMemory(List<OwnedMemory<byte>> buffers, uint size, ref uint currentPointer, out uint startPointer)
        {
            var bufferStart = currentPointer >> _bufferShift;
            var bufferEnd = (currentPointer + size) >> _bufferShift;
            if (bufferStart != bufferEnd)
            {
                buffers.Add(_allocator.AllocateNormalBuffer());
                currentPointer = (uint)((buffers.Count - 1) * _bufferSize);
                return AllocateMemory(buffers, size, ref currentPointer, out startPointer);
            }

            var nodeSpan = buffers[(int)bufferStart].Span.Slice((int)(currentPointer & _bufferMask), (int)size);
            startPointer = currentPointer;
            currentPointer += size;
            return nodeSpan;
        }

        private Span<uint> AllocateNode(ushort height, Span<byte> key, long data, byte state, out uint pointerStart)
        {
            var sizeNeeded = key.Length + (sizeof(ushort) * 2) + sizeof(long) + (height << 2);

            var nodeSpan = AllocateMemory(_keyBuffers, (uint)sizeNeeded, ref _currentKeyPointer, out pointerStart);
            // Key always goes first
            nodeSpan = nodeSpan.WriteAdvance(height);
            nodeSpan = nodeSpan.WriteAdvance((ushort)key.Length);
            nodeSpan = nodeSpan.WriteAdvance(((data << 8) | state));
            var returnSpan = nodeSpan.Slice(0, height << 2).NonPortableCast<byte, uint>();
            nodeSpan = nodeSpan.Slice(height << 2);
            key.CopyTo(nodeSpan);
            return returnSpan;
        }

        private long StoreData(Span<byte> data)
        {
            var span = AllocateMemory(_dataBuffers, (uint)(data.Length + sizeof(int)), ref _currentDataPointer, out var startPointer);
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

        public void Insert(Span<byte> key, Span<byte> data) => Insert(key, data, 0);

        public void Remove(Span<byte> key) => Insert(key, new Span<byte>(), 1);

        private void Insert(Span<byte> key, Span<byte> data, byte state)
        {
            var height = GetHeight();
            var dataStore = StoreData(data);
            var pointerSpan = AllocateNode((byte)height, key, dataStore, state, out var pointerStart);
            var currentPointerList = (Span<uint>)_headNode;
            for (var level = _height; level >= 0;)
            {
                var nextPointer = currentPointerList[level];
                if (nextPointer == 0)
                {
                    // End of the chain we need to drop down
                    if (level < height)
                    {
                        currentPointerList[level] = pointerStart;
                        pointerSpan[level] = 0;
                    }
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
                    continue;
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
                    continue;
                }
            }
            _count++;
        }

        public SearchResult TryFind(Span<byte> key, out Memory<byte> data)
        {
            var levels = _height;
            var currentPointerTable = (Span<uint>)_headNode;
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
                        data = GetDataFromPointer(nodeAtPointer.PointerTable[l]);
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

        public bool Next()
        {
            if (_currentNodePointer == -1)
            {
                _currentNodePointer = _headNode[0];
                return true;
            }

            var skip = GetNodeForPointer((uint)_currentNodePointer);
            if (skip.PointerTable[0] == 0)
            {
                return false;
            }

            _currentNodePointer = skip.PointerTable[0];
            return true;
        }
    }
}
