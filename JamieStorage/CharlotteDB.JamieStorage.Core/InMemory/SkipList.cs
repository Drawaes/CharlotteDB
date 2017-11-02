using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using CharlotteDB.JamieStorage.Core.Allocation;
using CharlotteDB.JamieStorage.Core.Keys;

namespace CharlotteDB.JamieStorage.Core.InMemory
{
    public class SkipList<TCompare, TAllocator> where TCompare : IKeyComparer where TAllocator : IAllocator
    {
        private List<Memory<byte>> _keyBuffers = new List<Memory<byte>>();
        private List<Memory<byte>> _dataBuffers = new List<Memory<byte>>();
        private int _bufferSize;
        private uint _currentKeyPointer;
        private uint _currentDataPointer;
        private TCompare _comparer;
        private TAllocator _allocator;
        private int _count;
        private int _height;
        private const double _heightProbability = 0.5;
        private Random _random;
        private uint _maxHeight;
        private int _bufferShift;
        private uint _bufferMask;
        private uint[] _headNode;

        public SkipList(int seed, TCompare comparer, TAllocator allocator)
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

        public long SpaceUsed => _currentDataPointer + _currentKeyPointer;

        private SkipListNode GetNodeForPointer(uint pointerToItem)
        {
            var bufferId = pointerToItem >> _bufferShift;
            var bufferIndex = pointerToItem & _bufferMask;
            var span = _keyBuffers[(int)bufferId].Span.Slice((int)bufferIndex);
            return new SkipListNode(span);
        }

        public SkipList(TCompare comparer, TAllocator allocator) : this(Environment.TickCount, comparer, allocator)
        {
        }

        public int Count => _count;

        private Span<byte> AllocateMemory(List<Memory<byte>> buffers, uint size, ref uint currentPointer, out uint startPointer)
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
            nodeSpan = nodeSpan.Write(height);
            nodeSpan = nodeSpan.Write((ushort)key.Length);
            nodeSpan = nodeSpan.Write(((data << 8) | state));
            var returnSpan = nodeSpan.Slice(0, (int)(height << 2)).NonPortableCast<byte, uint>();
            nodeSpan = nodeSpan.Slice(height << 2);
            key.CopyTo(nodeSpan);
            return returnSpan;
        }

        private long StoreData(Span<byte> data)
        {
            var span = AllocateMemory(_dataBuffers, (uint)(data.Length + 4), ref _currentDataPointer, out var startPointer);
            span.Write(data.Length);
            data.CopyTo(span);
            return startPointer;
        }

        public Span<byte> GetDataFromPointer(long pointer)
        {
            var buffer = pointer >> _bufferShift;
            var bufferIndex = pointer & _bufferMask;
            var span = _dataBuffers[(int)buffer].Span.Slice((int)bufferIndex);
            span = span.Read<int>(out var length);
            return span.Slice(0, length);
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
                var compareResult = _comparer.Compare(key, nextNode.Key);
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

        public bool TryFind(Span<byte> key, out Span<byte> data)
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
                var compareResult = _comparer.Compare(key, nodeAtPointer.Key);
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
                        return true;
                    }
                    else
                    {
                        data = default;
                        return false;
                    }
                }
                else if (compareResult < 0)
                {
                    //We overshot so we will just spin down a level
                    l--;
                }
            }

            return false;
        }

        //public void Remove(Span<byte> key)
        //{
        //    var currentPointerTable = (Span<uint>)_headNode;
        //    var currentPointer = 0L;
        //    for (var l = (int)_maxHeight - 1; l >= 0;)
        //    {
        //        var nextPointer = currentPointerTable[l];
        //        if (nextPointer == 0)
        //        {
        //            l--;
        //            continue;
        //        }
        //        var nextNode = GetNodeForPointer(nextPointer);
        //        var result = _comparer.Compare(key, nextNode.Key);
        //        if (result == 0)
        //        {
        //            // Matches so we need to rewrite 
        //            var nextPointerTable = nextNode.PointerTable;
        //            currentPointerTable[l] = nextPointerTable[l];
        //            l--;
        //            continue;
        //        }
        //        else if (result > 0)
        //        {
        //            currentPointerTable = nextNode.PointerTable;
        //            currentPointer = nextPointer;
        //            continue;
        //        }
        //        else if (result < 0)
        //        {
        //            l--;
        //            continue;
        //        }
        //    }
        //}

        /// <summary>
        /// Here we get the height for an inserted element we used "fixed dice" ensure that we don't 
        /// make levels more than 1 greater than the current.
        /// </summary>
        /// <param name="maxLevel"></param>
        /// <returns></returns>
        protected virtual int GetHeight()
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
    }
}
