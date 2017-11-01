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
        private List<Memory<byte>> _buffers = new List<Memory<byte>>();
        private int _bufferSize;
        private long _currentAllocatedPoint;
        private TCompare _comparer;
        private TAllocator _allocator;
        private int _count;
        private int _height;
        private const double _heightProbability = 0.5;
        private Random _random;
        private int _maxHeight;
        private int _bufferShift;
        private int _bufferMask;

        public SkipList(int seed, TCompare comparer, TAllocator allocator)
        {
            _comparer = comparer;
            _allocator = allocator;
            _bufferShift = (int)Math.Ceiling(Math.Log(allocator.NormalBufferSize, 2));
            _bufferSize = 1 << _bufferShift;
            _bufferMask = (1 << _bufferShift) - 1;
            _buffers.Add(_allocator.AllocateNormalBuffer());
            _random = new Random(seed);
            _maxHeight = 50;
            //Create head node, which is just a list of pointers going 0 -> _maxHeight - 1;
            _currentAllocatedPoint = sizeof(ulong) * _maxHeight;
        }

        private Span<long> HeadNode => _buffers[0].Span.Slice(0, (sizeof(long) * _maxHeight)).NonPortableCast<byte, long>();

        private Span<byte> GetKey(long pointerToItem)
        {
            var span = GetBufferForPointer(pointerToItem);
            span = span.Read(out int size);
            return span.Slice(0, size);
        }

        private Span<byte> GetBufferForPointer(long pointerToItem)
        {
            var bufferId = pointerToItem >> _bufferShift;
            var bufferIndex = pointerToItem & _bufferMask;
            var span = _buffers[(int)bufferId].Span.Slice((int)bufferIndex);
            return span;
        }

        public SkipList(TCompare comparer, TAllocator allocator) : this(Environment.TickCount, comparer, allocator)
        {
        }

        public int Count => _count;

        private Span<long> AllocateNode(byte height, Span<byte> key, Memory<byte> data, out long pointerStart)
        {
            var sizeNeeded = key.Length + sizeof(int);
            sizeNeeded += height * sizeof(ulong) + sizeof(byte) + Unsafe.SizeOf<Memory<byte>>();
            var pointerEnd = Interlocked.Add(ref _currentAllocatedPoint, sizeNeeded);
            pointerStart = pointerEnd - sizeNeeded;
            var bufferStart = pointerStart >> _bufferShift;
            var bufferEnd = pointerEnd >> _bufferShift;
            if (bufferStart != bufferEnd)
            {
                _buffers.Add(_allocator.AllocateNormalBuffer());
                _currentAllocatedPoint = (_buffers.Count - 1) * _bufferSize;
                return AllocateNode(height, key, data, out pointerStart);
            }
            // We have a buffer to write our node to, now we need to write the node data
            var nodeSpan = _buffers[(int)bufferStart].Span.Slice((int)(pointerStart % _bufferSize), sizeNeeded);
            // Key always goes first
            nodeSpan = nodeSpan.Write(key.Length);
            key.CopyTo(nodeSpan);
            nodeSpan = nodeSpan.Slice(key.Length);
            nodeSpan = nodeSpan.Write(data);

            // Height goes next
            nodeSpan = nodeSpan.Write((byte)height);
            // Then all we have is the list of pointers
            var pointerSpan = nodeSpan.NonPortableCast<byte, long>();
            return pointerSpan;
        }

        public void Insert(Span<byte> key, Memory<byte> data)
        {
            var height = GetHeight();

            var pointerSpan = AllocateNode((byte)height, key, data, out var pointerStart);
            var currentPointerList = HeadNode;
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
                var compareResult = _comparer.Compare(key, GetKey(nextPointer));
                if (compareResult == 0)
                {
                    throw new NotImplementedException("Duplicate key check, need to overwrite data");
                }
                else if (compareResult > 0)
                {
                    // bigger than the next node lets step into the next node and not change level
                    currentPointerList = GetPointerListForNode(nextPointer);
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

        private Span<long> GetPointerListForNode(long pointerToItem)
        {
            var span = GetBufferForPointer(pointerToItem);
            span = span.Read(out int size);
            span = span.Slice(size + Unsafe.SizeOf<Memory<byte>>());
            span = span.Read(out byte height);
            var heightSize = height * sizeof(long);
            return span.Slice(0, heightSize).NonPortableCast<byte, long>();
        }

        public bool TryFind(Span<byte> key, out Span<byte> data)
        {
            var levels = _height;
            var currentPointerTable = HeadNode;
            for (var l = levels - 1; l >= 0;)
            {
                if (currentPointerTable[l] == 0)
                {
                    l--;
                    continue;
                }
                // we have a valid pointer to an item
                var keyAtPointer = GetKey(currentPointerTable[l]);
                // now we compare
                var compareResult = _comparer.Compare(key, keyAtPointer);
                // if the key is > than we need to step into this next node
                // but not drop a level
                if (compareResult > 0)
                {
                    currentPointerTable = GetPointerListForNode(currentPointerTable[l]);
                    continue;
                }
                else if (compareResult == 0)
                {
                    data = GetDataFromNode(currentPointerTable[l]);
                    return true;
                }
                else if (compareResult < 0)
                {
                    //We overshot so we will just spin down a level
                    l--;
                }
            }

            return false;
        }

        private Span<byte> GetDataFromNode(long pointerToItem)
        {
            var span = GetBufferForPointer(pointerToItem);
            span = span.Read(out int size);
            span = span.Slice(size);
            span.Read(out Memory<byte> result);
            return result.Span;
        }

        public void Remove(Span<byte> key)
        {
            var currentPointerTable = HeadNode;
            var currentPointer = 0L;
            for (var l = _maxHeight - 1; l >= 0;)
            {
                var nextPointer = currentPointerTable[l];
                if (nextPointer == 0)
                {
                    l--;
                    continue;
                }
                var nextKey = GetKey(nextPointer);
                var result = _comparer.Compare(key, nextKey);
                if (result == 0)
                {
                    // Matches so we need to rewrite 
                    var nextPointerTable = GetPointerListForNode(nextPointer);
                    currentPointerTable[l] = nextPointerTable[l];
                    l--;
                    continue;
                }
                else if (result > 0)
                {
                    currentPointerTable = GetPointerListForNode(nextPointer);
                    currentPointer = nextPointer;
                    continue;
                }
                else if (result < 0)
                {
                    l--;
                    continue;
                }
            }
        }

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
