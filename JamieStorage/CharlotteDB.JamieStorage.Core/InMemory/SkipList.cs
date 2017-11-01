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
            _maxHeight = byte.MaxValue;
            //Create head node, which is just a list of pointers going 0 -> _maxHeight - 1;
            _currentAllocatedPoint = sizeof(ulong) * _maxHeight;
        }

        private Span<long> HeadNodePointers => _buffers[0].Span.Slice(0, (sizeof(long) * _maxHeight)).NonPortableCast<byte, long>();

        private SkipListNode GetNodeFromPointer(long pointer)
        {
            var bufferId = (int)(pointer >> _bufferShift);
            var bufferIndex = (int)(pointer & _bufferMask);
            var span = _buffers[bufferId].Span.Slice(bufferIndex);
            return new SkipListNode(span);
        }

        public SkipList(TCompare comparer, TAllocator allocator) : this(Environment.TickCount, comparer, allocator)
        {
        }

        public int Count => _count;

        private SkipListNode AllocateNode(byte height, Span<byte> key, Memory<byte> data, out long pointerStart)
        {
            var sizeNeeded = SkipListNode.LengthRequired(key, height);

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

            var bufferToWriteTo = _buffers[(int)bufferStart].Span.Slice((int)(pointerStart % _bufferSize), sizeNeeded);
            return SkipListNode.Create(bufferToWriteTo, key, data, height, 0);
        }

        public void Insert(Span<byte> key, Memory<byte> data)
        {
            var height = GetHeight();

            var newNode = AllocateNode((byte)height, key, data, out var pointerStart);
            var currentPointerList = HeadNodePointers;
            for (var level = _height; level >= 0;)
            {
                var nextPointer = currentPointerList[level];
                if (nextPointer == 0)
                {
                    // End of the chain we need to drop down
                    if (level < height)
                    {
                        currentPointerList[level] = pointerStart;
                        newNode.Pointers[level] = 0;
                    }
                    level--;
                    continue;
                }
                var currentNode = GetNodeFromPointer(nextPointer);
                // We have a next value so lets check if it is bigger
                var compareResult = _comparer.Compare(key, currentNode.Key);
                if (compareResult == 0)
                {
                    throw new NotImplementedException("Duplicate key check, need to overwrite data");
                }
                else if (compareResult > 0)
                {
                    // bigger than the next node lets step into the next node and not change level
                    currentPointerList = currentNode.Pointers;
                    continue;
                }
                else
                {
                    if (level < height)
                    {
                        // We are inserting between the next node
                        currentPointerList[level] = pointerStart;
                        newNode.Pointers[level] = nextPointer;
                    }

                    // drop a level
                    level--;
                    continue;
                }
            }
            _count++;
        }

        private bool TryFindNode(Span<byte> key, out SkipListNode pointerToNode)
        {
            var levels = _height;
            var currentPointerTable = HeadNodePointers;
            for (var l = levels - 1; l >= 0;)
            {
                if (currentPointerTable[l] == 0)
                {
                    l--;
                    continue;
                }
                // we have a valid pointer to an item
                var nodeAtPointer = GetNodeFromPointer(currentPointerTable[l]);

                // now we compare
                var compareResult = _comparer.Compare(key, nodeAtPointer.Key);
                // if the key is > than we need to step into this next node
                // but not drop a level
                if (compareResult > 0)
                {
                    currentPointerTable = nodeAtPointer.Pointers;
                    continue;
                }
                else if (compareResult == 0)
                {
                    pointerToNode = nodeAtPointer;
                    return true;
                }
                else if (compareResult < 0)
                {
                    //We overshot so we will just spin down a level
                    l--;
                }
            }
            pointerToNode = default;
            return false;
        }

        public bool TryFind(Span<byte> key, out Span<byte> data)
        {
            var result = TryFindNode(key, out var pointerToNode);
            if (result)
            {
                data = pointerToNode.Data.Span;
            }
            else
            {
                data = default;
            }
            return result;
        }

        public void Remove(Span<byte> key)
        {
            // We don't actually remove nodes, instead we will either find the node and update the state
            // to deleted or we will insert a "deleted" node. This is to stop previous versions of this node
            // becoming 'alive' if we delete a node that is in an SSTable
            var tryGetNodeResult = TryFindNode(key, out var pointerToNode);
            if (tryGetNodeResult)
            {
                // We found the node so just update the state
                throw new NotImplementedException("Remove node");
            }
            else
            {
                // We didn't find the node so we need to insert a deleted node
                throw new NotImplementedException("Remove node");
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
