using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using CharlotteDB.Core;
using CharlotteDB.Core.Keys;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    internal class BinaryTree
    {
        private int _headNode;
        private Memory<byte> _buffer;
        private IKeyComparer _comparer;

        public BinaryTree(Memory<byte> mappedFile, BTreeAndBloomFilter indexes, IKeyComparer comparer)
        {
            _comparer = comparer;
            _buffer = mappedFile.Slice(0, indexes.RegionLength + indexes.RegionIndex);
            _headNode = indexes.HeadNodeIndex;
        }

        public (SearchResult result, Memory<byte> data) FindNode(Span<byte> key)
        {
            var span = _buffer.Span;

            var currentNodeIndex = _headNode;

            while (true)
            {
                if (currentNodeIndex == 0)
                {
                    return (SearchResult.NotFound, default);
                }

                var key2 = GetNode(currentNodeIndex, out var header);
                var compResult = _comparer.Compare(key, key2);
                if (compResult == 0)
                {
                    return (SearchResult.Found, _buffer.Slice(currentNodeIndex + Unsafe.SizeOf<TreeHeader>() + header.KeySize, header.DataSize));
                }
                else if (compResult == -1)
                {
                    currentNodeIndex = header.LeftNode;
                }
                else
                {
                    currentNodeIndex = header.RightNode;
                }
            }
        }

        private Span<byte> GetNode(int nodeIndex, out TreeHeader header) => _buffer.Span.Slice(nodeIndex).ReadAdvance(out header).Slice(0, header.KeySize);
    }
}
