using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using CharlotteDB.Core;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.Hashing;
using CharlotteDB.JamieStorage.InMemory;

namespace CharlotteDB.JamieStorage.Core.StorageTables
{
    public class BinaryTreeWriter<TCompare> where TCompare : IKeyComparer
    {
        private SkipList2<TCompare> _inMemory;
        private Stream _file;
        private int _rootNode;
        private int _startOfData;
        private int _endOfData;
        private byte[] _tempData = new byte[Unsafe.SizeOf<TreeHeader>()];
        private BloomFilter<FNV1Hash> _bloomFilter;
        private int _bitsToUseForBloomFilter;
        private int _count;

        public BinaryTreeWriter(int bitsToUseForBloomFilter, Database<TCompare> database, int count, SkipList2<TCompare> inMemory)
        {
            _inMemory = inMemory;
            _count = count;
            _bitsToUseForBloomFilter = bitsToUseForBloomFilter;
            _bloomFilter = new BloomFilter<FNV1Hash>(count, _bitsToUseForBloomFilter, 2, database.Hasher);
        }

        public int RootNode => _rootNode;
        public int StartOfData => _startOfData;
        public int EndOfData => _endOfData;
        public BloomFilter<FNV1Hash> BloomFilter => _bloomFilter;

        public async Task WriteTreeAsync(Stream file)
        {
            _file = file;
            _startOfData = (int)file.Position;
            _inMemory.Reset();
            _rootNode = await Recurse(_count);
            _endOfData = (int)file.Position;
        }
        
        private async ValueTask<int> Recurse(int count)
        {
            if (count <= 0)
            {
                return 0;
            }

            var leftSize = count / 2;
            var leftNode = await Recurse(leftSize);
            
            _inMemory.Next();
            var currentNode = _inMemory.CurrentNode;
            while (currentNode.State != ItemState.Alive)
            {
                if (!_inMemory.Next())
                {
                    return 0;
                }
                currentNode = _inMemory.CurrentNode;
            }
            _bloomFilter.Add(currentNode.Key.Span);


            var header = new TreeHeader()
            {
                DataSize = currentNode.Data.Length,
                KeySize = (ushort)currentNode.Key.Length,
                LeftNode = leftNode, 
                RightNode = await Recurse((count - leftSize) - 1),
            };

            var index = (int)_file.Position;
            
            _tempData.AsSpan().WriteAdvance(header);
            await _file.WriteAsync(_tempData);
            await _file.WriteAsync(currentNode.Key);
            await _file.WriteAsync(currentNode.Data);
            return index;
        }
    }
}
