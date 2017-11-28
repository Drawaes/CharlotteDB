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
    public class BinaryTreeWriter
    {
        private IInMemoryStore _inMemory;
        private Stream _file;
        private int _rootNode;
        private int _startOfData;
        private int _endOfData;
        private byte[] _tempData = new byte[Unsafe.SizeOf<TreeHeader>()];
        private BloomFilter _bloomFilter;
        private int _bitsToUseForBloomFilter;
        private int _count;
        private ItemState _stateForMatch;

        public BinaryTreeWriter(int bitsToUseForBloomFilter, IDatabase database, int count, IInMemoryStore inMemory, ItemState stateForMatch)
        {
            _inMemory = inMemory;
            _stateForMatch = stateForMatch;
            _count = count;
            _bitsToUseForBloomFilter = bitsToUseForBloomFilter;
            _bloomFilter = new BloomFilter(count, _bitsToUseForBloomFilter, 2, database.Hasher);
        }

        public int RootNode => _rootNode;
        public int StartOfData => _startOfData;
        public int EndOfData => _endOfData;
        public BloomFilter BloomFilter => _bloomFilter;

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
            while (currentNode.State != _stateForMatch)
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
            if (currentNode.Data.Length > 0)
            {
                await _file.WriteAsync(currentNode.Data);
            }

            return index;
        }
    }
}
