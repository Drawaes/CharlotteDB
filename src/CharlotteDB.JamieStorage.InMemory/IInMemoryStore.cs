using System;
using System.Collections.Generic;
using System.Text;
using CharlotteDB.Core;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;

namespace CharlotteDB.JamieStorage.InMemory
{
    public interface IInMemoryStore : IDisposable
    {
        int Count { get; }
        long SpaceUsed { get; }
        Memory<byte> GetDataFromPointer(int pointer);
        SearchResult TryFind(Span<byte> key, out Memory<byte> data);
        void Insert(Span<byte> key, Span<byte> data);
        void Remove(Span<byte> key);
        MemoryNode CurrentNode { get; }
        void Reset();
        void UpdateState(ItemState state);
        bool Next();
        void Init(Allocator allocator, IKeyComparer comparer);
    }
}
