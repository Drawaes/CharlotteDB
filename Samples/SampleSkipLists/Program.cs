using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using CharlotteDB.JamieStorage.Core;
using CharlotteDB.JamieStorage.Core.Allocation;
using CharlotteDB.JamieStorage.Core.InMemory;
using CharlotteDB.JamieStorage.Core.Keys;

namespace SampleSkipLists
{
    class Program
    {
        static void Main(string[] args)
        {
            //var database = Database.Create("", new ByteByByteComparer(), new DummyAllocator(1024 * 1024));
            TestSkipList();
        }

        private static void TestSkipList()
        {
            var sw = new Stopwatch();
            
            var comparer = new ByteByByteComparer();
            var allocator = new DummyAllocator(32 * 1024);
            var skipList = new SkipList<ByteByByteComparer, DummyAllocator>(comparer, allocator);
            var sortedDict = new SortedDictionary<string, string>();
            var list = System.IO.File.ReadAllLines("C:\\code\\words.txt");
            sw.Start();
            for (var i = 0; i < list.Length; i++)
            {
                var l = list[i];
                var bytes = Encoding.UTF8.GetBytes(l);
                var span = new Span<byte>(bytes);
                var mem = new Memory<byte>(bytes);
                skipList.Insert(span, bytes);
                //sortedDict.Add(l, l);
            }

            var totalCount = skipList.Count;
            Console.Write($"time = {sw.ElapsedMilliseconds}");
            if(!skipList.TryFind(Encoding.UTF8.GetBytes(list[0]), out var data))
            {
                throw new NotImplementedException();
            }
        }
    }
}
