using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using CharlotteDB.JamieStorage.Core.Allocation;
using CharlotteDB.JamieStorage.Core.InMemory;
using CharlotteDB.JamieStorage.Core.Keys;

namespace SampleSkipLists
{
    class Program
    {
        static void Main(string[] args) => TestSkipList();

        private static void TestSkipList()
        {
            var sw = new Stopwatch();
            sw.Start();
            var comparer = new ByteByByteComparer();
            var allocator = new Allocator(1024 * 1024);
            var skipList = new SkipList<ByteByByteComparer, Allocator>(comparer, allocator);
            var sortedDict = new SortedDictionary<string, string>();
            var list = System.IO.File.ReadAllLines("C:\\code\\words.txt");

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
        }
    }
}
