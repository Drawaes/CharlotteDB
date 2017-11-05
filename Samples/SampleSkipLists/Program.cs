using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.Core;
using CharlotteDB.JamieStorage.InMemory;

namespace SampleSkipLists
{
    class Program
    {
        private static ManualResetEvent _event = new ManualResetEvent(false);

        static void Main(string[] args)
        {
            foreach (var f in System.IO.Directory.EnumerateFiles("c:\\code\\database"))
            {
                System.IO.File.Delete(f);
            }

            var ignore = TestDB();
            Console.WriteLine("Waiting to complete");
            _event.WaitOne();
        }

        private static async Task TestDB()
        {
            using (var database = Database.Create("c:\\code\\database", new ByteByByteComparer(), new DummyAllocator(1024 * 1024)))
            {
                var list = System.IO.File.ReadAllLines("C:\\code\\words.txt");
                var rnd = new Random();
                
                for (var i = 0; i < list.Length; i++)
                {
                    var l = list[i];
                    var bytes = Encoding.UTF8.GetBytes(l);
                    var span = new Memory<byte>(bytes);

                    if (rnd.NextDouble() < 0.05)
                    {
                        await database.TryRemoveAsync(span);
                    }
                    else
                    {
                        await database.PutAsync(span, span);
                    }
                }

                var b = Encoding.UTF8.GetBytes(list[0]);
                var mem = new Memory<byte>(b);
                var resultType =  database.TryGetDataAsync(mem);
            }
            _event.Set();
        }

        private static void TestSkipList()
        {

            var comparer = new ByteByByteComparer();
            var allocator = new DummyAllocator(1024 * 1024);
            var skipList = new SkipList<ByteByByteComparer>(comparer, allocator);
            var sortedDict = new SortedDictionary<string, string>();
            var list = System.IO.File.ReadAllLines("C:\\code\\words.txt");

            var sw = new Stopwatch();
            sw.Start();
            for (var i = 0; i < list.Length; i++)
            {
                var l = list[i];
                var bytes = Encoding.UTF8.GetBytes(l);
                var span = new Span<byte>(bytes);

                skipList.Insert(span, span);
                //sortedDict.Add(l, l);
            }

            var totalCount = skipList.Count;
            Console.Write($"time = {sw.ElapsedMilliseconds}");
        }
    }
}
