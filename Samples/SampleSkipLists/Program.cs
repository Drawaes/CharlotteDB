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
            //TestSkipList();
            Console.WriteLine("Waiting to complete");
            _event.WaitOne();
        }

        private static async Task TestDB()
        {
            var outputList = new List<(bool deleted, Memory<byte> bytes)>();
            using (var database = Database.Create("c:\\code\\database", new ByteByByteComparer(), new DummyAllocator(1024 * 1024)))
            {
                var list = System.IO.File.ReadAllLines("C:\\code\\words.txt");
                var rnd = new Random();

                for (var i = 0; i < list.Length; i++)
                {
                    var l = list[i];
                    var bytes = Encoding.UTF8.GetBytes(l);
                    var span = new Memory<byte>(bytes);

                    if (i != 0 && rnd.NextDouble() < 0.05)
                    {
                        await database.TryRemoveAsync(span);
                        outputList.Add((true, span));
                    }
                    else
                    {
                        await database.PutAsync(span, span);
                        outputList.Add((false, span));
                    }
                }

                for (var i = 0; i < outputList.Count; i++)
                {
                    var (deleted, bytes) = outputList[i];

                    var (found, data) = database.TryGetData(bytes);
                    if (!found)
                    {
                        throw new InvalidOperationException();
                    }
                    if (!data.Span.SequenceEqual(bytes.Span))
                    {
                        throw new InvalidOperationException();
                    }
                }
            }
            _event.Set();
        }

        private static void TestSkipList()
        {

            var comparer = new ByteByByteComparer();
            var allocator = new DummyAllocator(1024 * 1024);
            var skipList = new SkipList<ByteByByteComparer>(comparer, allocator);
            var sortedDict = new SortedDictionary<string, string>();
            var list = System.IO.File.ReadAllLines("C:\\code\\output.txt");

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

            var sb = new StringBuilder();
            while (skipList.Next())
            {
                sb.AppendLine(Encoding.UTF8.GetString(skipList.CurrentNode.Key.ToArray()));
            }

            System.IO.File.WriteAllText("C:\\code\\output.txt", sb.ToString());

            for (var i = 0; i < list.Length; i++)
            {
                var bytes = Encoding.UTF8.GetBytes(list[i]);
                var result = skipList.TryFind(bytes, out var data);
                if (result != CharlotteDB.Core.SearchResult.Found)
                {
                    throw new InvalidOperationException();
                }
                if (data.Length != bytes.Length)
                {
                    throw new InvalidOperationException();
                }
            }

            var totalCount = skipList.Count;
            Console.Write($"time = {sw.ElapsedMilliseconds}");
        }
    }
}
