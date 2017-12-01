using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CharlotteDB.Core.Allocation;
using CharlotteDB.Core.Keys;
using CharlotteDB.JamieStorage.Core;
using CharlotteDB.JamieStorage.InMemory;
using Microsoft.Extensions.Logging;

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
            var sw = Stopwatch.StartNew();
            //var ignore = TestDB();
            TestSkipList();
            Console.WriteLine("Waiting to complete");
            _event.WaitOne();
            Console.WriteLine("Time taken " + sw.ElapsedMilliseconds);
        }

        private static async Task TestDB()
        {
            var loggerFactory = new LoggerFactory();
            //loggerFactory.AddConsole();
            var logger = loggerFactory.CreateLogger<Program>();

            var outputList = new List<(bool deleted, Memory<byte> bytes)>();
            using (var database = Database.Create<ByteByByteComparer, SkipList2>("c:\\code\\database", new ByteByByteComparer(), new DummyAllocator(50 * 1024), loggerFactory))
            {
                var list = System.IO.File.ReadAllLines("C:\\code\\words.txt");
                var rnd = new Random(7777);

                logger.LogInformation("Adding records");
                for (var i = 0; i < list.Length; i++)
                {
                    var l = list[i];
                    var bytes = l.ToCharArray();
                    var span = new Memory<byte>(bytes.AsSpan().NonPortableCast<char, byte>().ToArray());

                    await database.PutAsync(span, span);
                    if (rnd.NextDouble() < 0.05)
                    {
                        outputList.Add((true, span));
                    }
                    else
                    {
                        outputList.Add((false, span));
                    }
                }
                logger.LogInformation("Information all logged");

                //database.WriteDebugSkipList("C:\\code\\database\\");

                await database.FlushToDisk();

                logger.LogInformation("Removing Records");
                for (var i = 0; i < outputList.Count; i++)
                {
                    var (d, b) = outputList[i];
                    if (d == true)
                    {
                        await database.TryRemoveAsync(b);
                    }
                }
                logger.LogInformation("Finished deleting records");

                await database.FlushToDisk();

                var notFound = 0;
                var foundDeleted = 0;
                for (var i = 0; i < outputList.Count; i++)
                {
                    var (deleted, bytes) = outputList[i];

                    var (found, data) = database.TryGetData(bytes);
                    if ((!deleted && !found))
                    {
                        logger.LogError("Not found total {count} ----- {notFound}", ++notFound, list[i]);
                    }
                    else if ((deleted && found))
                    {
                        logger.LogError("Found a deleted record {count} {foundItem}", ++foundDeleted, list[i]);
                    }
                    else if ((!deleted) && !data.Span.SequenceEqual(bytes.Span))
                    {
                        throw new InvalidOperationException();
                    }
                    if (i % 20000 == 0)
                    {
                        logger.LogInformation("Found {count} items still {remaining} to go", i + 1, outputList.Count - i);
                    }
                }
            }
            _event.Set();
        }

        private static void TestSkipList()
        {
            var comparer = new ByteByByteComparer();
            var allocator = new DummyAllocator(1024 * 1024);
            var skipList = new SkipList2();
            skipList.Init(allocator, comparer);

            var sortList = new InMemorySortedList();
            sortList.Init(allocator, comparer);

            var list = System.IO.File.ReadAllLines("C:\\code\\output.txt");

            var sw = new Stopwatch();
            sw.Start();
            var rnd = new Random(7777);
            for (var i = 0; i < list.Length; i++)
            {
                var l = list[i];
                var bytes = Encoding.UTF8.GetBytes(l);
                var span = new Span<byte>(bytes);

                skipList.Insert(span, span);
                sortList.Insert(span, span);
            }

            for (var i = 0; i < list.Length; i++)
            {
                if (rnd.NextDouble() < 0.10)
                {
                    var l = list[i];
                    var bytes = Encoding.UTF8.GetBytes(l);
                    var span = new Span<byte>(bytes);
                    skipList.Remove(span);
                    sortList.Remove(span);
                }
            }

            sortList.Reset();
            skipList.Reset();

            while (sortList.Next())
            {
                skipList.Next();

                var node1 = sortList.CurrentNode;
                var node2 = skipList.CurrentNode;

                if (node1.State != node2.State)
                {
                    throw new NotImplementedException();
                }

                if (!node1.Key.Span.SequenceEqual(node2.Key.Span))
                {
                    throw new NotImplementedException();
                }
                if (node1.State == ItemState.Alive && !node1.Data.Span.SequenceEqual(node2.Data.Span))
                {
                    throw new NotImplementedException();
                }
            }
        }
    }
}
