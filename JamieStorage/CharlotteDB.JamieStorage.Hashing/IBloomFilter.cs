using System;
using System.IO;
using System.Threading.Tasks;

namespace CharlotteDB.JamieStorage.Hashing
{
    public interface IBloomFilter
    {
        long Count { get; }
        double FalsePositiveErrorRate { get; }
        int OutputSize { get; }
        Task SaveAsync(Stream outputStream);
        void Add(Span<byte> buffer);
        bool PossiblyContains(Span<byte> buffer);
    }
}
