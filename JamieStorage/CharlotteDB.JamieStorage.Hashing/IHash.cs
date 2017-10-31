using System;

namespace CharlotteDB.JamieStorage.Hashing
{
    public interface IHash
    {
        ulong Hash(Span<byte> buffer);
        ulong ReHash(Span<byte> buffer, ulong previousHash);
    }
}
