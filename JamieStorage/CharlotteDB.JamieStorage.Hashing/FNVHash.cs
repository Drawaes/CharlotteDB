using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Hashing
{
    public class FNVHash : IHash
    {
        private ulong FNV_64_PRIME = 0x100000001b3UL;

        public ulong Hash(Span<byte> buffer) => ReHash(buffer, 0UL);

        public ulong ReHash(Span<byte> buffer, ulong previousHash)
        {
            for (var i = 0; i < buffer.Length; i++)
            {
                previousHash *= FNV_64_PRIME;
                previousHash ^= buffer[i];
            }
            return previousHash;
        }
    }
}
