using System;
using System.Collections.Generic;
using System.Text;

namespace CharlotteDB.JamieStorage.Hashing
{
    public class FNV1Hash : IHash
    {
        private ulong FNV_64_PRIME = 0x100000001b3UL;
        private ulong InitalValue = 0x84222325_cbf29ce4UL;

        public ulong Hash(Span<byte> buffer) => ReHash(buffer, InitalValue);

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
