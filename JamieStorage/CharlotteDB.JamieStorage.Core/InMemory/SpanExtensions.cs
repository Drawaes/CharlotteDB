using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace CharlotteDB.JamieStorage.Core.InMemory
{
    internal static class SpanExtensions
    {
        public static unsafe Span<byte> Write<T>(this Span<byte> inputSpan, T value)
            where T : struct
        {
            var size = Unsafe.SizeOf<T>();
            
            fixed(void* ptr = &inputSpan.DangerousGetPinnableReference())
            {
                Unsafe.Copy(ptr, ref value);
            }

            return inputSpan.Slice(size);
        }

        public static unsafe Span<byte> Read<T>(this Span<byte> inputSpan, out T value) where T : struct
        {
            var size = Unsafe.SizeOf<T>();

            fixed (void* ptr = &inputSpan.DangerousGetPinnableReference())
            {
                value = Unsafe.Read<T>(ptr);
            }
            return inputSpan.Slice(size);
        }
    }
}
