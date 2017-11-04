using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace CharlotteDB.Core
{
    public static class SpanExtensions
    {
        public static unsafe Span<byte> WriteAdvance<T>(this Span<byte> inputSpan, T value) where T : struct
        {
            var size = Unsafe.SizeOf<T>();
            var returnSpan = inputSpan.Slice(size);
            fixed (void* ptr = &inputSpan.DangerousGetPinnableReference())
            {
                Unsafe.Copy(ptr, ref value);
            }

            return returnSpan;
        }

        public static unsafe Span<byte> ReadAdvance<T>(this Span<byte> inputSpan, out T value) where T : struct
        {
            var size = Unsafe.SizeOf<T>();
            var returnSpan = inputSpan.Slice(size);
            value = Unsafe.As<byte, T>(ref inputSpan.DangerousGetPinnableReference());
            return returnSpan;
        }

        public static ref T Read<T>(this Span<byte> inputSpan) where T :struct
        {
            var size = Unsafe.SizeOf<T>();
            if(size > inputSpan.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(inputSpan));
            }

            return ref Unsafe.As<byte, T>(ref inputSpan.DangerousGetPinnableReference());
        }
    }
}
