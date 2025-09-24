using System;
using System.Security.Cryptography;
using System.Text;

namespace PeaceDatabase.Storage.InMemory.Internals
{
    internal static class HashUtil
    {
        internal static string NewId() => Guid.NewGuid().ToString("N");

        internal static string Sha1Hex(byte[] bytes)
        {
            using var sha1 = SHA1.Create();
            var hash = sha1.ComputeHash(bytes);
            var sb = new StringBuilder(hash.Length * 2);
            foreach (var b in hash) sb.Append(b.ToString("x2"));
            return sb.ToString();
        }

        /// <summary>
        ///  N-hash ревизии: {seq}-{sha1(body)}, где seq растёт на 1 от предыдущей ревизии.
        /// </summary>
        internal static string NextRev(string? prevRev, byte[] bodyBytes)
        {
            int n = 0;
            if (!string.IsNullOrEmpty(prevRev))
            {
                var dash = prevRev!.IndexOf('-');
                if (dash > 0 && int.TryParse(prevRev.AsSpan(0, dash), out var parsed))
                    n = parsed;
            }
            var hash = Sha1Hex(bodyBytes);
            return $"{n + 1}-{hash}";
        }
    }
}
