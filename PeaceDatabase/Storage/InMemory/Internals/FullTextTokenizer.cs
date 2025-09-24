using System.Collections.Generic;
using System.Text;

namespace PeaceDatabase.Storage.InMemory.Indexing
{
    /// <summary>
    /// Простой токенизатор: латиница/цифры, в нижнем регистре, токены длиной ≥ 2.
    /// </summary>
    internal static class FullTextTokenizer
    {
        internal static IEnumerable<string> Tokenize(string text)
        {
            if (string.IsNullOrWhiteSpace(text)) yield break;

            var s = text.ToLowerInvariant();
            var sb = new StringBuilder();
            foreach (var ch in s)
            {
                if (char.IsLetterOrDigit(ch)) sb.Append(ch);
                else { if (sb.Length > 1) { yield return sb.ToString(); } sb.Clear(); }
            }
            if (sb.Length > 1) yield return sb.ToString();
        }
    }
}
