using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.InMemory.Internals;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;


namespace PeaceDatabase.Storage.InMemory.Indexing
{
    /// <summary>
    /// Построение/снятие индексов (Eq/Num/Tags/FullText) + извлечение плоских полей из JSON.
    /// </summary>
    internal static class Indexer
    {
        internal static void Index(DbState st, Document doc)
        {
            var id = doc.Id;
            var fields = ExtractFlatFields(doc);

            // Eq
            foreach (var (field, val) in fields.Eq)
            {
                if (!st.EqIndex.TryGetValue(field, out var byVal))
                    st.EqIndex[field] = byVal = new Dictionary<string, HashSet<string>>(System.StringComparer.Ordinal);
                if (!byVal.TryGetValue(val, out var set))
                    byVal[val] = set = new HashSet<string>(System.StringComparer.Ordinal);
                set.Add(id);
            }

            // Num
            foreach (var (field, dval) in fields.Num)
            {
                if (!st.NumIndex.TryGetValue(field, out var tree))
                    st.NumIndex[field] = tree = new SortedDictionary<double, HashSet<string>>();
                if (!tree.TryGetValue(dval, out var set))
                    tree[dval] = set = new HashSet<string>(System.StringComparer.Ordinal);
                set.Add(id);
            }

            // Tags
            if (doc.Tags != null)
            {
                foreach (var tag in doc.Tags.Where(t => !string.IsNullOrWhiteSpace(t)))
                {
                    var key = tag.Trim();
                    if (!st.TagIndex.TryGetValue(key, out var set))
                        st.TagIndex[key] = set = new HashSet<string>(System.StringComparer.Ordinal);
                    set.Add(id);
                }
            }

            // FullText: Content + все string-поля
            foreach (var s in fields.StringsForFulltext)
                foreach (var tok in FullTextTokenizer.Tokenize(s))
                    AddToken(st, tok, id);

            if (!string.IsNullOrWhiteSpace(doc.Content))
                foreach (var tok in FullTextTokenizer.Tokenize(doc.Content))
                    AddToken(st, tok, id);
        }

        internal static void Unindex(DbState st, Document doc)
        {
            var id = doc.Id;
            var fields = ExtractFlatFields(doc);

            foreach (var (field, val) in fields.Eq)
                RemoveFromEq(st, field, val, id);

            foreach (var (field, dval) in fields.Num)
                RemoveFromNum(st, field, dval, id);

            if (doc.Tags != null)
            {
                foreach (var tag in doc.Tags.Where(t => !string.IsNullOrWhiteSpace(t)))
                    RemoveFromTag(st, tag.Trim(), id);
            }

            foreach (var s in fields.StringsForFulltext)
                foreach (var tok in FullTextTokenizer.Tokenize(s))
                    RemoveToken(st, tok, id);

            if (!string.IsNullOrWhiteSpace(doc.Content))
                foreach (var tok in FullTextTokenizer.Tokenize(doc.Content))
                    RemoveToken(st, tok, id);
        }

        internal static FlatFields ExtractFlatFields(Document doc)
        {
            var res = new FlatFields();

            void scanElement(string prefix, JsonElement el)
            {
                switch (el.ValueKind)
                {
                    case JsonValueKind.String:
                        var s = el.GetString() ?? "";
                        res.Eq[prefix] = s;
                        res.StringsForFulltext.Add(s);
                        break;
                    case JsonValueKind.Number:
                        if (el.TryGetDouble(out var d)) res.Num[prefix] = d;
                        break;
                    case JsonValueKind.True:
                    case JsonValueKind.False:
                        res.Eq[prefix] = el.GetBoolean() ? "true" : "false";
                        break;
                    case JsonValueKind.Object:
                        foreach (var prop in el.EnumerateObject())
                            scanElement($"{prefix}.{prop.Name}", prop.Value);
                        break;
                    case JsonValueKind.Array:
                        int i = 0;
                        foreach (var item in el.EnumerateArray())
                        {
                            scanElement($"{prefix}[{i}]", item);
                            i++;
                        }
                        break;
                }
            }

            var json = JsonSerializer.Serialize(doc, JsonUtil.JsonOpts);
            using var jd = JsonDocument.Parse(json);
            var root = jd.RootElement;

            if (root.TryGetProperty("Data", out var data)) scanElement("data", data);
            if (root.TryGetProperty("Tags", out var tags)) scanElement("tags", tags);
            if (root.TryGetProperty("Content", out var content)) scanElement("content", content);

            // Плоские верхнеуровневые поля (кроме служебных):
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.Name is "Id" or "Rev" or "Deleted" or "Data" or "Tags" or "Content") continue;
                scanElement(prop.Name, prop.Value);
            }

            return res;
        }

        // --- helpers ---

        internal static bool TryToDouble(object val, out double num)
            {
                switch (val)
                {
                    case JsonElement je:
                        switch (je.ValueKind)
                        {
                            case JsonValueKind.Number:
                                if (je.TryGetDouble(out num)) return true;
                                break;
                            case JsonValueKind.String:
                                if (double.TryParse(je.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out num))
                                    return true;
                                break;
                        }
                        num = 0; return false;

                    case byte b: num = b; return true;
                    case sbyte sb: num = sb; return true;
                    case short s: num = s; return true;
                    case ushort us: num = us; return true;
                    case int i: num = i; return true;
                    case uint ui: num = ui; return true;
                    case long l: num = l; return true;
                    case ulong ul: num = ul; return true;
                    case float f: num = f; return true;
                    case double d: num = d; return true;
                    case decimal dec: num = (double)dec; return true;
                    case string str when double.TryParse(str, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed):
                        num = parsed; return true;
                    default:
                        num = 0; return false;
                }
            }

        internal static string IndexerString(object val)
        {
            return val switch
            {
                null => string.Empty,
                JsonElement je => je.ValueKind switch
                {
                    JsonValueKind.String => je.GetString() ?? string.Empty,
                    JsonValueKind.Number => je.ToString(),
                    JsonValueKind.True => "true",
                    JsonValueKind.False => "false",
                    _ => je.ToString()
                },
                string s => s,
                bool b => b ? "true" : "false",
                IFormattable f => f.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty,
                _ => val.ToString() ?? string.Empty
            };
        }

        private static void AddToken(DbState st, string tok, string id)
            {
                if (!st.FullText.TryGetValue(tok, out var set))
                    st.FullText[tok] = set = new HashSet<string>(System.StringComparer.Ordinal);
                set.Add(id);
            }

        private static void RemoveToken(DbState st, string tok, string id)
        {
            if (st.FullText.TryGetValue(tok, out var set))
            {
                set.Remove(id);
                if (set.Count == 0) st.FullText.Remove(tok);
            }
        }

        private static void RemoveFromEq(DbState st, string field, string val, string id)
        {
            if (st.EqIndex.TryGetValue(field, out var byVal) &&
                byVal.TryGetValue(val, out var set))
            {
                set.Remove(id);
                if (set.Count == 0) byVal.Remove(val);
                if (byVal.Count == 0) st.EqIndex.Remove(field);
            }
        }

        private static void RemoveFromNum(DbState st, string field, double dval, string id)
        {
            if (st.NumIndex.TryGetValue(field, out var tree) &&
                tree.TryGetValue(dval, out var set))
            {
                set.Remove(id);
                if (set.Count == 0) tree.Remove(dval);
                if (tree.Count == 0) st.NumIndex.Remove(field);
            }
        }

        private static void RemoveFromTag(DbState st, string tag, string id)
        {
            if (st.TagIndex.TryGetValue(tag, out var set))
            {
                set.Remove(id);
                if (set.Count == 0) st.TagIndex.Remove(tag);
            }
        }
    }
}
