using System.Collections.Generic;
using System.Text.Json;

namespace PeaceDatabase.Core.Utils
{
    public static class JsonFlatten
    {
        // Flattens nested JSON objects/arrays into dotted keys
        public static Dictionary<string, object> Flatten(JsonElement root)
        {
            var result = new Dictionary<string, object>(System.StringComparer.Ordinal);
            Walk(root, prefix: "", result);
            return result;
        }

        private static void Walk(JsonElement el, string prefix, Dictionary<string, object> acc)
        {
            switch (el.ValueKind)
            {
                case JsonValueKind.Object:
                    foreach (var prop in el.EnumerateObject())
                    {
                        var next = string.IsNullOrEmpty(prefix) ? prop.Name : prefix + "." + prop.Name;
                        Walk(prop.Value, next, acc);
                    }
                    break;
                case JsonValueKind.Array:
                    int i = 0;
                    foreach (var item in el.EnumerateArray())
                    {
                        var next = string.IsNullOrEmpty(prefix) ? $"[{i}]" : prefix + $"[{i}]";
                        Walk(item, next, acc);
                        i++;
                    }
                    break;
                case JsonValueKind.String:
                    acc[prefix] = el.GetString() ?? string.Empty;
                    break;
                case JsonValueKind.Number:
                    if (el.TryGetDouble(out var d)) acc[prefix] = d; else acc[prefix] = el.ToString();
                    break;
                case JsonValueKind.True:
                case JsonValueKind.False:
                    acc[prefix] = el.GetBoolean();
                    break;
                case JsonValueKind.Null:
                case JsonValueKind.Undefined:
                    acc[prefix] = null!;
                    break;
            }
        }
    }
}


