using System.Text;
using System.Text.Json;
using PeaceDatabase.Core.Models;

namespace PeaceDatabase.Storage.InMemory.Internals
{
    internal static class JsonUtil
    {
        internal static readonly JsonSerializerOptions JsonOpts = new()
        {
            WriteIndented = false,
            PropertyNamingPolicy = null,
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.Never
        };

        internal static Document DeepClone(Document doc)
        {
            var json = JsonSerializer.Serialize(doc, JsonOpts);
            return JsonSerializer.Deserialize<Document>(json, JsonOpts)!;
        }

        /// <summary>
        /// Канонический снимок тела для хеширования ревизии (без Id/Rev).
        /// </summary>
        internal static byte[] CanonicalBodyBytes(Document doc)
        {
            var clone = DeepClone(doc);
            clone.Id = string.Empty;
            clone.Rev = null;
            var json = JsonSerializer.Serialize(clone, JsonOpts);
            return Encoding.UTF8.GetBytes(json);
        }
    }
}
