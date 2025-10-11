using Google.Protobuf.WellKnownTypes;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Protos;
using System.Collections.Generic;

namespace PeaceDatabase.Storage.Protobuf
{
    public static class ProtobufDocumentCodec
    {
        public static DocumentMessage ToProto(Document d)
        {
            var msg = new DocumentMessage
            {
                Id = d.Id ?? string.Empty,
                Rev = d.Rev ?? string.Empty,
                Deleted = d.Deleted,
                Content = d.Content ?? string.Empty
            };

            if (d.Tags != null)
                msg.Tags.AddRange(d.Tags);

            if (d.Data != null)
            {
                msg.Data = new Struct();
                foreach (var kv in d.Data)
                {
                    msg.Data.Fields[kv.Key] = ToValue(kv.Value);
                }
            }

            return msg;
        }

        public static Document FromProto(DocumentMessage m)
        {
            var doc = new Document
            {
                Id = m.Id,
                Rev = string.IsNullOrEmpty(m.Rev) ? null : m.Rev,
                Deleted = m.Deleted,
                Content = string.IsNullOrEmpty(m.Content) ? null : m.Content,
                Tags = m.Tags.Count > 0 ? new List<string>(m.Tags) : null,
                Data = m.Data != null ? StructToDict(m.Data) : null
            };
            return doc;
        }

        private static Value ToValue(object? o)
        {
            if (o is null) return Value.ForNull();
            return o switch
            {
                string s => Value.ForString(s),
                bool b => Value.ForBool(b),
                int i => Value.ForNumber(i),
                long l => Value.ForNumber(l),
                float f => Value.ForNumber(f),
                double d => Value.ForNumber(d),
                IEnumerable<string> list => new Value { ListValue = new ListValue { Values = { FromStrings(list) } } },
                Dictionary<string, object> dict => new Value { StructValue = DictToStruct(dict) },
                System.Text.Json.JsonElement je => JsonElementToValue(je),
                _ => Value.ForString(o.ToString() ?? string.Empty)
            };
        }

        private static IEnumerable<Value> FromStrings(IEnumerable<string> items)
        {
            foreach (var s in items) yield return Value.ForString(s);
        }

        private static Struct DictToStruct(Dictionary<string, object> dict)
        {
            var st = new Struct();
            foreach (var kv in dict)
            {
                st.Fields[kv.Key] = ToValue(kv.Value);
            }
            return st;
        }

        private static Dictionary<string, object> StructToDict(Struct st)
        {
            var d = new Dictionary<string, object>(System.StringComparer.Ordinal);
            foreach (var kv in st.Fields)
            {
                d[kv.Key] = FromValue(kv.Value);
            }
            return d;
        }

        private static object? FromValue(Value v)
        {
            switch (v.KindCase)
            {
                case Value.KindOneofCase.NullValue:
                    return null;
                case Value.KindOneofCase.StringValue:
                    return v.StringValue;
                case Value.KindOneofCase.BoolValue:
                    return v.BoolValue;
                case Value.KindOneofCase.NumberValue:
                    return v.NumberValue;
                case Value.KindOneofCase.ListValue:
                    var list = new List<string>();
                    foreach (var item in v.ListValue.Values)
                        list.Add(item.ToString());
                    return list;
                case Value.KindOneofCase.StructValue:
                    return StructToDict(v.StructValue);
                default:
                    return null;
            }
        }

        private static Value JsonElementToValue(System.Text.Json.JsonElement je)
        {
            switch (je.ValueKind)
            {
                case System.Text.Json.JsonValueKind.Null:
                case System.Text.Json.JsonValueKind.Undefined:
                    return Value.ForNull();
                case System.Text.Json.JsonValueKind.String:
                    return Value.ForString(je.GetString() ?? string.Empty);
                case System.Text.Json.JsonValueKind.True:
                    return Value.ForBool(true);
                case System.Text.Json.JsonValueKind.False:
                    return Value.ForBool(false);
                case System.Text.Json.JsonValueKind.Number:
                    return Value.ForNumber(je.GetDouble());
                case System.Text.Json.JsonValueKind.Array:
                {
                    var lv = new ListValue();
                    foreach (var it in je.EnumerateArray()) lv.Values.Add(JsonElementToValue(it));
                    return new Value { ListValue = lv };
                }
                case System.Text.Json.JsonValueKind.Object:
                {
                    var st = new Struct();
                    foreach (var prop in je.EnumerateObject()) st.Fields[prop.Name] = JsonElementToValue(prop.Value);
                    return new Value { StructValue = st };
                }
                default:
                    return Value.ForNull();
            }
        }
    }
}


