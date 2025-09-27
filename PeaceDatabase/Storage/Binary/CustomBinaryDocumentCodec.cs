// File: Storage/Binary/CustomBinaryDocumentCodec.cs
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using PeaceDatabase.Core.Models;

namespace PeaceDatabase.Storage.Binary
{
    /// <summary>
    /// Собственный бинарный формат сериализации Document (PeaceDBDocument v1, порядок записи Type-Length-Value).
    /// </summary>
    public static class CustomBinaryDocumentCodec
    {
        // Field IDs
        private const byte VersionField = 1;
        private const byte IdField = 2;
        private const byte RevField = 3;
        private const byte DeletedField = 4;
        private const byte DataField = 5;
        private const byte TagsField = 6;
        private const byte ContentField = 7;
        private const byte FormatVersion = 1;

        public static byte[] Serialize(Document doc)
        {
            using var ms = new MemoryStream();
            using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

            // Version
            bw.Write(VersionField);     // type
            bw.Write(1);                // length
            bw.Write(FormatVersion);    // value

            // Id
            if (!string.IsNullOrEmpty(doc.Id))
                WriteStringTLV(bw, IdField, doc.Id);
            // Rev
            if (!string.IsNullOrEmpty(doc.Rev))
                WriteStringTLV(bw, RevField, doc.Rev);
            // Deleted
            bw.Write(DeletedField);
            bw.Write(1);
            bw.Write(doc.Deleted ? (byte)1 : (byte)0);
            // Data
            if (doc.Data != null && doc.Data.Count > 0)
            {
                var dataBytes = SerializeData(doc.Data);
                bw.Write(DataField);
                bw.Write(dataBytes.Length);
                bw.Write(dataBytes);
            }
            // Tags
            if (doc.Tags != null && doc.Tags.Count > 0)
            {
                using var tagsMs = new MemoryStream();
                using var tagsBw = new BinaryWriter(tagsMs, Encoding.UTF8, leaveOpen: true);
                tagsBw.Write(doc.Tags.Count);
                foreach (var tag in doc.Tags)
                    WriteString(tagsBw, tag);
                tagsBw.Flush();
                var tagsBytes = tagsMs.ToArray();
                bw.Write(TagsField);
                bw.Write(tagsBytes.Length);
                bw.Write(tagsBytes);
            }
            // Content
            if (!string.IsNullOrEmpty(doc.Content))
                WriteStringTLV(bw, ContentField, doc.Content);

            bw.Flush();
            return ms.ToArray();
        }

        public static Document Deserialize(byte[] data)
        {
            using var ms = new MemoryStream(data);
            using var br = new BinaryReader(ms, Encoding.UTF8, leaveOpen: true);
            var doc = new Document();
            while (ms.Position < ms.Length)
            {
                var fieldId = br.ReadByte();
                var len = br.ReadInt32();
                var fieldStart = ms.Position;
                switch (fieldId)
                {
                    case VersionField:
                        var ver = br.ReadByte();
                        if (ver != FormatVersion)
                            throw new NotSupportedException($"Unsupported format version: {ver}");
                        break;
                    case IdField:
                        doc.Id = ReadString(br, len);
                        break;
                    case RevField:
                        doc.Rev = ReadString(br, len);
                        break;
                    case DeletedField:
                        doc.Deleted = br.ReadByte() != 0;
                        break;
                    case DataField:
                        doc.Data = DeserializeData(br.ReadBytes(len));
                        break;
                    case TagsField:
                        using (var tagsMs = new MemoryStream(br.ReadBytes(len)))
                        using (var tagsBr = new BinaryReader(tagsMs, Encoding.UTF8))
                        {
                            var count = tagsBr.ReadInt32();
                            var tags = new List<string>(count);
                            for (int i = 0; i < count; i++)
                                tags.Add(ReadString(tagsBr));
                            doc.Tags = tags;
                        }
                        break;
                    case ContentField:
                        doc.Content = ReadString(br, len);
                        break;
                    default:
                        // skip unknown field
                        ms.Position = fieldStart + len;
                        break;
                }
            }
            return doc;
        }

        // --- Helpers ---
        private static void WriteStringTLV(BinaryWriter bw, byte fieldId, string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            bw.Write(fieldId);
            bw.Write(bytes.Length);
            bw.Write(bytes);
        }
        private static void WriteString(BinaryWriter bw, string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            bw.Write(bytes.Length);
            bw.Write(bytes);
        }
        private static string ReadString(BinaryReader br, int len)
        {
            var bytes = br.ReadBytes(len);
            return Encoding.UTF8.GetString(bytes);
        }
        private static string ReadString(BinaryReader br)
        {
            var len = br.ReadInt32();
            var bytes = br.ReadBytes(len);
            return Encoding.UTF8.GetString(bytes);
        }

        // --- Data (Dictionary<string, object>) ---
        // Поддерживаем string, int, double, bool, null, list<string>, вложенные dict<string, object>
        private enum DataType : byte
        {
            Null = 0,
            String = 1,
            Int = 2,
            Double = 3,
            Bool = 4,
            ListString = 5,
            Dict = 6
        }
        private static byte[] SerializeData(Dictionary<string, object> data)
        {
            using var ms = new MemoryStream();
            using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);
            bw.Write(data.Count);
            foreach (var kv in data)
            {
                WriteString(bw, kv.Key);
                WriteDataValue(bw, kv.Value);
            }
            bw.Flush();
            return ms.ToArray();
        }
        private static Dictionary<string, object> DeserializeData(byte[] bytes)
        {
            var dict = new Dictionary<string, object>();
            using var ms = new MemoryStream(bytes);
            using var br = new BinaryReader(ms, Encoding.UTF8);
            var count = br.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                var key = ReadString(br);
                var type = (DataType)br.ReadByte();
                object? value = type switch
                {
                    DataType.Null => null,
                    DataType.String => ReadString(br),
                    DataType.Int => br.ReadInt32(),
                    DataType.Double => br.ReadDouble(),
                    DataType.Bool => br.ReadByte() != 0,
                    DataType.ListString =>
                        ReadStringList(br),
                    DataType.Dict =>
                        DeserializeData(br.ReadBytes(br.ReadInt32())),
                    _ => throw new NotSupportedException($"Unknown DataType: {type}")
                };
                dict[key] = value!;
            }
            return dict;
        }
        private static void WriteDataValue(BinaryWriter bw, object? value)
        {
            // Новая нормализация JsonElement -> CLR
            if (value is System.Text.Json.JsonElement je)
                value = JsonElementToClr(je);

            switch (value)
            {
                case null:
                    bw.Write((byte)DataType.Null);
                    break;
                case string s:
                    bw.Write((byte)DataType.String);
                    WriteString(bw, s);
                    break;
                case int i:
                    bw.Write((byte)DataType.Int);
                    bw.Write(i);
                    break;
                case long l:
                    bw.Write((byte)DataType.Int);
                    bw.Write(checked((int)l));
                    break;
                case double d:
                    bw.Write((byte)DataType.Double);
                    bw.Write(d);
                    break;
                case float f:
                    bw.Write((byte)DataType.Double);
                    bw.Write((double)f);
                    break;
                case bool b:
                    bw.Write((byte)DataType.Bool);
                    bw.Write(b ? (byte)1 : (byte)0);
                    break;
                case IEnumerable<string> list:
                    bw.Write((byte)DataType.ListString);
                    var arr = new List<string>(list);
                    bw.Write(arr.Count);
                    foreach (var s in arr)
                        WriteString(bw, s);
                    break;
                case Dictionary<string, object> dict:
                    bw.Write((byte)DataType.Dict);
                    var dictBytes = SerializeData(dict);
                    bw.Write(dictBytes.Length);
                    bw.Write(dictBytes);
                    break;
                default:
                    throw new NotSupportedException($"Unsupported data type: {value.GetType().Name}");
            }
        }

        // Рекурсивная конвертация JsonElement в поддерживаемые типы
        private static object? JsonElementToClr(System.Text.Json.JsonElement je)
        {
            switch (je.ValueKind)
            {
                case System.Text.Json.JsonValueKind.Null:
                case System.Text.Json.JsonValueKind.Undefined:
                    return null;
                case System.Text.Json.JsonValueKind.String:
                    return je.GetString();
                case System.Text.Json.JsonValueKind.Number:
                    // Пробуем int -> затем long -> double
                    if (je.TryGetInt32(out var vi)) return vi;
                    if (je.TryGetInt64(out var vl)) return vl; // потом сузим до int в switch
                    return je.GetDouble();
                case System.Text.Json.JsonValueKind.True:
                    return true;
                case System.Text.Json.JsonValueKind.False:
                    return false;
                case System.Text.Json.JsonValueKind.Array:
                {
                    // Поддерживаем только массив строк => ListString
                    var list = new List<string>();
                    foreach (var el in je.EnumerateArray())
                    {
                        if (el.ValueKind == System.Text.Json.JsonValueKind.String)
                            list.Add(el.GetString()!);
                        else
                            throw new NotSupportedException("Only arrays of strings are supported in Data");
                    }
                    return list;
                }
                case System.Text.Json.JsonValueKind.Object:
                {
                    var dict = new Dictionary<string, object>(StringComparer.Ordinal);
                    foreach (var prop in je.EnumerateObject())
                    {
                        var v = JsonElementToClr(prop.Value);
                        dict[prop.Name] = v!;
                    }
                    return dict;
                }
                default:
                    throw new NotSupportedException($"Unsupported JsonElement kind: {je.ValueKind}");
            }
        }

        private static List<string> ReadStringList(BinaryReader br)
        {
            var count = br.ReadInt32();
            var list = new List<string>(count);
            for (int i = 0; i < count; i++)
                list.Add(ReadString(br));
            return list;
        }
    }
}
