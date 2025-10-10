// File: Storage/Binary/CustomBinaryDocumentCodec.cs
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using PeaceDatabase.Core.Models;

namespace PeaceDatabase.Storage.Binary
{
    /// <summary>
    /// Собственный бинарный формат сериализации Document (PeaceDBDocument v1, порядок записи Type-Length-Value).
    /// Все числовые значения пишутся в little-endian вне зависимости от архитектуры.
    /// Строки кодируются строгим UTF-8 (без BOM), при некорректных байтах генерируется исключение.
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

        // Строгий UTF-8: без BOM, выбрасывает исключение на некорректных последовательностях
        private static readonly Encoding Utf8NoBomStrict = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

        public static byte[] Serialize(Document doc)
        {
            using var ms = new MemoryStream();

            // Version (TLV)
            WriteByte(ms, VersionField);     // type
            WriteInt32LE(ms, 1);             // length
            WriteByte(ms, FormatVersion);    // value

            // Id
            if (!string.IsNullOrEmpty(doc.Id))
                WriteStringTLV(ms, IdField, doc.Id);
            // Rev
            if (!string.IsNullOrEmpty(doc.Rev))
                WriteStringTLV(ms, RevField, doc.Rev);
            // Deleted
            WriteByte(ms, DeletedField);
            WriteInt32LE(ms, 1);
            WriteByte(ms, doc.Deleted ? (byte)1 : (byte)0);
            // Data
            if (doc.Data is { Count: > 0 })
            {
                var dataBytes = SerializeData(doc.Data);
                WriteByte(ms, DataField);
                WriteInt32LE(ms, dataBytes.Length);
                ms.Write(dataBytes, 0, dataBytes.Length);
            }
            // Tags
            if (doc.Tags is { Count: > 0 })
            {
                using var tagsMs = new MemoryStream();
                WriteInt32LE(tagsMs, doc.Tags.Count);
                foreach (var tag in doc.Tags)
                    WriteString(tagsMs, tag);
                var tagsBytes = tagsMs.ToArray();
                WriteByte(ms, TagsField);
                WriteInt32LE(ms, tagsBytes.Length);
                ms.Write(tagsBytes, 0, tagsBytes.Length);
            }
            // Content
            if (!string.IsNullOrEmpty(doc.Content))
                WriteStringTLV(ms, ContentField, doc.Content);

            return ms.ToArray();
        }

        public static Document Deserialize(byte[] data)
        {
            using var ms = new MemoryStream(data, writable: false);
            var doc = new Document();
            while (ms.Position < ms.Length)
            {
                var fieldId = ReadByte(ms);
                var len = ReadInt32LE(ms);
                var fieldStart = ms.Position;
                switch (fieldId)
                {
                    case VersionField:
                        {
                            var ver = ReadByte(ms);
                            if (ver != FormatVersion)
                                throw new NotSupportedException($"Unsupported format version: {ver}");
                            break;
                        }
                    case IdField:
                        doc.Id = ReadString(ms, len);
                        break;
                    case RevField:
                        doc.Rev = ReadString(ms, len);
                        break;
                    case DeletedField:
                        doc.Deleted = ReadByte(ms) != 0;
                        break;
                    case DataField:
                        {
                            var payload = ReadExact(ms, len);
                            doc.Data = DeserializeData(payload);
                            break;
                        }
                    case TagsField:
                        {
                            using var tagsMs = new MemoryStream(ReadExact(ms, len), writable: false);
                            var count = ReadInt32LE(tagsMs);
                            var tags = new List<string>(count);
                            for (int i = 0; i < count; i++)
                                tags.Add(ReadString(tagsMs));
                            doc.Tags = tags;
                            break;
                        }
                    case ContentField:
                        doc.Content = ReadString(ms, len);
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
        private static void WriteStringTLV(Stream s, byte fieldId, string value)
        {
            var bytes = Utf8NoBomStrict.GetBytes(value);
            WriteByte(s, fieldId);
            WriteInt32LE(s, bytes.Length);
            s.Write(bytes, 0, bytes.Length);
        }
        private static void WriteString(Stream s, string value)
        {
            var bytes = Utf8NoBomStrict.GetBytes(value);
            WriteInt32LE(s, bytes.Length);
            s.Write(bytes, 0, bytes.Length);
        }
        private static string ReadString(Stream s, int len)
        {
            var bytes = ReadExact(s, len);
            return Utf8NoBomStrict.GetString(bytes);
        }
        private static string ReadString(Stream s)
        {
            var len = ReadInt32LE(s);
            var bytes = ReadExact(s, len);
            return Utf8NoBomStrict.GetString(bytes);
        }

        private static void WriteByte(Stream s, byte b) => s.WriteByte(b);
        private static byte ReadByte(Stream s)
        {
            int b = s.ReadByte();
            if (b < 0) throw new EndOfStreamException();
            return (byte)b;
        }
        private static void WriteInt32LE(Stream s, int value)
        {
            Span<byte> buf = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(buf, value);
            s.Write(buf);
        }
        private static int ReadInt32LE(Stream s)
        {
            Span<byte> buf = stackalloc byte[4];
            ReadExact(s, buf);
            return BinaryPrimitives.ReadInt32LittleEndian(buf);
        }
        private static void WriteInt64LE(Stream s, long value)
        {
            Span<byte> buf = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(buf, value);
            s.Write(buf);
        }
        private static long ReadInt64LE(Stream s)
        {
            Span<byte> buf = stackalloc byte[8];
            ReadExact(s, buf);
            return BinaryPrimitives.ReadInt64LittleEndian(buf);
        }
        private static void WriteDoubleLE(Stream s, double value)
        {
            long bits = BitConverter.DoubleToInt64Bits(value);
            WriteInt64LE(s, bits);
        }
        private static double ReadDoubleLE(Stream s)
        {
            long bits = ReadInt64LE(s);
            return BitConverter.Int64BitsToDouble(bits);
        }
        private static byte[] ReadExact(Stream s, int len)
        {
            if (len < 0) throw new IOException("Negative length");
            var buffer = new byte[len];
            int read = 0;
            while (read < len)
            {
                int r = s.Read(buffer, read, len - read);
                if (r <= 0) throw new EndOfStreamException();
                read += r;
            }
            return buffer;
        }
        private static void ReadExact(Stream s, Span<byte> buffer)
        {
            int readTotal = 0;
            while (readTotal < buffer.Length)
            {
                int r = s.Read(buffer.Slice(readTotal));
                if (r <= 0) throw new EndOfStreamException();
                readTotal += r;
            }
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
            WriteInt32LE(ms, data.Count);
            foreach (var kv in data)
            {
                WriteString(ms, kv.Key);
                WriteDataValue(ms, kv.Value);
            }
            return ms.ToArray();
        }
        private static Dictionary<string, object> DeserializeData(byte[] bytes)
        {
            var dict = new Dictionary<string, object>();
            using var ms = new MemoryStream(bytes, writable: false);
            var count = ReadInt32LE(ms);
            for (int i = 0; i < count; i++)
            {
                var key = ReadString(ms);
                var type = (DataType)ReadByte(ms);
                object? value = type switch
                {
                    DataType.Null => null,
                    DataType.String => ReadString(ms),
                    DataType.Int => ReadInt32LE(ms),
                    DataType.Double => ReadDoubleLE(ms),
                    DataType.Bool => ReadByte(ms) != 0,
                    DataType.ListString => ReadStringList(ms),
                    DataType.Dict => DeserializeData(ReadNested(ms)),
                    _ => throw new NotSupportedException($"Unknown DataType: {type}")
                };
                dict[key] = value!;
            }
            return dict;
        }
        private static void WriteDataValue(Stream s, object? value)
        {
            // Новая нормализация JsonElement -> CLR
            if (value is System.Text.Json.JsonElement je)
                value = JsonElementToClr(je);

            switch (value)
            {
                case null:
                    WriteByte(s, (byte)DataType.Null);
                    break;
                case string str:
                    WriteByte(s, (byte)DataType.String);
                    WriteString(s, str);
                    break;
                case int i32:
                    WriteByte(s, (byte)DataType.Int);
                    WriteInt32LE(s, i32);
                    break;
                case long i64:
                    WriteByte(s, (byte)DataType.Int);
                    WriteInt32LE(s, checked((int)i64));
                    break;
                case double d:
                    WriteByte(s, (byte)DataType.Double);
                    WriteDoubleLE(s, d);
                    break;
                case float f:
                    WriteByte(s, (byte)DataType.Double);
                    WriteDoubleLE(s, (double)f);
                    break;
                case bool b:
                    WriteByte(s, (byte)DataType.Bool);
                    WriteByte(s, b ? (byte)1 : (byte)0);
                    break;
                case IEnumerable<string> list:
                    {
                        WriteByte(s, (byte)DataType.ListString);
                        var arr = new List<string>(list);
                        WriteInt32LE(s, arr.Count);
                        foreach (var item in arr)
                            WriteString(s, item);
                        break;
                    }
                case Dictionary<string, object> dict:
                    {
                        WriteByte(s, (byte)DataType.Dict);
                        var dictBytes = SerializeData(dict);
                        WriteInt32LE(s, dictBytes.Length);
                        s.Write(dictBytes, 0, dictBytes.Length);
                        break;
                    }
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

        private static List<string> ReadStringList(Stream s)
        {
            var count = ReadInt32LE(s);
            var list = new List<string>(count);
            for (int i = 0; i < count; i++)
                list.Add(ReadString(s));
            return list;
        }

        private static byte[] ReadNested(Stream s)
        {
            var nestedLen = ReadInt32LE(s);
            return ReadExact(s, nestedLen);
        }
    }
}
