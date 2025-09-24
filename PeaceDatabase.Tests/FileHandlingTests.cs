using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.InMemory;
using Xunit;

namespace PeaceDatabase.Tests.Storage
{
    public class FileHandlingTests
    {
        private readonly IDocumentService _svc;

        public FileHandlingTests()
        {
            _svc = new InMemoryDocumentService();
            _svc.CreateDb("files");
        }

        [Fact]
        public async Task Content_From_File_Should_Be_Searchable_FullText()
        {
            // создаём временный файл и пишем контент
            var tmp = Path.GetTempFileName();
            await File.WriteAllTextAsync(tmp, "Ghibli studio vibes. Cinematic light. Testing fulltext index.");

            try
            {
                var text = await File.ReadAllTextAsync(tmp);

                // Добавление документа с контентом из файла
                var post = _svc.Post("files", new Document
                {
                    Data = new Dictionary<string, object> { ["type"] = "article", ["size"] = text.Length },
                    Tags = new List<string> { "file", "text" },
                    Content = text
                });

                post.Ok.Should().BeTrue();
                var id = post.Doc!.Id;

                // Чтение
                var got = _svc.Get("files", id);
                got.Should().NotBeNull();
                got!.Content.Should().Contain("Cinematic light");

                // Полнотекст: ключевое слово из файла
                var ft = _svc.FullTextSearch("files", "ghibli cinematic").Should().ContainSingle(d => d.Id == id);

                // Изменение (повторно читаем файл -> правим контент → обновляем)
                var rev = got.Rev!;
                var newText = text + " Extra ending line.";
                var put = _svc.Put("files", new Document
                {
                    Id = id,
                    Rev = rev,
                    Data = new Dictionary<string, object> { ["type"] = "article", ["size"] = newText.Length },
                    Tags = new List<string> { "file", "text", "updated" },
                    Content = newText
                });

                put.Ok.Should().BeTrue();
                put.Doc!.Content.Should().Contain("Extra ending line.");

                // Удаление
                var del = _svc.Delete("files", id, put.Doc.Rev!);
                del.Ok.Should().BeTrue();

                // Проверка удаления
                _svc.Get("files", id).Should().BeNull();
            }
            finally
            {
                File.Delete(tmp);
            }
        }
    }
}
