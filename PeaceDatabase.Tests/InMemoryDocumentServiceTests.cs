using FluentAssertions;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage;
using PeaceDatabase.Storage.InMemory;
using Xunit;

namespace PeaceDatabase.Tests.Storage;


public class InMemoryDocumentServiceTests
{
    private readonly IDocumentService _svc;


    public InMemoryDocumentServiceTests()
    {
        _svc = new InMemoryDocumentService();
    }

    [Fact]
    public void CreateDb_Should_Be_Idempotent()
    {
        var first = _svc.CreateDb("testdb");
        var second = _svc.CreateDb("testdb");


        first.Ok.Should().BeTrue();
        second.Ok.Should().BeTrue("повторный вызов не должен падать");
        first.Error.Should().BeNull();
        second.Error.Should().BeNull();
    }


    [Fact]
    public void DeleteDb_Should_Not_Throw_When_Db_Not_Exist()
    {
        // Поведение может отличаться от вашей реализации. Если DeleteDb должен возвращать Ok=false,
        // скорректируйте проверку. Здесь проверяем, что метод не кидает исключение и сообщает понятный статус.
        var result = _svc.DeleteDb("missing-db");
        result.Ok.Should().BeTrue();
    }


    [Fact(Skip = "TODO: включить после реализации CRUD в IDocumentService")]
    public void Put_Get_Delete_Document_Happy_Path()
    {
        // Пример заготовки — скорректируйте под свои сигнатуры
        // _svc.CreateDb("docs");
        // var put = _svc.Put("docs", new Document { /* ... */ });
        // put.Ok.Should().BeTrue();
        // var get = _svc.Get("docs", put.Id);
        // get.Ok.Should().BeTrue();
        // var del = _svc.Delete("docs", put.Id);
        // del.Ok.Should().BeTrue();
    }
}