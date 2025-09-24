using System.Net;
using System.Net.Http.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;


namespace PeaceDatabase.Tests.Api;


public class StatsEndpointTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;


    public StatsEndpointTests(WebApplicationFactory<Program> factory)
    {
        _factory = factory.WithWebHostBuilder(builder =>
        {
            // при необходимости можно переопределить конфигурацию/DI для тестов
        });
    }


    [Fact]
    public async Task Stats_Should_Return_200_And_Int_Counter()
    {
        var client = _factory.CreateClient();


        using var resp = await client.GetAsync("/v1/_stats");
        resp.StatusCode.Should().Be(HttpStatusCode.OK);


        var payload = await resp.Content.ReadFromJsonAsync<StatsDto>();
        payload.Should().NotBeNull();
        payload!.requestsTotal.Should().BeGreaterThanOrEqualTo(0);

    }


    [Fact]
    public async Task Stats_Counter_Should_Increase_After_Extra_Request()
    {
        var client = _factory.CreateClient();

        // Считываем текущее значение
        var before = await client.GetFromJsonAsync<StatsDto>("/v1/_stats");
        before.Should().NotBeNull();

        // Делаем доп. запрос (любой) — сам запрос к /v1/_stats тоже инкрементит счетчик через middleware
        using var ping = await client.GetAsync("/v1/_stats");
        ping.EnsureSuccessStatusCode();

        var after = await client.GetFromJsonAsync<StatsDto>("/v1/_stats");
        after.Should().NotBeNull();

        // монотонность (не убывает)
        after!.requestsTotal.Should().BeGreaterThan(before!.requestsTotal);
    }

    private sealed record StatsDto(int requestsTotal);
}