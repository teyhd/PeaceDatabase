using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Sharding.Client;
using PeaceDatabase.Storage.Sharding.Configuration;
using PeaceDatabase.Storage.Sharding.Discovery;
using PeaceDatabase.Storage.Sharding.Routing;
using PeaceDatabase.Storage.InMemory;
using PeaceDatabase.Storage.Disk;
using PeaceDatabase.Storage.Disk.Internals;

namespace PeaceDatabase.Storage.Sharding;

/// <summary>
/// Фабрика для создания и конфигурации шардированного сервиса.
/// </summary>
public static class ShardingServiceBuilder
{
    /// <summary>
    /// Создаёт IDocumentService на основе конфигурации шардирования.
    /// </summary>
    public static IDocumentService Build(
        ShardingOptions options,
        StorageOptions? storageOptions = null,
        IHttpClientFactory? httpClientFactory = null,
        ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        // Если шардирование отключено, возвращаем обычный сервис
        if (!options.Enabled)
        {
            return CreateLocalService(storageOptions);
        }

        // Создаём роутер
        var router = new HashShardRouter(options);

        // Создаём discovery
        IShardDiscovery discovery = options.Discovery.Type switch
        {
            DiscoveryType.Docker => new DockerShardDiscovery(options, loggerFactory?.CreateLogger<DockerShardDiscovery>()),
            _ => new StaticShardDiscovery(options)
        };

        // Создаём клиенты шардов
        var clients = new List<IShardClient>();

        if (options.Mode == ShardingMode.Local)
        {
            // Локальный режим: создаём in-process шарды
            clients = CreateLocalClients(options, storageOptions, loggerFactory);
        }
        else
        {
            // Распределённый режим
            if (options.CurrentShardId.HasValue)
            {
                // Этот инстанс является шардом - возвращаем локальный сервис
                return CreateLocalService(storageOptions);
            }

            // Этот инстанс является роутером - создаём HTTP клиенты
            clients = CreateHttpClients(options, discovery, httpClientFactory, loggerFactory);
        }

        var logger = loggerFactory?.CreateLogger<ShardedDocumentService>();
        return new ShardedDocumentService(router, discovery, clients, logger);
    }

    private static IDocumentService CreateLocalService(StorageOptions? storageOptions)
    {
        if (storageOptions != null)
            return new FileDocumentService(storageOptions);
        return new InMemoryDocumentService();
    }

    private static List<IShardClient> CreateLocalClients(
        ShardingOptions options,
        StorageOptions? baseStorageOptions,
        ILoggerFactory? loggerFactory)
    {
        var clients = new List<IShardClient>();

        for (int i = 0; i < options.ShardCount; i++)
        {
            var shardInfo = new ShardInfo
            {
                Id = i,
                BaseUrl = $"local://shard-{i}",
                IsLocal = true,
                Status = ShardStatus.Healthy
            };

            IDocumentService shardService;

            if (baseStorageOptions != null)
            {
                // Каждый шард имеет свою директорию данных
                var shardStorageOptions = new StorageOptions
                {
                    DataRoot = Path.Combine(baseStorageOptions.DataRoot, $"shard-{i}"),
                    EnableSnapshots = baseStorageOptions.EnableSnapshots,
                    SnapshotEveryNOperations = baseStorageOptions.SnapshotEveryNOperations,
                    SnapshotMaxWalSizeMb = baseStorageOptions.SnapshotMaxWalSizeMb,
                    Durability = baseStorageOptions.Durability
                };
                shardService = new FileDocumentService(shardStorageOptions);
            }
            else
            {
                shardService = new InMemoryDocumentService();
            }

            clients.Add(new LocalShardClient(shardInfo, shardService));
        }

        return clients;
    }

    private static List<IShardClient> CreateHttpClients(
        ShardingOptions options,
        IShardDiscovery discovery,
        IHttpClientFactory? httpClientFactory,
        ILoggerFactory? loggerFactory)
    {
        var clients = new List<IShardClient>();
        var shards = discovery.GetShards();

        foreach (var shardInfo in shards)
        {
            var httpClient = httpClientFactory?.CreateClient($"shard-{shardInfo.Id}")
                ?? new HttpClient { Timeout = TimeSpan.FromSeconds(options.RequestTimeoutSeconds) };

            var logger = loggerFactory?.CreateLogger<HttpShardClient>();
            clients.Add(new HttpShardClient(shardInfo, httpClient, logger));
        }

        return clients;
    }
}

/// <summary>
/// Extension методы для регистрации шардирования в DI.
/// </summary>
public static class ShardingServiceExtensions
{
    /// <summary>
    /// Добавляет шардированный IDocumentService в DI контейнер.
    /// </summary>
    public static IServiceCollection AddShardedDocumentService(
        this IServiceCollection services,
        ShardingOptions shardingOptions,
        StorageOptions? storageOptions = null)
    {
        services.AddSingleton(shardingOptions);

        if (storageOptions != null)
            services.AddSingleton(storageOptions);

        // Добавляем HttpClient factory для HTTP клиентов
        services.AddHttpClient();

        services.AddSingleton<IDocumentService>(sp =>
        {
            var httpFactory = sp.GetService<IHttpClientFactory>();
            var loggerFactory = sp.GetService<ILoggerFactory>();
            return ShardingServiceBuilder.Build(shardingOptions, storageOptions, httpFactory, loggerFactory);
        });

        return services;
    }
}

