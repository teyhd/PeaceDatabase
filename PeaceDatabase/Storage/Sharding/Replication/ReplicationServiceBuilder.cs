using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Disk;
using PeaceDatabase.Storage.Disk.Internals;
using PeaceDatabase.Storage.InMemory;
using PeaceDatabase.Storage.Sharding.Configuration;
using PeaceDatabase.Storage.Sharding.Replication.Client;
using PeaceDatabase.Storage.Sharding.Replication.Configuration;
using PeaceDatabase.Storage.Sharding.Routing;

namespace PeaceDatabase.Storage.Sharding.Replication;

/// <summary>
/// Фабрика и extension методы для настройки репликации в DI.
/// </summary>
public static class ReplicationServiceBuilder
{
    /// <summary>
    /// Создаёт IDocumentService с поддержкой репликации.
    /// </summary>
    public static IDocumentService Build(
        ShardingOptions options,
        StorageOptions? storageOptions = null,
        IHttpClientFactory? httpClientFactory = null,
        ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        // Если репликация отключена, делегируем ShardingServiceBuilder
        if (!options.Replication.Enabled)
        {
            return ShardingServiceBuilder.Build(options, storageOptions, httpClientFactory, loggerFactory);
        }

        var router = new HashShardRouter(options);

        // Фабрика клиентов
        Func<ReplicaInfo, IReplicaClient> clientFactory;

        if (options.Mode == ShardingMode.Local)
        {
            // Локальный режим: создаём in-process сервисы для каждой реплики
            var replicaServices = new Dictionary<string, IDocumentService>();
            clientFactory = replica =>
            {
                if (!replicaServices.TryGetValue(replica.UniqueId, out var service))
                {
                    if (storageOptions != null)
                    {
                        var replicaStorageOptions = new StorageOptions
                        {
                            DataRoot = Path.Combine(storageOptions.DataRoot, $"shard-{replica.ShardId}", $"replica-{replica.ReplicaIndex}"),
                            EnableSnapshots = storageOptions.EnableSnapshots,
                            SnapshotEveryNOperations = storageOptions.SnapshotEveryNOperations,
                            SnapshotMaxWalSizeMb = storageOptions.SnapshotMaxWalSizeMb,
                            Durability = storageOptions.Durability
                        };
                        service = new FileDocumentService(replicaStorageOptions);
                    }
                    else
                    {
                        service = new InMemoryDocumentService();
                    }
                    replicaServices[replica.UniqueId] = service;
                }
                return new LocalReplicaClient(replica, service, replica.IsPrimary);
            };
        }
        else
        {
            // Распределённый режим: HTTP клиенты
            clientFactory = replica =>
            {
                var httpClient = httpClientFactory?.CreateClient($"replica-{replica.UniqueId}")
                    ?? new HttpClient { Timeout = TimeSpan.FromSeconds(options.RequestTimeoutSeconds) };

                var logger = loggerFactory?.CreateLogger<HttpReplicaClient>();
                return new HttpReplicaClient(replica, httpClient, logger);
            };
        }

        var coordinatorLogger = loggerFactory?.CreateLogger<ReplicationCoordinator>();
        var coordinator = new ReplicationCoordinator(options, clientFactory, coordinatorLogger);

        var serviceLogger = loggerFactory?.CreateLogger<ReplicatedDocumentService>();
        return new ReplicatedDocumentService(options, router, coordinator, serviceLogger);
    }
}

/// <summary>
/// Extension методы для регистрации репликации в DI.
/// </summary>
public static class ReplicationServiceExtensions
{
    /// <summary>
    /// Добавляет реплицированный IDocumentService в DI контейнер.
    /// </summary>
    public static IServiceCollection AddReplicatedDocumentService(
        this IServiceCollection services,
        ShardingOptions shardingOptions,
        StorageOptions? storageOptions = null)
    {
        services.AddSingleton(shardingOptions);
        services.AddSingleton(shardingOptions.Replication);

        if (storageOptions != null)
            services.AddSingleton(storageOptions);

        services.AddHttpClient();

        // Регистрируем ReplicationCoordinator
        services.AddSingleton<ReplicationCoordinator>(sp =>
        {
            var opts = sp.GetRequiredService<ShardingOptions>();
            var loggerFactory = sp.GetService<ILoggerFactory>();
            var httpFactory = sp.GetService<IHttpClientFactory>();

            Func<ReplicaInfo, IReplicaClient> clientFactory;
            if (opts.Mode == ShardingMode.Local)
            {
                var localServices = new Dictionary<string, IDocumentService>();
                clientFactory = replica =>
                {
                    if (!localServices.TryGetValue(replica.UniqueId, out var service))
                    {
                        var storageOpts = sp.GetService<StorageOptions>();
                        if (storageOpts != null)
                        {
                            var replicaStorageOptions = new StorageOptions
                            {
                                DataRoot = Path.Combine(storageOpts.DataRoot, $"shard-{replica.ShardId}", $"replica-{replica.ReplicaIndex}"),
                                EnableSnapshots = storageOpts.EnableSnapshots,
                                SnapshotEveryNOperations = storageOpts.SnapshotEveryNOperations,
                                SnapshotMaxWalSizeMb = storageOpts.SnapshotMaxWalSizeMb,
                                Durability = storageOpts.Durability
                            };
                            service = new FileDocumentService(replicaStorageOptions);
                        }
                        else
                        {
                            service = new InMemoryDocumentService();
                        }
                        localServices[replica.UniqueId] = service;
                    }
                    return new LocalReplicaClient(replica, service, replica.IsPrimary);
                };
            }
            else
            {
                clientFactory = replica =>
                {
                    var httpClient = httpFactory?.CreateClient($"replica-{replica.UniqueId}")
                        ?? new HttpClient { Timeout = TimeSpan.FromSeconds(opts.RequestTimeoutSeconds) };
                    var logger = loggerFactory?.CreateLogger<HttpReplicaClient>();
                    return new HttpReplicaClient(replica, httpClient, logger);
                };
            }

            var logger = loggerFactory?.CreateLogger<ReplicationCoordinator>();
            return new ReplicationCoordinator(opts, clientFactory, logger);
        });

        // Регистрируем IShardRouter
        services.AddSingleton<IShardRouter>(sp =>
        {
            var opts = sp.GetRequiredService<ShardingOptions>();
            return new HashShardRouter(opts);
        });

        // Регистрируем IDocumentService
        services.AddSingleton<IDocumentService>(sp =>
        {
            var opts = sp.GetRequiredService<ShardingOptions>();
            var router = sp.GetRequiredService<IShardRouter>();
            var coordinator = sp.GetRequiredService<ReplicationCoordinator>();
            var logger = sp.GetService<ILogger<ReplicatedDocumentService>>();
            return new ReplicatedDocumentService(opts, router, coordinator, logger);
        });

        // Регистрируем HealthMonitor как hosted service
        services.AddSingleton<HealthMonitor>(sp =>
        {
            var opts = sp.GetRequiredService<ShardingOptions>().Replication;
            var coordinator = sp.GetRequiredService<ReplicationCoordinator>();
            var loggerFactory = sp.GetService<ILoggerFactory>();
            var logger = sp.GetRequiredService<ILogger<HealthMonitor>>();

            Func<ReplicaInfo, IReplicaClient> clientFactory;
            if (sp.GetRequiredService<ShardingOptions>().Mode == ShardingMode.Local)
            {
                // Для локального режима HealthMonitor не нужен (все реплики в процессе)
                // Но создаём пустую фабрику для совместимости
                clientFactory = _ => throw new InvalidOperationException("Local mode doesn't use HTTP clients");
            }
            else
            {
                var httpFactory = sp.GetService<IHttpClientFactory>();
                clientFactory = replica =>
                {
                    var httpClient = httpFactory?.CreateClient($"health-{replica.UniqueId}")
                        ?? new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
                    var clientLogger = loggerFactory?.CreateLogger<HttpReplicaClient>();
                    return new HttpReplicaClient(replica, httpClient, clientLogger);
                };
            }

            return new HealthMonitor(opts, coordinator, clientFactory, logger);
        });

        // Регистрируем HealthMonitor как IHostedService только для распределённого режима
        services.AddHostedService<HealthMonitorHostedService>();

        return services;
    }
}

/// <summary>
/// Hosted service wrapper для HealthMonitor.
/// </summary>
internal sealed class HealthMonitorHostedService : IHostedService
{
    private readonly HealthMonitor _healthMonitor;
    private readonly ReplicationCoordinator _coordinator;
    private readonly ShardingOptions _options;
    private Task? _runningTask;
    private CancellationTokenSource? _cts;

    public HealthMonitorHostedService(
        HealthMonitor healthMonitor,
        ReplicationCoordinator coordinator,
        ShardingOptions options)
    {
        _healthMonitor = healthMonitor;
        _coordinator = coordinator;
        _options = options;

        // Подписываемся на события
        _healthMonitor.PrimaryDown += OnPrimaryDown;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Пропускаем для локального режима
        if (_options.Mode == ShardingMode.Local || !_options.Replication.Enabled)
            return Task.CompletedTask;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runningTask = _healthMonitor.StartAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_runningTask == null)
            return;

        _cts?.Cancel();

        try
        {
            await _healthMonitor.StopAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
    }

    private async void OnPrimaryDown(object? sender, PrimaryDownEventArgs e)
    {
        await _coordinator.HandlePrimaryDownAsync(e.ShardId);
    }
}

