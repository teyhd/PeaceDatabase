# Балансировка и «шардирование» трафика
## Цель: Построить на open-source NGINX (без NGINX Plus) связку для:
- классической балансировки нагрузки HTTP,
- детерминированного распределения запросов по «шардам» (по ключу user_id/tenant),
- безопасного rollout’а (A/B, канарейка),
- при необходимости — TCP-прокси (stream) для БД.

## Стенд и допущения
- NGINX ≥ 1.18 (поддерживает `hash ... consistent` в `upstream`).
- 3–5 backend-инстансов приложения на порту 8080.
- Канал проверки: curl/wrk, логирование в access_log.
- Активных health-checks нет (есть в NGINX Plus), используем пассивные (max_fails, fail_timeout).

## Конфиг
```
user  nginx;
worker_processes auto;

events { worker_connections 4096; }

http {
    include       mime.types;
    default_type  application/octet-stream;
    sendfile      on;

    log_format main_ups '$remote_addr - $request_id [$time_local] "$request" $status $body_bytes_sent '
                        '"$http_referer" "$http_user_agent" '
                        'rt=$request_time uct=$upstream_connect_time urt=$upstream_response_time '
                        'ua="$upstream_addr" us="$upstream_status" bucket="$ab_bucket" shard="$final_key"';
    access_log /var/log/nginx/access.log main_ups;

    # Таймауты прокси
    proxy_connect_timeout 3s;
    proxy_read_timeout    30s;
    proxy_send_timeout    30s;
    send_timeout          30s;

    map $http_x_user_id $user_key1 { default $http_x_user_id; "" $cookie_user_id; }
    map $user_key1      $final_key { default $user_key1;      "" $arg_uid; }

    split_clients "${remote_addr}${http_user_agent}" $ab_bucket {
        10% "beta";   # канарейка
        *   "stable";
    }

    map $arg_tenant $tenant_upstream {
        ~^(tenant_[a-mA-M]) app_cluster_1;
        ~^(tenant_[n-zN-Z]) app_cluster_2;
        default             app_cluster_1;
    }

    upstream app_all {
        least_conn;
        server 10.0.0.1:8080 max_fails=3 fail_timeout=30s;
        server 10.0.0.2:8080 max_fails=3 fail_timeout=30s;
        keepalive 64;
    }

    upstream app_stable {
        server 10.0.0.1:8080 max_fails=3 fail_timeout=30s;
        server 10.0.0.2:8080 max_fails=3 fail_timeout=30s;
        keepalive 64;
    }

    upstream app_beta {
        server 10.0.1.1:8080 max_fails=3 fail_timeout=30s;
        keepalive 64;
    }

    upstream app_sharded {
        hash $final_key consistent;
        server 10.0.0.1:8080 max_fails=3 fail_timeout=30s;
        server 10.0.0.2:8080 max_fails=3 fail_timeout=30s;
        server 10.0.0.3:8080 max_fails=3 fail_timeout=30s;
        keepalive 64;
    }

    upstream app_cluster_1 {
        least_conn;
        server 10.0.0.11:8080 max_fails=3 fail_timeout=30s;
        server 10.0.0.12:8080 max_fails=3 fail_timeout=30s;
        keepalive 64;
    }
    upstream app_cluster_2 {
        least_conn;
        server 10.0.0.21:8080 max_fails=3 fail_timeout=30s;
        server 10.0.0.22:8080 max_fails=3 fail_timeout=30s;
        keepalive 64;
    }

    server {
        listen 80;
        server_name lab.example.local;


        set $common 1;
        proxy_set_header Host               $host;
        proxy_set_header X-Request-Id       $request_id;
        proxy_set_header X-Forwarded-For    $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto  $scheme;

        location = / {
            proxy_pass http://app_$ab_bucket;
        }

        location /api/ {
            proxy_pass http://app_sharded;
        }

        location /edu/ {
            proxy_pass http://$tenant_upstream;
        }

        location / {
            proxy_pass http://app_all;
        }

        location = /nginx/healthz { return 200 "ok\n"; }
    }
}


stream {
    log_format stream_fmt '$remote_addr [$time_local] $protocol $status upstream=$upstream_addr bytes=$bytes_sent time=$session_time';
    access_log /var/log/nginx/stream_access.log stream_fmt;

    upstream pg_shards {
        hash $remote_addr consistent;   # здесь пример по IP; для БД лучше роутить в приложении
        server 10.0.0.10:5432;
        server 10.0.0.11:5432;
    }

    server {
        listen 5432;
        proxy_timeout 30s;
        proxy_pass pg_shards;
    }
}

```


## Проверка (реплицируемые шаги)
### Балансировка:

```
wrk -t4 -c128 -d30s http://lab.example.local/
```


### Шардирование по user_id:
```
curl -H 'X-User-Id: 12345' http://lab.example.local/api/ping
curl -H 'X-User-Id: 12345' http://lab.example.local/api/ping
```

## Два запроса должны уйти на один и тот же ua=. Поменяйте X-User-Id — увидите другой узел.

### Канареечный rollout (A/B):
```
for i in {1..5}; do curl -s http://lab.example.local/ -o /dev/null; done

```
## Для одного клиента bucket= стабилен. Доля попаданий в beta ≈ 10% по популяции клиентов.
## Multi-tenant:
```
curl 'http://lab.example.local/edu/?tenant=tenant_Alpha' # → app_cluster_1
curl 'http://lab.example.local/edu/?tenant=tenant_Zulu'  # → app_cluster_2
```

Отказоустойчивость: временно «роняю» один backend, слежу за us=/ua= и тем, что NGINX исключает узел на время fail_timeout.

## Ограничения и замечания

- Активные health-checks и sticky-cookie — это функциональность NGINX Plus; в OSS оставляю пассивные проверки и consistent hash.
- ip_hash не использую: не работает с весами, нестабилен за NAT, не привязан к реальному пользователю.
- Для БД реальное шардирование делает приложение; stream в конфиге — лишь баланс TCP-коннектов.
