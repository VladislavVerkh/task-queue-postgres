# Task Queue Sample App

## Что внутри

- Demo REST endpoint для постановки задач: `POST /api/tasks/log-sleep`
- Demo `TaskHandler` (`sample.log-sleep`) с JSON -> DTO mapping
- Обработка задачи: логирование + `sleep`
- Метрики Micrometer в Prometheus и дашборды Grafana (`Task Queue`, `Java Overview`, `PostgreSQL Overview`)

## Быстрый старт через Docker Compose

Из корня проекта:

```bash
docker compose up --build
```

## Локальный Kubernetes (kind)

Из корня проекта:

```bash
scripts/kind/up.sh
```

Масштабирование:

```bash
scripts/kind/scale.sh 8
```

Полное описание: [`docs/kind-local-testing.md`](../docs/kind-local-testing.md).
После `kind`-старта доступны: sample app `http://localhost:8080`, Prometheus `http://localhost:9090`, Grafana `http://localhost:3000`.
Если порт занят, можно переопределить его перед запуском, например: `PROMETHEUS_HOST_PORT=9091 scripts/kind/up.sh`.
Логи pod-ов доступны в Grafana через datasource `Loki`.

Сервисы:

- sample app: `http://localhost:8080`
- Prometheus: `http://localhost:9090`
- Loki API: `http://localhost:3100`
- Alloy UI: `http://localhost:12345`
- Grafana: `http://localhost:3000` (без авторизации)
- Postgres: `localhost:54032` (`task_queue` / `task_queue`)
- Swagger UI: `http://localhost:8080/swagger-ui/index.html`

После старта открой Grafana и выбери дашборд:

- `Task Queue / Java Overview`
- `Task Queue / PostgreSQL Overview`

Что есть в PostgreSQL дашборде:

- доступность и uptime Postgres;
- текущие подключения, TPS, cache hit, temp bytes;
- checkpoints, deadlocks/conflicts;
- top tables по `n_dead_tup`;
- top SQL по `pg_stat_statements`;
- таблица состояния vacuum и активный прогресс vacuum.

Если порты заняты, переопредели их через `.env`:

```bash
cp .env.example .env
# например, если 9090 занят
echo "PROMETHEUS_PORT=9091" >> .env
# при необходимости можно переопределить Alloy UI порт
echo "ALLOY_PORT=12346" >> .env
docker compose up --build
```

## Постановка demo-задачи

```bash
curl -X POST http://localhost:8080/api/tasks/log-sleep \
  -H 'Content-Type: application/json' \
  -d '{
    "partitionKey": "taxi-42",
    "message": "hello from queue",
    "sleepMs": 1500
  }'
```

## Проверка метрик

```bash
curl http://localhost:8080/actuator/prometheus | grep task_queue_process
```

## Просмотр логов в Grafana (Loki)

1. Открой Grafana: `http://localhost:3000`
2. Перейди в `Explore`
3. Выбери datasource `Loki`
4. Пример запроса логов sample приложения:

```logql
{container="task-queue-sample-app"}
```

1. Пример фильтра только ошибок:

```logql
{container="task-queue-sample-app"} |= "ERROR"
```

Логи в Loki собираются через `grafana/alloy`.
