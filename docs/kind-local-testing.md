# Локальное тестирование в Kubernetes (kind)

Ниже сценарий для запуска `task-queue-sample-app` в локальном multi-node кластере `kind`.

## Что будет поднято

- `kind` кластер: `1 control-plane + 3 worker`.
- `task-queue-postgres` в namespace `task-queue`.
- `task-queue-sample-app` (по умолчанию `4` реплики).
- `task-queue-prometheus`.
- `task-queue-loki`.
- `task-queue-alloy` (DaemonSet для сбора pod-логов в Loki).
- `task-queue-grafana` (дашборды подключаются автоматически).

## Предварительные требования

- Docker
- kind
- kubectl

## Быстрый старт

Из корня проекта:

```bash
scripts/kind/up.sh
```

После старта:

- API sample app: `http://localhost:8080`
- PostgreSQL: `localhost:54032` (`task_queue` / `task_queue`)
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

Логи в Grafana:

1. Открой `Explore`
2. Выбери datasource `Loki`
3. Пример запроса:

```logql
{namespace="task-queue", app="task-queue-sample-app"}
```

## Полезные команды

```bash
# Пересобрать образ и задеплоить заново
scripts/kind/redeploy.sh

# Масштабировать количество pod приложения
scripts/kind/scale.sh 8

# Проверить распределение pod по нодам
kubectl -n task-queue get pods -o wide

# Удалить кластер
scripts/kind/down.sh
```

## Настройка через переменные окружения

```bash
# Имя кластера (по умолчанию task-queue-local)
export CLUSTER_NAME=task-queue-local

# Имя образа sample app (по умолчанию task-queue-sample-app:kind)
export APP_IMAGE=task-queue-sample-app:kind

# Количество реплик при up/redeploy (по умолчанию 4)
export APP_REPLICAS=6

# Переопределение host-портов (если заняты)
export APP_HOST_PORT=18080
export POSTGRES_HOST_PORT=15432
export PROMETHEUS_HOST_PORT=19090
export GRAFANA_HOST_PORT=13000
```

`up.sh` и `redeploy.sh` учитывают эти значения автоматически.

## Примечания

- Базовые port mappings задаются в `k8s/kind/cluster-config.yaml`, но `up.sh` умеет переопределять host-порты через env.
- NodePort `30080` проброшен на хост `8080`.
- NodePort `30432` проброшен на хост `54032`.
- NodePort `30090` проброшен на хост `9090`.
- NodePort `30030` проброшен на хост `3000`.
- Если кластер уже создан, изменения host-портов применятся только после пересоздания:
  `scripts/kind/down.sh && scripts/kind/up.sh`.
- Для локального `kind` ограничены пулы JDBC:
  `SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE=4`, `SPRING_DATASOURCE_HIKARI_MINIMUM_IDLE=1`.
- Для Postgres в `kind` установлен `max_connections=200`.
