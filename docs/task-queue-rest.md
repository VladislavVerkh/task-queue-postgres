# Служебный REST API

Модуль `task-queue-rest` добавляет обычные Spring MVC endpoints для операционного управления
очередью. Модуль подключается отдельно от основного starter, поэтому REST API появляется только у
приложений, которые явно добавили зависимость.

Базовый путь по умолчанию: `/task-queue/admin/v1`.

Путь можно переопределить:

```properties
task.queue.rest.base-path=/internal/task-queue/v1
```

Отключение при подключенном модуле:

```properties
task.queue.rest.enabled=false
```

## Endpoints

| Метод | Путь | Назначение |
| --- | --- | --- |
| `GET` | `/task-queue/admin/v1/summary` | Сводка queue/DLQ/draining-состояния |
| `GET` | `/task-queue/admin/v1/partitions` | Снимок assignments и lag по всем партициям |
| `GET` | `/task-queue/admin/v1/workers` | Снимок зарегистрированных воркеров |
| `POST` | `/task-queue/admin/v1/rebalance` | Ручной rebalance |
| `POST` | `/task-queue/admin/v1/handoffs/reconcile` | Ручной reconcile DRAINING handoff |
| `POST` | `/task-queue/admin/v1/cleanup/expired-leases` | Очистка задач с истекшим lease |
| `POST` | `/task-queue/admin/v1/cleanup/dead-workers` | Очистка просроченных воркеров |
| `POST` | `/task-queue/admin/v1/dead-letters/{taskId}/requeue` | Возврат DLQ-задачи в основную очередь |

## Requeue из DLQ

Без тела задача возвращается с доступностью "сейчас" относительно времени PostgreSQL:

```http
POST /task-queue/admin/v1/dead-letters/018f0000-0000-7000-8000-000000000001/requeue
```

С задержкой относительно времени PostgreSQL:

```json
{
  "availableAfter": "PT30S"
}
```

С абсолютным временем:

```json
{
  "availableAt": "2026-04-26T10:30:00Z"
}
```

`availableAt` и `availableAfter` нельзя передавать вместе.

## Ошибки

Ошибки endpoints возвращаются как `application/problem+json`:

```json
{
  "type": "about:blank",
  "title": "Not found",
  "status": 404,
  "detail": "Dead-letter task not found: 018f0000-0000-7000-8000-000000000001"
}
```

