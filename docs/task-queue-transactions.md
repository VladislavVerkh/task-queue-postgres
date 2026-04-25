# Task Queue: Границы Транзакций

Ниже перечислены публичные транзакционные методы библиотеки (`@Transactional`) и их границы.

| Класс                       | Метод                           | Транзакция                                      | Что входит в границу                                                          |
|-----------------------------|---------------------------------|-------------------------------------------------|-------------------------------------------------------------------------------|
| `TaskQueueService`          | `enqueue(...)`                  | `@Transactional`                                | расчет партиции + insert в `task_queue`                                       |
| `TaskQueueService`          | `dequeueForWorker(...)`         | `@Transactional`                                | shared advisory lock + lock/update пачки задач                                |
| `TaskQueueService`          | `acknowledge(taskId, workerId)` | `@Transactional`                                | owner-checked delete задачи из `task_queue`                                   |
| `TaskQueueService`          | `renewLease(taskId, workerId)`  | `@Transactional`                                | owner-checked продление `lease_until` in-flight задачи                        |
| `TaskExecutionService`      | `handleAndAcknowledge(...)`     | `@Transactional(rollbackFor = Exception.class)` | `handler.handle(...)` + owner-checked `acknowledge(...)` в одной транзакции   |
| `TaskRetryService`          | `retryOrFinalize(..., workerId)` | `@Transactional`                               | owner-checked классификация + `delay(...)`, `remove(...)` или dead-letter copy |
| `WorkerCoordinationService` | `registerWorker(...)`           | `@Transactional`                                | insert worker + rebalance                                                     |
| `WorkerCoordinationService` | `heartbeatWorker(...)`          | `@Transactional`                                | update `heartbeat_last`                                                       |
| `WorkerCoordinationService` | `unregisterWorker(...)`         | `@Transactional`                                | release locked tasks + remove worker + rebalance                              |
| `WorkerCoordinationService` | `cleanUpDeadWorkers()`          | `@Transactional`                                | выборка dead workers (`FOR UPDATE SKIP LOCKED`) + release/remove + rebalance  |
| `WorkerCoordinationService` | `cleanUpExpiredTaskLeases()`    | `@Transactional`                                | выборка задач с истекшим lease (`FOR UPDATE SKIP LOCKED`) + release ownership |
| `WorkerCoordinationService` | `refreshQueueStateMetrics()`    | `@Transactional(readOnly = true)`               | aggregate-запрос состояния очереди для gauge-метрик                           |
| `WorkerCoordinationService` | `rebalance()`                   | `@Transactional`                                | полный rebalance под exclusive advisory lock                                  |
| `WorkerCoordinationService` | `reconcileHandoffs()`           | `@Transactional`                                | reconcile `DRAINING` assignments + timeout-policy под exclusive advisory lock |

## Дополнительно

- `QueueWorkerRuntime` остается нетранзакционным рантаймом, но бизнес-обработка + `ack` теперь
  оркестрируются транзакционно в `TaskExecutionService`.
- Режим `task.queue.handling-transaction-mode=NON_TRANSACTIONAL` отключает общую транзакцию
  `handler+ack` и оставляет только транзакцию на `ack`.
- Репозитории предполагают вызов внутри сервисных транзакций.
- Если `task.queue.dead-letter-enabled=true`, копирование в `task_queue_dead_letter` и удаление из
  `task_queue` выполняются в одной транзакции и с той же owner-check проверкой.
- Для согласованности выборки/ребаланса используется связка advisory lock:
    - `dequeueForWorker(...)` -> `pg_advisory_xact_lock_shared(...)`;
    - `rebalanceInternal()` / `reconcileHandoffs()` -> `pg_advisory_xact_lock(...)`.
