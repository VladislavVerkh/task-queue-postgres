# Task Queue: Архитектура

## Назначение

Библиотека реализует распределенную обработку задач в PostgreSQL-очереди с семантикой:

- последовательная обработка задач с одинаковым `partition_key`;
- горизонтальное масштабирование по воркерам;
- heartbeat/детект «мертвых» воркеров;
- ребаланс закрепления партиций между живыми воркерами;
- retry/backoff с классификацией retryable/non-retryable исключений.

## Модульная структура

- `task-queue-core`
    - доменные модели (`QueuedTask`, `TaskEnqueueRequest`);
    - алгоритмы (`TaskPartitioner`, `PartitionAssignmentPlanner`);
    - retry-политика и классификатор ошибок;
    - конфигурация `TaskQueueProperties`.
- `task-queue-jdbc`
    - JDBC-репозитории и SQL-операции;
    - сервисы с транзакциями;
    - runtime-движок воркеров (`QueueWorkerRuntime`);
    - метрики Micrometer.
- `task-queue-spring-boot-starter`
    - автоконфигурация и wiring всех бинов библиотеки.

## Поток обработки

### Постановка задачи (Producer)

1. Продюсер вызывает `TaskProducer.enqueue(...)` (по умолчанию реализован `TaskProducerService`).
2. `TaskQueueService.enqueue(...)`:
    - валидирует вход;
    - вычисляет `partition_num = hash(partition_key) % partition_count + 1`;
    - пишет запись в `task_queue`.

### Запуск runtime

При старте приложения `QueueWorkerRuntime`:

1. запускает `workerCount` worker-потоков;
2. каждый worker регистрируется в `task_worker_registry`;
3. при регистрации вызывается ребаланс партиций;
4. для каждого worker запускается heartbeat-монитор;
5. отдельный cleanup-цикл периодически удаляет «мертвые» воркеры и триггерит ребаланс;
6. в том же фоне периодически выполняется reconcile handoff-состояний (`DRAINING`), чтобы
   завершать/откатывать зависшие переназначения партиций по timeout-policy.

`task.queue.runtime-enabled=false` отключает runtime (producer-only режим).

### Выборка и обработка

1. Worker вызывает `TaskQueueService.dequeueForWorker(workerId, batchSize)`.
2. Выборка идет только из партиций, закрепленных за worker в состоянии `ACTIVE`.
   Партиции в состоянии `DRAINING` не отдают новые задачи старому владельцу.
3. SQL использует `FOR UPDATE SKIP LOCKED` для конкурентной выборки без конфликтов.
4. Режим обработки выбирается свойством `task.queue.handling-transaction-mode`:
    - `TRANSACTIONAL`: Worker вызывает `TaskExecutionService.handleAndAcknowledge(task, workerId)`,
      где `handler.handle(task)` и owner-checked `acknowledge(taskId, workerId)` идут в одной
      транзакции;
    - `NON_TRANSACTIONAL`: Worker выполняет `handler.handle(task)` вне общей транзакции, затем
      вызывает owner-checked `acknowledge(taskId, workerId)` отдельной транзакцией.
5. После отката при ошибке вызывается `TaskRetryService.retryOrFinalize(...)`:
    - non-retryable: удалить задачу;
    - retryable: либо `delay(...)`, либо удалить при исчерпании попыток.

## Диаграмма последовательности

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant TPS as TaskProducer
    participant TQS as TaskQueueService
    participant TES as TaskExecutionService
    participant W as QueueWorkerRuntime/Worker
    participant WCS as WorkerCoordinationService
    participant RS as TaskRetryService
    participant DB as PostgreSQL
    P ->> TPS: enqueue(type, partitionKey, payload)
    TPS ->> TQS: enqueue(request)
    TQS ->> TQS: partition = hash(key) % partitionCount + 1
    TQS ->> DB: INSERT INTO task_queue(...)
    DB -->> TQS: taskId
    TQS -->> TPS: taskId
    TPS -->> P: accepted
    W ->> WCS: registerWorker(workerId)
    WCS ->> DB: INSERT task_worker_registry
    WCS ->> DB: pg_advisory_xact_lock(rebalanceLockKey)
    WCS ->> DB: rebalance assignments
    WCS -->> W: registered + partitions

    loop poll cycle
        W ->> TQS: dequeueForWorker(workerId, batchSize)
        TQS ->> DB: pg_advisory_xact_lock_shared(rebalanceLockKey)
        TQS ->> DB: SELECT ... FOR UPDATE SKIP LOCKED
        DB -->> TQS: tasks
        TQS -->> W: tasks batch

        alt handled successfully
            W ->> TES: handleAndAcknowledge(task, workerId)
            TES ->> DB: BEGIN TX
            TES ->> TES: handler.handle(task)
            TES ->> TQS: acknowledge(taskId, workerId)
            TQS ->> DB: DELETE FROM task_queue WHERE task_id=?
            TES ->> DB: COMMIT TX
        else handler failed
            TES ->> DB: ROLLBACK TX
            W ->> RS: retryOrFinalize(taskId, delayCount, error, workerId)
            alt retryable and attempts left
                RS ->> DB: UPDATE available_at(backoff), delay_count
            else non-retryable or retry limit reached
                RS ->> DB: DELETE FROM task_queue WHERE task_id=?
            end
        end
    end

    par heartbeat loop
        loop every heartbeatInterval
            W ->> WCS: heartbeatWorker(workerId)
            WCS ->> DB: UPDATE task_worker_registry heartbeat_last
        end
    and cleanup/rebalance loop
        loop every cleanupInterval
            WCS ->> DB: find dead workers (FOR UPDATE SKIP LOCKED)
            WCS ->> DB: release locked tasks + remove workers
            WCS ->> DB: rebalance assignments (exclusive advisory lock)
            WCS ->> DB: reconcile DRAINING handoffs (timeout-policy)
        end
    end

    Note over W, WCS: Если worker признан мертвым, runtime делает re-register с новым workerId или останавливается
```

## Ребаланс и блокировки

Ребаланс выполняется в `WorkerCoordinationService.rebalanceInternal()` и включает
не только расчет целевого владельца партиции, но и state-machine handoff.

### Блокировки

Для согласованности выборки и ребаланса используется один и тот же advisory lock:

- `TaskQueueService.dequeueForWorker(...)` берет `pg_advisory_xact_lock_shared(rebalanceLockKey)`;
- `WorkerCoordinationService.rebalanceInternal()` берет `pg_advisory_xact_lock(rebalanceLockKey)`.

Это исключает гонку, когда assignment уже поменялся, а выборка задач еще работает по старому
снимку.

### Модель состояния assignment

Для каждой партиции в `task_worker_partition_assignment` есть состояние handoff:

- `ACTIVE` — обычное рабочее состояние; owner может забирать новые задачи партиции.
- `DRAINING` — инициирован перенос ownership; старому owner больше не отдаются новые задачи,
  но ему дается время завершить in-flight задачи.

Переходы:

```mermaid
stateDiagram-v2
    [*] --> ACTIVE
    ACTIVE --> DRAINING: target changed and in flight exists
    ACTIVE --> ACTIVE: target changed and in flight empty
    DRAINING --> ACTIVE: in flight drained complete handoff
    DRAINING --> ACTIVE: target reverted cancel handoff
    DRAINING --> DRAINING: timeout extend deadline
    DRAINING --> ACTIVE: timeout abort handoff
    DRAINING --> ACTIVE: timeout force handoff
```

### Алгоритм применения плана

1. Берется `pg_advisory_xact_lock(rebalanceLockKey)`.
2. Удаляются assignments с `partition_num > partitionCount`.
3. Загружаются live workers и текущие assignments.
4. Planner считает целевой `partition -> targetWorker`.
5. Для каждой партиции выполняется переход state-machine:
    - если assignment отсутствует: создается `ACTIVE` с `worker_id = targetWorker`;
    - если state=`ACTIVE`:
        - `currentWorker == targetWorker` -> ничего не меняется;
        - `currentWorker != targetWorker` и нет in-flight -> immediate handoff (`ACTIVE` на новом
          owner);
        - `currentWorker != targetWorker` и есть in-flight -> `DRAINING` +
          `pending_worker_id=targetWorker`
            + `drain_started_at` + `drain_deadline_at`;
    - если state=`DRAINING`:
        - если target снова совпал с текущим owner -> cancel (`ACTIVE`, pending очищается);
        - если target изменился -> обновляется `pending_worker_id`;
        - если in-flight завершены -> complete handoff (owner = pending, state=`ACTIVE`);
        - если наступил timeout:
            - `EXTEND` -> продлевается `drain_deadline_at`;
            - `ABORT` -> возврат в `ACTIVE` на старом owner;
            - `FORCE` -> принудительный complete handoff на pending owner.

Граф принятия решения по одной партиции:

```mermaid
flowchart TD
    A[partition + target owner] --> B{assignment exists?}
    B -- no --> C[upsert ACTIVE to target]
    B -- yes --> D{state is ACTIVE?}
    D -- yes --> E{current equals target?}
    E -- yes --> Z[no-op]
    E -- no --> F{has in-flight at current owner?}
    F -- no --> G[immediate handoff to target ACTIVE]
    F -- yes --> H[start DRAINING + pending + deadline]
    D -- no --> I{current equals target?}
    I -- yes --> J[cancel draining to ACTIVE on current owner]
    I -- no --> K[update pending if changed]
    K --> L{has in-flight at current owner?}
    L -- no --> M[complete handoff to ACTIVE on pending owner]
    L -- yes --> N{deadline reached?}
    N -- no --> Z
    N -- yes --> O{timeout action}
    O -- EXTEND --> P[extend deadline]
    O -- ABORT --> Q[cancel draining to ACTIVE on current owner]
    O -- FORCE --> R[complete handoff to ACTIVE on pending owner]
```

### Почему сохраняется упорядоченность по partition key

- Во время `DRAINING` старому owner не выдаются новые задачи партиции.
- Переход ownership в нового owner происходит после подтвержденного отсутствия in-flight задач
  у старого owner (или по явной timeout-policy `FORCE`).
- При `EXTEND/ABORT` можно полностью избежать пересечения старого и нового owner по одной партиции.

### Reconcile цикл

Даже если состав воркеров не меняется, `QueueWorkerRuntime` периодически вызывает
`WorkerCoordinationService.reconcileHandoffs()`:

- повторно запускается тот же `rebalanceInternal()`;
- застрявшие `DRAINING` assignments проверяются на завершение/timeout;
- timeout-policy применяется без ожидания нового события регистрации/cleanup.

## Retry-классификация

Порядок принятия решения в `RetryExceptionClassifier`:

1. Если исключение (или его cause, если включено) попало в `not-retryable-exceptions` -> retry
   запрещен.
2. Иначе, если попало в `retryable-exceptions` -> retry разрешен.
3. Иначе используется `retry-default-retryable`.

## Схема хранения

Используются таблицы:

- `task_worker_registry` — реестр воркеров и heartbeat;
- `task_worker_partition_assignment` — закрепление партиций за воркерами и состояние handoff;
- `task_queue` — очередь задач.

```mermaid
erDiagram
    task_worker_registry {
        worker_id varchar_100 PK
        heartbeat_last timestamptz
        timeout_sec int
        created_at timestamptz
    }

    task_worker_partition_assignment {
        partition_num int PK
        worker_id varchar_100 FK
        handoff_state varchar_16
        pending_worker_id varchar_100 FK
        drain_started_at timestamptz
        drain_deadline_at timestamptz
        owner_changed_at timestamptz
        owner_change_count bigint
    }

    task_queue {
        task_id uuid PK
        task_type varchar_128
        payload text
        partition_key varchar_512
        partition_num int
        available_at timestamptz
        delay_count bigint
        worker_id varchar_100 FK
        created_at timestamptz
    }

    task_worker_registry ||--o{ task_worker_partition_assignment : "worker_id (ON DELETE CASCADE)"
    task_worker_registry ||--o{ task_worker_partition_assignment : "pending_worker_id (ON DELETE SET NULL)"
    task_worker_registry ||--o{ task_queue : "worker_id (ON DELETE SET NULL)"
    task_worker_partition_assignment ||--o{ task_queue : "partition_num logical mapping (no FK)"
```

В ER-диаграмме связь `task_worker_partition_assignment -> task_queue` по `partition_num`
помечена как логическая: явный FK на
`task_worker_partition_assignment.partition_num` в схеме не задан.

DDL лежит в Liquibase changeset:

- `task-queue-jdbc/src/main/resources/db/changelog/db.changelog-master.yaml`
- `task-queue-jdbc/src/main/resources/db/changelog/changes/001-task-queue-init.sql`
- `task-queue-jdbc/src/main/resources/db/changelog/changes/005-partition-handoff-state.sql`

Для таблиц `task_queue`, `task_worker_registry` и `task_worker_partition_assignment`
заданы более агрессивные per-table параметры autovacuum/analyze, так как это
high-churn таблицы (частые `insert`/`update`/`delete`).

## Жизненный цикл данных в таблицах

| Таблица                            | Ключевой момент                                         | Где происходит                                                                                                       | Какие поля меняются                                                                                                                                                                           |
|------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `task_worker_registry`             | Регистрация воркера                                     | `WorkerCoordinationService.registerWorker()` -> `WorkerRegistryRepository.registerWorker()`                          | `insert`: `worker_id`, `heartbeat_last=now`, `timeout_sec`, `created_at=now`                                                                                                                  |
| `task_worker_registry`             | Heartbeat                                               | `WorkerCoordinationService.heartbeatWorker()` -> `WorkerRegistryRepository.heartbeatWorker()`                        | `update`: `heartbeat_last=now`                                                                                                                                                                |
| `task_worker_registry`             | Разрегистрация воркера (штатная)                        | `WorkerCoordinationService.unregisterWorker()` -> `WorkerRegistryRepository.removeWorker()`                          | `delete` строки воркера                                                                                                                                                                       |
| `task_worker_registry`             | Cleanup мертвых воркеров                                | `WorkerCoordinationService.cleanUpDeadWorkers()` -> `WorkerRegistryRepository.removeWorker()`                        | `delete` строк dead worker                                                                                                                                                                    |
| `task_worker_partition_assignment` | Первичное назначение партиции воркеру                   | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.upsertActiveAssignment()`               | `insert`: `partition_num`, `worker_id`, `handoff_state='ACTIVE'`, `pending_worker_id=null`, `drain_started_at=null`, `drain_deadline_at=null`, `owner_changed_at=now`, `owner_change_count=1` |
| `task_worker_partition_assignment` | Immediate handoff (без in-flight у текущего owner)      | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.upsertActiveAssignment()`               | `update`: `worker_id` (новый owner), `handoff_state='ACTIVE'`, `pending_worker_id/drain_* = null`, при смене owner: `owner_changed_at=now`, `owner_change_count + 1`                          |
| `task_worker_partition_assignment` | Старт drain/handoff (есть in-flight у текущего owner)   | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.startDraining()`                        | `update`: `handoff_state='DRAINING'`, `pending_worker_id=target`, `drain_started_at=now`, `drain_deadline_at=now + handoffDrainTimeout`                                                       |
| `task_worker_partition_assignment` | Смена целевого owner во время drain/handoff             | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.updatePendingWorker()`                  | `update`: `pending_worker_id`                                                                                                                                                                 |
| `task_worker_partition_assignment` | Завершение handoff после дренажа in-flight              | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.completeHandoff()`                      | `update`: `worker_id=pending`, `handoff_state='ACTIVE'`, очистка `pending_worker_id/drain_*`, при смене owner: `owner_changed_at=now`, `owner_change_count + 1`                               |
| `task_worker_partition_assignment` | Отмена handoff (target вернулся к текущему owner)       | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.cancelDraining()`                       | `update`: `handoff_state='ACTIVE'`, `pending_worker_id/drain_* = null`                                                                                                                        |
| `task_worker_partition_assignment` | Timeout handoff + `EXTEND`                              | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.extendDrainDeadline()`                  | `update`: `drain_deadline_at = now + handoffDrainTimeout`                                                                                                                                     |
| `task_worker_partition_assignment` | Timeout handoff + `ABORT`                               | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.cancelDraining()`                       | `update`: `handoff_state='ACTIVE'`, `pending_worker_id/drain_* = null`                                                                                                                        |
| `task_worker_partition_assignment` | Timeout handoff + `FORCE`                               | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.completeHandoff()`                      | `update`: `worker_id=pending`, `handoff_state='ACTIVE'`, очистка `pending_worker_id/drain_*`, при смене owner: `owner_changed_at=now`, `owner_change_count + 1`                               |
| `task_worker_partition_assignment` | Очистка партиций вне диапазона                          | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.removeAssignmentsAbovePartition()`      | `delete` строк с `partition_num > partition_count`                                                                                                                                            |
| `task_worker_partition_assignment` | Нет живых воркеров                                      | `WorkerCoordinationService.rebalanceInternal()` -> `WorkerRegistryRepository.clearAssignments()`                     | `delete` всех строк                                                                                                                                                                           |
| `task_worker_partition_assignment` | Удаление воркера из реестра                             | FK `worker_id -> task_worker_registry.worker_id` (`ON DELETE CASCADE`)                                               | Автоматический `delete` дочерних assignments этого воркера                                                                                                                                    |
| `task_worker_partition_assignment` | Удаление pending owner из реестра                       | FK `pending_worker_id -> task_worker_registry.worker_id` (`ON DELETE SET NULL`)                                      | Автоматический `update`: `pending_worker_id = null`                                                                                                                                           |
| `task_queue`                       | Постановка задачи                                       | `TaskQueueService.enqueue()` -> `TaskQueueRepository.enqueue()`                                                      | `insert`: `task_id`, `task_type`, `payload`, `partition_key`, `partition_num`, `available_at`, `delay_count=0`, `worker_id=null`, `created_at=now`                                            |
| `task_queue`                       | Захват задач воркером на обработку                      | `TaskQueueService.dequeueForWorker()` -> `TaskQueueRepository.lockNextTasksForWorker()`                              | `update`: `worker_id = :workerId` для выбранных задач                                                                                                                                         |
| `task_queue`                       | Успешное завершение задачи (ack)                        | `TaskExecutionService.handleAndAcknowledge()` или `TaskQueueService.acknowledge()`                                   | owner-checked `delete` строки задачи по `task_id` + `worker_id`                                                                                                                               |
| `task_queue`                       | Retry после ошибки                                      | `TaskRetryService.retryOrFinalize()` -> `TaskQueueRepository.delayOwnedBy()`                                         | owner-checked `update`: `available_at = now + backoff`, `delay_count = delay_count + 1`, `worker_id = null`                                                                                   |
| `task_queue`                       | Финализация без retry (non-retryable или лимит попыток) | `TaskRetryService.retryOrFinalize()` -> `TaskQueueRepository.removeOwnedBy()`                                        | owner-checked `delete` строки задачи по `task_id` + `worker_id`                                                                                                                               |
| `task_queue`                       | Освобождение захваченных задач при удалении воркера     | `WorkerCoordinationService.unregisterWorker()/cleanUpDeadWorkers()` -> `TaskQueueRepository.releaseLockedByWorker()` | `update`: `worker_id = null` для задач удаляемого воркера                                                                                                                                     |
| `task_queue`                       | Удаление воркера из реестра                             | FK `worker_id -> task_worker_registry.worker_id` (`ON DELETE SET NULL`)                                              | Автоматический `update`: `worker_id = null` в связанных задачах                                                                                                                               |

## Метрики

Основные метрики Micrometer (`task.queue.process.*`):

- `register.success`, `register.failure`
- `heartbeat.success`, `heartbeat.failure`, `heartbeat.timeout`, `heartbeat.not_found`,
  `heartbeat.latency`
- `cleanup.runs`, `cleanup.removed`
- `rebalance.runs`, `rebalance.failures`, `rebalance.latency`
- `handoff.started`, `handoff.completed`, `handoff.cancelled`
- `handoff.timeout`, `handoff.timeout.extended`, `handoff.timeout.aborted`, `handoff.timeout.forced`
- `handoff.duration`
- `handoff.draining.partitions` (gauge)

В Prometheus они экспонируются через стандартное преобразование в snake_case с суффиксами `_total`,
`_seconds` и т.д.
