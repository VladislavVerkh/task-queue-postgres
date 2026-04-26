# Task Queue: Конфигурационные Свойства

Префикс свойств: `task.queue`.

| Имя свойства                                       | Тип            | Описание                                                          | Значение по умолчанию | Ограничения                                   |
|----------------------------------------------------|----------------|-------------------------------------------------------------------|-----------------------|-----------------------------------------------|
| `task.queue.runtime-enabled`                       | `boolean`      | Включает/выключает запуск worker runtime                          | `true`                | `true/false`                                  |
| `task.queue.partition-count`                       | `int`          | Количество логических партиций                                    | `20`                  | `> 0`                                         |
| `task.queue.worker-count`                          | `int`          | Количество worker-потоков runtime в одном инстансе                | `2`                   | `> 0`                                         |
| `task.queue.poll-batch-size`                       | `int`          | Максимальный размер пачки задач за один poll                      | `50`                  | `> 0`                                         |
| `task.queue.poll-max-tasks-per-partition`          | `int`          | Максимум задач из одной партиции за один poll                     | `5`                   | `> 0`                                         |
| `task.queue.poll-interval`                         | `Duration`     | Пауза между poll при пустой выборке                               | `250ms`               | `>= 1ms`                                      |
| `task.queue.task-lease-timeout`                    | `Duration`     | Максимальное время владения in-flight задачей до cleanup          | `5m`                  | `>= 1ms`                                      |
| `task.queue.heartbeat-interval`                    | `Duration`     | Период heartbeat                                                  | `1s`                  | `>= 1ms`                                      |
| `task.queue.heartbeat-task-timeout`                | `Duration`     | Таймаут одной операции heartbeat                                  | `3s`                  | `>= 1ms`                                      |
| `task.queue.process-timeout`                       | `Duration`     | Базовый timeout «живости» процесса/воркера                        | `10s`                 | `>= 1ms`                                      |
| `task.queue.heartbeat-deviation`                   | `Duration`     | Допустимое отклонение heartbeat при cleanup                       | `3s`                  | `>= 0`                                        |
| `task.queue.dead-process-timeout-multiplier`       | `int`          | Множитель к timeout воркера при определении dead worker           | `3`                   | `> 0`                                         |
| `task.queue.unregistration-action`                 | `enum`         | Действие при проблемах регистрации/heartbeat                      | `REREGISTER`          | `STOP` / `REREGISTER`                         |
| `task.queue.stop-application-on-heartbeat-timeout` | `boolean`      | Запрашивать остановку приложения при heartbeat timeout            | `false`               | `true/false`                                  |
| `task.queue.cleanup-interval`                      | `Duration`     | Интервал запуска cleanup dead workers                             | `1s`                  | `>= 1ms`                                      |
| `task.queue.queue-metrics-interval`                | `Duration`     | Интервал обновления aggregate gauge-метрик очереди                | `10s`                 | `>= 1ms`                                      |
| `task.queue.partition-lag-metrics-enabled`         | `boolean`      | Включает per-partition lag gauge-метрики                          | `false`               | `true/false`                                  |
| `task.queue.shutdown-timeout`                      | `Duration`     | Сколько ждать завершения in-flight задач при shutdown             | `20s`                 | `>= 1ms`                                      |
| `task.queue.cleanup-batch-size`                    | `int`          | Максимум удаляемых dead workers за один cleanup                   | `32`                  | `> 0`                                         |
| `task.queue.retry-max-attempts`                    | `int`          | Максимум retry-попыток                                            | `3`                   | `>= 0`                                        |
| `task.queue.retry-backoff-strategy`                | `enum`         | Стратегия задержки retry                                          | `EXPONENTIAL`         | `FIXED` / `EXPONENTIAL`                       |
| `task.queue.retry-initial-delay`                   | `Duration`     | Начальная задержка retry                                          | `5s`                  | `>= 1ms`                                      |
| `task.queue.retry-max-delay`                       | `Duration`     | Верхняя граница retry-задержки                                    | `5m`                  | `>= 1ms`, `>= retry-initial-delay`            |
| `task.queue.retry-backoff-multiplier`              | `double`       | Множитель экспоненциального backoff                               | `2.0`                 | `> 0`                                         |
| `task.queue.retry-jitter-factor`                   | `double`       | Джиттер (случайное отклонение) для backoff                        | `0.0`                 | `0.0..1.0`                                    |
| `task.queue.retryable-exceptions`                  | `List<String>` | Список FQCN retryable-исключений                                  | `[]`                  | Классы должны существовать и быть `Throwable` |
| `task.queue.not-retryable-exceptions`              | `List<String>` | Список FQCN non-retryable-исключений                              | `[]`                  | Классы должны существовать и быть `Throwable` |
| `task.queue.retry-exception-traverse-causes`       | `boolean`      | Проверять цепочку `cause` при классификации                       | `true`                | `true/false`                                  |
| `task.queue.retry-default-retryable`               | `boolean`      | Поведение по умолчанию, если класс не матчится                    | `true`                | `true/false`                                  |
| `task.queue.dead-letter-enabled`                   | `boolean`      | Копировать финализированные задачи в `task_queue_dead_letter`     | `false`               | `true/false`                                  |
| `task.queue.dead-letter-retention`                 | `Duration`     | Сколько хранить DLQ-записи перед фоновой очисткой (`0` = off)     | `0`                   | `>= 0`                                        |
| `task.queue.handling-transaction-mode`             | `enum`         | Режим обработки хэндлера: в общей транзакции с `ack` или без нее  | `TRANSACTIONAL`       | `TRANSACTIONAL` / `NON_TRANSACTIONAL`         |
| `task.queue.handoff-drain-timeout`                 | `Duration`     | Максимальная длительность `DRAINING` до применения timeout policy | `30s`                 | `>= 1ms`                                      |
| `task.queue.handoff-reconcile-interval`            | `Duration`     | Период фоновой синхронизации `DRAINING` handoff                   | `1s`                  | `>= 1ms`                                      |
| `task.queue.handoff-timeout-action`                | `enum`         | Действие при таймауте дренажа                                     | `EXTEND`              | `EXTEND` / `ABORT` / `FORCE`                  |
| `task.queue.rebalance-lock-key`                    | `long`         | Ключ PostgreSQL advisory lock для ребаланса/выборки               | `584231947015`        | `long`                                        |
| `task.queue.jdbc-statement-timeout`                | `Duration`     | Query timeout для task-queue `JdbcTemplate` (`0` = off)           | `0`                   | `>= 0`                                        |

## Пример конфигурации

```properties
# Базовая настройка
task.queue.partition-count=20
task.queue.worker-count=4
task.queue.runtime-enabled=true
task.queue.poll-max-tasks-per-partition=5
task.queue.task-lease-timeout=5m
task.queue.queue-metrics-interval=10s
task.queue.partition-lag-metrics-enabled=false
task.queue.shutdown-timeout=20s
task.queue.jdbc-statement-timeout=30s
# Retry
task.queue.retry-max-attempts=5
task.queue.retry-backoff-strategy=EXPONENTIAL
task.queue.retry-initial-delay=1s
task.queue.retry-max-delay=30s
task.queue.retry-backoff-multiplier=2.0
task.queue.dead-letter-enabled=true
task.queue.dead-letter-retention=14d
# Классификация ошибок
task.queue.retryable-exceptions=java.io.IOException,java.util.concurrent.TimeoutException
task.queue.not-retryable-exceptions=java.lang.IllegalArgumentException
# Режим транзакций хэндлера
task.queue.handling-transaction-mode=TRANSACTIONAL
```

## Handoff/Drain: как читать настройки

- `task-lease-timeout`: максимальное время, в течение которого задача может оставаться in-flight у
  одного worker без `ack/retry`; runtime автоматически продлевает lease во время обработки, а после
  истечения cleanup освобождает задачу для повторной выдачи.
- `queue-metrics-interval`: как часто cleanup-loop пересчитывает aggregate gauge-метрики очереди.
- `partition-lag-metrics-enabled`: включает отдельные `partition.*` lag gauges; по умолчанию они
  выключены, чтобы не делать лишние grouped-запросы на больших очередях.
- `shutdown-timeout`: сколько runtime ждет завершения текущих задач при штатном shutdown перед
  принудительным interrupt worker-потоков.
- `handoff-drain-timeout`: сколько максимум ждать завершения in-flight задач у старого owner в состоянии `DRAINING`.
- `handoff-reconcile-interval`: как часто runtime перепроверяет `DRAINING` партиции даже без новых register/cleanup событий.
- `handoff-timeout-action`:
  - `EXTEND` — безопасный дефолт: ownership не передается, дедлайн продлевается.
  - `ABORT` — откат handoff: партиция возвращается в `ACTIVE` у текущего owner.
  - `FORCE` — принудительная передача ownership новому owner даже при наличии in-flight у старого.

## Dead Letter

`task.queue.dead-letter-enabled=false` сохраняет старое поведение: non-retryable задачи и задачи с
исчерпанными retry удаляются из основной очереди без архивирования.

Если `task.queue.dead-letter-enabled=true`, перед удалением такая задача копируется в
`task_queue_dead_letter` вместе с payload, partition key, количеством попыток, workerId, причиной
финализации (`NON_RETRYABLE` или `RETRY_EXHAUSTED`) и последней ошибкой. Запись выполняется в той же
транзакции, что и owner-checked удаление из `task_queue`.

`TaskDeadLetterService.requeue(taskId)` возвращает запись из `task_queue_dead_letter` в основную
очередь с `delay_count=0` и доступностью "сейчас"; перегрузка с `availableAt` позволяет отложить
повторную обработку. Для новых задач предпочтительна перегрузка
`TaskProducer.enqueueDelayed(..., Duration delay)`: задержка считается от времени PostgreSQL, без
зависимости от clock приложения. `TaskDeadLetterService.deleteOlderThan(...)` удаляет DLQ-записи
вручную, а `task.queue.dead-letter-retention > 0` включает фоновую retention-очистку.

## Partition-count guard

При первом старте библиотека сохраняет `task.queue.partition-count` в `task_queue_metadata`. При
последующих стартах значение проверяется fail-fast: если конфигурация изменилась, приложение не
стартует до выполнения отдельной миграционной процедуры. Это защищает порядок по `partition_key`,
потому что номер партиции вычисляется как `hash(partition_key) % partition_count + 1`.

## DB/pool hardening

Библиотека использует application `DataSource` и `transactionManager`, поэтому sizing пула задается
стандартными свойствами приложения. Практический минимум:

- `maximumPoolSize >= task.queue.worker-count + 2` на инстанс: worker-потоки, heartbeat/cleanup и
  пользовательские SQL внутри handlers.
- `task.queue.jdbc-statement-timeout=30s..60s` для защиты внутренних SQL очереди от зависаний.
- PostgreSQL JDBC `socketTimeout` и `connectTimeout` задавайте в JDBC URL, например
  `?connectTimeout=5&socketTimeout=60`.
- Для Hikari используйте `spring.datasource.hikari.connection-timeout` меньше внешнего SLA, чтобы
  worker быстро получал ошибку при исчерпании пула.
