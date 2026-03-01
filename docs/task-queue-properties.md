# Task Queue: Конфигурационные Свойства

Префикс свойств: `task.queue`.

| Имя свойства                                       | Тип            | Описание                                                          | Значение по умолчанию | Ограничения                                   |
|----------------------------------------------------|----------------|-------------------------------------------------------------------|-----------------------|-----------------------------------------------|
| `task.queue.runtime-enabled`                       | `boolean`      | Включает/выключает запуск worker runtime                          | `true`                | `true/false`                                  |
| `task.queue.partition-count`                       | `int`          | Количество логических партиций                                    | `20`                  | `> 0`                                         |
| `task.queue.worker-count`                          | `int`          | Количество worker-потоков runtime в одном инстансе                | `2`                   | `> 0`                                         |
| `task.queue.poll-batch-size`                       | `int`          | Максимальный размер пачки задач за один poll                      | `50`                  | `> 0`                                         |
| `task.queue.poll-interval`                         | `Duration`     | Пауза между poll при пустой выборке                               | `250ms`               | `>= 1ms`                                      |
| `task.queue.heartbeat-interval`                    | `Duration`     | Период heartbeat                                                  | `1s`                  | `>= 1ms`                                      |
| `task.queue.heartbeat-task-timeout`                | `Duration`     | Таймаут одной операции heartbeat                                  | `3s`                  | `>= 1ms`                                      |
| `task.queue.process-timeout`                       | `Duration`     | Базовый timeout «живости» процесса/воркера                        | `10s`                 | `>= 1ms`                                      |
| `task.queue.heartbeat-deviation`                   | `Duration`     | Допустимое отклонение heartbeat при cleanup                       | `3s`                  | `>= 0`                                        |
| `task.queue.dead-process-timeout-multiplier`       | `int`          | Множитель к timeout воркера при определении dead worker           | `3`                   | `> 0`                                         |
| `task.queue.unregistration-action`                 | `enum`         | Действие при проблемах регистрации/heartbeat                      | `REREGISTER`          | `STOP` / `REREGISTER`                         |
| `task.queue.stop-application-on-heartbeat-timeout` | `boolean`      | Принудительно останавливать JVM при heartbeat timeout             | `false`               | `true/false`                                  |
| `task.queue.cleanup-interval`                      | `Duration`     | Интервал запуска cleanup dead workers                             | `1s`                  | `>= 1ms`                                      |
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
| `task.queue.handling-transaction-mode`             | `enum`         | Режим обработки хэндлера: в общей транзакции с `ack` или без нее  | `TRANSACTIONAL`       | `TRANSACTIONAL` / `NON_TRANSACTIONAL`         |
| `task.queue.handoff-drain-timeout`                 | `Duration`     | Максимальная длительность `DRAINING` до применения timeout policy | `30s`                 | `>= 1ms`                                      |
| `task.queue.handoff-reconcile-interval`            | `Duration`     | Период фоновой синхронизации `DRAINING` handoff                   | `1s`                  | `>= 1ms`                                      |
| `task.queue.handoff-timeout-action`                | `enum`         | Действие при таймауте дренажа                                     | `EXTEND`              | `EXTEND` / `ABORT` / `FORCE`                  |
| `task.queue.rebalance-lock-key`                    | `long`         | Ключ PostgreSQL advisory lock для ребаланса/выборки               | `584231947015`        | `long`                                        |

## Пример конфигурации

```properties
# Базовая настройка
task.queue.partition-count=20
task.queue.worker-count=4
task.queue.runtime-enabled=true
# Retry
task.queue.retry-max-attempts=5
task.queue.retry-backoff-strategy=EXPONENTIAL
task.queue.retry-initial-delay=1s
task.queue.retry-max-delay=30s
task.queue.retry-backoff-multiplier=2.0
# Классификация ошибок
task.queue.retryable-exceptions=java.io.IOException,java.util.concurrent.TimeoutException
task.queue.not-retryable-exceptions=java.lang.IllegalArgumentException
# Режим транзакций хэндлера
task.queue.handling-transaction-mode=TRANSACTIONAL
```

## Handoff/Drain: как читать настройки

- `handoff-drain-timeout`: сколько максимум ждать завершения in-flight задач у старого owner в состоянии `DRAINING`.
- `handoff-reconcile-interval`: как часто runtime перепроверяет `DRAINING` партиции даже без новых register/cleanup событий.
- `handoff-timeout-action`:
  - `EXTEND` — безопасный дефолт: ownership не передается, дедлайн продлевается.
  - `ABORT` — откат handoff: партиция возвращается в `ACTIVE` у текущего owner.
  - `FORCE` — принудительная передача ownership новому owner даже при наличии in-flight у старого.
