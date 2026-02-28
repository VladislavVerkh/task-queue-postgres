create table if not exists task_worker_registry (
    worker_id       varchar(100) primary key,
    heartbeat_last  timestamptz not null,
    timeout_sec     integer not null check (timeout_sec >= 0),
    created_at      timestamptz not null
);

comment on table task_worker_registry is
    'Реестр активных воркеров очереди задач и их heartbeat-состояния.';
comment on column task_worker_registry.worker_id is
    'Уникальный идентификатор воркера (обычно host/pid/thread/случайный суффикс).';
comment on column task_worker_registry.heartbeat_last is
    'Время последнего успешного heartbeat от воркера.';
comment on column task_worker_registry.timeout_sec is
    'Базовый таймаут живости воркера в секундах.';
comment on column task_worker_registry.created_at is
    'Время регистрации воркера.';

create table if not exists task_worker_partition_assignment (
    partition_num   integer primary key,
    worker_id       varchar(100) not null references task_worker_registry(worker_id) on delete cascade,
    owner_changed_at timestamptz not null,
    owner_change_count bigint not null
);

comment on table task_worker_partition_assignment is
    'Текущее закрепление логических партиций очереди за воркерами.';
comment on column task_worker_partition_assignment.partition_num is
    'Номер логической партиции (1..partition_count).';
comment on column task_worker_partition_assignment.worker_id is
    'Воркер, за которым сейчас закреплена эта партиция.';
comment on column task_worker_partition_assignment.owner_changed_at is
    'Время последнего назначения/переназначения партиции.';
comment on column task_worker_partition_assignment.owner_change_count is
    'Количество изменений владельца этой партиции.';

create index if not exists task_worker_partition_assignment_worker_i
    on task_worker_partition_assignment (worker_id);

create table if not exists task_queue (
    task_id          uuid primary key,
    task_type        varchar(128) not null,
    payload          text not null,
    partition_key    varchar(512),
    partition_num    integer not null,
    available_at     timestamptz not null,
    delay_count      bigint not null default 0,
    worker_id        varchar(100) references task_worker_registry(worker_id) on delete set null,
    created_at       timestamptz not null
);

alter table task_queue set (
    autovacuum_vacuum_scale_factor = 0.005,
    autovacuum_vacuum_threshold = 100,
    autovacuum_vacuum_insert_scale_factor = 0.01,
    autovacuum_vacuum_insert_threshold = 200,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 100,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 2
);

comment on table task_queue is
    'Персистентная очередь задач с отложенными ретраями и блокировкой воркером.';
comment on column task_queue.task_id is
    'Уникальный идентификатор задачи.';
comment on column task_queue.task_type is
    'Тип задачи для выбора соответствующего обработчика.';
comment on column task_queue.payload is
    'Полезная нагрузка задачи (JSON/текст), передаваемая обработчику.';
comment on column task_queue.partition_key is
    'Ключ партиционирования для сохранения порядка обработки связанных задач.';
comment on column task_queue.partition_num is
    'Вычисленный номер логической партиции на основе хэша partition_key.';
comment on column task_queue.available_at is
    'Минимальное время, после которого задачу можно забирать в обработку.';
comment on column task_queue.delay_count is
    'Количество уже примененных задержек ретрая для задачи.';
comment on column task_queue.worker_id is
    'Воркер, который сейчас захватил задачу для обработки; null, если задача не захвачена.';
comment on column task_queue.created_at is
    'Время создания задачи.';

create index if not exists task_queue_fetch_i
    on task_queue (partition_num, worker_id, available_at, task_id);

create index if not exists task_queue_worker_i
    on task_queue (worker_id);
