create table if not exists task_queue_dead_letter (
    task_id             uuid primary key,
    task_type           varchar(128) not null,
    payload             text not null,
    partition_key       varchar(512),
    partition_num       integer not null,
    delay_count         bigint not null,
    attempt_count       bigint not null,
    failed_worker_id    varchar(100) not null,
    reason              varchar(64) not null,
    error_class         varchar(512),
    error_message       text,
    original_created_at timestamptz not null,
    dead_lettered_at    timestamptz not null
);

alter table task_queue_dead_letter
    drop constraint if exists task_queue_dead_letter_reason_ck;

alter table task_queue_dead_letter
    add constraint task_queue_dead_letter_reason_ck
        check (reason in ('NON_RETRYABLE', 'RETRY_EXHAUSTED'));

create index if not exists idx_task_queue_dead_letter_dead_lettered_at
    on task_queue_dead_letter (dead_lettered_at);

create index if not exists idx_task_queue_dead_letter_task_type
    on task_queue_dead_letter (task_type);

comment on table task_queue_dead_letter is
    'Архив финализированных задач, удаленных из основной очереди после non-retryable ошибки или исчерпания retry.';
comment on column task_queue_dead_letter.task_id is
    'Идентификатор исходной задачи из task_queue.';
comment on column task_queue_dead_letter.task_type is
    'Тип задачи для выбора обработчика.';
comment on column task_queue_dead_letter.payload is
    'Полезная нагрузка исходной задачи.';
comment on column task_queue_dead_letter.partition_key is
    'Ключ партиционирования исходной задачи.';
comment on column task_queue_dead_letter.partition_num is
    'Логическая партиция исходной задачи.';
comment on column task_queue_dead_letter.delay_count is
    'Количество уже примененных retry-задержек на момент финализации.';
comment on column task_queue_dead_letter.attempt_count is
    'Логический номер попытки, на которой задача была финализирована.';
comment on column task_queue_dead_letter.failed_worker_id is
    'WorkerId, владевший задачей в момент финализации.';
comment on column task_queue_dead_letter.reason is
    'Причина финализации: NON_RETRYABLE или RETRY_EXHAUSTED.';
comment on column task_queue_dead_letter.error_class is
    'Класс последней ошибки обработки, если доступен.';
comment on column task_queue_dead_letter.error_message is
    'Сообщение последней ошибки обработки, если доступно.';
comment on column task_queue_dead_letter.original_created_at is
    'Время создания исходной задачи.';
comment on column task_queue_dead_letter.dead_lettered_at is
    'Время переноса задачи в dead-letter.';
