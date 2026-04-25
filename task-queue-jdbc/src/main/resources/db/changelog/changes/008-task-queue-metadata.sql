create table if not exists task_queue_metadata (
    metadata_key   varchar(128) primary key,
    metadata_value text not null,
    updated_at     timestamptz not null default clock_timestamp()
);

comment on table task_queue_metadata is
    'Системные metadata-настройки очереди, которые должны оставаться совместимыми между запусками.';
comment on column task_queue_metadata.metadata_key is
    'Ключ metadata-настройки.';
comment on column task_queue_metadata.metadata_value is
    'Значение metadata-настройки.';
comment on column task_queue_metadata.updated_at is
    'Время последнего изменения metadata-настройки.';
