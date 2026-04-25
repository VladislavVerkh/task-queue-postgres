alter table if exists task_queue
    add column if not exists locked_at timestamptz,
    add column if not exists lease_until timestamptz;

create index if not exists idx_task_queue_lease_until_worker_id
    on task_queue (lease_until, worker_id)
    where worker_id is not null;

comment on column task_queue.locked_at is
    'Время, когда задача была захвачена воркером для обработки.';
comment on column task_queue.lease_until is
    'Дедлайн lease для in-flight задачи; после него cleanup может освободить задачу.';
