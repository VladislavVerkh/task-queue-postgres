-- Единый формат имен secondary-индексов: idx_<table>_<columns>.
alter index if exists task_worker_partition_assignment_worker_i
    rename to idx_task_worker_partition_assignment_worker_id;

alter index if exists task_queue_fetch_i
    rename to idx_task_queue_partition_num_worker_id_available_at_task_id;

alter index if exists task_queue_worker_i
    rename to idx_task_queue_worker_id;

-- Ускоряет выборку "старых" heartbeat при cleanup dead workers.
create index if not exists idx_task_worker_registry_heartbeat_last
    on task_worker_registry (heartbeat_last);
