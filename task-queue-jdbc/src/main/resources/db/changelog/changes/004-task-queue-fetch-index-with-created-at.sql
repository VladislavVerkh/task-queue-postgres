-- Align dequeue index with ORDER BY (available_at, created_at, task_id).
create index if not exists idx_task_queue_partition_num_worker_id_available_at_created_at_task_id
    on task_queue (partition_num, worker_id, available_at, created_at, task_id);

-- Remove legacy index that does not include created_at in ordering key.
drop index if exists idx_task_queue_partition_num_worker_id_available_at_task_id;
