-- Применяем такие же агрессивные per-table autovacuum/analyze параметры,
-- как у task_queue, к высокоизменяемым таблицам координации воркеров.
alter table if exists task_worker_registry set (
    autovacuum_vacuum_scale_factor = 0.005,
    autovacuum_vacuum_threshold = 100,
    autovacuum_vacuum_insert_scale_factor = 0.01,
    autovacuum_vacuum_insert_threshold = 200,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 100,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 2
);

alter table if exists task_worker_partition_assignment set (
    autovacuum_vacuum_scale_factor = 0.005,
    autovacuum_vacuum_threshold = 100,
    autovacuum_vacuum_insert_scale_factor = 0.01,
    autovacuum_vacuum_insert_threshold = 200,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 100,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 2
);
