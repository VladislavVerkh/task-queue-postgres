-- Добавляем состояние handoff партиции, pending-owner и дедлайны дренажа.
alter table if exists task_worker_partition_assignment
    add column if not exists handoff_state varchar(16);

update task_worker_partition_assignment
   set handoff_state = 'ACTIVE'
 where handoff_state is null;

alter table if exists task_worker_partition_assignment
    alter column handoff_state set default 'ACTIVE';

alter table if exists task_worker_partition_assignment
    alter column handoff_state set not null;

alter table if exists task_worker_partition_assignment
    drop constraint if exists task_worker_partition_assignment_handoff_state_ck;

alter table if exists task_worker_partition_assignment
    add constraint task_worker_partition_assignment_handoff_state_ck
        check (handoff_state in ('ACTIVE', 'DRAINING'));

alter table if exists task_worker_partition_assignment
    add column if not exists pending_worker_id varchar(100),
    add column if not exists drain_started_at timestamptz,
    add column if not exists drain_deadline_at timestamptz;

alter table if exists task_worker_partition_assignment
    drop constraint if exists task_worker_partition_assignment_pending_worker_fk;

alter table if exists task_worker_partition_assignment
    add constraint task_worker_partition_assignment_pending_worker_fk
        foreign key (pending_worker_id) references task_worker_registry(worker_id) on delete set null;

create index if not exists idx_task_worker_partition_assignment_active_worker_partition
    on task_worker_partition_assignment (worker_id, partition_num)
    where handoff_state = 'ACTIVE';

create index if not exists idx_task_worker_partition_assignment_handoff_state
    on task_worker_partition_assignment (handoff_state);

comment on column task_worker_partition_assignment.handoff_state is
    'Состояние закрепления: ACTIVE (обычная работа) или DRAINING (ожидание завершения in-flight перед сменой владельца).';
comment on column task_worker_partition_assignment.pending_worker_id is
    'Целевой владелец партиции после завершения дренажа; null для ACTIVE.';
comment on column task_worker_partition_assignment.drain_started_at is
    'Время старта дренажа партиции.';
comment on column task_worker_partition_assignment.drain_deadline_at is
    'Дедлайн дренажа партиции, после которого применяется политика handoffTimeoutAction.';
