alter table investment_review_plan_modifications
    add column is_deleted tinyint not null default 0 after plan_snapshot_json,
    add column deleted_at datetime default null after is_deleted,
    add index idx_investment_review_plan_modifications_deleted (plan_id, is_deleted, modification_time);

alter table investment_review_executions
    add column is_deleted tinyint not null default 0 after note,
    add column deleted_at datetime default null after is_deleted,
    add index idx_investment_review_executions_deleted (plan_id, is_deleted, execution_time);
