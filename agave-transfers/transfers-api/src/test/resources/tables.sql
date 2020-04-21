SET PROPERTY "sql.enforce_strict_size" TRUE;
SET PROPERTY "sql.ignorecase" FALSE;

DROP TABLE IF EXISTS transfertasks;

CREATE TABLE IF NOT EXISTS transfertasks (
    "id" BIGINT IDENTITY PRIMARY KEY,
    "attempts" INTEGER,
    "lastAttempt" TIMESTAMP,
    "nextAttempt" TIMESTAMP,
    "bytes_transferred" DOUBLE,
    "created" TIMESTAMP,
    "dest" VARCHAR(2048) NOT NULL,
    "end_time" TIMESTAMP,
    "event_id" VARCHAR(255),
    "last_updated" TIMESTAMP NOT NULL,
    "owner" VARCHAR(32) NOT NULL,
    "source" VARCHAR(2048) NOT NULL,
    "start_time" TIMESTAMP,
    "status" VARCHAR(16),
    "tenant_id" VARCHAR(128) NOT NULL,
    "total_size" DOUBLE,
    "transfer_rate" DOUBLE,
    "parent_task" BIGINT,
    "root_task" BIGINT,
    "uuid" VARCHAR(64) NOT NULL,
    "total_files" INTEGER DEFAULT 0 NOT NULL,
    "total_skipped_files" BIGINT DEFAULT 0 NOT NULL,
    CONSTRAINT "fk_parent_task" FOREIGN KEY ("parent_task") REFERENCES transfertasks,
    CONSTRAINT "fk_root_task" FOREIGN KEY ("root_task") REFERENCES transfertasks,
    CONSTRAINT "uuid" UNIQUE ("uuid")
);