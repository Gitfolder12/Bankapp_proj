-- Create HWM audit log table
CREATE TABLE hwm_audit_log (
    log_date       DATE         DEFAULT SYSDATE,
    owner          VARCHAR2(30),
    table_name     VARCHAR2(128),
    partition_name VARCHAR2(128),
    allocated_mb   NUMBER,      -- total allocated MB
    unused_mb      NUMBER,      -- unused MB above HWM
    reclaim_pct    NUMBER(5,2), -- reclaimable percentage
    status         VARCHAR2(20) -- 'HWM_ISSUE' or 'ERROR'
)
TABLESPACE USERS;  -- choose appropriate tablespace

-- Optional index to speed up queries
CREATE INDEX idx_hwm_audit_log_owner
    ON hwm_audit_log(owner, table_name, partition_name);

-- Optional: add comments for clarity
COMMENT ON TABLE hwm_audit_log IS 'Logs tables/partitions exceeding HWM threshold for space reclaim';
COMMENT ON COLUMN log_date IS 'Timestamp of the scan';
COMMENT ON COLUMN allocated_mb IS 'Total allocated MB for the segment/partition';
COMMENT ON COLUMN unused_mb IS 'Unused MB above HWM';
COMMENT ON COLUMN reclaim_pct IS 'Percentage of reclaimable space';
COMMENT ON COLUMN status IS 'HWM status: HWM_ISSUE or ERROR';


create or replace PROCEDURE prc_identify_hwm_issues (
    p_owner              IN VARCHAR2 DEFAULT USER,
    p_table_name         IN VARCHAR2 DEFAULT NULL,
    p_min_size_mb        IN NUMBER   DEFAULT 500,
    p_reclaim_threshold  IN NUMBER   DEFAULT 30
)
IS
    -- Space variables
    l_total_blocks       NUMBER;
    l_total_bytes        NUMBER;
    l_unused_blocks      NUMBER;
    l_unused_bytes       NUMBER;
    l_last_used_extent   NUMBER;
    l_last_used_block    NUMBER;
    l_last_used_block_id NUMBER;
    l_reclaim_pct        NUMBER;

BEGIN

    /*
      Unified cursor:
      - TABLE PARTITION
      - TABLE SUBPARTITION
    */
    FOR r IN (
        SELECT s.owner,
               s.segment_name     AS table_name,
               s.partition_name   AS partition_or_sub,
               s.segment_type,
               s.bytes
        FROM   dba_segments s
        WHERE  s.owner = UPPER(p_owner)
        AND    s.segment_type IN ('TABLE PARTITION', 'TABLE SUBPARTITION')
        AND    s.segment_name = UPPER(p_table_name)
        AND    s.bytes >= p_min_size_mb * 1024 * 1024
    )
    LOOP
        BEGIN
            -- Get unused space
            DBMS_SPACE.UNUSED_SPACE(
                segment_owner             => r.owner,
                segment_name              => r.table_name,
                segment_type              => r.segment_type,
                partition_name            => r.partition_or_sub,
                total_blocks              => l_total_blocks,
                total_bytes               => l_total_bytes,
                unused_blocks             => l_unused_blocks,
                unused_bytes              => l_unused_bytes,
                last_used_extent_file_id  => l_last_used_extent,
                last_used_extent_block_id => l_last_used_block,
                last_used_block           => l_last_used_block_id
            );

            -- Calculate reclaim %
            IF l_total_bytes > 0 THEN
                l_reclaim_pct :=
                    ROUND((l_unused_bytes / l_total_bytes) * 100, 2);
            ELSE
                l_reclaim_pct := 0;
            END IF;

            -- Log only if threshold exceeded
            IF l_reclaim_pct >= p_reclaim_threshold THEN
                INSERT INTO hwm_audit_log (
                    log_date,
                    owner,
                    table_name,
                    partition_name,
                    allocated_mb,
                    unused_mb,
                    reclaim_pct,
                    status
                )
                VALUES (
                    SYSDATE,
                    r.owner,
                    r.table_name,
                    r.partition_or_sub,
                    ROUND(l_total_bytes / 1024 / 1024, 2),
                    ROUND(l_unused_bytes / 1024 / 1024, 2),
                    l_reclaim_pct,
                    'HWM_ISSUE'
                );
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                INSERT INTO hwm_audit_log (
                    log_date,
                    owner,
                    table_name,
                    partition_name,
                    allocated_mb,
                    unused_mb,
                    reclaim_pct,
                    status
                )
                VALUES (
                    SYSDATE,
                    r.owner,
                    r.table_name,
                    r.partition_or_sub,
                    NULL,
                    NULL,
                    NULL,
                    'ERROR'
                );
        END;
    END LOOP;

    COMMIT;

END prc_identify_hwm_issues;
