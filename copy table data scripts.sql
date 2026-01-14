
CREATE TABLE HWM_COPY_LOG (
    src_owner        VARCHAR2(30),
    src_table        VARCHAR2(128),
    dst_owner        VARCHAR2(30),
    dst_table        VARCHAR2(128),
    partition_name   VARCHAR2(128),
    copied_at        DATE,
    status           VARCHAR2(20),   -- SUCCESS / ERROR
    error_msg        VARCHAR2(4000)
);

============================================================
CREATE OR REPLACE PROCEDURE log_hwm_copy (
    p_src_owner      IN VARCHAR2,
    p_src_table      IN VARCHAR2,
    p_dst_owner      IN VARCHAR2,
    p_dst_table      IN VARCHAR2,
    p_partition_name IN VARCHAR2,
    p_status         IN VARCHAR2,       -- 'SUCCESS' or 'ERROR'
    p_error_msg      IN VARCHAR2 DEFAULT NULL
) IS
BEGIN
    INSERT INTO HWM_COPY_LOG (
        src_owner, src_table,
        dst_owner, dst_table,
        partition_name,
        copied_at,
        status,
        error_msg
    )
    VALUES (
        UPPER(p_src_owner),
        UPPER(p_src_table),
        UPPER(p_dst_owner),
        UPPER(p_dst_table),
        p_partition_name,
        SYSDATE,
        UPPER(p_status),
        SUBSTR(p_error_msg,1,4000)
    );

    COMMIT;
END;
/




========================================================================================
CREATE OR REPLACE PROCEDURE copy_partitions_step3 (
    p_src_owner   IN VARCHAR2,
    p_src_table   IN VARCHAR2,
    p_dst_owner   IN VARCHAR2,
    p_dst_table   IN VARCHAR2,
    p_parallel    IN PLS_INTEGER DEFAULT 8
) IS
    l_exists NUMBER;
BEGIN
    -- Enable parallel DML
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

    DBMS_OUTPUT.PUT_LINE('>>> Parallel DML enabled (degree = ' || p_parallel || ')');
    DBMS_OUTPUT.PUT_LINE('>>> Starting partition-by-partition copy...');

    FOR r IN (
        SELECT partition_name
        FROM   dba_tab_partitions
        WHERE  table_owner = UPPER(p_src_owner)
        AND    table_name  = UPPER(p_src_table)
        ORDER BY partition_position
    )
    LOOP
        ----------------------------------------------------------------------
        -- Skip if already logged as SUCCESS (resume support)
        ----------------------------------------------------------------------
        SELECT COUNT(*)
        INTO   l_exists
        FROM   HWM_COPY_LOG
        WHERE  src_owner      = UPPER(p_src_owner)
        AND    src_table      = UPPER(p_src_table)
        AND    dst_owner      = UPPER(p_dst_owner)
        AND    dst_table      = UPPER(p_dst_table)
        AND    partition_name = r.partition_name
        AND    status         = 'SUCCESS';

        IF l_exists > 0 THEN
            DBMS_OUTPUT.PUT_LINE('>>> Skipping '||r.partition_name||' (already done).');
            CONTINUE;
        END IF;

        ----------------------------------------------------------------------
        -- Copy partition
        ----------------------------------------------------------------------
        DBMS_OUTPUT.PUT_LINE('>>> Copying partition: ' || r.partition_name);

        BEGIN
            EXECUTE IMMEDIATE '
                INSERT /*+ APPEND PARALLEL(dst,' || p_parallel || ') */ INTO 
                    ' || p_dst_owner || '.' || p_dst_table || ' PARTITION(' || r.partition_name || ') dst
                SELECT /*+ PARALLEL(src,' || p_parallel || ') */ * 
                FROM   ' || p_src_owner || '.' || p_src_table || ' PARTITION(' || r.partition_name || ') src
            ';

            COMMIT;

            DBMS_OUTPUT.PUT_LINE('    -> '||r.partition_name||' copied & committed.');

            ----------------------------------------------------------------------
            -- Log success using the ONE unified logging procedure
            ----------------------------------------------------------------------
            log_hwm_copy(
                p_src_owner      => p_src_owner,
                p_src_table      => p_src_table,
                p_dst_owner      => p_dst_owner,
                p_dst_table      => p_dst_table,
                p_partition_name => r.partition_name,
                p_status         => 'SUCCESS'
            );

        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('    -> ERROR on '||r.partition_name||': '||SQLERRM);

                ----------------------------------------------------------------------
                -- Log error with ONE logging procedure
                ----------------------------------------------------------------------
                log_hwm_copy(
                    p_src_owner      => p_src_owner,
                    p_src_table      => p_src_table,
                    p_dst_owner      => p_dst_owner,
                    p_dst_table      => p_dst_table,
                    p_partition_name => r.partition_name,
                    p_status         => 'ERROR',
                    p_error_msg      => SQLERRM
                );

                RAISE;  -- stop on first failure (safe)
        END;

    END LOOP;

    DBMS_OUTPUT.PUT_LINE('>>> All partitions processed.');
END;
/
