--------------------------------------------------------
--  File created - Friday-January-30-2026   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure LOAD_PARTITIONS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "CR"."LOAD_PARTITIONS" (
    p_src_owner    IN VARCHAR2,
    p_dst_owner    IN VARCHAR2,
    p_src_table    IN VARCHAR2,
    p_dst_table    IN VARCHAR2,
    p_parallel     IN PLS_INTEGER DEFAULT 8,
    p_max_runtime  IN NUMBER DEFAULT 60  -- in minutes
) IS
    l_dummy      NUMBER;
    l_src_cnt    NUMBER := 0;
    l_trg_cnt    NUMBER := 0;
    v_start_time INTEGER := DBMS_UTILITY.GET_TIME;

    ex_runtime_exceeded EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_runtime_exceeded, -20001);

    PROCEDURE check_runtime IS
    BEGIN
        IF (DBMS_UTILITY.GET_TIME - v_start_time)/100 > (p_max_runtime*60) THEN
            RAISE ex_runtime_exceeded;
        END IF;
    END;
BEGIN
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

    FOR p IN (
        SELECT partition_name
        FROM dba_tab_partitions
        WHERE table_owner = UPPER(p_src_owner)
          AND table_name  = UPPER(p_src_table)
        ORDER BY partition_position
    ) LOOP
        check_runtime;

        -- Skip already successful partitions
        BEGIN
            SELECT 1 INTO l_dummy
            FROM hwm_copy_log
            WHERE p_src_owner = UPPER(p_src_owner)
              AND src_table  = UPPER(p_src_table)
              AND p_dst_owner = UPPER(p_dst_owner)
              AND dst_table  = UPPER(p_dst_table)
              AND object_name = p.partition_name
              AND status = 'SUCCESS';
            DBMS_OUTPUT.PUT_LINE('Skipping partition ' || p.partition_name || ' (already loaded)');
            CONTINUE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
        END;

        BEGIN
            -- Truncate target partition for idempotent restart
            EXECUTE IMMEDIATE 'ALTER TABLE ' || p_dst_owner || '.' || p_dst_table ||
                              ' TRUNCATE PARTITION "' || p.partition_name || '"';

            -- Load partition
            EXECUTE IMMEDIATE '
                INSERT /*+ APPEND PARALLEL(' || p_parallel || ') */
                INTO ' || p_dst_owner || '.' || p_dst_table || ' PARTITION("' || p.partition_name || '")
                SELECT /*+ PARALLEL(' || p_parallel || ') */ *
                FROM ' || p_src_owner || '.' || p_src_table || ' PARTITION("' || p.partition_name || '")';

            l_trg_cnt := SQL%ROWCOUNT;
            COMMIT;

            -- Validate row count
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_src_owner || '.' || p_src_table ||
                              ' PARTITION("' || p.partition_name || '")' INTO l_src_cnt;

            IF l_src_cnt = l_trg_cnt THEN
                log_hwm_copy(
                    p_src_owner   => p_src_owner,
                    p_src_table   => p_src_table,
                    p_object_name => p.partition_name,
                    p_dst_owner   => p_dst_owner,
                    p_dst_table   => p_dst_table,
                    p_status      => 'SUCCESS',
                    p_src_cnt     => l_src_cnt,
                    p_trg_cnt     => l_trg_cnt,
                    p_action_ts   => SYSDATE
                );
                DBMS_OUTPUT.PUT_LINE('Partition ' || p.partition_name || ' loaded successfully (rows=' || l_trg_cnt || ')');
            ELSE
                ROLLBACK;
                log_hwm_copy(
                    p_src_owner   => p_src_owner,
                    p_src_table   => p_src_table,
                    p_object_name => p.partition_name,
                    p_dst_owner   => p_dst_owner,
                    p_dst_table   => p_dst_table,
                    p_status      => 'ERROR',
                    p_error_msg   => 'Row mismatch: src='||l_src_cnt||', trg='||l_trg_cnt,
                    p_src_cnt     => l_src_cnt,
                    p_trg_cnt     => l_trg_cnt,
                    p_action_ts   => SYSDATE
                );
                RAISE_APPLICATION_ERROR(-20001,'Row mismatch on partition ' || p.partition_name);
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                log_hwm_copy(
                    p_src_owner   => p_src_owner,
                    p_src_table   => p_src_table,
                    p_object_name => p.partition_name,
                    p_dst_owner   => p_dst_owner,
                    p_dst_table   => p_dst_table,
                    p_status      => 'ERROR',
                    p_error_msg   => SQLERRM,
                    p_src_cnt     => l_src_cnt,
                    p_trg_cnt     => l_trg_cnt,
                    p_action_ts   => SYSDATE
                );
                RAISE;
        END;

    END LOOP;

EXCEPTION
    WHEN ex_runtime_exceeded THEN
        DBMS_OUTPUT.PUT_LINE('Maximum runtime of ' || p_max_runtime || ' minutes reached. Exiting load partitions.');
END load_partitions;

/
