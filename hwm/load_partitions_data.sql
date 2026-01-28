create or replace PROCEDURE copy_partitions_step (
    p_owner     IN VARCHAR2,
    p_src_table IN VARCHAR2,
    p_dst_table IN VARCHAR2,
    p_parallel  IN PLS_INTEGER DEFAULT 16
) IS
    l_owner     VARCHAR2(30)  := UPPER(p_owner);
    l_src_table VARCHAR2(128) := UPPER(p_src_table);
    l_dst_table VARCHAR2(128) := UPPER(p_dst_table);
    l_dummy     NUMBER;
    p_trg_owner varchar(20) := 'CR';
    v_error_msg VARCHAR2(4000);
    v_start_time NUMBER;
    v_execution_time NUMBER;
    v_rows_copied NUMBER;
BEGIN
    
    -- Enable NOLOGGING on target table for better performance
    EXECUTE IMMEDIATE 'ALTER TABLE ' || l_owner || '.' || l_dst_table || ' NOLOGGING';
    
    -- Drop LOCAL indexes on target table before copy
    FOR idx IN (SELECT index_name, locality FROM dba_indexes 
                WHERE table_owner = l_owner AND table_name = l_dst_table AND uniqueness = 'NONUNIQUE' AND locality = 'LOCAL')
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP INDEX ' || l_owner || '.' || idx.index_name;
            DBMS_OUTPUT.PUT_LINE('Dropped LOCAL index: ' || idx.index_name);
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
    
    -- Enable parallel DML and optimize for large data volumes
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
    EXECUTE IMMEDIATE 'ALTER SESSION SET PARALLEL_MIN_PERCENT = 0';
    EXECUTE IMMEDIATE 'ALTER SESSION SET PARALLEL_EXECUTION_MESSAGE_SIZE = 16384';
--    EXECUTE IMMEDIATE 'ALTER TABLE ' || p_trg_owner || '.' ||l_dst_table||' NOLOGGING';

    -- Loop through partitions using DBA_TAB_PARTITIONS
    FOR r IN (
        SELECT partition_name
        FROM   dba_tab_partitions
        WHERE  table_owner = l_owner
        AND    table_name  = l_src_table
        AND    composited = 'NO'
        ORDER  BY partition_position
    )
    LOOP
        -- Skip already copied partitions
        BEGIN
            SELECT 1
            INTO   l_dummy
            FROM   hwm_copy_log
            WHERE  owner       = l_owner
            AND    src_table   = l_src_table
            AND    object_name = r.partition_name
            AND    dst_table   = l_dst_table
            AND    status      = 'SUCCESS';

            DBMS_OUTPUT.PUT_LINE('>>> Skipping ' || r.partition_name || ' (already done)');
            CONTINUE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                NULL;
        END;

        -- Copy partition

        BEGIN
            -- Logging progress
            DBMS_OUTPUT.PUT_LINE('Copying partition: ' || r.partition_name);
            -- Log the start time
            v_start_time := DBMS_UTILITY.GET_TIME;

            EXECUTE IMMEDIATE '
                INSERT /*+ APPEND PARALLEL(' || l_dst_table || ',' || p_parallel || ') NOLOGGING */
                INTO ' || l_owner || '.' || l_dst_table || ' PARTITION("' || r.partition_name || '")
                SELECT /*+ PARALLEL(' || l_src_table || ',' || p_parallel || ') FULL */ *
                FROM   ' || l_owner || '.' || l_src_table || ' PARTITION("' || r.partition_name || '")
            ' INTO v_rows_copied;
            
            -- Capture execution time and rows copied
            v_execution_time := DBMS_UTILITY.GET_TIME - v_start_time;

            -- Log success
            log_hwm_copy(
                p_owner       => l_owner,
                p_src_table   => l_src_table,
                p_object_name => r.partition_name,
                p_dst_table   => l_dst_table,
                p_status      => 'SUCCESS'
            );

            COMMIT;

            DBMS_OUTPUT.PUT_LINE('    -> ' || r.partition_name || ' copied (' || v_rows_copied || ' rows in ' || v_execution_time || 'cs)');

        EXCEPTION
            WHEN OTHERS THEN
                -- Log error
                log_hwm_copy(
                    p_owner       => l_owner,
                    p_src_table   => l_src_table,
                    p_object_name => r.partition_name,
                    p_dst_table   => l_dst_table,
                    p_status      => 'ERROR',
                    p_error_msg   => SQLERRM
                );

                -- Error handling for partition copy
                v_error_msg := SQLERRM;
                -- Log the error message to a table or output
                INSERT INTO error_log (error_message, error_time) VALUES (v_error_msg, SYSDATE);
                
                ROLLBACK;
                RAISE;
        END;
    END LOOP;
    
    -- Re-enable LOGGING on target table
    EXECUTE IMMEDIATE 'ALTER TABLE ' || l_owner || '.' || l_dst_table || ' LOGGING';
    
    -- Recreate LOCAL indexes on target table (GLOBAL indexes remain intact)
    FOR idx IN (SELECT index_name, locality
                FROM dba_indexes 
                WHERE table_owner = l_owner 
                AND table_name = l_src_table 
                AND uniqueness = 'NONUNIQUE'
                AND locality = 'LOCAL'
                ORDER BY index_name)
    LOOP
        BEGIN
            -- Get index DDL from source and apply to destination table
            DECLARE
                v_index_ddl VARCHAR2(4000);
            BEGIN
                v_index_ddl := DBMS_METADATA.GET_DDL('INDEX', idx.index_name, l_owner);
                -- Replace source table reference with destination table
                v_index_ddl := REPLACE(v_index_ddl, l_src_table, l_dst_table);
                EXECUTE IMMEDIATE v_index_ddl;
                DBMS_OUTPUT.PUT_LINE('Recreated LOCAL index: ' || idx.index_name);
            END;
        EXCEPTION WHEN OTHERS THEN 
            DBMS_OUTPUT.PUT_LINE('Error recreating index ' || idx.index_name || ': ' || SQLERRM);
        END;
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('Partition copy and index recreation completed successfully');
END;

