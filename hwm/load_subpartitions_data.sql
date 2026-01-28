create or replace PROCEDURE copy_subpartitions_step (
    p_owner     IN VARCHAR2,
    p_src_table IN VARCHAR2,
    p_dst_table IN VARCHAR2,
    p_parallel  IN PLS_INTEGER DEFAULT 16
) IS
    l_owner     VARCHAR2(30)  := UPPER(p_owner);
    l_src_table VARCHAR2(128) := UPPER(p_src_table);
    l_dst_table VARCHAR2(128) := UPPER(p_dst_table);
    l_dummy     NUMBER;
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
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
    
    -- Enable parallel DML and optimize for large data volumes
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
    EXECUTE IMMEDIATE 'ALTER SESSION SET PARALLEL_MIN_PERCENT = 0';
    EXECUTE IMMEDIATE 'ALTER SESSION SET PARALLEL_EXECUTION_MESSAGE_SIZE = 16384';

    FOR r IN (
        SELECT s.subpartition_name
        FROM   dba_tab_subpartitions s
        JOIN   dba_tables t ON s.table_owner = t.owner AND s.table_name = t.table_name
        WHERE  s.table_owner = l_owner
        AND    s.table_name  = l_src_table
        AND    t.subpartitioned = 'YES'
        ORDER  BY s.partition_position, s.subpartition_position
    )
    LOOP
        ------------------------------------------------------------
        -- Skip already copied subpartitions
        ------------------------------------------------------------
        BEGIN
            SELECT 1
            INTO   l_dummy
            FROM   hwm_copy_log
            WHERE  owner       = l_owner
            AND    src_table   = l_src_table
            AND    object_name = r.subpartition_name
            AND    dst_table   = l_dst_table
            AND    status      = 'SUCCESS';

            DBMS_OUTPUT.PUT_LINE('>>> Skipping ' || r.subpartition_name || ' (already done)' );
            CONTINUE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                NULL;
        END;

        BEGIN
            EXECUTE IMMEDIATE '
                INSERT /*+ APPEND PARALLEL(' || l_dst_table || ',' || p_parallel || ') NOLOGGING */
                INTO ' || l_owner || '.' || l_dst_table ||
                ' SUBPARTITION (' || r.subpartition_name || ')
                SELECT /*+ PARALLEL(' || l_src_table || ',' || p_parallel || ') FULL */ *
                FROM   ' || l_owner || '.' || l_src_table ||
                ' SUBPARTITION (' || r.subpartition_name || ')
            ';

            log_hwm_copy(
                p_owner       => l_owner,
                p_src_table   => l_src_table,
                p_object_name => r.subpartition_name,
                p_dst_table   => l_dst_table,
                p_status      => 'SUCCESS'
            );

            COMMIT;

            DBMS_OUTPUT.PUT_LINE('    -> ' || r.subpartition_name || ' copied' );

        EXCEPTION
            WHEN OTHERS THEN
                log_hwm_copy(
                    p_owner       => l_owner,
                    p_src_table   => l_src_table,
                    p_object_name => r.subpartition_name,
                    p_dst_table   => l_dst_table,
                    p_status      => 'ERROR',
                    p_error_msg   => SQLERRM
                );

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
                v_new_index_name VARCHAR2(128);
            BEGIN
                v_new_index_name := idx.index_name || '_nw';
                v_index_ddl := DBMS_METADATA.GET_DDL('INDEX', idx.index_name, l_owner);
                -- Replace source table reference with destination table and index name
                v_index_ddl := REPLACE(v_index_ddl, l_src_table, l_dst_table);
                v_index_ddl := REPLACE(v_index_ddl, 'CREATE INDEX ' || l_owner || '.' || idx.index_name, 
                                                     'CREATE INDEX ' || l_owner || '.' || v_new_index_name);
                EXECUTE IMMEDIATE v_index_ddl;
                DBMS_OUTPUT.PUT_LINE('Recreated LOCAL index: ' || v_new_index_name);
            END;
        EXCEPTION WHEN OTHERS THEN 
            DBMS_OUTPUT.PUT_LINE('Error recreating index ' || idx.index_name || ': ' || SQLERRM);
        END;
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('Subpartition copy and index recreation completed successfully');
    
END;