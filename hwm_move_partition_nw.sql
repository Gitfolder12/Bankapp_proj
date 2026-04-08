CREATE OR REPLACE PROCEDURE hwm_move_partition (
    p_schema       IN VARCHAR2,
    p_table_tbs    IN VARCHAR2,
    p_index_tbs    IN VARCHAR2,
    p_max_runtime  IN NUMBER DEFAULT 260
)
AUTHID CURRENT_USER
IS
    v_start_time        INTEGER := DBMS_UTILITY.GET_TIME;
    v_table_name        VARCHAR2(128);
    v_partition_name    VARCHAR2(128);
    v_subpartition_name VARCHAR2(128);
    v_error_msg         VARCHAR2(4000);
    v_status            VARCHAR2(20);
    v_object_name       VARCHAR2(128);
    v_object_type       VARCHAR2(20);

    ------------------------------------------------------------------
    -- Runtime check
    ------------------------------------------------------------------
    FUNCTION runtime_exceeded RETURN BOOLEAN IS
    BEGIN
        RETURN (DBMS_UTILITY.GET_TIME - v_start_time)/100 > (p_max_runtime * 60);
    END;

    ------------------------------------------------------------------
    -- Logging
    ------------------------------------------------------------------
    PROCEDURE log_result IS
    BEGIN
        log_action(
            p_owner       => p_schema,
            p_base_table  => v_table_name,
            p_object_name => v_object_name,
            p_object_type => v_object_type,
            p_status      => v_status,
            p_error_msg   => v_error_msg
        );
    END;

BEGIN
    ------------------------------------------------------------------
    -- LOOP TABLES
    ------------------------------------------------------------------
    FOR tbl IN (
        SELECT table_name
        FROM dba_tables
        WHERE owner = p_schema
          AND partitioned = 'YES'
    )
    LOOP
        v_table_name := tbl.table_name;

        ------------------------------------------------------------------
        -- SUBPARTITIONS
        ------------------------------------------------------------------
        FOR sp IN (
            SELECT subpartition_name
            FROM dba_tab_subpartitions
            WHERE table_owner = p_schema
              AND table_name  = v_table_name
        )
        LOOP
            EXIT WHEN runtime_exceeded;

            v_subpartition_name := sp.subpartition_name;
            v_object_name := v_subpartition_name;
            v_object_type := 'SUBPARTITION';

            BEGIN
                EXECUTE IMMEDIATE
                    'ALTER TABLE '||p_schema||'.'||v_table_name||
                    ' MOVE SUBPARTITION '||v_subpartition_name||
                    ' TABLESPACE '||p_table_tbs;

                -- LOB MOVE
                FOR lob IN (
                    SELECT column_name
                    FROM dba_lobs
                    WHERE owner = p_schema
                      AND table_name = v_table_name
                      AND partitioned = 'YES'
                )
                LOOP
                    EXECUTE IMMEDIATE
                        'ALTER TABLE '||p_schema||'.'||v_table_name||
                        ' MOVE SUBPARTITION '||v_subpartition_name||
                        ' LOB ('||lob.column_name||') STORE AS (TABLESPACE '||p_table_tbs||')';
                END LOOP;

                -- INDEX REBUILD
                rebuild_indexes(
                    p_owner        => p_schema,
                    p_table_name   => v_table_name,
                    p_subpart_name => v_subpartition_name,
                    p_index_tbs    => p_index_tbs
                );

                -- STATS
                DBMS_STATS.GATHER_TABLE_STATS(
                    ownname     => p_schema,
                    tabname     => v_table_name,
                    partname    => v_subpartition_name,
                    granularity => 'SUBPARTITION'
                );

                v_status := 'SUCCESS';
                v_error_msg := NULL;

            EXCEPTION
                WHEN OTHERS THEN
                    v_status := 'FAILED';
                    v_error_msg := SQLERRM;
            END;

            log_result;
        END LOOP;

        ------------------------------------------------------------------
        -- PARTITIONS
        ------------------------------------------------------------------
        FOR pt IN (
            SELECT partition_name
            FROM dba_tab_partitions
            WHERE table_owner = p_schema
              AND table_name  = v_table_name
        )
        LOOP
            EXIT WHEN runtime_exceeded;

            v_partition_name := pt.partition_name;
            v_object_name := v_partition_name;
            v_object_type := 'PARTITION';

            BEGIN
                EXECUTE IMMEDIATE
                    'ALTER TABLE '||p_schema||'.'||v_table_name||
                    ' MOVE PARTITION '||v_partition_name||
                    ' TABLESPACE '||p_table_tbs;

                -- LOB MOVE
                FOR lob IN (
                    SELECT column_name
                    FROM dba_lobs
                    WHERE owner = p_schema
                      AND table_name = v_table_name
                      AND partitioned = 'YES'
                )
                LOOP
                    EXECUTE IMMEDIATE
                        'ALTER TABLE '||p_schema||'.'||v_table_name||
                        ' MOVE PARTITION '||v_partition_name||
                        ' LOB ('||lob.column_name||') STORE AS (TABLESPACE '||p_table_tbs||')';
                END LOOP;

                -- INDEX REBUILD
                rebuild_indexes(
                    p_owner      => p_schema,
                    p_table_name => v_table_name,
                    p_part_name  => v_partition_name,
                    p_index_tbs  => p_index_tbs
                );

                -- STATS
                DBMS_STATS.GATHER_TABLE_STATS(
                    ownname     => p_schema,
                    tabname     => v_table_name,
                    partname    => v_partition_name,
                    granularity => 'PARTITION'
                );

                v_status := 'SUCCESS';
                v_error_msg := NULL;

            EXCEPTION
                WHEN OTHERS THEN
                    v_status := 'FAILED';
                    v_error_msg := SQLERRM;
            END;

            log_result;
        END LOOP;

    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        v_status := 'FAILED';
        v_error_msg := SQLERRM || ' | ' || DBMS_UTILITY.format_error_backtrace;
        log_result;
        RAISE;
END;
/