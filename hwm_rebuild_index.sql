create or replace PROCEDURE hwm_rebuild_index (
    p_schema      IN VARCHAR2,
    p_table_tbs   IN VARCHAR2,
    p_index_tbs   IN VARCHAR2,
    p_max_runtime IN NUMBER DEFAULT 160
)
AUTHID CURRENT_USER
IS
    v_start_time    INTEGER := DBMS_UTILITY.GET_TIME;
    v_object_type   VARCHAR2(20);
    v_base_table    VARCHAR2(128);
    v_object_name   VARCHAR2(128);
    v_status        VARCHAR2(20);
    v_error_msg     VARCHAR2(4000);

    FUNCTION runtime_exceeded RETURN BOOLEAN IS
    BEGIN
        RETURN (DBMS_UTILITY.GET_TIME - v_start_time)/100 > (p_max_runtime * 60);
    END;

    PROCEDURE log_result IS
    BEGIN
        log_action(
            p_owner       => p_schema,
            p_base_table  => v_base_table,
            p_object_name => v_object_name,
            p_object_type => v_object_type,
            p_status      => v_status,
            p_error_msg   => v_error_msg
        );
    END;

BEGIN
    -- Loop all moved Tables ,PARTITIONS & SUBPARTITIONS that still need rebuild
    FOR mv IN (
        SELECT base_table,
               object_name,
               object_type
        FROM   hwm_log
        WHERE  owner          = p_schema
        AND    status         = 'SUCCESS'
        AND    object_type   IN ('PARTITION','SUBPARTITION')
        AND    index_rebuilt IS NULL
        ORDER BY base_table, object_name
    )
    LOOP
        v_base_table  := mv.base_table;
        v_object_type := mv.object_type;
        v_object_name := mv.object_name;

        -- STOP IF RUNTIME EXCEEDED
        --------------------------------------------------------------------
        IF runtime_exceeded THEN
            RETURN;
        END IF;

        -- INDEX SUBPARTITION REBUILD (matching by partition_position)
        --------------------------------------------------------------------
        IF v_object_type = 'SUBPARTITION' THEN

            FOR idx IN (
                SELECT isp.index_owner AS owner,
                       isp.index_name,
                       isp.subpartition_name,
                       isp.tablespace_name
                FROM   dba_ind_subpartitions isp
                JOIN   dba_indexes ix
                  ON   ix.index_name = isp.index_name
                 AND   ix.owner      = isp.index_owner
                WHERE  ix.table_owner   = p_schema
                AND    ix.table_name    = v_base_table
                AND    isp.subpartition_name = v_object_name      
                AND   ( isp.status = 'UNUSABLE'
                        OR isp.tablespace_name IN (
                               p_schema||'_DATA',
                               p_schema||'_DATA_IDX'
                           )
                      )
            )
            LOOP

                IF idx.tablespace_name = p_schema||'_DATA_IDX' THEN
                    EXECUTE IMMEDIATE
                        ' ALTER INDEX '
                        ||idx.owner
                        ||'.'
                        ||idx.index_name
                        ||' REBUILD SUBPARTITION '
                        ||idx.subpartition_name
                        ||' TABLESPACE '
                        ||p_index_tbs
                        ;
                ELSE
                    EXECUTE IMMEDIATE
                        ' ALTER INDEX '
                        ||idx.owner
                        ||'.'
                        ||idx.index_name
                        ||' REBUILD SUBPARTITION '
                        ||idx.subpartition_name
                        ||' TABLESPACE '
                        ||p_table_tbs
                        ;
                END IF;

            END LOOP;

        --  PART 2: INDEX PARTITION REBUILD (match by partition_name)
        --------------------------------------------------------------------
        ELSE
             FOR idx IN (
                SELECT ip.index_owner AS owner,
                       ip.index_name,
                       ip.partition_name,
                       ip.tablespace_name
                FROM   dba_ind_partitions ip
                JOIN   dba_indexes ix
                  ON   ix.owner      = ip.index_owner
                 AND   ix.index_name = ip.index_name
                WHERE  ix.table_owner = p_schema
                AND    ix.table_name  = v_base_table
                AND    ip.partition_name = v_object_name
                AND   ( ip.status = 'UNUSABLE'
                        OR ip.tablespace_name IN (
                               p_schema||'_DATA',
                               p_schema||'_DATA_IDX'
                           )
                      )
            )
            LOOP

                IF idx.tablespace_name = p_schema||'_DATA_IDX' THEN
                    EXECUTE IMMEDIATE
                        ' ALTER INDEX '
                        ||idx.owner
                        ||'.'
                        ||idx.index_name
                        ||' REBUILD PARTITION '
                        ||idx.partition_name
                        ||' TABLESPACE '
                        ||p_index_tbs
                        ;
                ELSE
                    EXECUTE IMMEDIATE
                        ' ALTER INDEX '
                        ||idx.owner
                        ||'.'
                        ||idx.index_name
                        ||' REBUILD PARTITION '
                        ||idx.partition_name
                        ||' TABLESPACE '
                        ||p_table_tbs;
                END IF;

            END LOOP;

        END IF;

        --Global indexes on PK 
        FOR gix IN (
            SELECT 
                   index_name
            FROM   dba_indexes
            WHERE  table_owner     = p_schema
              AND  table_name      = v_base_table
              AND  index_type      = 'NORMAL'
              AND  tablespace_name IN (
                       p_schema||'_DATA',
                       p_schema||'_DATA_IDX'
                   )                                   
        )
        LOOP

            EXECUTE IMMEDIATE
                ' ALTER INDEX '
                ||p_schema
                ||'.'
                ||gix.index_name
                ||' REBUILD TABLESPACE '
                ||p_index_tbs ;
        END LOOP;

        -- Mark as REBUILT
        --------------------------------------------------------------------
        UPDATE hwm_log
        SET    index_rebuilt = 'YES'
        WHERE  owner        = p_schema
        AND    base_table   = v_base_table
        AND    object_name  = v_object_name
        AND    object_type  = v_object_type;

        COMMIT;

    END LOOP;
    
EXCEPTION
    WHEN OTHERS THEN
        v_status := 'FAILED';
        v_error_msg := SQLERRM;
        log_result;
        RAISE;
END;