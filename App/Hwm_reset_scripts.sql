create or replace PROCEDURE reset_hwm_for_schema (
    p_schema           IN VARCHAR2,
    p_table_tbs        IN VARCHAR2,
    p_index_tbs        IN VARCHAR2,
    p_max_runtime      IN NUMBER DEFAULT 60 -- 1 hour default
) IS
    v_start_time        INTEGER := DBMS_UTILITY.GET_TIME;
    v_table_name        VARCHAR2(128);
    v_partition_name    VARCHAR2(128);
    v_subpartition_name VARCHAR2(128);
    v_error_msg         VARCHAR2(128);
    v_subpart_count     INTEGER;
    v_status            VARCHAR2(4000);
    v_object_name       VARCHAR2(128);
    v_object_type       VARCHAR2(128);
    v_exists            NUMBER;

    PROCEDURE check_runtime IS
    BEGIN
        IF (DBMS_UTILITY.GET_TIME - v_start_time) / 100 > (p_max_runtime * 60) THEN
            RETURN;
        END IF;
    END;

BEGIN
  -- Partitioned tables in schema
    FOR tbl IN (
        SELECT 
               t.table_name
          FROM dba_tables t
         WHERE t.owner = p_schema
           AND t.partitioned = 'YES' 
           AND t.table_name NOT IN ('HWM_LOG') -- exclude specific tables
    )LOOP
        v_table_name := tbl.table_name;

        FOR part IN (
            SELECT partition_name
            FROM dba_tab_partitions
            WHERE table_owner = p_schema 
              AND table_name = v_table_name
            ORDER BY partition_name 
        ) LOOP
            check_runtime;

            v_partition_name := part.partition_name;

             --- If partition already complete, skip it (idempotent)
            SELECT COUNT(*) INTO v_exists
            FROM fdr_new.hwm_log
            WHERE owner = p_schema
              AND base_table = v_table_name
              AND object_type = 'PARTITION'
              AND object_name = v_partition_name
              AND status = 'SUCCESS';

            IF v_exists > 0 THEN
                CONTINUE;
            END IF;

            -- Get Count pending subpartitions for this partition 
            SELECT COUNT(*) 
            INTO v_subpart_count
            FROM dba_tab_subpartitions s
            WHERE s.table_owner    = p_schema 
              AND s.table_name     = v_table_name 
              AND s.partition_name = v_partition_name
              AND  NOT EXISTS (
                              SELECT 1
                              FROM fdr_new.hwm_log l
                              WHERE l.owner = p_schema
                                AND l.base_table   = v_table_name
                                AND l.object_type  = 'SUBPARTITION'
                                AND l.object_name  = s.subpartition_name
                                AND l.status = 'SUCCESS'
                          );

          BEGIN
            IF v_subpart_count > 0 THEN
                -- Subpartitioned table
                FOR subpart IN (
                    SELECT subpartition_name
                    FROM dba_tab_subpartitions s
                    WHERE s.table_owner = p_schema 
                      AND s.table_name = v_table_name 
                      AND s.partition_name = v_partition_name
                      AND NOT EXISTS (
                              SELECT 1
                              FROM fdr_new.hwm_log l
                              WHERE l.owner = p_schema
                                AND l.base_table   = v_table_name
                                AND l.object_type  = 'SUBPARTITION'
                                AND l.object_name  = s.subpartition_name
                                AND l.status = 'SUCCESS'
                          )
                          ORDER BY s.partition_name
                ) LOOP
                        
                        v_subpartition_name := subpart.subpartition_name;
                        -- Move Subpartition
                        EXECUTE IMMEDIATE ' ALTER TABLE ' 
                                          || p_schema 
                                          || '.' 
                                          || v_table_name 
                                          || ' MOVE SUBPARTITION ' 
                                          || v_subpartition_name 
                                          || ' TABLESPACE ' 
                                          || p_table_tbs 
                                          ;
                        -- Move Lobs
                        FOR rec IN (
                            SELECT sp.lob_subpartition_name, l.column_name
                            FROM dba_lobs l
                            JOIN dba_lob_subpartitions sp ON l.owner = sp.table_owner
                                AND l.table_name = sp.table_name 
                                AND l.column_name = sp.lob_name
                            WHERE l.owner = p_schema AND l.table_name = v_table_name 
                        ) LOOP

                            EXECUTE IMMEDIATE ' ALTER TABLE ' 
                                              || p_schema 
                                              || '.' 
                                              || v_table_name 
                                              || ' MOVE SUBPARTITION ' 
                                              || rec.lob_subpartition_name 
                                              || ' LOB(' 
                                              || rec.column_name 
                                              || ') STORE AS (TABLESPACE ' 
                                              || p_table_tbs || ')';
                        END LOOP;

                        --Rebuild local index
                        FOR idxsub IN (
                            SELECT isp.index_name, isp.subpartition_name, isp.tablespace_name
                            FROM dba_ind_subpartitions isp
                            JOIN dba_indexes ix ON isp.index_name = ix.index_name 
                                AND isp.index_owner = ix.owner
                            WHERE ix.table_owner = p_schema 
                              AND ix.table_name = v_table_name
                              AND isp.status = 'UNUSABLE' 
                              AND isp.subpartition_name = v_subpartition_name
                        ) LOOP

                            IF idxsub.tablespace_name = p_schema||'_DATA_IDX' THEN
                                    EXECUTE IMMEDIATE ' ALTER INDEX ' 
                                                      || p_schema 
                                                      || '.' 
                                                      || idxsub.index_name 
                                                      || ' REBUILD SUBPARTITION ' 
                                                      || idxsub.subpartition_name 
                                                      || ' TABLESPACE ' 
                                                      || p_index_tbs
                                                      ;
                            END IF;

                            IF idxsub.tablespace_name = p_schema||'_DATA' THEN
                                 EXECUTE IMMEDIATE ' ALTER INDEX ' 
                                                      || p_schema 
                                                      || '.' 
                                                      || idxsub.index_name 
                                                      || ' REBUILD SUBPARTITION ' 
                                                      || idxsub.subpartition_name 
                                                      || ' TABLESPACE ' 
                                                      || p_table_tbs
                                                      ;
                           END IF;
                        END LOOP;

                        -- Gather stats on subpartition
                        DBMS_STATS.GATHER_TABLE_STATS(
                                    ownname           => p_schema, 
                                    tabname           => v_table_name, 
                                    partname          => v_subpartition_name,
                                    granularity       => 'SUBPARTITION',
                                    cascade           => TRUE,
                                    estimate_percent  => DBMS_STATS.AUTO_SAMPLE_SIZE
                                );


                       v_object_name := v_subpartition_name;
                       v_object_type := 'SUBPARTITION';
                END LOOP;

            ELSE
                    check_runtime;

                    SELECT COUNT(*) INTO v_exists
                    FROM fdr_new.hwm_log
                    WHERE owner = p_schema
                      AND base_table = v_table_name
                      AND object_name = v_partition_name
                      AND object_type = 'PARTITION'
                      AND status = 'SUCCESS';

                    IF v_exists > 0 THEN
                        CONTINUE;
                    END IF;

                    -- Regular partition
                    EXECUTE IMMEDIATE '  ALTER TABLE ' 
                                      || p_schema 
                                      || '.' 
                                      || v_table_name 
                                      || ' MOVE PARTITION ' 
                                      || v_partition_name 
                                      || ' TABLESPACE ' 
                                      || p_table_tbs
                                      ;
                    -- Move lobs
                    FOR rec IN (
                        SELECT column_name
                        FROM dba_lobs
                        WHERE owner = p_schema AND table_name = v_table_name
                    ) LOOP

                        EXECUTE IMMEDIATE ' ALTER TABLE ' 
                                          || p_schema 
                                          || '.' 
                                          || v_table_name 
                                          || ' MOVE LOB(' 
                                          || rec.column_name 
                                          || ') STORE AS (TABLESPACE ' 
                                          || p_table_tbs 
                                          || ')';
                    END LOOP;

                    --Rebuild local index
                    FOR idx IN (
                        SELECT ip.index_name, ip.partition_name, ip.tablespace_name
                        FROM dba_ind_partitions ip
                        JOIN dba_indexes ix ON ip.index_name = ix.index_name AND ip.index_owner = ix.owner
                        WHERE ix.table_owner = p_schema 
                          AND ix.table_name = v_table_name
                          AND ip.status = 'UNUSABLE' 
                          AND ip.partition_name = v_partition_name
                    ) LOOP

                        IF idx.tablespace_name = p_schema||'_DATA_IDX' THEN
                                EXECUTE IMMEDIATE ' ALTER INDEX ' 
                                                  || p_schema 
                                                  || '.' 
                                                  || idx.index_name 
                                                  || ' REBUILD PARTITION ' 
                                                  || idx.partition_name 
                                                  || ' TABLESPACE '
                                                  || p_index_tbs
                                                  || ' PARALLEL 4 ' 
                                                  ;
                       END IF;

                       IF idx.tablespace_name = p_schema||'_DATA' THEN
                                EXECUTE IMMEDIATE ' ALTER INDEX ' 
                                                  || p_schema 
                                                  || '.' 
                                                  || idx.index_name 
                                                  || ' REBUILD PARTITION ' 
                                                  || idx.partition_name 
                                                  || ' TABLESPACE '
                                                  || p_table_tbs
                                                  ;
                       END IF;
                    END LOOP;

                    -- Gather stats on partition
                    DBMS_STATS.GATHER_TABLE_STATS(
                        ownname     => p_schema, 
                        tabname     => v_table_name, 
                        partname    => v_partition_name,
                        granularity => 'PARTITION',
                        cascade     => TRUE,
                        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE
                    );


                        v_object_name := v_partition_name;
                        v_object_type := 'PARTITION';          
            END IF;

                        v_status := 'SUCCESS';
                EXCEPTION
                    WHEN OTHERS THEN
                        v_status    := 'FAILED';
                        v_error_msg := 'FAILED: ' || SQLERRM;

            END;
            
            dbms_output.put_line('partition --> ' || v_partition_name ||' : ' || 'subpartition --> ' || v_subpartition_name);
                --log action 
                log_action( p_owner        => p_schema,
                            p_base_table   => v_table_name,
                            p_object_name  => v_object_name ,
                            p_object_type  => v_object_type,
                            p_status       => v_status ,
                            p_error_msg    => v_error_msg 
                            );
        END LOOP;
    END LOOP;


    -- Process non-partitioned tables
    FOR tb1 IN (
        SELECT t.table_name
        FROM dba_tables t
        WHERE owner = p_schema
          AND t.partitioned = 'NO'
          AND t.table_name NOT IN ('HWM_LOG')
          AND NOT EXISTS (
                  SELECT 1
                  FROM fdr_new.HWM_LOG l
                  WHERE l.base_table = t.table_name
                    AND l.object_type = 'TABLE'
                    AND l.status = 'SUCCESS')

    ) LOOP
            check_runtime;

        BEGIN
            v_table_name := tb1.table_name;
            v_partition_name := NULL;

            -- Move table
            EXECUTE IMMEDIATE ' ALTER TABLE ' 
                              || p_schema 
                              || '.' 
                              || v_table_name 
                              || ' MOVE TABLESPACE ' 
                              || p_table_tbs
                              || ' UPDATE INDEXES '
                              ;

            -- Move LOBs
            FOR lob_rec IN (
                SELECT column_name
                FROM dba_lobs
                WHERE owner = p_schema
                  AND table_name = v_table_name
            ) LOOP
                EXECUTE IMMEDIATE ' ALTER TABLE ' 
                                  || p_schema 
                                  || '.' 
                                  || v_table_name 
                                  || ' MOVE LOB(' 
                                  || lob_rec.column_name 
                                  || ') STORE AS (TABLESPACE ' 
                                  || p_table_tbs 
                                  || ')';
            END LOOP;

            -- Rebuild unusable indexes
            FOR ix IN (
                SELECT index_name, tablespace_name
                FROM dba_indexes
                WHERE owner = p_schema
                  AND table_name = v_table_name
                  AND status = 'UNUSABLE'
            ) LOOP

                IF ix.tablespace_name = p_schema||'_DATA_IDX' THEN
                    EXECUTE IMMEDIATE  ' ALTER INDEX ' 
                                      || p_schema 
                                      || '.' 
                                      || ix.index_name 
                                      || ' REBUILD TABLESPACE ' 
                                      || p_index_tbs 
                                      ;
                END IF ;

                IF ix.tablespace_name = p_schema||'_DATA' THEN
                    EXECUTE IMMEDIATE  ' ALTER INDEX ' 
                                      || p_schema 
                                      || '.' 
                                      || ix.index_name 
                                      || ' REBUILD TABLESPACE ' 
                                      || p_table_tbs 
                                      ;
                END IF ;

             END LOOP;

            -- Gather statistics
            DBMS_STATS.GATHER_TABLE_STATS(
                ownname           => p_schema,
                tabname           => v_table_name,
                cascade           => TRUE,
                estimate_percent  => DBMS_STATS.AUTO_SAMPLE_SIZE
            );

                v_status := 'SUCCESS';

        EXCEPTION
            WHEN OTHERS THEN
                v_status    := 'FAILED';
                v_error_msg := 'FAILED: ' || SQLERRM;
        END;

        -- Log action              
         log_action( p_owner        => p_schema,
                     p_base_table   => v_table_name,
                     p_object_name  => null ,
                     p_object_type  => 'TABLE',
                     p_status       => v_status ,
                     p_error_msg    => v_error_msg 
                            );

    END LOOP;

END reset_hwm_for_schema;