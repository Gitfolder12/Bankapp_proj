create or replace PROCEDURE reset_hwm_for_schema (
    p_schema           IN VARCHAR2,
    p_table_tbs        IN VARCHAR2,
    p_index_tbs        IN VARCHAR2,
    p_max_runtime      IN NUMBER DEFAULT 60, -- 1 hour default
    p_country          IN VARCHAR2 DEFAULT NULL
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
    v_pending_subparts  NUMBER;

--ISSUE TO ATTEND 
--P20191231	P20191231_S1
--P20191231	P20191231_S1000
--P20191231	P20191231_S1001
--P20191231	P20191231_S1002

--PNULL	PNULL_SA
--PNULL	PNULL_SV
--PNULL	PNULL_SW
--PNULL	PNULL_SE
--PNULL	PNULL_SU

--P20191231	P20191231_SES
--P20191231	P20191231_SPL
--P20191231	P20191231_SRO
--P20191231	P20191231_SUK

    ex_runtime_exceeded EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_runtime_exceeded, -20001);

    PROCEDURE check_runtime IS
    BEGIN
        IF (DBMS_UTILITY.GET_TIME - v_start_time) / 100 > (p_max_runtime * 60) THEN
            RAISE_APPLICATION_ERROR( -20001,'Maximum runtime of ' || p_max_runtime || ' minutes exceeded. Procedure stopped.');
        END IF;
    END;

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
 -- Partitioned tables in schema
  FOR tbl IN (
        SELECT t.table_name
        FROM dba_tables t
        WHERE t.owner = p_schema
          AND t.partitioned = 'YES' 
          AND t.table_name NOT IN ('HWM_LOG')
          AND t.table_name IN ( 'RR_BALANCES_OUT')
    ) LOOP
        v_table_name := tbl.table_name;

        FOR part IN (
            SELECT partition_name
            FROM dba_tab_partitions
            WHERE table_owner = p_schema
              AND table_name = v_table_name
            ORDER BY partition_name
        ) LOOP
            v_partition_name := part.partition_name;

            -- Count pending subpartitions
            SELECT COUNT(*) INTO v_subpart_count
            FROM dba_tab_subpartitions s
            WHERE s.table_owner = p_schema
              AND s.table_name = v_table_name
              AND s.partition_name = v_partition_name 
--              AND (p_country IS NULL OR s.subpartition_name LIKE '%' || p_country || '%')
              AND (
                    ( p_country IS NULL
                      AND REGEXP_LIKE(s.subpartition_name, '(^|_)S[0-9]+(_|$)', 'i')
                      AND NOT REGEXP_LIKE(s.subpartition_name, '(^|_)S(UK|PL|RO|ES)(_|$)', 'i') )
                 OR ( REGEXP_LIKE(p_country, '^[0-9]+$')
                      AND REGEXP_LIKE(s.subpartition_name, '(^|_)S' || p_country || '(_|$)', 'i') )
                 OR ( REGEXP_LIKE(p_country, '^[A-Za-z]{2}$')
                      AND REGEXP_LIKE(s.subpartition_name, '(^|_)S' || UPPER(p_country) || '(_|$)', 'i') )
                  )

              AND NOT EXISTS (
                  SELECT 1
                  FROM fdr_new.hwm_log l
                  WHERE l.owner = p_schema
                    AND l.base_table = v_table_name
                    AND l.object_type = 'SUBPARTITION'
                    AND l.object_name = s.subpartition_name
                    AND l.status = 'SUCCESS'
              );

            BEGIN
                IF v_subpart_count > 0 THEN
                    FOR subpart IN (
                        SELECT subpartition_name
                        FROM dba_tab_subpartitions s
                        WHERE s.table_owner = p_schema
                          AND s.table_name = v_table_name
                          AND s.partition_name = v_partition_name                       
--                          AND (p_country IS NULL OR s.subpartition_name LIKE '%' || p_country || '%')
                         AND (
                                ( p_country IS NULL
                                  AND REGEXP_LIKE(s.subpartition_name, '(^|_)S[0-9]+(_|$)', 'i')
                                  AND NOT REGEXP_LIKE(s.subpartition_name, '(^|_)S(UK|PL|RO|ES)(_|$)', 'i') )
                             OR ( REGEXP_LIKE(p_country, '^[0-9]+$')
                                  AND REGEXP_LIKE(s.subpartition_name, '(^|_)S' || p_country || '(_|$)', 'i') )
                             OR ( REGEXP_LIKE(p_country, '^[A-Za-z]{2}$')
                                  AND REGEXP_LIKE(s.subpartition_name, '(^|_)S' || UPPER(p_country) || '(_|$)', 'i') )
                              )

                          AND NOT EXISTS (
                              SELECT 1
                              FROM fdr_new.hwm_log l
                              WHERE l.owner = p_schema
                                AND l.base_table = v_table_name
                                AND l.object_type = 'SUBPARTITION'
                                AND l.object_name = s.subpartition_name
                                AND l.status = 'SUCCESS'
                          )
                        ORDER BY s.subpartition_name
                    ) LOOP

                        check_runtime;

                        v_subpartition_name := subpart.subpartition_name;

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


                        -- Log each subpartition
                        v_object_name := v_subpartition_name;
                        v_object_type := 'SUBPARTITION';
                        v_status := 'SUCCESS';
                        v_error_msg := NULL;
                        log_result;
                        
                    END LOOP;

                    -- Log partition after all subparts done
                    SELECT COUNT(*) INTO v_pending_subparts
                    FROM dba_tab_subpartitions s
                    WHERE s.table_owner = p_schema
                      AND s.table_name = v_table_name
                      AND s.partition_name = v_partition_name
                      AND NOT EXISTS (
                          SELECT 1
                          FROM fdr_new.hwm_log l
                          WHERE l.owner = p_schema
                            AND l.base_table = v_table_name
                            AND l.object_type = 'SUBPARTITION'
                            AND l.object_name = s.subpartition_name
                            AND l.status = 'SUCCESS'
                      );

                    IF v_pending_subparts = 0 THEN
                        v_object_name := v_partition_name;
                        v_object_type := 'PARTITION';
                        v_status := 'SUCCESS';
                        v_error_msg := NULL;
                        log_result;
                    END IF;
                ELSE
                     
                    -- Skip if already processed
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
                     
                    check_runtime;
                    
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
                    
                    -- Log partition
                    v_object_name := v_partition_name;
                    v_object_type := 'PARTITION';
                    v_status := 'SUCCESS';
                    v_error_msg := NULL;
                    log_result;
                END IF;

            EXCEPTION
                WHEN ex_runtime_exceeded THEN
                    -- clean log message
                    v_object_name := NVL(v_subpartition_name, v_partition_name);
                    v_object_type := CASE WHEN v_subpartition_name IS NOT NULL THEN 'SUBPARTITION' ELSE 'PARTITION' END;
                    v_status := 'FAILED';
                    v_error_msg := 'Maximum runtime of ' || p_max_runtime || ' minutes exceeded. Procedure stopped.';
                    log_result;
                    DBMS_OUTPUT.PUT_LINE(v_error_msg);
                    RETURN; -- stop the procedure
                WHEN OTHERS THEN
                    v_status := 'FAILED';
                    v_error_msg := SQLERRM; -- normal errors
                    log_result;
            END;
        END LOOP;
    END LOOP;

END reset_hwm_for_schema;