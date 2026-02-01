create or replace PROCEDURE reset_hwm_for_schema (
    p_schema       IN VARCHAR2,
    p_table_tbs    IN VARCHAR2,
    p_index_tbs    IN VARCHAR2,
    p_max_runtime  IN NUMBER DEFAULT 60,
    p_country      IN VARCHAR2 DEFAULT NULL
) IS
    v_start_time        INTEGER := DBMS_UTILITY.GET_TIME;
    v_table_name        VARCHAR2(128);
    v_partition_name    VARCHAR2(128);
    v_subpartition_name VARCHAR2(128);
    v_error_msg         VARCHAR2(4000);
    v_subpart_count     INTEGER;
    v_status            VARCHAR2(20);
    v_object_name       VARCHAR2(128);
    v_object_type       VARCHAR2(20);
    v_pending_subparts  NUMBER;
    v_exists            NUMBER;

    ------------------------------------------------------------------
    -- Runtime window check (CLEAN EXIT – NOT AN ERROR)
    ------------------------------------------------------------------
    FUNCTION runtime_exceeded RETURN BOOLEAN IS
    BEGIN
        RETURN (DBMS_UTILITY.GET_TIME - v_start_time) / 100 >
               (p_max_runtime * 60);
    END;

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
          AND table_name NOT IN ('HWM_LOG')
    ) LOOP
        v_table_name := tbl.table_name;

        ------------------------------------------------------------------
        -- LOOP PARTITIONS
        ------------------------------------------------------------------
        FOR part IN (
            SELECT partition_name
            FROM dba_tab_partitions
            WHERE table_owner = p_schema
              AND table_name  = v_table_name
            ORDER BY partition_position
        ) LOOP
            v_partition_name := part.partition_name;

            IF runtime_exceeded THEN
                DBMS_OUTPUT.PUT_LINE(
                    'Runtime window reached. Exiting cleanly before partition ' ||
                    v_partition_name
                );
                RETURN;
            END IF;

            ------------------------------------------------------------------
            -- COUNT PENDING SUBPARTITIONS (LOAD + NOT RESET)
            ------------------------------------------------------------------
            SELECT COUNT(*) INTO v_subpart_count
            FROM dba_tab_subpartitions s
            WHERE s.table_owner    = p_schema
              AND s.table_name     = v_table_name
              AND s.partition_name = v_partition_name
              AND (p_country IS NULL OR s.subpartition_name LIKE '%' || p_country || '%')
              AND EXISTS (
                  SELECT 1
                  FROM cr.hwm_copy_log c
                  WHERE c.dst_owner   = p_schema
                    AND c.dst_table   = v_table_name
                    AND c.object_name = s.subpartition_name
                    AND c.status      = 'SUCCESS'
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM cr.hwm_log l
                  WHERE l.owner       = p_schema
                    AND l.base_table  = v_table_name
                    AND l.object_type = 'SUBPARTITION'
                    AND l.object_name = s.subpartition_name
                    AND l.status      = 'SUCCESS'
              );

            ------------------------------------------------------------------
            -- SUBPARTITION PATH
            ------------------------------------------------------------------
            IF v_subpart_count > 0 THEN
                FOR subpart IN (
                    SELECT subpartition_name
                    FROM dba_tab_subpartitions s
                    WHERE s.table_owner    = p_schema
                      AND s.table_name     = v_table_name
                      AND s.partition_name = v_partition_name
                      AND (p_country IS NULL OR s.subpartition_name LIKE '%' || p_country || '%')
                      AND EXISTS (
                          SELECT 1
                          FROM cr.hwm_copy_log c
                          WHERE c.dst_owner   = p_schema
                            AND c.dst_table   = v_table_name
                            AND c.object_name = s.subpartition_name
                            AND c.status      = 'SUCCESS'
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM cr.hwm_log l
                          WHERE l.owner       = p_schema
                            AND l.base_table  = v_table_name
                            AND l.object_type = 'SUBPARTITION'
                            AND l.object_name = s.subpartition_name
                            AND l.status      = 'SUCCESS'
                      )
                    ORDER BY s.subpartition_name
                ) LOOP
                    IF runtime_exceeded THEN
                        DBMS_OUTPUT.PUT_LINE(
                            'Runtime window reached. Exiting cleanly before subpartition ' ||
                            subpart.subpartition_name
                        );
                        RETURN;
                    END IF;

                    v_subpartition_name := subpart.subpartition_name;

                    -- Move subpartition
                    EXECUTE IMMEDIATE
                        'ALTER TABLE ' || p_schema || '.' || v_table_name ||
                        ' MOVE SUBPARTITION ' || v_subpartition_name ||
                        ' TABLESPACE ' || p_table_tbs;

                    -- LOBs
                    FOR rec IN (
                        SELECT sp.lob_subpartition_name, l.column_name
                        FROM dba_lobs l
                        JOIN dba_lob_subpartitions sp
                          ON l.owner = sp.table_owner
                         AND l.table_name = sp.table_name
                         AND l.column_name = sp.lob_name
                        WHERE l.owner = p_schema
                          AND l.table_name = v_table_name
                          AND sp.subpartition_name = v_subpartition_name
                    ) LOOP
                        EXECUTE IMMEDIATE
                            'ALTER TABLE ' || p_schema || '.' || v_table_name ||
                            ' MOVE SUBPARTITION ' || rec.lob_subpartition_name ||
                            ' LOB(' || rec.column_name ||
                            ') STORE AS (TABLESPACE ' || p_table_tbs || ')';
                    END LOOP;

                    -- Indexes
                    FOR idx IN (
                        SELECT index_name
                        FROM dba_ind_subpartitions
                        WHERE index_owner = p_schema
                          AND subpartition_name = v_subpartition_name
                          AND status = 'UNUSABLE'
                    ) LOOP
                        EXECUTE IMMEDIATE
                            'ALTER INDEX ' || p_schema || '.' || idx.index_name ||
                            ' REBUILD SUBPARTITION ' || v_subpartition_name ||
                            ' TABLESPACE ' || p_index_tbs;
                    END LOOP;

                    DBMS_STATS.GATHER_TABLE_STATS(
                        ownname     => p_schema,
                        tabname     => v_table_name,
                        partname    => v_subpartition_name,
                        granularity => 'SUBPARTITION',
                        cascade     => TRUE
                    );

                    v_object_name := v_subpartition_name;
                    v_object_type := 'SUBPARTITION';
                    v_status      := 'SUCCESS';
                    v_error_msg   := NULL;
                    log_result;
                END LOOP;

            ------------------------------------------------------------------
            -- PARTITION-ONLY PATH
            ------------------------------------------------------------------
            ELSE
                -- ensure load completed
                SELECT COUNT(*) INTO v_exists
                FROM cr.hwm_copy_log
                WHERE dst_owner   = p_schema
                  AND dst_table   = v_table_name
                  AND object_name = v_partition_name
                  AND status      = 'SUCCESS';

                IF v_exists = 0 THEN
                    CONTINUE;
                END IF;

                -- skip if already reset
                SELECT COUNT(*) INTO v_exists
                FROM cr.hwm_log
                WHERE owner       = p_schema
                  AND base_table  = v_table_name
                  AND object_type = 'PARTITION'
                  AND object_name = v_partition_name
                  AND status      = 'SUCCESS';

                IF v_exists > 0 THEN
                    CONTINUE;
                END IF;

                IF runtime_exceeded THEN
                    DBMS_OUTPUT.PUT_LINE(
                        'Runtime window reached. Exiting cleanly before partition ' ||
                        v_partition_name
                    );
                    RETURN;
                END IF;

                -- Move partition
                EXECUTE IMMEDIATE
                    'ALTER TABLE ' || p_schema || '.' || v_table_name ||
                    ' MOVE PARTITION ' || v_partition_name ||
                    ' TABLESPACE ' || p_table_tbs;

                -- LOBs
                FOR l IN (
                    SELECT column_name
                    FROM dba_lobs
                    WHERE owner = p_schema
                      AND table_name = v_table_name
                ) LOOP
                    EXECUTE IMMEDIATE
                        'ALTER TABLE ' || p_schema || '.' || v_table_name ||
                        ' MOVE PARTITION ' || v_partition_name ||
                        ' LOB(' || l.column_name ||
                        ') STORE AS (TABLESPACE ' || p_table_tbs || ')';
                END LOOP;

                -- Indexes
                FOR ix IN (
                    SELECT index_name
                    FROM dba_ind_partitions
                    WHERE index_owner = p_schema
                      AND partition_name = v_partition_name
                      AND status = 'UNUSABLE'
                ) LOOP
                    EXECUTE IMMEDIATE
                        'ALTER INDEX ' || p_schema || '.' || ix.index_name ||
                        ' REBUILD PARTITION ' || v_partition_name ||
                        ' TABLESPACE ' || p_index_tbs;
                END LOOP;

                DBMS_STATS.GATHER_TABLE_STATS(
                    ownname     => p_schema,
                    tabname     => v_table_name,
                    partname    => v_partition_name,
                    granularity => 'PARTITION',
                    cascade     => TRUE
                );

                v_object_name := v_partition_name;
                v_object_type := 'PARTITION';
                v_status      := 'SUCCESS';
                v_error_msg   := NULL;
                log_result;
            END IF;

        END LOOP; -- partitions
    END LOOP; -- tables

    DBMS_OUTPUT.PUT_LINE('HWM reset completed (or runtime window reached).');

END reset_hwm_for_schema;
