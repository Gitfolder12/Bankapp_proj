CREATE OR REPLACE PROCEDURE reset_hwm_for_schema (
    p_schema       IN VARCHAR2,
    p_table_tbs    IN VARCHAR2,
    p_max_runtime  IN NUMBER DEFAULT 60,
    p_country      IN VARCHAR2 DEFAULT NULL
) IS
    v_start_time        INTEGER := DBMS_UTILITY.GET_TIME;
    v_table_name        VARCHAR2(128);
    v_partition_name    VARCHAR2(128);
    v_subpartition_name VARCHAR2(128);
    v_error_msg         VARCHAR2(4000);
    v_status            VARCHAR2(20);
    v_object_name       VARCHAR2(128);
    v_object_type       VARCHAR2(20);

    ------------------------------------------------------------------
    -- Check runtime window
    ------------------------------------------------------------------
    FUNCTION runtime_exceeded RETURN BOOLEAN IS
    BEGIN
        RETURN (DBMS_UTILITY.GET_TIME - v_start_time)/100 > (p_max_runtime * 60);
    END;

    ------------------------------------------------------------------
    -- Logging procedure
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
    -- Loop over all partitioned tables
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
        -- Handle subpartitions > 1GB
        ------------------------------------------------------------------
        FOR subpart IN (
            SELECT s.subpartition_name
            FROM dba_tab_subpartitions s
            JOIN dba_segments seg
              ON seg.owner = s.table_owner
             AND seg.segment_name = s.subpartition_name
             AND seg.partition_name = s.partition_name
             AND seg.segment_type IN ('TABLE SUBPARTITION','TABLE PARTITION')
            WHERE s.table_owner = p_schema
              AND s.table_name  = v_table_name
              AND (p_country IS NULL OR s.subpartition_name LIKE '%' || p_country || '%')
              AND seg.bytes > 1024*1024*1024  -- size > 1 GB
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
        ) LOOP
            IF runtime_exceeded THEN
                DBMS_OUTPUT.PUT_LINE('Runtime window reached. Exiting before subpartition ' || subpart.subpartition_name);
                RETURN;
            END IF;

            v_subpartition_name := subpart.subpartition_name;

            BEGIN
                -- Move subpartition
                EXECUTE IMMEDIATE 'ALTER TABLE ' || p_schema || '.' || v_table_name ||
                                  ' MOVE SUBPARTITION ' || v_subpartition_name ||
                                  ' TABLESPACE ' || p_table_tbs;

                -- Move any LOBs in this subpartition
                FOR l IN (
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
                    EXECUTE IMMEDIATE 'ALTER TABLE ' || p_schema || '.' || v_table_name ||
                                      ' MOVE SUBPARTITION ' || l.lob_subpartition_name ||
                                      ' LOB(' || l.column_name || ') STORE AS (TABLESPACE ' || p_table_tbs || ')';
                END LOOP;

                v_object_name := v_subpartition_name;
                v_object_type := 'SUBPARTITION';
                v_status      := 'SUCCESS';
                v_error_msg   := NULL;
                log_result;
            EXCEPTION
                WHEN OTHERS THEN
                    v_object_name := v_subpartition_name;
                    v_object_type := 'SUBPARTITION';
                    v_status      := 'FAILED';
                    v_error_msg   := SQLERRM;
                    log_result;
            END;
        END LOOP;

        ------------------------------------------------------------------
        -- Handle partitions without subpartitions > 1GB
        ------------------------------------------------------------------
        FOR part IN (
            SELECT p.partition_name
            FROM dba_tab_partitions p
            JOIN dba_segments seg
              ON seg.owner = p.table_owner
             AND seg.segment_name = p.partition_name
             AND seg.segment_type IN ('TABLE PARTITION')
            WHERE p.table_owner = p_schema
              AND p.table_name  = v_table_name
              AND NOT EXISTS (
                  SELECT 1
                  FROM dba_tab_subpartitions s
                  WHERE s.table_owner = p_schema
                    AND s.table_name  = v_table_name
                    AND s.partition_name = p.partition_name
              )
              AND seg.bytes > 1024*1024*1024  -- size > 1 GB
              AND EXISTS (
                  SELECT 1
                  FROM cr.hwm_copy_log c
                  WHERE c.dst_owner   = p_schema
                    AND c.dst_table   = v_table_name
                    AND c.object_name = p.partition_name
                    AND c.status      = 'SUCCESS'
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM cr.hwm_log l
                  WHERE l.owner       = p_schema
                    AND l.base_table  = v_table_name
                    AND l.object_type = 'PARTITION'
                    AND l.object_name = p.partition_name
                    AND l.status      = 'SUCCESS'
              )
        ) LOOP
            IF runtime_exceeded THEN
                DBMS_OUTPUT.PUT_LINE('Runtime window reached. Exiting before partition ' || part.partition_name);
                RETURN;
            END IF;

            v_partition_name := part.partition_name;

            BEGIN
                -- Move partition
                EXECUTE IMMEDIATE 'ALTER TABLE ' || p_schema || '.' || v_table_name ||
                                  ' MOVE PARTITION ' || v_partition_name ||
                                  ' TABLESPACE ' || p_table_tbs;

                -- Move any LOBs in this partition
                FOR l IN (
                    SELECT column_name
                    FROM dba_lobs
                    WHERE owner = p_schema
                      AND table_name = v_table_name
                ) LOOP
                    EXECUTE IMMEDIATE 'ALTER TABLE ' || p_schema || '.' || v_table_name ||
                                      ' MOVE PARTITION ' || v_partition_name ||
                                      ' LOB(' || l.column_name || ') STORE AS (TABLESPACE ' || p_table_tbs || ')';
                END LOOP;

                v_object_name := v_partition_name;
                v_object_type := 'PARTITION';
                v_status      := 'SUCCESS';
                v_error_msg   := NULL;
                log_result;
            EXCEPTION
                WHEN OTHERS THEN
                    v_object_name := v_partition_name;
                    v_object_type := 'PARTITION';
                    v_status      := 'FAILED';
                    v_error_msg   := SQLERRM;
                    log_result;
            END;
        END LOOP;

    END LOOP;

    DBMS_OUTPUT.PUT_LINE('HWM reset completed (or runtime window reached).');

END reset_hwm_for_schema;
/