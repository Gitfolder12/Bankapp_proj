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
    v_error_msg         VARCHAR2(4000);
    v_subpart_count     INTEGER;
    v_status            VARCHAR2(4000);
    v_object_name       VARCHAR2(128);
    v_object_type       VARCHAR2(128);
    v_exists            NUMBER;
    v_pending_subparts  NUMBER;

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
    -- Loop over partitioned tables
    FOR tbl IN (
        SELECT t.table_name
        FROM dba_tables t
        WHERE t.owner = p_schema
          AND t.partitioned = 'YES' 
          AND t.table_name NOT IN ('HWM_LOG')
          AND t.table_name IN ('TEST_PARTITION', 'SALES_DATA')
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

            -- Skip if already processed
            SELECT COUNT(*) INTO v_exists
            FROM hr.hwm_log
            WHERE owner = p_schema
              AND base_table = v_table_name
              AND object_type = 'PARTITION'
              AND object_name = v_partition_name
              AND status = 'SUCCESS';

            IF v_exists > 0 THEN
                CONTINUE;
            END IF;

            -- Count pending subpartitions
            SELECT COUNT(*) INTO v_subpart_count
            FROM dba_tab_subpartitions s
            WHERE s.table_owner = p_schema
              AND s.table_name = v_table_name
              AND s.partition_name = v_partition_name
              AND NOT EXISTS (
                  SELECT 1
                  FROM hr.hwm_log l
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
                          AND NOT EXISTS (
                              SELECT 1
                              FROM hr.hwm_log l
                              WHERE l.owner = p_schema
                                AND l.base_table = v_table_name
                                AND l.object_type = 'SUBPARTITION'
                                AND l.object_name = s.subpartition_name
                                AND l.status = 'SUCCESS'
                          )
                        ORDER BY s.subpartition_name
                    ) LOOP
                        DBMS_LOCK.SLEEP(10);

                        check_runtime;

                        v_subpartition_name := subpart.subpartition_name;
                        dbms_output.put_line('partition --> ' || v_partition_name || ' : subpartition --> ' || v_subpartition_name);

                        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_schema || '.' || v_table_name ||
                                          ' MOVE SUBPARTITION ' || v_subpartition_name ||
                                          ' TABLESPACE ' || p_table_tbs;

                        -- (LOB + index rebuild + stats same as your code…)

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
                          FROM hr.hwm_log l
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
                    check_runtime;
                    EXECUTE IMMEDIATE 'ALTER TABLE ' || p_schema || '.' || v_table_name ||
                                      ' MOVE PARTITION ' || v_partition_name ||
                                      ' TABLESPACE ' || p_table_tbs;
                    -- (LOB + index rebuild + stats same as your code…)

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
