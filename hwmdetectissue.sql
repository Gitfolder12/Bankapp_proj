create or replace procedure         hwm_tbs_analyze_issue (p_tbs_data varchar2)is

    v_tablespace   VARCHAR2(30) := p_tbs_data; -- << CHANGE AS NEEDED

    -- Outputs from DBMS_SPACE
    l_total_bytes      NUMBER;
    l_unused_bytes     NUMBER;

    -- Dummy OUT parameters required by DBMS_SPACE
    l_total_blocks     NUMBER;
    l_unused_blocks    NUMBER;
    l_last_file_id     NUMBER;
    l_last_block_id    NUMBER;
    l_last_used_block  NUMBER;

    -- Calculated values
    v_total_mb   NUMBER;
    v_unused_mb  NUMBER;
    v_used_mb    NUMBER;
    v_waste_pct  NUMBER;
    v_action     VARCHAR2(5);
    v_errmsg VARCHAR2(4000);

BEGIN
    DBMS_OUTPUT.PUT_LINE('--- Tablespace HWM Waste Report for ' || v_tablespace || ' ---');
    DBMS_OUTPUT.PUT_LINE('---------------------------------------------------------------');

    FOR r IN (
        SELECT owner, segment_name, partition_name, segment_type, bytes
        FROM   dba_segments
        WHERE  tablespace_name = UPPER(v_tablespace)
        AND    bytes >= 50 * 1024 * 1024   -- Only analyze segments >= 50 MB
        ORDER  BY bytes DESC
    )
    LOOP
        BEGIN
            ----------------------------------------------------------------
            -- DBMS_SPACE: get total and unused bytes
            ----------------------------------------------------------------
            DBMS_SPACE.UNUSED_SPACE(
                segment_owner               => r.owner,
                segment_name                => r.segment_name,
                segment_type                => r.segment_type,
                total_blocks                => l_total_blocks,
                total_bytes                 => l_total_bytes,
                unused_blocks               => l_unused_blocks,
                unused_bytes                => l_unused_bytes,
                last_used_extent_file_id    => l_last_file_id,
                last_used_extent_block_id   => l_last_block_id,
                last_used_block             => l_last_used_block,
                partition_name              => r.partition_name
            );

            ----------------------------------------------------------------
            -- CALCULATE METRICS
            ----------------------------------------------------------------
            v_total_mb  := l_total_bytes  / 1024 / 1024;
            v_unused_mb := l_unused_bytes / 1024 / 1024;
            v_used_mb   := v_total_mb - v_unused_mb;

            IF l_total_bytes > 0 THEN
                v_waste_pct := ROUND((l_unused_bytes / l_total_bytes) * 100, 2);
            ELSE
                v_waste_pct := 0;
            END IF;

            ----------------------------------------------------------------
            -- ACTION DECISION (Real Tablespace Pressure Indicator)
            ----------------------------------------------------------------
            IF v_waste_pct >= 30 THEN
                v_action := 'YES';
            ELSE
                v_action := 'NO';
            END IF;

            ----------------------------------------------------------------
            -- LOG SUCCESS
            ----------------------------------------------------------------
            INSERT INTO rdr_new.HWM_TBS_ANALYSIS_LOG (
                run_timestamp,
                tablespace_name,
                owner,
                segment_name,
                partition_name,
                segment_type,
                total_mb,
                used_mb,
                unused_mb,
                waste_pct,
                actionable,
                error_message
            )
            VALUES (
                SYSDATE,
                v_tablespace,
                r.owner,
                r.segment_name,
                r.partition_name,
                r.segment_type,
                v_total_mb,
                v_used_mb,
                v_unused_mb,
                v_waste_pct,
                v_action,
                NULL
            );

            ----------------------------------------------------------------
            -- PRINT ONLY SEGMENTS THAT NEED ACTION
            ----------------------------------------------------------------
            IF v_action = 'YES' THEN
                DBMS_OUTPUT.PUT_LINE(
                    RPAD(r.owner, 12) ||
                    RPAD(r.segment_name, 40) ||
                    RPAD(NVL(r.partition_name,'-'), 20) ||
                    RPAD(r.segment_type, 20) ||
                    ' Waste%=' || v_waste_pct ||
                    ' UnusedMB=' || ROUND(v_unused_mb,1) ||
                    ' TotalMB=' || ROUND(v_total_mb,1)
                );
            END IF;

        EXCEPTION WHEN OTHERS THEN
            v_errmsg := SUBSTR(SQLERRM, 1, 4000);
            ----------------------------------------------------------------
            -- LOG ERRORS SAFELY
            ----------------------------------------------------------------
            INSERT INTO rdr_new.HWM_TBS_ANALYSIS_LOG (
                run_timestamp,
                tablespace_name,
                owner,
                segment_name,
                partition_name,
                segment_type,
                total_mb,
                used_mb,
                unused_mb,
                waste_pct,
                actionable,
                error_message
            )
            VALUES (
                SYSDATE,
                v_tablespace,
                r.owner,
                r.segment_name,
                r.partition_name,
                r.segment_type,
                NULL,
                NULL,
                NULL,
                NULL,
                'NO',
                v_errmsg
            );

            DBMS_OUTPUT.PUT_LINE(
                'ERROR on ' || r.segment_name || ' : ' || SQLERRM
            );
        END;
    END LOOP;

    COMMIT;
END;