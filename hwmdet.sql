CREATE TABLE hwm_tbs_audit_log (
    run_time           DATE,
    tablespace_name    VARCHAR2(30),
    owner              VARCHAR2(30),
    segment_name       VARCHAR2(128),
    partition_name     VARCHAR2(128),
    segment_type       VARCHAR2(30),
    total_mb           NUMBER,
    unused_mb          NUMBER,
    reclaimable_pct    NUMBER,
    recommendation     VARCHAR2(20)
);


CREATE OR REPLACE PROCEDURE audit_tablespace_hwm (
    p_tablespace_name   IN VARCHAR2,
    p_min_size_mb       IN NUMBER DEFAULT 1024,   -- check only segments > 1GB
    p_min_reclaim_pct   IN NUMBER DEFAULT 20      -- log only if >20% reclaimable
)
IS

    l_total_blocks        NUMBER;
    l_total_bytes         NUMBER;
    l_unused_blocks       NUMBER;
    l_unused_bytes        NUMBER;
    l_last_used_extent    NUMBER;
    l_last_used_block     NUMBER;
    l_last_used_block_id  NUMBER;

BEGIN

    FOR r IN (
        SELECT owner,
               segment_name,
               partition_name,
               segment_type,
               bytes
        FROM   dba_segments
        WHERE  tablespace_name = UPPER(p_tablespace_name)
        AND    bytes >= p_min_size_mb * 1024 * 1024
        AND    segment_type IN (
               'TABLE',
               'TABLE PARTITION',
               'TABLE SUBPARTITION',
               'INDEX',
               'INDEX PARTITION',
               'INDEX SUBPARTITION'
        )
        ORDER BY bytes DESC
    )
    LOOP

        BEGIN
            DBMS_SPACE.UNUSED_SPACE (
                segment_owner               => r.owner,
                segment_name                => r.segment_name,
                segment_type                => r.segment_type,
                total_blocks                => l_total_blocks,
                total_bytes                 => l_total_bytes,
                unused_blocks               => l_unused_blocks,
                unused_bytes                => l_unused_bytes,
                last_used_extent_file_id    => l_last_used_extent,
                last_used_extent_block_id   => l_last_used_block,
                last_used_block             => l_last_used_block_id,
                partition_name              => r.partition_name
            );

            IF l_total_bytes > 0 THEN

                DECLARE
                    l_pct NUMBER;
                BEGIN
                    l_pct := ROUND((l_unused_bytes / l_total_bytes) * 100, 2);

                    IF l_pct >= p_min_reclaim_pct THEN

                        INSERT INTO hwm_tbs_audit_log
                        VALUES (
                            SYSDATE,
                            p_tablespace_name,
                            r.owner,
                            r.segment_name,
                            r.partition_name,
                            r.segment_type,
                            ROUND(l_total_bytes/1024/1024,2),
                            ROUND(l_unused_bytes/1024/1024,2),
                            l_pct,
                            'HWM_ISSUE'
                        );

                    END IF;

                END;

            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                NULL; -- skip unsupported segments
        END;

    END LOOP;

    COMMIT;

END;
/