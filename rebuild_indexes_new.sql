CREATE OR REPLACE PROCEDURE rebuild_indexes (
    p_owner        IN VARCHAR2,
    p_table_name   IN VARCHAR2,
    p_part_name    IN VARCHAR2 DEFAULT NULL,
    p_subpart_name IN VARCHAR2 DEFAULT NULL,
    p_index_tbs    IN VARCHAR2
)
AUTHID CURRENT_USER
IS
BEGIN
    ------------------------------------------------------------------
    -- SUBPARTITION INDEXES
    ------------------------------------------------------------------
    IF p_subpart_name IS NOT NULL THEN
        FOR i IN (
            SELECT index_name, subpartition_name
            FROM   dba_ind_subpartitions
            WHERE  index_owner = p_owner
              AND  table_name  = p_table_name
              AND  subpartition_name = p_subpart_name
              AND  status = 'UNUSABLE'
        )
        LOOP
            EXECUTE IMMEDIATE
                'ALTER INDEX '||p_owner||'.'||i.index_name||
                ' REBUILD SUBPARTITION '||i.subpartition_name||
                ' TABLESPACE '||p_index_tbs;
        END LOOP;
    END IF;

    ------------------------------------------------------------------
    -- PARTITION INDEXES
    ------------------------------------------------------------------
    IF p_part_name IS NOT NULL THEN
        FOR i IN (
            SELECT index_name, partition_name
            FROM   dba_ind_partitions
            WHERE  index_owner = p_owner
              AND  table_name  = p_table_name
              AND  partition_name = p_part_name
              AND  status = 'UNUSABLE'
        )
        LOOP
            EXECUTE IMMEDIATE
                'ALTER INDEX '||p_owner||'.'||i.index_name||
                ' REBUILD PARTITION '||i.partition_name||
                ' TABLESPACE '||p_index_tbs;
        END LOOP;
    END IF;

    ------------------------------------------------------------------
    -- GLOBAL INDEXES
    ------------------------------------------------------------------
    FOR g IN (
        SELECT index_name
        FROM   dba_indexes
        WHERE  owner = p_owner
          AND  table_name = p_table_name
          AND  partitioned = 'NO'
          AND  status = 'UNUSABLE'
    )
    LOOP
        EXECUTE IMMEDIATE
            'ALTER INDEX '||p_owner||'.'||g.index_name||
            ' REBUILD TABLESPACE '||p_index_tbs;
    END LOOP;

END;
/