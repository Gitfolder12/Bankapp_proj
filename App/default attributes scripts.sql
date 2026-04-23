---3.1 Capture BEFORE Snapshot — Tables

-- Normal partition tables
SELECT TABLE_NAME,
       PARTITIONING_TYPE,
       SUBPARTITIONING_TYPE,
       DEF_TABLESPACE_NAME                               AS default_tbs,
       CASE
         WHEN DEF_TABLESPACE_NAME = 'SLR_DATA_NEW'
         THEN 'OK'
         ELSE 'NOT SET'
       END                                               AS status
FROM   DBA_PART_TABLES
WHERE  OWNER                = 'SLR'
AND    SUBPARTITIONING_TYPE = 'NONE'
ORDER  BY TABLE_NAME;


-- Composite partition tables (table level)
SELECT TABLE_NAME,
       PARTITIONING_TYPE,
       SUBPARTITIONING_TYPE,
       DEF_TABLESPACE_NAME                               AS default_tbs,
       CASE
         WHEN DEF_TABLESPACE_NAME = 'SLR_DATA_NEW'
         THEN 'OK'
         ELSE 'NOT SET'
       END                                               AS status
FROM   DBA_PART_TABLES
WHERE  OWNER                = 'SLR'
AND    SUBPARTITIONING_TYPE != 'NONE'
ORDER  BY TABLE_NAME;


-- Composite partition tables (partition level)
SELECT TABLE_NAME,
       PARTITION_NAME,
       DEF_TABLESPACE_NAME                               AS default_tbs,
       CASE
         WHEN DEF_TABLESPACE_NAME = 'SLR_DATA_NEW'
         THEN 'OK'
         ELSE 'NOT SET'
       END                                               AS status
FROM   DBA_TAB_PARTITIONS
WHERE  TABLE_OWNER = 'SLR'
ORDER  BY TABLE_NAME,
          PARTITION_POSITION;

--- Capture BEFORE Snapshot — Indexes

-- Normal partition indexes
SELECT INDEX_NAME,
       TABLE_NAME,
       PARTITIONING_TYPE,
       SUBPARTITIONING_TYPE,
       DEF_TABLESPACE_NAME                               AS default_tbs,
       CASE
         WHEN DEF_TABLESPACE_NAME = 'SLR_DATA_IDX_NEW'
         THEN 'OK'
         ELSE 'NOT SET'
       END                                               AS status
FROM   DBA_PART_INDEXES
WHERE  OWNER                = 'SLR'
AND    SUBPARTITIONING_TYPE = 'NONE'
ORDER  BY TABLE_NAME,
          INDEX_NAME;


-- Composite partition indexes (partition level)
SELECT ip.INDEX_NAME,
       ip.TABLE_NAME,
       ip.PARTITION_NAME,
       ip.DEF_TABLESPACE_NAME                            AS default_tbs,
       CASE
         WHEN ip.DEF_TABLESPACE_NAME = 'SLR_DATA_IDX_NEW'
         THEN 'OK'
         ELSE 'NOT SET'
       END                                               AS status
FROM   DBA_IND_PARTITIONS ip
JOIN   DBA_PART_INDEXES   i
  ON   i.INDEX_NAME = ip.INDEX_NAME
 AND   i.OWNER      = 'SLR'
WHERE  i.SUBPARTITIONING_TYPE != 'NONE'
ORDER  BY ip.TABLE_NAME,
          ip.INDEX_NAME,
          ip.PARTITION_POSITION;
		  
		  
=================================================================================================
--Step 3. SET DEFAULT ATTRIBUTES (VERY IMPORTANT)
---Execute — Set Default for Tables
-- Normal partition tables
DECLARE
  v_new_tbs VARCHAR2(30) := 'SLR_DATA_NEW';
  v_schema  VARCHAR2(30) := 'SLR';
BEGIN
  FOR t IN (
    SELECT TABLE_NAME
    FROM   DBA_PART_TABLES
    WHERE  OWNER                = v_schema
    AND    SUBPARTITIONING_TYPE = 'NONE'
    ORDER  BY TABLE_NAME
  )
  LOOP
    BEGIN
      EXECUTE IMMEDIATE
        'ALTER TABLE ' || v_schema || '.' || t.TABLE_NAME ||
        ' MODIFY DEFAULT ATTRIBUTES TABLESPACE ' || v_new_tbs;
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;
  END LOOP;
END;
/


-- Composite partition tables
DECLARE
  v_new_tbs VARCHAR2(30) := 'SLR_DATA_NEW';
  v_schema  VARCHAR2(30) := 'SLR';
BEGIN
  FOR t IN (
    SELECT TABLE_NAME
    FROM   DBA_PART_TABLES
    WHERE  OWNER                = v_schema
    AND    SUBPARTITIONING_TYPE != 'NONE'
    ORDER  BY TABLE_NAME
  )
  LOOP
    BEGIN
      EXECUTE IMMEDIATE
        'ALTER TABLE ' || v_schema || '.' || t.TABLE_NAME ||
        ' MODIFY DEFAULT ATTRIBUTES TABLESPACE ' || v_new_tbs;
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;

    FOR p IN (
      SELECT PARTITION_NAME
      FROM   DBA_TAB_PARTITIONS
      WHERE  TABLE_OWNER = v_schema
      AND    TABLE_NAME  = t.TABLE_NAME
      ORDER  BY PARTITION_POSITION
    )
    LOOP
      BEGIN
        EXECUTE IMMEDIATE
          'ALTER TABLE ' || v_schema || '.' || t.TABLE_NAME ||
          ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' || p.PARTITION_NAME ||
          ' TABLESPACE ' || v_new_tbs;
      EXCEPTION
        WHEN OTHERS THEN NULL;
      END;
    END LOOP;

  END LOOP;
END;
/

---Execute — Set Default for Indexes
-- Normal partition indexes
DECLARE
  v_new_tbs VARCHAR2(30) := 'SLR_DATA_IDX_NEW';
  v_schema  VARCHAR2(30) := 'SLR';
BEGIN
  FOR i IN (
    SELECT INDEX_NAME
    FROM   DBA_PART_INDEXES
    WHERE  OWNER                = v_schema
    AND    SUBPARTITIONING_TYPE = 'NONE'
    ORDER  BY INDEX_NAME
  )
  LOOP
    BEGIN
      EXECUTE IMMEDIATE
        'ALTER INDEX ' || v_schema || '.' || i.INDEX_NAME ||
        ' MODIFY DEFAULT ATTRIBUTES TABLESPACE ' || v_new_tbs;
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;
  END LOOP;
END;
/


-- Composite partition indexes
DECLARE
  v_new_tbs VARCHAR2(30) := 'SLR_DATA_IDX_NEW';
  v_schema  VARCHAR2(30) := 'SLR';
BEGIN
  FOR i IN (
    SELECT INDEX_NAME
    FROM   DBA_PART_INDEXES
    WHERE  OWNER                = v_schema
    AND    SUBPARTITIONING_TYPE != 'NONE'
    ORDER  BY INDEX_NAME
  )
  LOOP
    BEGIN
      EXECUTE IMMEDIATE
        'ALTER INDEX ' || v_schema || '.' || i.INDEX_NAME ||
        ' MODIFY DEFAULT ATTRIBUTES TABLESPACE ' || v_new_tbs;
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;

    FOR p IN (
      SELECT PARTITION_NAME
      FROM   DBA_IND_PARTITIONS
      WHERE  INDEX_OWNER = v_schema
      AND    INDEX_NAME  = i.INDEX_NAME
      ORDER  BY PARTITION_POSITION
    )
    LOOP
      BEGIN
        EXECUTE IMMEDIATE
          'ALTER INDEX ' || v_schema || '.' || i.INDEX_NAME ||
          ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' || p.PARTITION_NAME ||
          ' TABLESPACE ' || v_new_tbs;
      EXCEPTION
        WHEN OTHERS THEN NULL;
      END;
    END LOOP;

  END LOOP;
END;
/