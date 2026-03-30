ORACLE TABLESPACE MIGRATION & HWM RESET – COMPLETE STEP GUIDE

## OBJECTIVE

Reclaim disk space (GB/TB level) by eliminating HWM and migrating objects to a new tablespace.

## CORE PRINCIPLE

Disk space is only reclaimed when the datafile tail is empty.
Deleting data or shrinking segments alone does NOT reclaim disk.

---

## STEP 1: BASELINE VALIDATION (BEFORE ANY CHANGE)

-- Tablespace Size
SELECT tablespace_name,
ROUND(SUM(bytes)/1024/1024/1024,2) total_gb
FROM dba_data_files
GROUP BY tablespace_name;

-- Used vs Free
SELECT df.tablespace_name,
ROUND(SUM(df.bytes)/1024/1024/1024,2) total_gb,
ROUND(SUM(fs.bytes)/1024/1024/1024,2) free_gb
FROM dba_data_files df
LEFT JOIN dba_free_space fs
ON df.tablespace_name = fs.tablespace_name
GROUP BY df.tablespace_name;

-- Datafile HWM
WITH hwm AS (
SELECT file_id, MAX(block_id + blocks - 1) hwm_block
FROM dba_extents
GROUP BY file_id
)
SELECT df.file_name,
ROUND(df.bytes/1024/1024/1024,2) file_size_gb,
ROUND((h.hwm_block * ts.block_size)/1024/1024/1024,2) min_possible_gb,
ROUND((df.bytes - (h.hwm_block * ts.block_size))/1024/1024/1024,2) reclaimable_gb
FROM dba_data_files df
JOIN hwm h ON df.file_id = h.file_id
JOIN dba_tablespaces ts ON df.tablespace_name = ts.tablespace_name;

-- Segment Summary
SELECT tablespace_name,
COUNT(*) segment_count,
ROUND(SUM(bytes)/1024/1024/1024,2) size_gb
FROM dba_segments
WHERE owner = 'CR'
GROUP BY tablespace_name;

-- Segment Type Summary
SELECT tablespace_name,
segment_type,
COUNT(*) segment_count,
ROUND(SUM(bytes)/1024/1024/1024,2) size_gb
FROM dba_segments
WHERE owner = 'CR'
GROUP BY tablespace_name, segment_type;

---

## STEP 2: PREPARE NEW TABLESPACE

CREATE TABLESPACE HWM_NEW_DATA
DATAFILE '.../hwm_new_data01.dbf' SIZE 10G AUTOEXTEND ON;

ALTER USER CR DEFAULT TABLESPACE HWM_NEW_DATA;

ALTER USER CR QUOTA UNLIMITED ON HWM_NEW_DATA;

---

## STEP 3: SET DEFAULT ATTRIBUTES (VERY IMPORTANT)

-- Partitioned Tables
BEGIN
FOR r IN (
SELECT owner, table_name
FROM dba_part_tables
WHERE owner = 'CR'
)
LOOP
EXECUTE IMMEDIATE
'ALTER TABLE ' || r.owner || '.' || r.table_name ||
' MODIFY DEFAULT ATTRIBUTES TABLESPACE HWM_NEW_DATA';
END LOOP;
END;
/

-- Partitioned Indexes
BEGIN
FOR r IN (
SELECT owner, index_name
FROM dba_part_indexes
WHERE owner = 'CR'
)
LOOP
EXECUTE IMMEDIATE
'ALTER INDEX ' || r.owner || '.' || r.index_name ||
' MODIFY DEFAULT ATTRIBUTES TABLESPACE HWM_NEW_DATA';
END LOOP;
END;
/

---

## STEP 4: VALIDATE SETUP

SELECT tablespace_name FROM dba_tablespaces;

SELECT username, tablespace_name, max_bytes
FROM dba_ts_quotas
WHERE username = 'CR';

---

## STEP 5: MIGRATION

-- Move Non-Partitioned Tables
ALTER TABLE table_name MOVE TABLESPACE HWM_NEW_DATA;

-- Move Partitioned Tables
ALTER TABLE table_name MOVE PARTITION partition_name TABLESPACE HWM_NEW_DATA;

-- Move Subpartitions
ALTER TABLE table_name MOVE SUBPARTITION subpartition_name TABLESPACE HWM_NEW_DATA;

-- Rebuild Indexes
ALTER INDEX index_name REBUILD TABLESPACE HWM_NEW_DATA;

-- Rebuild Index Partitions
ALTER INDEX index_name REBUILD PARTITION partition_name TABLESPACE HWM_NEW_DATA;

-- Gather Statistics
BEGIN
DBMS_STATS.GATHER_SCHEMA_STATS('CR');
END;
/

---

## STEP 6: POST-MIGRATION VALIDATION

-- Confirm Migration
SELECT tablespace_name,
COUNT(*) segment_count,
ROUND(SUM(bytes)/1024/1024/1024,2) size_gb
FROM dba_segments
WHERE owner = 'CR'
GROUP BY tablespace_name;

-- Old Tablespace Should Be Empty
SELECT *
FROM dba_segments
WHERE tablespace_name = 'HWM_DATA';

-- Segment Type Check
SELECT segment_type, COUNT(*)
FROM dba_segments
WHERE tablespace_name = 'HWM_DATA'
GROUP BY segment_type;

-- Datafile Segment Count
SELECT df.file_name,
COUNT(s.segment_name) segment_count
FROM dba_data_files df
LEFT JOIN dba_segments s
ON df.file_id = s.header_file
WHERE df.tablespace_name = 'HWM_DATA'
GROUP BY df.file_name;

---

## STEP 7: SPACE RECLAMATION

-- Resize Datafile
ALTER DATABASE DATAFILE '.../hwm_data01.dbf' RESIZE 1G;

-- OR Drop Tablespace (Preferred)
DROP TABLESPACE HWM_DATA INCLUDING CONTENTS AND DATAFILES;

---

## SUCCESS CRITERIA

* All objects moved to HWM_NEW_DATA
* Old tablespace contains 0 segments
* Segment types count = 0
* Datafiles show reclaimable space
* Disk space reduced

---

## FINAL RULE

SET DEFAULT → MOVE OBJECTS → VALIDATE → RESIZE/DROP

---
