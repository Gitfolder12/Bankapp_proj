--simple partition
DECLARE
  v_new_tbs  VARCHAR2(30) := 'SLR_DATA_NEW';
  v_schema   VARCHAR2(30) := 'YOUR_SCHEMA';
BEGIN
  FOR t IN (
    SELECT TABLE_NAME
    FROM DBA_PART_TABLES
    WHERE OWNER                = v_schema
    AND   SUBPARTITIONING_TYPE = 'NONE'
    ORDER BY TABLE_NAME
  )
  LOOP
      EXECUTE IMMEDIATE ' ALTER TABLE '
                   	    || v_schema 
						|| '.' 
						|| t.TABLE_NAME 
						|| ' MODIFY DEFAULT ATTRIBUTES TABLESPACE ' 
						|| v_new_tbs;
  END LOOP;
EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERR: ' || t.TABLE_NAME || ' - ' || SQLERRM);
END;
/


--composite partition
DECLARE
  v_new_tbs  VARCHAR2(30) := 'SLR_DATA_NEW';
  v_schema   VARCHAR2(30) := 'YOUR_SCHEMA';
BEGIN
   -- Table level default
  FOR t IN (
    SELECT TABLE_NAME
    FROM DBA_PART_TABLES
    WHERE OWNER                 = v_schema
    AND   SUBPARTITIONING_TYPE != 'NONE'
    ORDER BY TABLE_NAME
  )
  LOOP
      -- Table level default
      EXECUTE IMMEDIATE   ' ALTER TABLE ' 
				        || v_schema 
						|| '.' 
						|| t.TABLE_NAME 
						|| ' MODIFY DEFAULT ATTRIBUTES TABLESPACE ' 
						|| v_new_tbs;


    -- Partition level default (covers future subpartitions)
    FOR p IN (
      SELECT PARTITION_NAME
      FROM DBA_TAB_PARTITIONS
      WHERE TABLE_OWNER = v_schema
      AND   TABLE_NAME  = t.TABLE_NAME
      ORDER BY PARTITION_POSITION
    )
    LOOP
      BEGIN
        EXECUTE IMMEDIATE ' ALTER TABLE ' 
		                  || v_schema 
						  || '.' 
						  || t.TABLE_NAME 
						  || ' MODIFY DEFAULT ATTRIBUTES FOR PARTITION ' 
						  || p.PARTITION_NAME 
						  || ' TABLESPACE ' 
						  || v_new_tbs;


    END LOOP;
 END LOOP;
 
EXCEPTION
        WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('  ERR (PART): ' || p.PARTITION_NAME || ' - ' || SQLERRM);
END;
/