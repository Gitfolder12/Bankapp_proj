--------------------------------------------------------
--  File created - Friday-January-30-2026   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure LOG_HWM_COPY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "CR"."LOG_HWM_COPY" (
    p_owner        IN VARCHAR2,
    p_src_table    IN VARCHAR2,
    p_object_name  IN VARCHAR2,   -- partition or subpartition
    p_dst_table    IN VARCHAR2,
    p_status       IN VARCHAR2,
    p_error_msg    IN VARCHAR2 DEFAULT NULL,
    p_src_cnt      IN NUMBER   DEFAULT NULL,
    p_trg_cnt      IN NUMBER   DEFAULT NULL
) IS
BEGIN
    INSERT INTO CR.hwm_copy_log (
        owner,
        src_table,
        object_name,
        dst_table,
        copied_at,
        status,
        error_msg,
        src_row_count,
        trg_row_count
    )
    VALUES (
        UPPER(p_owner),
        UPPER(p_src_table),
        p_object_name,
        UPPER(p_dst_table),
        SYSDATE,
        UPPER(p_status),
        SUBSTR(p_error_msg, 1, 4000),
        p_src_cnt,
        p_trg_cnt
    );
END;

/
