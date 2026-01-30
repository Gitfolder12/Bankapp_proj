--------------------------------------------------------
--  File created - Friday-January-30-2026   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure LOG_HWM_COPY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "CR"."LOG_HWM_COPY" (
    p_src_owner    IN VARCHAR2,   -- source schema
    p_src_table    IN VARCHAR2,
    p_object_name  IN VARCHAR2,   -- partition or subpartition
    p_dst_owner    IN VARCHAR2,   -- target schema
    p_dst_table    IN VARCHAR2,
    p_status       IN VARCHAR2,
    p_error_msg    IN VARCHAR2 DEFAULT NULL,
    p_src_cnt      IN NUMBER   DEFAULT NULL,
    p_trg_cnt      IN NUMBER   DEFAULT NULL,
    p_action_ts    IN DATE     DEFAULT SYSDATE  -- action timestamp
) IS
BEGIN
    INSERT INTO CR.hwm_copy_log (
        P_SRC_OWNER,
        SRC_TABLE,
        OBJECT_NAME,
        P_DST_OWNER,
        DST_TABLE,
        ACTION_TS,
        STATUS,
        ERROR_MSG,
        SRC_ROW_COUNT,
        TRG_ROW_COUNT
    )
    VALUES (
        UPPER(p_src_owner),
        UPPER(p_src_table),
        p_object_name,
        UPPER(p_dst_owner),
        UPPER(p_dst_table),
        NVL(p_action_ts, SYSDATE),
        UPPER(p_status),
        SUBSTR(p_error_msg, 1, 4000),
        p_src_cnt,
        p_trg_cnt
    );
END log_hwm_copy;

/
