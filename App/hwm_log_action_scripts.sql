CREATE TABLE HWM_LOG
(   owner        VARCHAR2(100 BYTE),
    BASE_TABLE   VARCHAR2(1000 BYTE), 
    OBJECT_NAME  VARCHAR2(4000 BYTE), 
    OBJECT_TYPE  VARCHAR2(100 BYTE), 
    STATUS       VARCHAR2(100 BYTE), 
    ERROR_MSG    VARCHAR2(4000 BYTE), 
    ACTION_TS    TIMESTAMP (6) DEFAULT SYSTIMESTAMP,
    CONSTRAINT chk_status_valid 
        CHECK (STATUS IN ('SUCCESS', 'FAILED'))
);


--log action
create or replace PROCEDURE log_action (
    p_owner         IN VARCHAR2,
    p_base_table    IN VARCHAR2,
    p_object_name   IN VARCHAR2,
    p_object_type   IN VARCHAR2,
    p_status        IN VARCHAR2,
    p_error_msg     IN VARCHAR2 DEFAULT NULL
) IS
BEGIN
    INSERT INTO HWM_LOG (
        log_id,
	    owner,
        base_table,
        OBJECT_NAME,
        OBJECT_TYPE,
        STATUS,
        ERROR_MSG,
        ACTION_TS
    )
    VALUES (
        hwm_log_seq.NEXTVAL,
	    p_owner,
        p_base_table,
        p_object_name,
        p_object_type,
        p_status,
        p_error_msg,
        SYSDATE
    );
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END log_action;