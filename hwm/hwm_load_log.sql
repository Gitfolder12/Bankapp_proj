create or replace PROCEDURE log_hwm_copy (
    p_owner       IN VARCHAR2,
    p_src_table   IN VARCHAR2,
    p_object_name IN VARCHAR2,   -- partition or subpartition
    p_dst_table   IN VARCHAR2,
    p_status      IN VARCHAR2,
    p_error_msg   IN VARCHAR2 DEFAULT NULL
) IS
BEGIN
    INSERT INTO hwm_copy_log (
        owner,
        src_table,
        object_name,
        dst_table,
        copied_at,
        status,
        error_msg
    )
    VALUES (
        UPPER(p_owner),
        UPPER(p_src_table),
        p_object_name,
        UPPER(p_dst_table),
        SYSDATE,
        UPPER(p_status),
        SUBSTR(p_error_msg, 1, 4000)
    );
END;
