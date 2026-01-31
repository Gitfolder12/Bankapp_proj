--------------------------------------------------------
--  File created - Saturday-January-31-2026   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DISABLE_ALL_FKS_IN_SCHEMA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HR"."DISABLE_ALL_FKS_IN_SCHEMA" (
    p_owner IN VARCHAR2
) IS
BEGIN
    ------------------------------------------------------------------
    -- Disable all enabled foreign key constraints in the schema
    ------------------------------------------------------------------
    FOR c IN (
        SELECT owner, table_name, constraint_name
        FROM   dba_constraints
        WHERE  owner           = UPPER(p_owner)
        AND    constraint_type = 'R'
        AND    status          = 'ENABLED'
        ORDER  BY table_name
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE
                'ALTER TABLE ' || c.owner || '.' || c.table_name ||
                ' DISABLE CONSTRAINT ' || c.constraint_name;

            DBMS_OUTPUT.PUT_LINE(
                'Disabled FK: ' || c.owner || '.' || c.table_name ||
                ' -> ' || c.constraint_name
            );
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(
                    'Failed to disable FK: ' || c.constraint_name ||
                    ' : ' || SQLERRM
                );
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE(
        'All foreign keys disabled for schema ' || UPPER(p_owner)
    );
END disable_all_fks_in_schema;

/
