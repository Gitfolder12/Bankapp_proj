SELECT
    owner,
    table_name,
    constraint_name,
    status,
    validated,
    fk_role,
    r_owner,
    r_constraint_name
FROM (
    -- CHILD FKs
    SELECT
        c.owner,
        c.table_name,
        c.constraint_name,
        c.status,
        c.validated,
        'CHILD' AS fk_role,
        c.r_owner,
        c.r_constraint_name
    FROM dba_constraints c
    WHERE c.constraint_type = 'R'
      AND c.owner = 'HR'
--      AND c.table_name = :p_table

    UNION ALL

    -- PARENT FKs
    SELECT
        c.owner,
        c.table_name,
        c.constraint_name,
        c.status,
        c.validated,
        'PARENT' AS fk_role,
        c.r_owner,
        c.r_constraint_name
    FROM dba_constraints c
    WHERE c.constraint_type = 'R'
      AND c.r_owner = 'HR'
      AND c.r_constraint_name IN (
            SELECT constraint_name
            FROM dba_constraints
            WHERE owner = 'HR'
--              AND table_name = :p_table
              AND constraint_type IN ('P','U')
      )
)
ORDER BY fk_role, owner, table_name, constraint_name;
