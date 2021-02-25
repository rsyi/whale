SELECT 
    c.table_catalog as cluster, c.table_schema as schema, c.table_name as name, pgtd.description as description
    , c.column_name as col_name, c.data_type as col_type, pgcd.description as col_description
    , ordinal_position as col_sort_order, CASE WHEN b.table_name IS NOT NULL THEN 1 ELSE 0 END AS is_view
FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN pg_catalog.pg_statio_all_tables st
        on c.table_schema=st.schemaname and c.table_name=st.relname
    LEFT JOIN pg_partitions prt 
        on c.table_schema = prt.partitionschemaname and c.table_name = prt.partitiontablename
    LEFT JOIN
        pg_catalog.pg_description pgcd 
            on pgcd.objoid=st.relid and pgcd.objsubid=c.ordinal_position
    LEFT JOIN
        pg_catalog.pg_description pgtd 
            on pgtd.objoid=st.relid and pgtd.objsubid=0
    LEFT JOIN 
        INFORMATION_SCHEMA.VIEWS b 
            ON c.table_catalog = b.table_catalog and c.table_schema = b.table_schema and c.table_name = b.table_name
WHERE prt.partitionschemaname IS NULL and prt.partitiontablename IS NULL;
