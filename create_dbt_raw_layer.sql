/* This is intended to run in Snowflake only. */
/* Set database */
set source_n = 'STITCH_DATA';
/* Set Schema */
set schema_n = 'STRIPE';
/* Set table to convert to DBT view */
set table_n = 'CHARGES';

/* Set the data source name used in DBT. 
The output will use {{source('dbt_source','table_n')}} in your from statement */
set dbt_source = 'talent_hack_app';



/* Don't change these */
set full_table = '"' || $source_n || '"."'|| $schema_n ||'"."'|| $table_n ||'"';
set info = '"' || $source_n || '"."INFORMATION_SCHEMA"."COLUMNS"';
set info_t = '"' || $source_n || '"."INFORMATION_SCHEMA"."TABLES"';

/* Create select statement for DBT */
SELECT
  TABLE_CATALOG
  ,TABLE_SCHEMA
  ,TABLE_NAME
  ,('"' || TABLE_CATALOG || '"."' || TABLE_SCHEMA || '"."' || TABLE_NAME || '"') as "FULL_TABLE"
  ,('"' || TABLE_CATALOG || '"."INFORMATION_SCHEMA"."COLUMNS"') as COLUMN_TABLE
  
,(SELECT
    LISTAGG(CASE WHEN MERGE_ORDER = 1 THEN REPLACE(QUERY_NAME,',','') ELSE QUERY_NAME END) within group (order by merge_order)
FROM
    (
    SELECT * FROM (      
          (SELECT 
          0::int as merge_order
          ,'{{
    config(
        materialized = \'view\',
        unique_key = \'ID\'
    )
}}

SELECT' as QUERY_NAME)

        UNION ALL    
    
(SELECT
 ordinal_position   
 ,'
    ,' || COLUMN_NAME || '::' || CASE WHEN DATA_TYPE = 'TEXT' then 'VARCHAR(' || CHARACTER_MAXIMUM_LENGTH ||')' ELSE DATA_TYPE END || ' as ' || COLUMN_NAME  as "SELECT"
FROM
    identifier($info)
WHERE
    TABLE_SCHEMA = $schema_n
    and TABLE_NAME = $table_n
ORDER BY ORDINAL_POSITION)
      
        UNION ALL

(select 
1000000
,'
FROM
    {{source(\''|| lower($dbt_source)||'\',\'' || lower(replace($table_n,'PUBLIC_','')) || '\')}}'
)))

) as DBT

FROM
  identifier($info_t)
  //"SOURCE_STITCH_DATA"."INFORMATION_SCHEMA"."TABLES"    
WHERE
    TABLE_NAME = $table_n
