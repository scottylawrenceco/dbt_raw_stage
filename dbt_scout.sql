

/* This script allows you to set a few varaibles and automatically generate the DBT select script and the dbdiagram.io code */


set table_n = 'ORDERS';

/* Set additional variables*/
set schema_n = 'HEROKU_POSTGRES_PUBLIC';
set source_n = 'SOURCE_FIVETRAN';
set dbt_source = 'talent_hack_app';


/* Don't change these */
set full_table = '"' || $source_n || '"."'|| $schema_n ||'"."'|| $table_n ||'"';
set info = '"' || $source_n || '"."INFORMATION_SCHEMA"."COLUMNS"';
//select $full_table;
//select $info;

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
    {{source(\''|| lower($dbt_source)||'\',\'' || lower($table_n) || '\')}}'
)))

) as DBT


/*  Create DiagramDB text fields */

,(SELECT
    lower(replace(replace(REPLACE(regexp_replace(regexp_replace(GET_DDL('table',$full_table),'[(]','{',1,1),'[\)][;]','}',1),',',''),'NOT NULL','[not null]'),'create or replace TABLE ','table '))
) as diagramdb

FROM
  "SOURCE_FIVETRAN"."INFORMATION_SCHEMA"."TABLES"    
WHERE
    TABLE_NAME = $table_n
