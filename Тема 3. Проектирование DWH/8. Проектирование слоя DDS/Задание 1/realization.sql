SELECT current_database();

drop schema if exists dds;
create schema dds;

SELECT schema_name 
FROM information_schema.schemata
ORDER BY schema_name;