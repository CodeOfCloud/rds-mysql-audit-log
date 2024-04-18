-- create datbase in glue metadata store
CREATE DATABASE rds_mysql_audit_log

-- create table
CREATE EXTERNAL TABLE rds_mysql_audit_log (
    timestamp STRING,
    serverhost STRING,
    username STRING,
    host STRING,
    connectionid STRING,
    queryid STRING,
    operation STRING,
    `database` STRING,
    object STRING,
    retcode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://rds-mysql-audit-log/small-mysql_demo/';

-- sample query
select * from rds_mysql_audit_log where database like 'demo%' limit 10;
