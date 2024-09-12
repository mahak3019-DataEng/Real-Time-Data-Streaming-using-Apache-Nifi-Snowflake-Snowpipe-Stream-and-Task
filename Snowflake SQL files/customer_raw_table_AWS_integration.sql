use database demo;
create or replace schema nifi_schema;

use schema nifi_schema;

create or replace storage integration nifi_integration
type = external_stage
storage_provider = 'S3'
enabled = True
storage_aws_role_arn = 'arn:aws:iam::533267281673:role/json_role'
storage_allowed_locations = ('s3://apache-nifi-snowflake-project/stream_data/');

desc integration nifi_integration;


create or replace file format nifi_file_format
type = 'CSV'
field_delimiter = ','
skip_header = 1
field_optionally_enclosed_by = '"'
null_if = ('null','NULL')
empty_field_as_null = True;

create or replace stage nifi_stage
storage_integration = nifi_integration
file_format = (format_name =  nifi_file_format)
url = 's3://apache-nifi-snowflake-project/stream_data/';

list @nifi_stage;



create or replace table demo.nifi_schema.customer_raw (
customer_id integer,
first_name varchar,
last_name varchar,
email varchar,
street varchar,
city varchar,
state varchar,
country varchar
);
copy into demo.nifi_schema.customer_raw
from @demo.nifi_schema.nifi_stage
file_format = (format_name = nifi_file_format);

select count(distinct customer_id) from demo.nifi_schema.customer_raw;
truncate table demo.nifi_schema.customer_raw;

create or replace pipe nifi_snowpipe
auto_ingest =True
AS
copy into demo.nifi_schema.customer_raw
from @demo.nifi_schema.nifi_stage;

desc pipe nifi_snowpipe;
show pipes;

select count(*) from demo.nifi_schema.customer_raw;
select * from demo.nifi_schema.customer_raw;

create or replace stream demo.nifi_schema.nifi_stream on table demo.nifi_schema.customer;




