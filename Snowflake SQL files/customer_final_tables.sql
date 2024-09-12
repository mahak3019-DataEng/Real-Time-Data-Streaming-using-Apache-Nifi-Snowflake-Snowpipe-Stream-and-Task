use database demo;
use schema demo.nifi_schema;

create or replace table customer (
customer_id integer,
first_name varchar,
last_name varchar,
email varchar,
street varchar,
city varchar,
state varchar,
country varchar,
update_timestamp timestamp_ntz default current_timestamp()
);

create or replace table customer_history (
customer_id integer,
first_name varchar,
last_name varchar,
email varchar,
street varchar,
city varchar,
state varchar,
country varchar,
start_time timestamp_ntz default current_timestamp(),
end_time timestamp_ntz default current_timestamp(),
is_current boolean
);