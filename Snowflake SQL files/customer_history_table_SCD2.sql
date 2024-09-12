use database demo;
use schema demo.nifi_schema;

show streams;
select * from demo.nifi_schema.nifi_stream;
select count(*) from demo.nifi_schema.nifi_stream;



// Applying some insert, update and delete operations on the customer table. All this will get stored in stream.
insert into customer values (223138,'Aavish','Jain','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut'
                            ,'Cape Verde',current_timestamp());
update demo.nifi_schema.customer set first_name = 'Aditya', last_name ='Agrawal' where customer_id = 4;
delete from demo.nifi_schema.customer where customer_id = 74;

select * from  customer where customer_id in (4,74,223138);
select * from  demo.nifi_schema.nifi_stream where customer_id in (4,74,223138);






//creating view and then we will merge with customer_history table
create or replace view demo.nifi_schema.view_history as
(select 
customer_id,
first_name,
last_name,
email,
street,
city,
state,
country,
start_time,
end_time,
is_current,
'I' as dml_type
from (
select customer_id, first_name, last_name, email, street, city, state, country,
update_timestamp as start_time,
lag(update_timestamp) over(partition by customer_id order by start_time desc) as end_time_raw,
case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
case when end_time_raw is null then True else False end as is_current
from (
select customer_id, first_name, last_name, email, street, city, state, country, update_timestamp
from demo.nifi_schema.nifi_stream 
where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE='FALSE'
)
))

union


(select 
customer_id,
first_name,
last_name,
email,
street,
city,
state,
country,
start_time,
end_time,
is_current,
dml_type
from (
select customer_id, first_name, last_name, email, street, city, state, country,
update_timestamp as start_time,
lag(update_timestamp) over(partition by customer_id order by start_time desc) as end_time_raw,
case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
case when end_time_raw is null then True else False end as is_current,
dml_type
from (

select customer_id, first_name, last_name, email, street, city, state, country, update_timestamp, 'I' as dml_type
from demo.nifi_schema.nifi_stream 
where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE='TRUE'
union
select customer_id, null, null, null, null, null, null, null, start_time, 'U' as dml_type
from demo.nifi_schema.customer_history
where customer_id in (select distinct customer_id from demo.nifi_schema.nifi_stream where METADATA$ACTION = 'DELETE' and METADATA$ISUPDATE='TRUE') and
is_current = True)
)
)

union

(select customer_id, null, null, null, null, null, null, null, start_time, current_timestamp()::timestamp_ntz,null,'D' as dml_type
from demo.nifi_schema.customer_history
where customer_id in (select distinct customer_id from demo.nifi_schema.nifi_stream where METADATA$ACTION = 'DELETE' and METADATA$ISUPDATE='FALSE') and
is_current = True);

select * from demo.nifi_schema.view_history where dml_type = 'I';




merge into demo.nifi_schema.customer_history ch
using demo.nifi_schema.view_history vh
on ch.customer_id = vh.customer_id and ch.start_time = vh.start_time
when matched and vh.dml_type='U' and ch.is_current= True then update
                    set ch.end_time = vh.end_time, ch.is_current =False
when matched and vh.dml_type='D' and ch.is_current= True then update
                    set ch.end_time = vh.end_time, ch.is_current =False

when not matched and vh.dml_type='I' then insert
(customer_id, first_name, last_name, email, street, city, state, country,start_time,end_time,is_current)
values (vh.customer_id, vh.first_name, vh.last_name, vh.email, vh.street, vh.city, vh.state, vh.country, vh.start_time, vh.end_time,vh.is_current);

//Applying the operations on the table

insert into customer values(223136,'Akash','Arnold','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut'
                            ,'Cape Verde',current_timestamp());
update customer set FIRST_NAME='Omarao' where customer_id=7523;
delete from customer where customer_id =136;
select count(*),customer_id from customer group by customer_id having count(*)=1;
select * from customer_history where customer_id =223136;
select * from customer_history where IS_CURRENT=TRUE;

select * from demo.nifi_schema.view_history where dml_type = 'U';