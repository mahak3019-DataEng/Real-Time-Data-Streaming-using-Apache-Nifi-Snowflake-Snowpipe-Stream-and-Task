use database demo;
use schema demo.nifi_schema;

MERGE INTO demo.nifi_schema.customer c
using demo.nifi_schema.customer_raw cr
on c.customer_id = cr.customer_id
when matched and c.customer_id <> cr.customer_id or
                 c.first_name <> cr.first_name or
                 c.last_name <> cr.last_name or
                 c.email <> cr.email or
                 c.street <> cr.street or
                 c.city <> cr.city or
                 c.state <> cr.state or
                 c.country <> cr.country then update
             set c.customer_id = cr.customer_id,
                 c.first_name = cr.first_name,
                 c.last_name = cr.last_name,
                 c.email = cr.email,
                 c.street = cr.street,
                 c.city = cr.city,
                 c.state = cr.state,
                 c.country = cr.country,
                 update_timestamp = current_timestamp()

when not matched then insert
(c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
values (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);


select count(*) from demo.nifi_schema.customer;



// creating the stored procedure so that we don't have to run this manually.
CREATE OR REPLACE PROCEDURE merge_procedure()
returns string not null
language javascript
as 
    $$
        var cmd = `
        MERGE INTO demo.nifi_schema.customer c
        using demo.nifi_schema.customer_raw cr
        on c.customer_id = cr.customer_id
        when matched and c.customer_id <> cr.customer_id or
                         c.first_name <> cr.first_name or
                         c.last_name <> cr.last_name or
                         c.email <> cr.email or
                         c.street <> cr.street or
                         c.city <> cr.city or
                         c.state <> cr.state or
                         c.country <> cr.country then update
                set c.customer_id = cr.customer_id,
                    c.first_name = cr.first_name,
                    c.last_name = cr.last_name,
                    c.email = cr.email,
                    c.street = cr.street,
                    c.city = cr.city,
                    c.state = cr.state,
                    c.country = cr.country,
                    update_timestamp = current_timestamp()

        when not matched then insert
        (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
        values (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);`

        var cmd1 = `truncate table demo.nifi_schema.customer_raw`
        var sql = snowflake.createStatement({sqlText: cmd});
        var sql2 = snowflake.createStatement({sqlText: cmd1});
        var result = sql.execute();
        var result1 = sql2.execute();
        return cmd + '\n'+ cmd1;
    $$;
call merge_procedure();

//Setting up the permissions on role for task.
// because also want to automate the stored procedure.
use role securityadmin;
// i have created the following role to grant the role taskadmin to the users so that, they can interact with the task.
create or replace role taskadmin;


use role accountadmin;

//now i will grant the permission to the role taskadmin
grant execute task on account to role taskadmin;

use role securityadmin; // we used this role to grant access to sysadmin role
grant role taskadmin to role sysadmin;

SHOW GRANTS ON DATABASE DEMO;
// now we will be creating task and automating the stored procedure.
create or replace task demo.nifi_schema.scd1_raw_to_customer warehouse = COMPUTE_WH schedule = '1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE = False
as
call demo.nifi_schema.merge_procedure();

show tasks;
alter task demo.nifi_schema.scd1_raw_to_customer resume;
alter task demo.nifi_schema.scd1_raw_to_customer suspend;

use role accountadmin;
select * from demo.nifi_schema.customer_raw;
select * from demo.nifi_schema.customer where customer_id=0;

//verifying the automated stored procedure
select * from demo.nifi_schema.customer;




