-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- Personal Development
-----------------------------------------------------------------------------
-- Creation Date: 20240708
-- Last Modification Date: 20240708
-- Authors: mateoper
-- Last Authors: mateoper
-- Description:    Creation of a table to store the master of validations
-----------------------------------------------------------------------------
---------------------------------- INPUTS -----------------------------------
-- 
---------------------------------- OUTPUTS ----------------------------------
-- {zona_r}.{prefix}_tests_masterval
-----------------------------------------------------------------------------
-------------------------------- Query Start --------------------------------
--
drop table if exists {}.{}_tests_masterval;
--
create table {}.{}_table stored as parquet as 
select 
    id serial primary key,
    name varchar(100) not null,
    description text,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
from {}.{}_tests_masterval;
--
compute stats {}.{}_tests_masterval;
--
--------------------------------- Query End ---------------------------------
