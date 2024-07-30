
--drop table dt_inovar_dev_stg.in_backlog_sandbox_snapshot_0509_test

/*create table dt_inovar_dev_stg.in_backlog_sandbox_snapshot_0509_test as
select * from dt_inovar_prod_stg.in_backlog_sandbox_snapshot*/


select * from dt_inovar_dev_stg.in_backlog_sandbox_snapshot_0509_test

select count(*) from dt_inovar_dev_stg.in_backlog_sandbox_snapshot_0509_test


select invoice_amount ,* from dt_inovar_dev_stg.in_amherst_fact_ap_invoice 


