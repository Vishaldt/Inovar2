select 'test', businessunit, /*date_trunc('year',sdate), */count(*) , sum(elapsed) elapsed, sum(footused) from dt_inovar_dev_stg.u_mat_sales_bookings_eod_dashboard_test
where is_operations = 1 /*and sdate>='2024-01-01'*/
group by 1,2


select 'edw' ,businessunit, count(*) , sum(elapsed) elapsed, sum(footused) 
from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard 
where is_operations = 1/* and sdate>='2024-01-01'*/
group by 1,2

------------------------------------------------------EFI

select * from dt_inovar_prod_stg.aberdeen_jobplan  


select * from dt_inovar_prod_stg.aberdeen_jobpart 



select * from  dt_inovar_prod_stg.in_aberdeen_dim_activitycode

select job.ccmasterid,jc.jcmasterid,act.jcdescription
from dt_inovar_prod_stg.in_aberdeen_fact_job job 
	inner join dt_inovar_prod_stg.in_aberdeen_fact_jobcost jc
		on jc.ccmasterid = job.ccmasterid
	left join dt_inovar_prod_stg.in_aberdeen_dim_activitycode act
	on act.jcmasterid = jc.jcmasterid
where job.ccmasterid = '30055'


	and jc.jcmasterid = '70000'
	
	select * 
from dt_inovar_prod_stg.in_aberdeen_fact_job job 
where job.ccmasterid = '14782'
	

select distinct status  from dt_inovar_prod_stg.aberdeen_jobplan 





select a.ccmasterid  , a.ccstatus, b.sysdescription
from dt_inovar_prod_stg.in_aberdeen_fact_job a  
left join dt_inovar_prod_stg.in_aberdeen_dim_jobstatus b 
on a.ccstatus::text = b.sysstatusid::text
where ccmasterid = '30055'


select distinct status from dt_inovar_prod_stg.aberdeen_jobplan 












