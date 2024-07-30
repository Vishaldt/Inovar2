-----------11/06 added job part, join condition
/*with actvsest as(*/
--CREATE TABLE dt_inovar_prod_edw.u_mat_aberdeen_jobcost AS 
--SELECT * FROM dt_inovar_prod_edw.v_aberdeen_jobcost
--
--drop table dt_inovar_prod_edw.u_mat_aberdeen_jobcost
--
--drop view dt_inovar_prod_edw.v_aberdeen_jobcost
--
--TRUNCATE table dt_inovar_prod_edw.u_mat_aberdeen_jobcost
----
--	
--INSERT INTO dt_inovar_prod_edw.u_mat_aberdeen_jobcost
--SELECT * FROM dt_inovar_prod_edw.v_aberdeen_jobcost
--
--CREATE OR REPLACE VIEW dt_inovar_prod_edw.v_aberdeen_jobcost as



select count(*) from dt_inovar_prod_edw.u_mat_aberdeen_jobcost 
where category_id ='Unma'

select * from dt_inovar_dev_stg.u_mat_invoice_sandbox_temp 


	SELECT 
		generate_series(date_trunc('year', current_date) - INTERVAL '1 year' , date_trunc('year', current_date) + INTERVAL '1 year - 1 day' , '1 day')::date AS date_series 


with date_series AS (

	SELECT 
		generate_series(date_trunc('year', current_date) - INTERVAL '1 year' , date_trunc('year', current_date) )::date AS date_series 

		),
astvsest as (
select
		job.ccmasterid								as job,	
		job.ccdatesetup::date						as dt,
		coalesce(jc.esmasterid, 'Unmapped')			as estimate_id,
		case 
			when dept.jcdeptid is null then '000'
			else dept.jcdeptid
		end 										as department_id,
		case 
			when dept.jcdeptdescription is null then 'Materials'
			else dept.jcdeptdescription
		end											as department_name,
		coalesce(jc.jcmasterid, 'Unmapped') 		as category_id,
		coalesce(act.jcdescription,'Unmapped') 		as category_name,
		--jobpart.ccquotedprice						as quote_id,
		salesperson.arsalesname						as salesPerson,
		csr.arcsrname								as csr,
		cust.arcustname								as customer,
		jc.ccjobpart								as part,
--		max(jobpart.originalquotedpriceperm)		as quotepermonth,
		jobpart.qty_shipped,
		jobpart.qty_ordered,
		jobpart.total_act_hours,
		jobpart.material_cost,
		max(case
			when jc.jcchargeclass = 1 then jc.jcesthours
			else 0
		end) 										as estimated_hours,
		max(case when jc.jcchargeclass = 9 then jc.jcacthours
			else 0	
			end)									as actual_hours,
		sum(case
			when jc.jcchargeclass = 1 then jc.jcestimatedcost
			else 0
		end)										as estimated_cost,
		sum(case when jc.jcchargeclass = 9 then jc.jcactcost
			else 0	
			end)									as actual_cost
from
	dt_inovar_prod_stg.in_aberdeen_fact_job job
left join dt_inovar_prod_stg.in_aberdeen_dim_manufacturinglocation mfglocation on
	mfglocation.id = job.manufacturinglocation
left join dt_inovar_prod_stg.in_aberdeen_fact_jobcost jc on
	jc.ccmasterid = job.ccmasterid
left join dt_inovar_prod_stg.in_aberdeen_dim_activitycode act on
	act.jcmasterid = jc.jcmasterid
left join dt_inovar_prod_stg.in_aberdeen_dim_costcenter cc on
	cc.jccostcenterid = act.jccostcenterid
left join dt_inovar_prod_stg.in_aberdeen_dim_department dept on
	dept.jcdeptid = cc.jcdeptid
left join(
	select ccmasterid,
			ccjobpart,
		max(jobpart.ccqtyshipped)					as qty_shipped, --
		max(jobpart.ccqtyordered)					as qty_ordered, --
		sum(jobpart.cctotalhours)					as total_act_hours,
		sum(jobpart.ccjobcost01)					as material_cost --
	from dt_inovar_prod_stg.in_aberdeen_fact_jobpart jobpart
	group by 1,2) jobpart on
	jobpart.ccmasterid = job.ccmasterid and jobpart.ccjobpart = jc.ccjobpart
left join dt_inovar_prod_stg.in_aberdeen_dim_salesperson salesperson on
	salesperson.arsalesid = job.arsalesid
left join dt_inovar_prod_stg.in_aberdeen_dim_csr csr on
	csr.arcsrid = job.arcsrid
left join dt_inovar_dev_stg.in_aberdeen_dim_customer cust on 
	job.armasterid = cust.armasterid
where
	job.ccdatesetup::date >= '2022-01-01'
--	and job.ccmasterid = 'H1300'
--and  dept.jcdeptid is not null  and jc.esmasterid is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
select
	astvsest.job,
	date_series.dt,
	astvsest.estimate_id,
	astvsest.department_id,
	astvsest.department_name,
	astvsest.category_id,
	astvsest.category_name,
	astvsest.salesPerson,
	astvsest.csr,
	astvsest.customer,
	astvsest.part,
	astvsest.qty_shipped,
	astvsest.qty_ordered,
	astvsest.total_act_hours,
	astvsest.material_cost,
	astvsest.estimated_hours,
	astvsest.actual_hours,
	astvsest.estimated_cost,
	astvsest.actual_cost
from date_series left join astvsest 
on date_series::date = dt::date




-----------CHECK


select
		job.ccdatesetup::date						as dt,
		sum(case
			when jc.jcchargeclass = 1 then jc.jcestimatedcost
			else 0
		end)										as estimated_cost,
		sum(case when jc.jcchargeclass = 9 then jc.jcactcost
			else 0	
			end)									as actual_cost
from
	dt_inovar_prod_stg.in_aberdeen_fact_job job
left join dt_inovar_prod_stg.in_aberdeen_dim_manufacturinglocation mfglocation on
	mfglocation.id = job.manufacturinglocation
left join dt_inovar_prod_stg.in_aberdeen_fact_jobcost jc on
	jc.ccmasterid = job.ccmasterid
left join dt_inovar_prod_stg.in_aberdeen_dim_activitycode act on
	act.jcmasterid = jc.jcmasterid
left join dt_inovar_prod_stg.in_aberdeen_dim_costcenter cc on
	cc.jccostcenterid = act.jccostcenterid
left join dt_inovar_prod_stg.in_aberdeen_dim_department dept on
	dept.jcdeptid = cc.jcdeptid
left join(
	select ccmasterid,
			ccjobpart,
		max(jobpart.ccqtyshipped)					as qty_shipped,
		max(jobpart.ccqtyordered)					as qty_ordered,
		sum(jobpart.cctotalhours)					as total_act_hours,
		sum(jobpart.ccjobcost01)					as material_cost
	from dt_inovar_prod_stg.in_aberdeen_fact_jobpart jobpart
	where ccmasterid = 'H1300'
	group by 1,2) jobpart on
	jobpart.ccmasterid = job.ccmasterid and jobpart.ccjobpart = jc.ccjobpart
left join dt_inovar_prod_stg.in_aberdeen_dim_salesperson salesperson on
	salesperson.arsalesid = job.arsalesid
left join dt_inovar_prod_stg.in_aberdeen_dim_csr csr on
	csr.arcsrid = job.arcsrid
left join dt_inovar_dev_stg.in_aberdeen_dim_customer cust on 
	job.armasterid = cust.armasterid
where
	job.ccdatesetup::date >= '2022-01-01'
--	and job.ccmasterid = 'S7019'
--and  dept.jcdeptid is not null  and jc.esmasterid is not null
group by 1



-------------------------------12-06

select * from dt_inovar_prod_edw.u_mat_aberdeen_jobcost
/*where estimate_id ='Unmapped'
and category_name  = 'U
nmapped'*/
where /*category_id  = 'Unmapped'*/
job = 'H1300'



