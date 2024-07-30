-----------04/06
/*with actvsest as(*/


/*
CREATE TABLE dt_inovar_prod_edw.u_mat_aberdeen_jobcost AS 
SELECT * FROM dt_inovar_prod_edw.v_aberdeen_jobcost

drop table dt_inovar_prod_edw.u_mat_aberdeen_jobcost

drop view dt_inovar_prod_edw.v_aberdeen_jobcost

CREATE OR REPLACE VIEW dt_inovar_prod_edw.v_aberdeen_jobcost as*/

select job,dt::date,sum(actual_hours) actual_hours, sum(estimated_hours)estimated_hours  from dt_inovar_prod_edw.u_mat_aberdeen_jobcost 
where dt::date>='2024-01-01'
group by 1,2

select
		job.ccmasterid								as job,	
		job.ccdatesetup::date						as dt,
		jc.esmasterid								as estimate_id,
		case 
			when dept.jcdeptid is null then '000'
			else dept.jcdeptid
		end 										as department_id,
		case 
			when dept.jcdeptdescription is null then 'Materials'
			else dept.jcdeptdescription
		end											as department_name,
		jc.jcmasterid								as category_id,
		act.jcdescription							as category_name,
		--jobpart.ccquotedprice						as quote_id,
		salesperson.arsalesname						as salesPerson,
		csr.arcsrname								as csr,
		cust.arcustname								as customer,
--		max(jobpart.originalquotedpriceperm)		as quotepermonth,
		max(jobpart.ccqtyshipped)					as qty_shipped,
		max(jobpart.ccqtyordered)					as qty_ordered,
		max(jobpart.cctotalhours)					as total_act_hours,
		max(jobpart.ccjobcost01)					as material_cost,
		sum(case
			when jc.jcchargeclass = 1 then jc.jcesthours
			else 0
		end) 										as estimated_hours,
		sum(case when jc.jcchargeclass = 9 then jc.jcacthours
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
left join dt_inovar_prod_stg.in_aberdeen_fact_jobpart jobpart on
	jobpart.ccmasterid = job.ccmasterid
left join dt_inovar_prod_stg.in_aberdeen_dim_salesperson salesperson on
	salesperson.arsalesid = job.arsalesid
left join dt_inovar_prod_stg.in_aberdeen_dim_csr csr on
	csr.arcsrid = job.arcsrid
left join dt_inovar_dev_stg.in_aberdeen_dim_customer cust on 
	job.armasterid = cust.armasterid
where
	job.ccdatesetup >= '2024-01-01'::date
	and job.ccmasterid = 'S8286'
--and  dept.jcdeptid is not null  and jc.esmasterid is not null
group by 1,2,3,4,5,6,7,8,9,10


)
select  job ,
	  estimate_id/*,
	count(distinct (concat(job,estimate_id))),
	count(distinct (concat(job,category_id))),
	count(distinct (concat(job,department_id,category_id)))*/
from actvsest
where  estimate_id is not null 



select distinct ccmasterid from
	dt_inovar_prod_stg.in_aberdeen_fact_job job




select jcstartdate ,* from dt_inovar_prod_stg.in_aberdeen_fact_jobcost
where jcstartdate::date >='2023-01-01'












