-----------11/06 added job part, join condition
-- Added Date series on 12-06
-- Removed date series on 13-06
-- Added  Logic to handle 0's
-- Added date*dept*BU  on 14-06

/*with actvsest as(*/
/*

select businessunit, count(*),sum(material_cost)material_cost, sum(total_act_hours)total_act_hours,
	sum(qty_ordered)qty_ordered, sum(qty_shipped) qty_shipped,
sum(actual_cost) actual_cost, sum(estimated_cost) estimated_cost,
	sum(estimated_hours)estimated_hours, sum(actual_hours) actual_hours from dt_inovar_prod_edw.u_mat_aberdeen_jobcost
	group by 1

*/
	
	
/*
CREATE TABLE dt_inovar_prod_edw.u_mat_aberdeen_jobcost_test AS 
SELECT * FROM dt_inovar_prod_edw.v_aberdeen_jobcost_test


drop table dt_inovar_prod_edw.u_mat_aberdeen_jobcost

drop view dt_inovar_prod_edw.v_aberdeen_jobcost




TRUNCATE table dt_inovar_prod_edw.u_mat_aberdeen_jobcost
--
	
INSERT INTO dt_inovar_prod_edw.u_mat_aberdeen_jobcost
SELECT * FROM dt_inovar_prod_edw.v_aberdeen_jobcost



CREATE OR REPLACE VIEW dt_inovar_prod_edw.v_aberdeen_jobcost as*/
with date_series AS (
	select date(generate_series('01-Jan-2022',date_trunc('year',now()::date)::date + '1 year'::interval - '1 day'::interval,'1 day'::interval)) as dt
		),
bu_dept as (
select distinct mfglocation.description	AS businessunit,
	case 
			when dept.jcdeptdescription is null then 'Materials'
			else dept.jcdeptdescription
		end											as department_name
from
	dt_inovar_prod_stg.in_aberdeen_fact_job job
left join dt_inovar_prod_stg.in_aberdeen_dim_manufacturinglocation mfglocation 
	on mfglocation.id = job.manufacturinglocation
inner join dt_inovar_prod_stg.in_aberdeen_fact_jobcost jc
	on jc.ccmasterid = job.ccmasterid
left join dt_inovar_prod_stg.in_aberdeen_dim_activitycode act
	on act.jcmasterid = jc.jcmasterid
left join dt_inovar_prod_stg.in_aberdeen_dim_costcenter cc on
	cc.jccostcenterid = act.jccostcenterid
left join dt_inovar_prod_stg.in_aberdeen_dim_department dept on
	dept.jcdeptid = cc.jcdeptid
where
	job.ccdatesetup::date >= '2022-01-01'
),
bu_dateseries as (select * 
		from date_series cross join bu_dept
),
act_est as (
	select		
		mfglocation.description						AS businessunit,
		job.ccmasterid								as job,	
		job.ccdatesetup::date						as order_date,
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
		coalesce(act.jcdescription, act1.jcdescription, act2.jcdescription, act3.jcdescription, act4.jcdescription, act5.jcdescription,'Unmapped') as  category_name,
		--jobpart.ccquotedprice						as quote_id,
		coalesce(salesperson.arsalesname,'Unmapped') as salesPerson,
		coalesce(csr.arcsrname,'Unmapped')			 as csr,
		coalesce(cust.arcustname,'Unmapped')		 as customer,
		coalesce(jc.ccjobpart,'Unmapped')			 as part,
--		max(jobpart.originalquotedpriceperm)		as quotepermonth,
		jobpart.qty_shipped							 as qty_shipped,
		jobpart.qty_ordered							 as qty_ordered,
		jobpart.total_act_hours		  				 as total_act_hours,
		jobpart.material_cost						 as material_cost,
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
left join dt_inovar_prod_stg.in_aberdeen_dim_manufacturinglocation mfglocation 
	on mfglocation.id = job.manufacturinglocation
inner join dt_inovar_prod_stg.in_aberdeen_fact_jobcost jc
	on jc.ccmasterid = job.ccmasterid
left join dt_inovar_prod_stg.in_aberdeen_dim_activitycode act
	on act.jcmasterid = jc.jcmasterid
left join dt_inovar_prod_stg.in_aberdeen_dim_activitycode act1
	on act1.jcmasterid::text = concat('0',jc.jcmasterid::text)     and jc.jcmasterid is not null
left join dt_inovar_prod_stg.in_aberdeen_dim_activitycode act2 
	on act2.jcmasterid::text = concat('00',jc.jcmasterid::text)    and jc.jcmasterid is not null
left join dt_inovar_prod_stg.in_aberdeen_dim_activitycode act3 
	on act3.jcmasterid::text = concat('000',jc.jcmasterid::text)   and jc.jcmasterid is not null
left join dt_inovar_prod_stg.in_aberdeen_dim_activitycode act4
	on act4.jcmasterid::text = concat('0000',jc.jcmasterid::text)  and jc.jcmasterid is not null
left join dt_inovar_prod_stg.in_aberdeen_dim_activitycode act5
	on act5.jcmasterid::text = concat('00000',jc.jcmasterid::text) and jc.jcmasterid is not null
left join dt_inovar_prod_stg.in_aberdeen_dim_costcenter cc on
	cc.jccostcenterid = act.jccostcenterid
left join dt_inovar_prod_stg.in_aberdeen_dim_department dept on
	dept.jcdeptid = cc.jcdeptid
left join(
	select ccmasterid,
			ccjobpart,
		sum(jobpart.ccqtyshipped)					as qty_shipped, 
		sum(jobpart.ccqtyordered)					as qty_ordered, 
		sum(jobpart.cctotalhours)					as total_act_hours,
		sum(jobpart.ccjobcost01)					as material_cost 
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
--and  dept.jcdeptid is not null  
	--and jc.jcmasterid is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
	select 
	act_est.businessunit,
	act_est.job,
	bu_dateseries.dt,
	act_est.estimate_id,
	act_est.department_id,
	act_est.department_name,
	act_est.category_id,
	act_est.category_name,
	act_est.salesPerson,
	act_est.csr,
	act_est.customer,
	act_est.part,
	act_est.qty_shipped,
	act_est.qty_ordered,
	act_est.total_act_hours,
	act_est.material_cost,
	act_est.estimated_hours,
	act_est.actual_hours,
	act_est.estimated_cost,
	act_est.actual_cost
	from bu_dateseries left join act_est
	on act_est.order_date::date = bu_dateseries.dt::date  and act_est.businessunit = bu_dateseries.businessunit and act_est.department_name = bu_dateseries.department_name
where bu_dateseries.dt::date <= CURRENT_DATE-1



