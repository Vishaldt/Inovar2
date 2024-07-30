      with cimarronsouth_aberdeen as
(
select
	mfglocation.description								AS location,
	mfglocation.description								as legal_entity,
	mfglocation.description								AS businessunit,
	case
    	when mfglocation.id = '1' 		then concat('WP-', job.ccmasterid)
    	when mfglocation.id = '5001' 	then concat('CS-', job.ccmasterid)
	end													AS company_ticket_number,
	null::text 											AS company_customer_number,
	null::text 											AS company_invoice_number,
	job.ccdatesetup 									as orderdate,
	null::date 											AS invoice_date,
	null::text 											AS billcountry,
	null::text 											AS billzip,
	null::text 											AS shipzip,
	null::text 											AS currency,
	null::text 											AS customername,
	null::text 											AS invoice_type,
	null::text 											AS otsname,
	null::text 											AS salesrep_number,
	null::text 											AS ticketstatus,
	null::text 											AS stocktickettype,
	null::date 											AS datedone,
	null::date 											AS ship_by_date,
	null::text 											AS custponum,
	'Unmapped' 											AS shipvia,
	null::TEXT 											AS generaldescr,
	null::TEXT 											AS priority,
	null::text 											AS shippingstatus,
	null::text   										AS press,
	null::TEXT 											as press_type,
	null::TEXT 											AS notes,
	null::text 											AS shipaddr1,
	null::text 											AS shiplocation,
	null::text 											AS tickettype,
	null::text 											AS line_item_type,
	NULL::TEXT 											as wip_in_finishing,
	null::text 											AS ticket_item_id,
	null::text 											AS productnumber,
	null::TEXT 											AS product_description,
	'Others'   											as product_category,
	0::float   											AS orderquantity,
	0::float   											AS machinecount,
	null::text 											AS stockproductid,
	0::float   											AS booking_revenue,
	0::float   											AS price_per_unit,
	0::float   											AS ship_quantity,
	0::float   											AS Invoice_revenue,
	0::float   											AS backlog_amount,
	null::text 											AS backlog_type,
	null::text 											AS financial_statement_class,
	null::text 											as invoice_item_id,
	0::float   											AS budget,
	0::float  											AS contribution_margin_amount,
	0::float											as va,
	0::float											as actstockcost,
	0::float											as actualtotallaborcost,
	0::float											as actualtotalpocost,
	0::float											as actualtotalmatandfreightcost,
	0::float											as acttotalcost,
	null::date 											as backlog_snpashot_date,
	null       											as anomaly_flag,
	'f' 	   											AS new_customer,
	null::text   										as timecard_id,
	null::text 											as workoperation,
	null::date											as ops_start_date,
	0::float 											as elapsed,
	0::float 											as footused,
	null::text 											as ticket_pressequip,
	null::text 											as Equipment_Grouping,
	null::text 											as assocno,
	0::float											as Total_Hours_Worked,
	0::float 											as Total_OT_Hours,
	0::float											as Total_Hours,
	0::float											as actpresstime,
    0::float											as estpresstime,
    0::float											as eststockcost,
    0::float											as customer_total,
    0::float											as estrunhrs,
    null::text 											as pricemode,
	null::text											as associate_lastname,
	null::text											as associate_firstname,
	null::text											as user_type,
	null::text											as complaint_id,
	null::date											as complaint_issued_date,
	null::text 											as equipment_ptype,
	null::text 											as equipment_description,
	null::date											as shipdate,
	0::float 											as on_time_flag,
	0::float 											as in_full_flag,
	null::text 											as stock_ticket_flag,
	null::text 											as packing_slip_number,
	null::text 											as opportunity_id,
	null::text 											as opportunity_accountid,
	null::text 											as opportunity_name,
	null::text 											as opportunity_stagename,
	0::float 											as opportunity_amount,
	0::float 											as opportunity_probability,
	null::date											as opportunity_closedate,
	'f' 	   											as opportunity_is_closed,
	0::float 											as msi_sold,
	null::text 											as prophix_account,
	null::date 											as prophix_date,
	0::float 											as prophix_amount,
	0::float											as total_actpresstime,
	0::float 											as actualpresshours,
	0::float 											as actualtotalhours,
	0::float 											as estpresstime_ops,
	0::float 											as estfootage_ops_waste,
	0::float 											as est_spoilfootage_ops_waste,
	0::float 											as est_setupfootage_ops_waste,
	0::float 											as actquantity_ops_waste,
	0::float 											as ticquantity_ops_waste,
	0::float 											as actfootage_ops_waste,
	null::text 											as entryby,
	0::float 											as sku_count,
	null::timestamp										as job_start_time,
	0::float											as totalworkorderfootagebyoperator,
	0::float											as totalwasteperoperator,
	null::text 											as employee_name,
	null::date 											as credit_issued_date,
	null::text 											as complaint_description,
	null::text 											as otsname_from_invoice,
	jc.jcmasterid,
	dept.jcdeptid,
	jc.jcchargeclass,
	jc.jcestimatedcost,
	jc.jcactcost,
	dept.jcdeptdescription
from dt_inovar_prod_stg.in_aberdeen_fact_job job
left join dt_inovar_prod_stg.in_aberdeen_dim_manufacturinglocation mfglocation on mfglocation.id = job.manufacturinglocation
inner join dt_inovar_prod_stg.in_aberdeen_fact_jobcost jc on jc.ccmasterid = job.ccmasterid
left join dt_inovar_prod_stg.aberdeen_activitycode act on act.jcmasterid = jc.jcmasterid
left join dt_inovar_prod_stg.aberdeen_costcenter cc on cc.jccostcenterid = act.jccostcenterid
left join dt_inovar_prod_stg.aberdeen_department dept on dept.jcdeptid = cc.jcdeptid
WHERE job.ccdatesetup >= '2022-01-01'::date
)
select * from cimarronsouth_aberdeen









---------------------TO_DO_NEXT
-- Add ect hours, act hours from job table
-- In Job Part table, we have -
						-- ccmasterid,esmasterid,ccquotedprice,originalquotedpriceperm,ccqtyshipped,ccqtyordered,cctotalhours,ccjobcost01
						-- may be usefull
-- Query as per discussion with gaurav and ankit		jcesthours,jcacthours,jcestimatedcost,jcactcost
	select
		job.ccmasterid,	
		jc.jcmasterid,
		dept.jcdeptid,
		jc.esmasterid,
		dept.jcdeptdescription,
		act.jcdescription,
		sum(case
			when jc.jcchargeclass = 1 then jc.jcesthours
			else 0
		end) 											as jcesthours,
		sum(case when jc.jcchargeclass = 9 then jc.jcacthours
			else 0	
			end)										as jcacthours,
		sum(case
			when jc.jcchargeclass = 1 then jc.jcestimatedcost
			else 0
		end)											as jcestimatedcost,
		sum(case when jc.jcchargeclass = 9 then jc.jcactcost
			else 0	
			end)										as jcactcost
from
	dt_inovar_prod_stg.in_aberdeen_fact_job job
left join dt_inovar_prod_stg.in_aberdeen_dim_manufacturinglocation mfglocation on
	mfglocation.id = job.manufacturinglocation
inner join dt_inovar_prod_stg.in_aberdeen_fact_jobcost jc on
	jc.ccmasterid = job.ccmasterid
left join dt_inovar_prod_stg.aberdeen_activitycode act on
	act.jcmasterid = jc.jcmasterid
left join dt_inovar_prod_stg.aberdeen_costcenter cc on
	cc.jccostcenterid = act.jccostcenterid
left join dt_inovar_prod_stg.aberdeen_department dept on
	dept.jcdeptid = cc.jcdeptid
--where
	--job.ccdatesetup >= '2022-01-01'::date
	-- job.ccmasterid = 'S7868'
group by 1,2,3,4,5,6
order by 2

----- Job Part - usefull -
	select ccmasterid,esmasterid,ccquotedprice,originalquotedpriceperm,ccqtyshipped,ccqtyordered,cctotalhours,ccjobcost01  ,* from  dt_inovar_prod_stg.in_aberdeen_fact_jobpart iafj 
where ccmasterid ='S7868'

--1002541


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

with cimarronsouth_aberdeen as (
select
	job.ccmasterid::text,
	job.ccdatesetup,
	jc.jcmasterid,
	dept.jcdeptid,
	mfglocation.id::text,
	mfglocation.description,
	jobpart.esmasterid,
	dept.jcdeptdescription,
	act.jcdescription,
	jobpart.ccquotedprice,
	jobpart.originalquotedpriceperm,
	jobpart.ccqtyshipped,
	jobpart.ccqtyordered,
	--jc.jcchargeclass,
	sum(case
			when jc.jcchargeclass = 1 then jc.jcesthours
		else 0
	end) 											as jcesthours,
	sum(case
			when jc.jcchargeclass = 9 then jc.jcacthours
		else 0	
		end)										as jcacthours,
	sum(case
			when jc.jcchargeclass = 1 then jc.jcestimatedcost
		else 0
	end)											as jcestimatedcost,
	sum(case 
			when jc.jcchargeclass = 9 then jc.jcactcost
		else 0	
		end)										as jcactcost					
from
	dt_inovar_prod_stg.in_aberdeen_fact_job job
left join dt_inovar_prod_stg.in_aberdeen_dim_manufacturinglocation mfglocation on
	mfglocation.id = job.manufacturinglocation
left join dt_inovar_prod_stg.in_aberdeen_fact_jobpart jobpart on
	job.ccmasterid = jobpart.ccmasterid
inner join dt_inovar_prod_stg.in_aberdeen_fact_jobcost jc on
	jc.ccmasterid = job.ccmasterid
left join dt_inovar_prod_stg.aberdeen_activitycode act on
	act.jcmasterid = jc.jcmasterid
left join dt_inovar_prod_stg.aberdeen_costcenter cc on
	cc.jccostcenterid = act.jccostcenterid
left join dt_inovar_prod_stg.aberdeen_department dept on
	dept.jcdeptid = cc.jcdeptid
where
	job.ccdatesetup >= '2022-01-01'::date
--where job.ccmasterid ='S7868'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)/*,
with cimarronsouth_aberdeen_final as(*/
select
	cs.description								AS location,
	cs.description								as legal_entity,
	cs.description								AS businessunit,
	case
    	when cs.id = '1' 		then concat('WP-', cs.ccmasterid)
    	when cs.id = '5001' 	then concat('CS-', cs.ccmasterid)
	end													AS company_ticket_number,
	null::text 											AS company_customer_number,
	null::text 											AS company_invoice_number,
	cs.ccdatesetup 										as orderdate,
	null::date 											AS invoice_date,
	null::text 											AS billcountry,
	null::text 											AS billzip,
	null::text 											AS shipzip,
	null::text 											AS currency,
	null::text 											AS customername,
	null::text 											AS invoice_type,
	null::text 											AS otsname,
	null::text 											AS salesrep_number,
	null::text 											AS ticketstatus,
	null::text 											AS stocktickettype,
	null::date 											AS datedone,
	null::date 											AS ship_by_date,
	null::text 											AS custponum,
	'Unmapped' 											AS shipvia,
	null::TEXT 											AS generaldescr,
	null::TEXT 											AS priority,
	null::text 											AS shippingstatus,
	null::text   										AS press,
	null::TEXT 											as press_type,
	null::TEXT 											AS notes,
	null::text 											AS shipaddr1,
	null::text 											AS shiplocation,
	null::text 											AS tickettype,
	null::text 											AS line_item_type,
	NULL::TEXT 											as wip_in_finishing,
	null::text 											AS ticket_item_id,
	null::text 											AS productnumber,
	null::TEXT 											AS product_description,
	'Others'   											as product_category,
	0::float   											AS orderquantity,
	0::float   											AS machinecount,
	null::text 											AS stockproductid,
	0::float   											AS booking_revenue,
	0::float   											AS price_per_unit,
	0::float   											AS ship_quantity,
	0::float   											AS Invoice_revenue,
	0::float   											AS backlog_amount,
	null::text 											AS backlog_type,
	null::text 											AS financial_statement_class,
	null::text 											as invoice_item_id,
	0::float   											AS budget,
	0::float  											AS contribution_margin_amount,
	0::float											as va,
	0::float											as actstockcost,
	0::float											as actualtotallaborcost,
	0::float											as actualtotalpocost,
	0::float											as actualtotalmatandfreightcost,
	0::float											as acttotalcost,
	null::date 											as backlog_snpashot_date,
	null       											as anomaly_flag,
	'f' 	   											AS new_customer,
	null::text   										as timecard_id,
	null::text 											as workoperation,
	null::date											as ops_start_date,
	0::float 											as elapsed,
	0::float 											as footused,
	null::text 											as ticket_pressequip,
	null::text 											as Equipment_Grouping,
	null::text 											as assocno,
	0::float											as Total_Hours_Worked,
	0::float 											as Total_OT_Hours,
	0::float											as Total_Hours,
	0::float											as actpresstime,
    0::float											as estpresstime,
    0::float											as eststockcost,
    0::float											as customer_total,
    0::float											as estrunhrs,
    null::text 											as pricemode,
	null::text											as associate_lastname,
	null::text											as associate_firstname,
	null::text											as user_type,
	null::text											as complaint_id,
	null::date											as complaint_issued_date,
	null::text 											as equipment_ptype,
	null::text 											as equipment_description,
	null::date											as shipdate,
	0::float 											as on_time_flag,
	0::float 											as in_full_flag,
	null::text 											as stock_ticket_flag,
	null::text 											as packing_slip_number,
	null::text 											as opportunity_id,
	null::text 											as opportunity_accountid,
	null::text 											as opportunity_name,
	null::text 											as opportunity_stagename,
	0::float 											as opportunity_amount,
	0::float 											as opportunity_probability,
	null::date											as opportunity_closedate,
	'f' 	   											as opportunity_is_closed,
	0::float 											as msi_sold,
	null::text 											as prophix_account,
	null::date 											as prophix_date,
	0::float 											as prophix_amount,
	0::float											as total_actpresstime,
	0::float 											as actualpresshours,
	0::float 											as actualtotalhours,
	0::float 											as estpresstime_ops,
	0::float 											as estfootage_ops_waste,
	0::float 											as est_spoilfootage_ops_waste,
	0::float 											as est_setupfootage_ops_waste,
	0::float 											as actquantity_ops_waste,
	0::float 											as ticquantity_ops_waste,
	0::float 											as actfootage_ops_waste,
	null::text 											as entryby,
	0::float 											as sku_count,
	null::timestamp										as job_start_time,
	0::float											as totalworkorderfootagebyoperator,
	0::float											as totalwasteperoperator,
	null::text 											as employee_name,
	null::date 											as credit_issued_date,
	null::text 											as complaint_description,
	null::text 											as otsname_from_invoice,
	cs.jcmasterid,
	cs.jcdeptid,
	cs.esmasterid,
	cs.jcdeptdescription,
	cs.jcdescription,
	cs.ccquotedprice,
	cs.originalquotedpriceperm,
	--cs.jcchargeclass,
	cs.ccqtyshipped,
	cs.jcesthours,
	cs.jcacthours,
	cs.jcestimatedcost,
	cs.jcactcost,
	cs.ccqtyordered
from cimarronsouth_aberdeen cs


--jcesthours,jcacthours,jcestimatedcost,jcactcost


---------------------------------2905




select
		job.ccmasterid				as job,	
		jc.jcmasterid				as category_id,
		dept.jcdeptid				as department_id,
		jc.esmasterid				as estimate_id,
		dept.jcdeptdescription 		as department_name,
		act.jcdescription			as category_name,
		jobpart.ccquotedprice		as quote_id,
		jobpart.originalquotedpriceperm	as quotepermonth,
		jobpart.ccqtyshipped		as qty_shipped,
		jobpart.ccqtyordered		as qty_ordered,
		jobpart.cctotalhours		as act_hours,
		jobpart.ccjobcost01			as material_cost, 
		sum(case
			when jc.jcchargeclass = 1 then jc.jcesthours
			else 0
		end) 											as estimated_hours,
		sum(case when jc.jcchargeclass = 9 then jc.jcacthours
			else 0	
			end)										as actual_hours,
		sum(case
			when jc.jcchargeclass = 1 then jc.jcestimatedcost
			else 0
		end)											as estimated_cost,
		sum(case when jc.jcchargeclass = 9 then jc.jcactcost
			else 0	
			end)										as actual_cost
from
	dt_inovar_prod_stg.in_aberdeen_fact_job job
left join dt_inovar_prod_stg.in_aberdeen_dim_manufacturinglocation mfglocation on
	mfglocation.id = job.manufacturinglocation
inner join dt_inovar_prod_stg.in_aberdeen_fact_jobcost jc on
	jc.ccmasterid = job.ccmasterid
left join dt_inovar_prod_stg.aberdeen_activitycode act on
	act.jcmasterid = jc.jcmasterid
left join dt_inovar_prod_stg.aberdeen_costcenter cc on
	cc.jccostcenterid = act.jccostcenterid
left join dt_inovar_prod_stg.aberdeen_department dept on
	dept.jcdeptid = cc.jcdeptid
left join dt_inovar_prod_stg.in_aberdeen_fact_jobpart jobpart on
	jobpart.ccmasterid = job.ccmasterid
where
	--job.ccdatesetup >= '2022-01-01'::date
	 job.ccmasterid = 'S7868'
group by 1,2,3,4,5,6,7,8,9,10,11,12
order by 2




----- Job Part - usefull -
	select ccmasterid,esmasterid,ccquotedprice,originalquotedpriceperm,ccqtyshipped,ccqtyordered,cctotalhours,ccjobcost01  ,* from  dt_inovar_prod_stg.in_aberdeen_fact_jobpart iafj 
where ccmasterid ='S7868'

-----------04/06
with actvsest as(


select
		job.ccmasterid								as job,	
		--jc.esmasterid								as estimate_id,
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
left join dt_inovar_prod_stg.aberdeen_activitycode act on
	act.jcmasterid = jc.jcmasterid
left join dt_inovar_prod_stg.aberdeen_costcenter cc on
	cc.jccostcenterid = act.jccostcenterid
left join dt_inovar_prod_stg.aberdeen_department dept on
	dept.jcdeptid = cc.jcdeptid
left join dt_inovar_prod_stg.in_aberdeen_fact_jobpart jobpart on
	jobpart.ccmasterid = job.ccmasterid
left join dt_inovar_prod_stg.in_aberdeen_dim_salesperson salesperson on
	salesperson.arsalesid = job.arsalesid
left join dt_inovar_prod_stg.in_aberdeen_dim_csr csr on
	csr.arcsrid = job.arcsrid
left join dt_inovar_dev_stg.in_aberdeen_dim_customer cust 
	ON job.armasterid = cust.armasterid
where
	--job.ccdatesetup >= '2022-01-01'::date
	job.ccmasterid = 'H3601'
--and  dept.jcdeptid is not null  and jc.esmasterid is not null
group by 1,2,3,4,5,6,7,8


)


select count(*),count(distinct (concat(job,category_id))),count(distinct (concat(job,department_id,category_id))),count(distinct (concat(job,department_id,category_id,quote_id)))
from actvsest
where  department_id is not null 



and estimate_id is not null and job is not null




where department_id is not null  and category_id is not null

------------
891727	737101	738639

select distinct ccmasterid from
	dt_inovar_prod_stg.in_aberdeen_fact_job



















----------------------

select arcsrid,armasterid ,arsalesid  ,* from dt_inovar_prod_stg.in_aberdeen_fact_job
where
	--job.ccdatesetup >= '2022-01-01'::date
	 ccmasterid = 'S7868'

	
	 select arsalesname,arsalesid,* from dt_inovar_prod_stg.aberdeen_salesperson as2
	where arsalesid = 3
	 
	 select arsalesid,arcsrid from dt_inovar_prod_stg.aberdeen_job aj 
	 where ccmasterid = 'H3601'
	 
	 
	 
	 	 select * from dt_inovar_prod_stg.aberdeen_csr
	 where arcsrid = 6
	 
	  	 select arcsrid,* from dt_inovar_prod_stg.in_aberdeen_fact_job  
	 where arcsrid = '6'
	 
	 
	 
	 
	 
	 
	 
	 
	 
	 
	 
	 
	 select * from dt_inovar_prod_stg.in_aberdeen_dim_salesperson
	 
	 select arsalesid,arcsrid  ,* from dt_inovar_prod_stg.in_aberdeen_fact_job iafj 
	 
	 
	 select * from dt_inovar_prod_stg.aberdeen_csr
	 where arcsrid = '6'
	 
	 
	 select count(*),count(distinct arcustname)  from dt_inovar_prod_stg.aberdeen_customer
--	 where arcustname ilike '%WESTERN HOTEL SUPPLY%'
	 
	 select arcustname,cust.arsalesid, job.arsalesid ,cust.* from dt_inovar_prod_stg.in_aberdeen_fact_job  job
	 	left join  dt_inovar_prod_stg.aberdeen_customer cust  on job.arsalesid = cust.arsalesid 
	 where job.ccmasterid = 'H3601' /*and job.arsalesid = '6'*/
	-- and arcustname ilike '%WESTERN HOTEL SUPPLY%'
	 
	 
	 select id,* from dt_inovar_prod_stg.in_aberdeen_fact_job 
	 
	 select arcsrid,arcustname ,* from dt_inovar_prod_stg.in_aberdeen_dim_customer 
	 where arcsrid='6'
	 
	 
	 
	 
	 
	 
	 
	 
	 
	 select arsalesid from dt_inovar_prod_stg.in_aberdeen_fact_job
	 where ccmasterid = 'H3601'
	 
	 
	 	 select distinct arcustname  from dt_inovar_prod_stg.aberdeen_customer
	 where arcustname ilike '%western%'

	 select *  from dt_inovar_prod_stg.aberdeen_customer
	 


select contactfirstname ,contactlastname ,* from dt_inovar_prod_stg.in_aberdeen_fact_job 


select count(*), count(distinct concat (ccmasterid,jcmasterid )) from dt_inovar_prod_stg.in_aberdeen_fact_jobcost