with new_project AS (
select
	business_unit,
	company_customer_number,
	product_number,
	new_business_id
FROM dt_inovar_prod_stg.in_gs_new_business_tracker
	union all
select
	'Dallas' 									AS business_unit, 
	concat('DALLAS-', custnum)					AS company_customer_number,
	prodnum 									AS product_number,
	name3										AS new_business_id
FROM dt_inovar_prod_stg.in_dallas_dim_product 
WHERE name3 IS NOT NULL AND trim(name3) <> '' 
	union all
select 
	'Milwaukee' 								AS business_unit, 
	concat('FG-', custnum)						AS company_customer_number,
	prodnum 									AS product_number,
	name3										AS new_business_id
FROM dt_inovar_prod_stg.in_butler_dim_product  
WHERE name3 IS NOT NULL AND trim(name3) <> ''
	union all
select 
	'Ft. Lauderdale'							AS business_unit, 
	concat('DAVIE-', custnum)					AS company_customer_number,
	prodnum 									AS product_number,
	name3										AS new_business_id
FROM dt_inovar_prod_stg.in_davie_dim_product  
WHERE name3 IS NOT NULL AND trim(name3) <> ''
	union all
select
	'Newburyport'								AS business_unit, 
	concat('NE-', custnum)						AS company_customer_number,
	prodnum 									AS product_number,
	name3										AS new_business_id
FROM dt_inovar_prod_stg.in_newburyport_dim_product 
WHERE name3 IS NOT NULL AND trim(name3) <> ''
)
select distinct trim(new_business_id),business_unit   from new_project


where new_business_id in ('2023001','2023003','2023007','2023012','2023017','2023025','2023031','2023032','2023035','2023036','2023037','2023040','2023041','2023042','2023043','2023047','2024001')




select distinct a.*
--select new_business_id,* 
from dt_inovar_prod_edw.u_mat_new_business_project a
where new_business_id not in (select distinct new_business_id  FROM dt_inovar_prod_stg.in_gs_new_business_tracker)






select * FROM dt_inovar_prod_stg.in_gs_new_business_tracker

/*select * from dt_inovar_prod_stg.in_davie_fact_ap_invoice idfai 
--where name3 = '2023050'*/

select * from dt_inovar_prod_edw.u_mat_cm_sandbox_invoices_lt umcsil  --no


select * from dt_inovar_prod_edw.v_cm_sandbox  
where new_business_id in ('2023050' , '2024001', '2023047', '2023042', '2023043')



select distinct new_business_id  FROM dt_inovar_prod_stg.in_gs_new_business_tracker



SELECT b.name, a.* FROM 
dt_inovar_prod_edw.u_mat_new_business_project a
LEFT JOIN dt_inovar_prod_stg.in_r_sf_opportunity b ON a.opportunity_id = b.id
--where new_business_id in ('2023012')

--2023050 , 2024001, 2023047, 2023042, 2023043











-----------------------------------------------------Daily_Check----------------
select 
date_trunc('month', invoice_date::date)::date	as invoice_date,
sum(invoice_revenue)				as invoice_revenue,
sum(actstockcost) 					as actstockcost,
sum(actualtotallaborcost)			as actualtotallaborcost,
sum(actualtotalpocost)				as actualtotalpocost,
sum(actualtotalmatandfreightcost)	as actualtotalmatandfreightcost,
sum(acttotalcost) 					as 	acttotalcost,
sum(invoice_revenue - acttotalcost) as CM, 
sum(invoice_revenue - actstockcost) as VA 
from dt_inovar_prod_edw.u_mat_cm_sandbox_optimization_precision
where date_trunc('month', invoice_date::date)::date >= '2023-01-01'
group by 1
