with press as (
select
	CASE
		WHEN "Business_Unit" = 'FLEXOGRAPHICS' 			THEN 'Butler'
		WHEN "Business_Unit" = 'SOUTHWEST' 				THEN 'Dallas'
		WHEN "Business_Unit" = 'SOUTHEAST' 				THEN 'Davie'
		WHEN "Business_Unit" = 'NEWENGLAND' 			THEN 'NewBuryPort'
		WHEN "Business_Unit" = 'Cimarron North' 		THEN 'Cimarron North'
		WHEN "Business_Unit" = 'Amherst Label' 			THEN 'Amherst'
		WHEN "Business_Unit" = 'DION LABEL' 			THEN 'Westfield'
		WHEN "Business_Unit" = 'Cimarron South' 		THEN 'Cimarron South'
		WHEN "Business_Unit" = 'Western Printing' 		THEN 'Aberdeen'
		else "Business_Unit"
	END														as entity,
	trim(pd."PressNo")          							as press,
	pd."PressType"          								as press_type
from dt_inovar_prod_stg.in_press_description pd
),
tabco_booklet_orders AS (
SELECT
	distinct
	case 
		when 
		(regexp_match(
			(case 
				when house_number ilike '%.%'
					then substring(house_number, 1, position('.' in house_number)-1)
				else house_number
			  	end), '([A-Z])') is not null or 
		regexp_match(
			(case 
				when house_number ilike '%.%'
					then substring(house_number, 1, position('.' in house_number)-1)
				else house_number
			  	end), '([a-z])') is not null)
			then substring((case 
								when house_number ilike '%.%'
									then substring(house_number, 1, position('.' in house_number)-1)
								else house_number
							  	end), 1, length((case 
													when house_number ilike '%.%'
														then substring(house_number, 1, position('.' in house_number)-1)
													else house_number
												  	end))-1)		
		else 
		(case 
			when house_number ilike '%.%'
				then substring(house_number, 1, position('.' in house_number)-1)
			else house_number
			end)
	end 												as DT_tktnumber,
	tkt.house_number,
	finished_product_type
from dt_inovar_prod_stg.in_kansascity_fact_orders tkt
join (select cyrel__ as cyrel , min(finished_product_type) as finished_product_type
			from dt_inovar_prod_stg.in_kansascity_fact_worksheets_orden
			where finished_product_type ilike '%booklet%' and (finished_product_type ilike '%onsert%' or finished_product_type ilike '%base%') group by 1) as orden
	on orden.cyrel = tkt.cyrel__
where replace(date_order,'None','1900-01-01') >= '2019-01-01'
),
tabco_booklet_suborders AS (
SELECT DISTINCT house_number
FROM tabco_booklet_orders
WHERE dt_tktnumber <> house_number
),
tabco as (
select 
	replace(startdate,'None','1900-01-01')::date 		as sdate,
	replace(stopdate,'None','1900-01-01')::date 		as edate,
	starttime 											as stime,
	stoptime 											as etime,
	kpt.house_number 									as ticket_number,
	case 
		when trim(lower(kpt.which_press)) ilike '%press 1%' then 'Press 1'
		when trim(lower(kpt.which_press)) ilike '%press 2%' then 'Press 2'
		when trim(lower(kpt.which_press)) ilike '%press 3%' then 'Press 3'
		when trim(lower(kpt.which_press)) ilike '%press 4%' then 'Press 4'
	end 												as whichpress,
	case 
		when trim(kpt.setup_time_type) ilike '%prod%' then 'Run'
		when trim(kpt.setup_time_type) ilike '%set%' then 'make ready'
		when (trim(kpt.setup_time_type) ilike '%clean%' or trim(kpt.setup_time_type) ilike '%test%') then 'DT-downtime'
		else trim(kpt.setup_time_type)
	end 												as workoperation,
	replace(footage_ran,'nan','0')::float				as footused,
	replace(footage_ran_good,'nan','0')::float			as footused_tabco_good,
	case 
		when tbs.house_number is not null then 0
		else replace(footage_ran,'nan','0')::float
	end													as footused_tabco_ex_booklet,
	case 
		when tbs.house_number is not null then 0
		else replace(footage_ran_good,'nan','0')::float
	end													as footused_tabco_good_ex_booklet,
	sum(((replace(stopdate,'None','1900-01-01')::date - replace(startdate,'None','1900-01-01')::date) *86400) + extract(epoch from (stoptime::time - starttime::time)))  	as eltime
from dt_inovar_prod_stg.in_kansascity_fact_presstime kpt
left join tabco_booklet_suborders tbs
	on tbs.house_number = kpt.house_number
where replace(startdate,'None','1900-01-01')::date >= '2021-01-01' and replace(stopdate,'None','1900-01-01')::date >= '2021-01-01'
group by 1,2,3,4,5,6,7,8,9,10,11
),
tabco_elapsed as (
select 
	'Kansas City' 																			as location,
	'Tabco LLC' 																			as company,
	'Kansas City' 																			as businessunit,
	null::text 																				as id,
	null::text 																				as assocno,
	concat('KSKA51-T-',kpt_cte.ticket_number)												as company_ticket_number,
	workoperation,
	kpt_cte.sdate 								 											as sdate,
	kpt_cte.edate::text 																	as edate,
	kpt_cte.stime::text																		as stime,
	kpt_cte.etime::text																		as etime,
	kpt_cte.eltime 																			as elapsed,
	null::text 																				as closed,
	null::text 																				as finishedpieces,
	kpt_cte.whichpress 																		as pressno,
	kpt_cte.footused::float 																as footused,
	kpt_cte.footused_tabco_good,
	footused_tabco_ex_booklet,
	footused_tabco_good_ex_booklet,
	null::text 																				as totalizer,
	null::text 																				as offpress,
	null::text 																				as packaged,
	null::text 																				as ticket_pressequip,
	case 
		when kpt_cte.whichpress::text in ('Press 1','Press 2','Press 3','Press 4') then 'Flexo'
		else 'Undefined'
	end 																					as equipment_grouping,
	p.press_type,
	SUM(footused::float) OVER (PARTITION BY ticket_number)									as ticket_level_footage,
	count(1) OVER (PARTITION BY ticket_number) 												as ticket_level_count
from tabco kpt_cte
left join press p on lower(p.press) = lower(kpt_cte.whichpress) and p.entity ilike 'TABCO'
),
tabco_footused as (
select
	distinct 
	case 
		when tkt.sum_roll_length_in_ft_one_calc = '' 
			then 
				case 
					when tkt.sum_roll_length_in_ft_two_calc = '' 
						then 
							case 
								when tkt.sum_roll_length_in_ft_lam_calc = '' then '0'
								else tkt.sum_roll_length_in_ft_lam_calc
							end
					else tkt.sum_roll_length_in_ft_two_calc
				end
		else tkt.sum_roll_length_in_ft_one_calc
	end																																	as footused,
	case
		when dt_gross_footage = '' then 0
		else dt_gross_footage::float
	end 																																as dt_gross_footage,
	case
		when dt_gross_footage_order = '' then 0
		else dt_gross_footage_order::float
	end 																																as dt_gross_footage_order,
	case
		when lineal_inches_shipped = '' then 0
		else lineal_inches_shipped::float / 12
	end 																																as good_footage,
	case
		when tbs.house_number is not null then 0
		when lineal_inches_shipped = '' then 0
		else lineal_inches_shipped::float / 12
	end 																																as good_footage_order,
	tkt.house_number,
	replace(date_order,'None','1900-01-01')																								as orderdate,
	tkt.customer_number,
	COALESCE(cus.customer_name, tkt.customer_name_calc) 																				AS customer_name,
	row_number() over(partition by tkt.house_number order by date_order desc, tkt.cyrel__ desc, record_created desc, ship_via_one desc)		as row_number,
	trim(initcap(press_operator))																										as press_operator
from dt_inovar_prod_stg.in_kansascity_fact_orders tkt
left join (select customer_number, max(customer_name) as customer_name from dt_inovar_prod_stg.in_kansascity_dim_customers group by 1) cus
	ON cus.customer_number::text = tkt.customer_number::text
left join tabco_booklet_suborders tbs
	on tbs.house_number = tkt.house_number
where replace(date_order,'None','1900-01-01') >= '2021-01-01' and tkt.house_number <> ''
),
tabco_footused_clean as (
select 
	house_number,
	orderdate,
	dt_gross_footage,
	(dt_gross_footage - good_footage) 				as waste_footage,
	dt_gross_footage_order,
	(dt_gross_footage_order - good_footage_order) 	as waste_footage_order,
	good_footage,
	good_footage_order,
	customer_number,
	customer_name,
	press_operator
from tabco_footused
where row_number = 1
),
pl_work_sales_order as (
	select
		workordersales.workorderid::text,
		qtyorderedperm
	from dt_inovar_prod_stg.in_precision_fact_workorderitemsalesorder workordersales
),
pl_work_order_quantity as (
	select 
		workorderid,
		sum(qtyorderedperm) AS work_order_quantity
	from pl_work_sales_order
	group by pl_work_sales_order.workorderid
),
pl_work_order_mapping as (
	select 
		pl_work_sales_order.workorderid,
		sum(pl_work_sales_order.qtyorderedperm) / nullif(pl_work_order_quantity.work_order_quantity, 0) ratio_of_qty
	from pl_work_sales_order
	left join pl_work_order_quantity on pl_work_order_quantity.workorderid = pl_work_sales_order.workorderid
	group by pl_work_sales_order.workorderid,
			pl_work_order_quantity.work_order_quantity
),
pl_ops_metrics as (
	select 
		workorderid,
		min(workorderfinishdate) AS workorderfinishdate, 
		sum(makereadytimebyoperator::float) as make_ready_time,
		sum(totaltimebyoperator::float) as total_time,
		sum(totalrunfootagebyoperator::float) as good_footage
	from
		dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date
	group by 
		workorderid,
		pressname,
		pressdescription,
		presstype				
),
pl_work_sales_order_metrics as (
	select
		pl_work_order_mapping.workorderid,
		pl_ops_metrics.workorderfinishdate,
		sum(pl_ops_metrics.make_ready_time * pl_work_order_mapping.ratio_of_qty) as make_ready_time,
		sum(pl_ops_metrics.total_time * pl_work_order_mapping.ratio_of_qty) as total_time,
		sum(pl_ops_metrics.good_footage * pl_work_order_mapping.ratio_of_qty) as good_footage
	from pl_work_order_mapping 
	left join pl_ops_metrics on pl_ops_metrics.workorderid = pl_work_order_mapping.workorderid
	GROUP BY pl_work_order_mapping.workorderid,
		pl_ops_metrics.workorderfinishdate
),
pl_press_associate_mapping as (
	SELECT 		
		workorderid,
		presstype
	FROM ( 
			SELECT 
				workorderid,
				presstype,
				row_number() over (partition by workorderid order by good_footage desc) max_footage
			FROM (			
					SELECT 
						workorderid,
						presstype,
						sum(totalrunfootagebyoperator::float) AS good_footage
					FROM dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date
						WHERE (totalrunfootagebyoperator::float <> 0 OR totalworkorderfootagebyoperator::float <> 0 OR totaltimebyoperator::float <> 0)
					GROUP BY 1,2
				) a
			) b	
		WHERE max_footage = 1				
),
pl_ops_data AS (
SELECT
	pl_work_sales_order_metrics.workorderfinishdate,
	pl_press_associate_mapping.presstype,
	pl_work_sales_order_metrics.make_ready_time AS makereadytimebyoperator,
	pl_work_sales_order_metrics.total_time AS totaltimebyoperator,
	pl_work_sales_order_metrics.good_footage AS totalrunfootagebyoperator
FROM pl_work_sales_order_metrics
	LEFT JOIN pl_press_associate_mapping on pl_work_sales_order_metrics.workorderid = pl_press_associate_mapping.workorderid
),
final_ops as (
select
	'Oceanside' 																	as businessunit,
	pbd.workorderfinishdate::date 													as sdate,
	(pbd.totaltimebyoperator::float - pbd.makereadytimebyoperator::float) * 3600 	as elapsed,
	pbd.totalrunfootagebyoperator::float											as footused,
	pbd.presstype::text																as press_type,
	0::float																		as footused_tabco_ex_booklet,
	0::float																		as footused_tabco_good_ex_booklet
from pl_ops_data pbd
UNION ALL 
select
	'Oceanside' 																	as businessunit,
	pbd.workorderfinishdate::date 													as sdate,
	pbd.makereadytimebyoperator::float * 3600 										as elapsed,
	0::float																		as footused,
	pbd.presstype::text																as press_type,
	0::float																		as footused_tabco_ex_booklet,
	0::float																		as footused_tabco_good_ex_booklet
from pl_ops_data pbd
UNION ALL 
select
	'Oceanside' 																	as businessunit,
	pbd.workorderfinishdate::date 													as sdate,
	(pbd.totaltimebyoperator::float - pbd.makereadytimebyoperator::float) * 3600 	as elapsed,
	pbd.totalrunfootagebyoperator::float 											as footused,
	pbd.presstype::text																as press_type,
	0::float																		as footused_tabco_ex_booklet,
	0::float																		as footused_tabco_good_ex_booklet
from dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date pbd		------
where pbd.workorderfinishdate::date >= '2021-01-01'
	union all
select
	'Oceanside' 																	as businessunit,
	pbd.runstepfinishdate::date 													as sdate,
	- coalesce(pbd.totaldowntime,0) * 3600 											as elapsed, 
	0::float 																		as footused,
	pbd.presstype::text																as press_type,
	0::float																		as footused_tabco_ex_booklet,
	0::float																		as footused_tabco_good_ex_booklet
from dt_inovar_prod_stg.in_fact_kpi_downtime_hours_by_date pbd
where pbd.runstepfinishdate::date >= '2021-01-01'
	union all
select
	'Oceanside' 																	as businessunit,
	pbd.workorderfinishdate::date 													as sdate,
	pbd.makereadytimebyoperator::float * 3600 										as elapsed,
	0::float																		as footused,
	pbd.presstype::text																as press_type,
	0::float																		as footused_tabco_ex_booklet,
	0::float																		as footused_tabco_good_ex_booklet
from dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date pbd	
where workorderfinishdate::date >= '2021-01-01'
	union all
select
	'Oceanside' 																	as businessunit,
	pbd.runstepfinishdate::date 													as sdate,
	pbd.totaldowntime::float * 3600 												as elapsed,
	0::float 																		as footused,
	pbd.presstype::text																as press_type,
	0::float																		as footused_tabco_ex_booklet,
	0::float																		as footused_tabco_good_ex_booklet
from dt_inovar_prod_stg.in_fact_kpi_downtime_hours_by_date pbd
where runstepfinishdate::date >= '2021-01-01'
	union all
select 
	businessunit,
	sdate::date,
	elapsed,
	te.footused,
	press_type,
	footused_tabco_ex_booklet,
	footused_tabco_good_ex_booklet
from tabco_elapsed te
left join tabco_footused_clean tf 
	on concat('KSKA51-T-',tf.house_number) = te.company_ticket_number
where pressno is not null and pressno <> ''
	union all
select
	CASE
		WHEN tc.entity = 'Butler' 				THEN 'Milwaukee'
		WHEN tc.entity = 'Dallas' 				THEN 'Dallas'
		WHEN tc.entity = 'Davie' 				THEN 'Ft. Lauderdale'
		WHEN tc.entity = 'NewBuryPort' 			THEN 'Newburyport'
		WHEN tc.entity = 'Cimarron North' 		THEN 'Cimarron North'
		WHEN tc.entity = 'Amherst' 				THEN 'Amherst Label'
		WHEN tc.entity = 'Westfield' 			THEN 'Westfield'
	END																		AS businessunit,
	tc.sdate,
	case
		when tc.edate = '' then EXTRACT(EPOCH FROM tc.elapsed::time)
		else (((edate::date - sdate::date) * 86400) + extract(epoch from (etime::time - stime::time)))
	end													as elapsed,
    p.press_type,
	0::float											as footused_tabco_ex_booklet,
	0::float											as footused_tabco_good_ex_booklet
from (select *, case when entity = 'Cimarron North' and workoperation ~ '^\d' then right(workoperation, (length(workoperation) - 4)) else workoperation end as workoperation_clean 
		from dt_inovar_prod_edw.fact_timecard) tc
left join press p
	on trim(lower(p.press)) = trim(lower(tc.pressno)) and p.entity = tc.entity
left join dt_inovar_prod_edw.fact_ticket_header tkt 
	on tkt.number = tc.ticket_no and tkt.entity = tc.entity
left join press pt 
	on trim(lower(pt.press)) = trim(lower(tkt.press)) and pt.entity = tkt.entity
where tc.sdate >= '2015-01-01'
	and tc.sdate <= current_date
	and tc.workoperation not in ('Punch In', 'Punch Out')
	and tc.pressno is not null and tc.pressno <> ''
)
select 
	ops.businessunit,
	sdate,
	elapsed,
	footused,
	case 
		when trim(lower(press_type)) = 'flexo' then 'Flexo Press'
		when trim(lower(press_type)) = 'digital' then 'Digital Press'
		when trim(lower(press_type)) = 'hybrid' then 'Hybrid Press'
		when trim(lower(press_type)) = 'digital finishing' then 'Digital Finishing Equipment'
		when trim(lower(press_type)) = 'large format' then 'Large Format Press'
		when trim(lower(press_type)) = 'other' then 'Other'
		else initcap(trim(press_type))
	end						as press_type,
	footused_tabco_good,
	footused_tabco_ex_booklet,
	footused_tabco_good_ex_booklet 
from final_ops ops
where sdate::date>='2023-01-01' and sdate::date<='2023-12-31' 
group by 1,press_type

-- ops view end


