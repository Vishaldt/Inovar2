/*
 * 04/12: Added Associate for PL in 2 unions of KPI WO
 */

-- v_inovar_operations



--DROP VIEW dt_inovar_prod_edw.v_inovar_operations

--CREATE OR REPLACE VIEW dt_inovar_prod_edw.v_inovar_operations AS 


select businessunit,
		sum(elapsed::float) elapsed,
		sum(footused) footused,
		sum(footused_tabco_ex_booklet) footused_tabco_ex_booklet,
		sum(footused_tabco_good_ex_booklet) footused_tabco_good_ex_booklet
		from dt_inovar_prod_edw.u_mat_inovar_operations
			where sdate::date>='2023-01-01' and sdate::date<='2023-12-31'
	group by 1
	
select businessunit,
		sum(elapsed::float) elapsed,
		sum(footused) footused,
		sum(footused_tabco_ex_booklet) footused_tabco_ex_booklet,
		sum(footused_tabco_good_ex_booklet) footused_tabco_good_ex_booklet
		from dt_inovar_dev_stg.u_mat_inovar_operations_test		--new table created
		where sdate::date>='2023-01-01' and sdate::date<='2023-12-31'
	group by 1


	
	
/*create table dt_inovar_dev_stg.u_mat_inovar_operations_test
as select * from dt_inovar_dev_stg.v_inovar_operations_test

drop view dt_inovar_dev_stg.v_inovar_operations_test

drop table dt_inovar_dev_stg.u_mat_inovar_operations_test

CREATE OR REPLACE VIEW dt_inovar_dev_stg.v_inovar_operations_test AS */
with /*press as (
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
	trim(pd."PressNo")          							as press/*,
	pd."PressType"          								as press_type*/
from dt_inovar_prod_stg.in_press_description pd
),*/
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
	tkt.house_number
--	finished_product_type
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
	/*replace(stopdate,'None','1900-01-01')::date 		as edate,
	starttime 											as stime,
	stoptime 											as etime,
	kpt.house_number 									as ticket_number,*/
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
	replace(footage_ran,'nan','0')::float				as footused,	--imp
	/*replace(footage_ran_good,'nan','0')::float			as footused_tabco_good,*/
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
group by startdate,
		whichpress,
		footused,
		footused_tabco_ex_booklet,
		tbs.house_number,
		kpt.footage_ran_good,workoperation
),
tabco_elapsed as (
select 
	/*'Kansas City' 																		as location,
	'Tabco LLC' 																			as company,*/
	'Kansas City' 																			as businessunit,
/*	null::text 																				as id,
	null::text 																				as assocno,
	concat('KSKA51-T-',kpt_cte.ticket_number)												as company_ticket_number,*/
	workoperation,
	kpt_cte.sdate 								 											as sdate,
/*	kpt_cte.edate::text 																	as edate,
	kpt_cte.stime::text																		as stime,
	kpt_cte.etime::text																		as etime,*/
	kpt_cte.eltime 																			as elapsed,
/*	null::text 																				as closed,
	null::text 																				as finishedpieces,*/
	kpt_cte.whichpress 																		as pressno,
	kpt_cte.footused::float 																as footused,
--	kpt_cte.footused_tabco_good,
	footused_tabco_ex_booklet,
	footused_tabco_good_ex_booklet
	/*null::text 																				as totalizer,
	null::text 																				as offpress,
	null::text 																				as packaged,
	null::text 																				as ticket_pressequip,
	case 
		when kpt_cte.whichpress::text in ('Press 1','Press 2','Press 3','Press 4') then 'Flexo'
		else 'Undefined'
	end 																					as equipment_grouping,
	p.press_type,
	SUM(footused::float) OVER (PARTITION BY ticket_number)									as ticket_level_footage,
	count(1) OVER (PARTITION BY ticket_number) 												as ticket_level_count*/
from tabco kpt_cte
--left join press p on lower(p.press) = lower(kpt_cte.whichpress) and p.entity ilike 'TABCO'
),
/*tabco_footused as (
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
),*/
/*tabco_footused_clean as (
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
),*/
---------------------------- tabco cte's till now
---------------------------- pl cte starts
-- ordered qty at Salesorder + Linekey + Item level 
/*pl_tkt_head as (
	select 
		tkt_head.salesorderno,
		tkt_head.customerno,
		tkt_head.orderdate
	from dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader tkt_head
		where date_trunc('year', orderdate) >= '2021-01-01'
	UNION  
	select 
		tkt_head.salesorderno,
		tkt_head.customerno,
		tkt_head.orderdate
	from dt_inovar_prod_stg.in_fact_sage_so_salesorderheader tkt_head
		where date_trunc('year', orderdate) >= '2021-01-01'	
),*/
/*pl_sku_count AS (
	SELECT 
		salesorderno,
		count(DISTINCT itemno) AS sku_count
	FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistorydetail
		WHERE itemtype = '1'
		AND salesorderno NOT IN ('', '.')
	GROUP BY 1
	UNION
	SELECT 
		salesorderno,
		count(DISTINCT itemcode) AS sku_count
	FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderdetail
		WHERE itemtype = '1'
		AND salesorderno NOT IN ('', '.')
	GROUP BY 1
),*/
-- workorder id data at Salesorder + Linekey + Item level
pl_work_sales_order as (
	select
		workordersales.workorderid::text,
		/*salesorderno,
		salesorderdate,
		linekey,
		itemno,*/
		qtyorderedperm
	from dt_inovar_prod_stg.in_precision_fact_workorderitemsalesorder workordersales
--		where date_trunc('year', salesorderdate) >= '2021-01-01'
),
-- Total Qty for each workorderid across Salesorder + Linekey + Item
pl_work_order_quantity as (
	select 
		workorderid,
		sum(qtyorderedperm) AS work_order_quantity
	from pl_work_sales_order
	group by pl_work_sales_order.workorderid
),
-- finding proportion of qty for a salesorder with respect to all salesorder in workorder
pl_work_order_mapping as (		-----------------
	select 
		pl_work_sales_order.workorderid,
		/*pl_work_sales_order.salesorderno,
		pl_work_sales_order.linekey,
		pl_work_sales_order.itemno,*/
--		******************************
		sum(pl_work_sales_order.qtyorderedperm) / nullif(pl_work_order_quantity.work_order_quantity, 0) ratio_of_qty
--		******************************
	from pl_work_sales_order
	left join pl_work_order_quantity on pl_work_order_quantity.workorderid = pl_work_sales_order.workorderid
	group by pl_work_sales_order.workorderid,
			/*pl_work_sales_order.salesorderno,
			pl_work_sales_order.linekey,
			pl_work_sales_order.itemno,*/
			pl_work_order_quantity.work_order_quantity
),
-- ops metrics at workorderid level
pl_ops_metrics as (
	select 
		workorderid,
		min(workorderfinishdate) AS workorderfinishdate,
		sum(makereadytimebyoperator::float) as make_ready_time,
		sum(totaltimebyoperator::float) as total_time,
		sum(totalrunfootagebyoperator::float) as good_footage/*,
		sum(totalworkorderfootagebyoperator::float) as total_footage,
		sum(totalestimatedpressfootage::float) as est_press_footage,
		sum(totalfeetoflabelsbyoperator::float) as labels_footage,
		sum(totalpredictedwastefootagebyoperator::float) as est_waste_footage,
		sum(totalwasteperoperator::float) as act_waste_footage*/
	from
		dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date
	group by 
		workorderid
		/*pressname,
		pressdescription,
		presstype	*/			
),
-- apportioning metrics across different salesorder for a workorderid
pl_work_sales_order_metrics as (
	select
		/*pl_work_order_mapping.workorderid,
		pl_work_order_mapping.salesorderno,*/
		pl_ops_metrics.workorderfinishdate,
		sum(pl_ops_metrics.make_ready_time * pl_work_order_mapping.ratio_of_qty) as make_ready_time,
		sum(pl_ops_metrics.total_time * pl_work_order_mapping.ratio_of_qty) as total_time,
		sum(pl_ops_metrics.good_footage * pl_work_order_mapping.ratio_of_qty) as good_footage/*,
		sum(pl_ops_metrics.total_footage * pl_work_order_mapping.ratio_of_qty) as total_footage,
		sum(pl_ops_metrics.est_press_footage * pl_work_order_mapping.ratio_of_qty) as est_press_footage,
		sum(pl_ops_metrics.labels_footage * pl_work_order_mapping.ratio_of_qty) as labels_footage,
		sum(pl_ops_metrics.est_waste_footage * pl_work_order_mapping.ratio_of_qty) as est_waste_footage,
		sum(pl_ops_metrics.act_waste_footage * pl_work_order_mapping.ratio_of_qty) as act_waste_footage	*/	
	from pl_work_order_mapping 
	left join pl_ops_metrics on pl_ops_metrics.workorderid = pl_work_order_mapping.workorderid
	GROUP BY /*pl_work_order_mapping.workorderid,
		pl_work_order_mapping.salesorderno,*/
		pl_ops_metrics.workorderfinishdate
),
/*pl_press_associate_mapping as (
	SELECT 		
		workorderid,
		pressname,
		pressdescription,
		presstype
	FROM ( 
			SELECT 
				workorderid,
				pressname,
				pressdescription,
				presstype,
				good_footage,
				row_number() over (partition by workorderid order by good_footage desc) max_footage
			FROM (			
					SELECT 
						workorderid,
						pressname,
						pressdescription,
						presstype,
						sum(totalrunfootagebyoperator::float) AS good_footage
					FROM dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date
						WHERE (totalrunfootagebyoperator::float <> 0 OR totalworkorderfootagebyoperator::float <> 0 OR totaltimebyoperator::float <> 0)
					GROUP BY 1,2,3,4
				) a
			) b	
		WHERE max_footage = 1				
),*/
pl_ops_data AS (
SELECT
	/*pl_work_sales_order_metrics.workorderid,
	concat('Carlsbad-', pl_work_sales_order_metrics.salesorderno) AS company_ticket_number,*/
	pl_work_sales_order_metrics.workorderfinishdate,
	pl_press_associate_mapping.pressname,
	/*pl_press_associate_mapping.pressdescription,
	pl_press_associate_mapping.presstype,
	concat('Carlsbad-', pl_tkt_head.customerno) AS company_customer_number,
	pl_tkt_head.orderdate,
	pl_sku_count.sku_count,*/
	pl_work_sales_order_metrics.make_ready_time AS makereadytimebyoperator,
	pl_work_sales_order_metrics.total_time AS totaltimebyoperator,
	pl_work_sales_order_metrics.good_footage AS totalrunfootagebyoperator
/*	pl_work_sales_order_metrics.total_footage AS totalworkorderfootagebyoperator,
	pl_work_sales_order_metrics.act_waste_footage AS totalwasteperoperator*/
FROM pl_work_sales_order_metrics
	/*LEFT JOIN pl_press_associate_mapping on pl_work_sales_order_metrics.workorderid = pl_press_associate_mapping.workorderid
	LEFT JOIN pl_tkt_head ON pl_tkt_head.salesorderno = pl_work_sales_order_metrics.salesorderno
	LEFT JOIN pl_sku_count ON pl_sku_count.salesorderno = pl_work_sales_order_metrics.salesorderno*/
),
-------------------------- pl cte ends
final_ops as (
--******************************************** pl data at ticket level starts
select
	/*'Oceanside' 																	as location,
	'Oceanside' 																	as company,*/
	'Oceanside' 																	as businessunit,	--
	/*'Carlsbad-Ticket' 																as id,
	null::text 																		as assocno,
	pbd.company_ticket_number,*/
	'run'::text 																	as workoperation,
	pbd.workorderfinishdate::date 													as sdate,
	/*pbd.workorderfinishdate::text 													as edate,		
	null::text 																		as stime,		
	null::text 																		as etime,*/
	(pbd.totaltimebyoperator::float - pbd.makereadytimebyoperator::float) * 3600 	as elapsed,		--imp
	/*null::text 																		as closed,
	null::text 																		as finishedpieces,*/
	pbd.pressname  																	as pressno, 
	pbd.totalrunfootagebyoperator::float											as footused,	--imp	
	/*null::text 																		as totalizer,
	null::text 																		as offpress,
	null::text 																		as packaged,
	null::text 																		as ticket_pressequip,
	case 
		when trim(lower(pbd.pressname)) ilike '%outsourced%'
		then 'Undefined'
		else
		pbd.presstype::text 
	end 																			as equipment_grouping,
	pbd.presstype::text																as press_type,	--
	null::text																		as customername,
	company_customer_number,
	null::text																		as associate_lastname,
	null::text																		as associate_firstname,
	null::date 																		as datedone, 
	null::text 																		as ticketstatus,
	null::text 																		as tickettype,
	null::text																		as equipment_ptype,
	null::text																		as equipment_description,
	null::text																		as ticket_number,
	null::date																		as orderdate,
	0::float																		as estpresstime,
	0::float																		as estfootage,
	0::float																		as est_spoilfootage,
	0::float																		as est_setupfootage,
	0::float																		as actquantity,
	0::float																		as ticquantity,
	0::float																		as actfootage,
	0::float																		as ticket_level_footage,
	0::float																		as ticket_level_count,
	null::text																		as ticket_press,
	null::text																		as ticket_press_type,
	sku_count,
	totalworkorderfootagebyoperator,
	totalwasteperoperator,
	0::float																		as footused_tabco_good,*/
	0::float																		as footused_tabco_ex_booklet,	--imp
	0::float																		as footused_tabco_good_ex_booklet	--imp
/*	0::float																		as dt_gross_footage,
	0::float																		as waste_footage,
	0::float																		as dt_gross_footage_order,
	0::float																		as waste_footage_order*/
from pl_ops_data pbd		------
--where pbd.workorderfinishdate::date >= '2021-01-01'
UNION ALL 
select
	/*'Oceanside' 																	as location,
	'Oceanside' 																	as company,*/
	'Oceanside' 																	as businessunit,
	/*'Carlsbad-Ticket' 															as id,
	null::text 																		as assocno,
	company_ticket_number,*/
	'make ready'::text 																as workoperation,
	pbd.workorderfinishdate::date 													as sdate,		
	/*pbd.workorderfinishdate::text 													as edate,		
	null::text 																		as stime,
	null::text 																		as etime,*/
	pbd.makereadytimebyoperator::float * 3600 										as elapsed,	--imp
	/*null::text 																		as closed,
	null::text 																		as finishedpieces, */
	pbd.pressname  																	as pressno,
	0::float																		as footused, --imp
	/*null::text 																		as totalizer,
	null::text 																		as offpress,
	null::text 																		as packaged,
	null::text 																		as ticket_pressequip,
	case 
		when trim(lower(pbd.pressname)) ilike '%outsourced%'
		then 'Undefined'
		else
		pbd.presstype::text 
	end 																			as equipment_grouping,
	pbd.presstype::text																as press_type,
	null::text																		as customername,
	company_customer_number,
	null::text																		as associate_lastname,
	null::text																		as associate_firstname,
	null::date 																		as datedone, 
	null::text 																		as ticketstatus,
	null::text 																		as tickettype,
	null::text																		as equipment_ptype,
	null::text																		as equipment_description,
	null::text																		as ticket_number,
	null::date																		as orderdate,
	0::float																		as estpresstime,
	0::float																		as estfootage,
	0::float																		as est_spoilfootage,
	0::float																		as est_setupfootage,
	0::float																		as actquantity,
	0::float																		as ticquantity,
	0::float																		as actfootage,
	0::float																		as ticket_level_footage,
	0::float																		as ticket_level_count,
	null::text																		as ticket_press,
	null::text																		as ticket_press_type,
	sku_count,
	0::float																		as totalworkorderfootagebyoperator,
	0::float																		as totalwasteperoperator,
	0::float																		as footused_tabco_good,*/
	0::float																		as footused_tabco_ex_booklet,	--imp
	0::float																		as footused_tabco_good_ex_booklet	--imp
	/*0::float																		as dt_gross_footage,
	0::float																		as waste_footage,
	0::float																		as dt_gross_footage_order,
	0::float																		as waste_footage_order*/
from pl_ops_data pbd		-----
--where workorderfinishdate::date >= '2021-01-01'
--******************************************** pl data at ticket level ends
UNION ALL 
select
	/*'Oceanside' 																	as location,
	'Oceanside' 																	as company,*/
	'Oceanside' 																	as businessunit,
	/*'Carlsbad-Associate' 															as id,
	null::text 																		as assocno,
	null::text	 																	as company_ticket_number,*/
	'run'::text 																	as workoperation,
	pbd.workorderfinishdate::date 													as sdate,
	/*pbd.workorderfinishdate::text 													as edate,		
	null::text 																		as stime,		
	null::text 																		as etime,*/
	(pbd.totaltimebyoperator::float - pbd.makereadytimebyoperator::float) * 3600 	as elapsed,	--imp
	/*null::text 																		as closed,
	null::text 																		as finishedpieces,*/
	pbd.pressname  																	as pressno, 
	pbd.totalrunfootagebyoperator::float 											as footused,	--imp
	/*null::text 																		as totalizer,
	null::text 																		as offpress,
	null::text 																		as packaged,
	null::text 																		as ticket_pressequip,
	case 
		when trim(lower(pbd.pressname)) ilike '%outsourced%'
		then 'Undefined'
		else
		pbd.presstype::text 
	end 																			as equipment_grouping,
	pbd.presstype::text																as press_type,
	null::text																		as customername,
	null::text																		as company_customer_number,
	null::text																		as associate_lastname,
	operator																		as associate_firstname,
	null::date 																		as datedone, 
	null::text 																		as ticketstatus,
	null::text 																		as tickettype,
	null::text																		as equipment_ptype,
	null::text																		as equipment_description,
	null::text																		as ticket_number,
	null::date																		as orderdate,
	0::float																		as estpresstime,
	0::float																		as estfootage,
	0::float																		as est_spoilfootage,
	0::float																		as est_setupfootage,
	0::float																		as actquantity,
	0::float																		as ticquantity,
	0::float																		as actfootage,
	0::float																		as ticket_level_footage,
	0::float																		as ticket_level_count,
	null::text																		as ticket_press,
	null::text																		as ticket_press_type,
	0::float																		as sku_count,
	totalworkorderfootagebyoperator::float											as totalworkorderfootagebyoperator,
	totalwasteperoperator::float													as totalwasteperoperator,
	0::float																		as footused_tabco_good,*/
	0::float																		as footused_tabco_ex_booklet,	--imp
	0::float																		as footused_tabco_good_ex_booklet	--imp
	/*0::float																		as dt_gross_footage,
	0::float																		as waste_footage,
	0::float																		as dt_gross_footage_order,
	0::float																		as waste_footage_order*/
from dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date pbd		------
where pbd.workorderfinishdate::date >= '2021-01-01'
	union all
select
	/*'Oceanside' 																	as location,
	'Oceanside' 																	as company,*/
	'Oceanside' 																	as businessunit,
	/*'Carlsbad-Associate' 															as id,
	null::text 																		as assocno,
	null::text	 																	as company_ticket_number,*/
	'run'::text 																	as workoperation,
	pbd.runstepfinishdate::date 													as sdate,
	/*pbd.runstepfinishdate::text 													as edate,		
	null::text 																		as stime,		
	null::text 																		as etime,*/
	-coalesce(pbd.totaldowntime,0) * 3600 											as elapsed,	--imp
	/*null::text 																		as closed,
	null::text 																		as finishedpieces,*/
	pbd.pressname  																	as pressno, 
	0::float 																		as footused,	----
	/*null::text 																		as totalizer,
	null::text 																		as offpress,
	null::text 																		as packaged,
	null::text 																		as ticket_pressequip,
	case 
		when trim(lower(pbd.pressname)) ilike '%outsourced%'
		then 'Undefined'
		else
		pbd.presstype::text 
	end 																			as equipment_grouping,
	pbd.presstype::text																as press_type,
	null::text																		as customername,
	null::text																		as company_customer_number,
	null::text																		as associate_lastname,
	null::TEXT																		as associate_firstname,
	null::date 																		as datedone, 
	null::text 																		as ticketstatus,
	null::text 																		as tickettype,
	null::text																		as equipment_ptype,
	null::text																		as equipment_description,
	null::text																		as ticket_number,
	null::date																		as orderdate,
	0::float																		as estpresstime,
	0::float																		as estfootage,
	0::float																		as est_spoilfootage,
	0::float																		as est_setupfootage,
	0::float																		as actquantity,
	0::float																		as ticquantity,
	0::float																		as actfootage,
	0::float																		as ticket_level_footage,
	0::float																		as ticket_level_count,
	null::text																		as ticket_press,
	null::text																		as ticket_press_type,
	0::float																		as sku_count,
	0::float																		as totalworkorderfootagebyoperator,
	0::float																		as totalwasteperoperator,
	0::float																		as footused_tabco_good,*/
	0::float																		as footused_tabco_ex_booklet,	--imp
	0::float																		as footused_tabco_good_ex_booklet	--imp
	/*0::float																		as dt_gross_footage,
	0::float																		as waste_footage,
	0::float																		as dt_gross_footage_order,
	0::float																		as waste_footage_order*/
from dt_inovar_prod_stg.in_fact_kpi_downtime_hours_by_date pbd
where pbd.runstepfinishdate::date >= '2021-01-01'
	union all
select
	/*'Oceanside' 																	as location,
	'Oceanside' 																	as company,*/
	'Oceanside' 																	as businessunit,
	/*'Carlsbad-Associate' 															as id,
	null::text 																		as assocno,
	null::text	 																	as company_ticket_number,*/
	'make ready'::text 																as workoperation,
	pbd.workorderfinishdate::date 													as sdate,		
	/*pbd.workorderfinishdate::text 													as edate,		
	null::text 																		as stime,
	null::text 																		as etime,*/
	pbd.makereadytimebyoperator::float * 3600 										as elapsed,
	/*null::text 																		as closed,
	null::text 																		as finishedpieces,*/
	pbd.pressname  																	as pressno, 
	0::float																		as footused,
/*	null::text 																		as totalizer,
	null::text 																		as offpress,
	null::text 																		as packaged,
	null::text 																		as ticket_pressequip,
	case 
		when trim(lower(pbd.pressname)) ilike '%outsourced%'
		then 'Undefined'
		else
		pbd.presstype::text 
	end 																			as equipment_grouping,
	pbd.presstype::text																as press_type,
	null::text																		as customername,
	null::text																		as company_customer_number,
	null::text																		as associate_lastname,
	operator																		as associate_firstname,
	null::date 																		as datedone, 
	null::text 																		as ticketstatus,
	null::text 																		as tickettype,
	null::text																		as equipment_ptype,
	null::text																		as equipment_description,
	null::text																		as ticket_number,
	null::date																		as orderdate,
	0::float																		as estpresstime,
	0::float																		as estfootage,
	0::float																		as est_spoilfootage,
	0::float																		as est_setupfootage,
	0::float																		as actquantity,
	0::float																		as ticquantity,
	0::float																		as actfootage,
	0::float																		as ticket_level_footage,
	0::float																		as ticket_level_count,
	null::text																		as ticket_press,
	null::text																		as ticket_press_type,
	0::float																		as sku_count,
	0::float																		as totalworkorderfootagebyoperator,
	0::float																		as totalwasteperoperator,
	0::float																		as footused_tabco_good,*/
	0::float																		as footused_tabco_ex_booklet,
	0::float																		as footused_tabco_good_ex_booklet
/*	0::float																		as dt_gross_footage,
	0::float																		as waste_footage,
	0::float																		as dt_gross_footage_order,
	0::float																		as waste_footage_order*/
from dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date pbd		-----
where workorderfinishdate::date >= '2021-01-01'			-----
	union all
select
	/*'Oceanside' 																	as location,
	'Oceanside' 																	as company,*/
	'Oceanside' 																	as businessunit,
/*	'Carlsbad-Associate' 															as id,
	null::text 																		as assocno,
	null::text	 																	as company_ticket_number,*/
	concat('DT-', downtimereason)													as workoperation,
	pbd.runstepfinishdate::date 													as sdate,
	/*pbd.runstepfinishdate::text 													as edate,
	null::text 																		as stime,
	null::text 																		as etime,*/
	pbd.totaldowntime::float * 3600 												as elapsed,
/*	null::text 																		as closed,
	null::text 																		as finishedpieces,*/
	pbd.pressname  																	as pressno, 
	0::float 																		as footused,
/*	null::text 																		as totalizer,
	null::text 																		as offpress,
	null::text 																		as packaged,
	null::text 																		as ticket_pressequip,
	case 
		when trim(lower(pbd.pressname)) ilike '%outsourced%'
		then 'Undefined'
		else
		pbd.presstype::text 
	end 																			as equipment_grouping,
	pbd.presstype::text																as press_type,
	null::text																		as customername,
	null::text																		as company_customer_number,
	null::text																		as associate_lastname,
	null::text																		as associate_firstname,
	null::date 																		as datedone, 
	null::text 																		as ticketstatus,
	null::text 																		as tickettype,
	null::text																		as equipment_ptype,
	null::text																		as equipment_description,
	null::text																		as ticket_number,
	null::date																		as orderdate,
	0::float																		as estpresstime,
	0::float																		as estfootage,
	0::float																		as est_spoilfootage,
	0::float																		as est_setupfootage,
	0::float																		as actquantity,
	0::float																		as ticquantity,
	0::float																		as actfootage,
	0::float																		as ticket_level_footage,
	0::float																		as ticket_level_count,
	null::text																		as ticket_press,
	null::text																		as ticket_press_type,
	0::float																		as sku_count,
	0::float																		as totalworkorderfootagebyoperator,
	0::float																		as totalwasteperoperator,
	0::float																		as footused_tabco_good,*/
	0::float																		as footused_tabco_ex_booklet,
	0::float																		as footused_tabco_good_ex_booklet
/*	0::float																		as dt_gross_footage,
	0::float																		as waste_footage,
	0::float																		as dt_gross_footage_order,
	0::float																		as waste_footage_order*/
from dt_inovar_prod_stg.in_fact_kpi_downtime_hours_by_date pbd
where runstepfinishdate::date >= '2021-01-01'
	union all
select 
	/*location,
	company,*/
	businessunit,
	/*id,
	assocno,
	te.company_ticket_number,*/
	workoperation,
	sdate::date,
	/*edate,*/
--	stime,
--	etime,
	elapsed,
	/*closed,
	finishedpieces,*/
	pressno,
	te.footused,
	/*totalizer,
	offpress,
	packaged,
	ticket_pressequip,
	equipment_grouping,
	press_type,
	tf.customer_name							as customername,
	concat('KSKA51-T-',tf.customer_number)		as company_customer_number, 
	null::text							as associate_lastname,
	tf.press_operator					as associate_firstname,
	null::date 							as datedone, 
	null::text 							as ticketstatus,
	null::text 							as tickettype,
	null::text							as equipment_ptype,
	null::text							as equipment_description,
	tf.house_number						as ticket_number,
	tf.orderdate::date,
	0::float							as estpresstime,
	0::float							as estfootage,
	0::float							as est_spoilfootage,
	0::float							as est_setupfootage,
	0::float							as actquantity,
	0::float							as ticquantity,
	0::float							as actfootage,
	0::float							as ticket_level_footage,
	0::float							as ticket_level_count,
	null::text							as ticket_press,
	null::text							as ticket_press_type,
	1::float							as sku_count,
	0::float							as totalworkorderfootagebyoperator,
	0::float							as totalwasteperoperator,
	footused_tabco_good,*/
	footused_tabco_ex_booklet,
	footused_tabco_good_ex_booklet
/*	case
		when ticket_level_footage > 0 	then (coalesce(dt_gross_footage,0) * te.footused / ticket_level_footage)
		when ticket_level_count > 0 	then (coalesce(dt_gross_footage,0) / ticket_level_count)
		else 0
	end									as dt_gross_footage,
	case
		when ticket_level_footage > 0 	then (coalesce(waste_footage,0) * te.footused / ticket_level_footage)
		when ticket_level_count > 0 	then (coalesce(waste_footage,0) / ticket_level_count)
		else 0
	end									as waste_footage,
	case
		when ticket_level_footage > 0 	then (coalesce(dt_gross_footage_order,0) * te.footused / ticket_level_footage)
		when ticket_level_count > 0 	then (coalesce(dt_gross_footage_order,0) / ticket_level_count)
		else 0
	end									as dt_gross_footage_order,
	case
		when ticket_level_footage > 0 	then (coalesce(waste_footage_order,0) * te.footused / ticket_level_footage)
		when ticket_level_count > 0 	then (coalesce(waste_footage_order,0) / ticket_level_count)
		else 0
	end									as waste_footage_order*/
from tabco_elapsed te
/*left join tabco_footused_clean tf 
	on concat('KSKA51-T-',tf.house_number) = te.company_ticket_number*/
where pressno is not null and pressno <> ''
	union all
select 
    /*CASE
		WHEN tc.entity = 'Butler' 				THEN 'Milwaukee'
		WHEN tc.entity = 'Dallas' 				THEN 'Dallas'
		WHEN tc.entity = 'Davie' 				THEN 'Ft. Lauderdale'
		WHEN tc.entity = 'NewBuryPort' 			THEN 'Newbury Port'
		WHEN tc.entity = 'Cimarron North' 		THEN 'Cimarron North'
		WHEN tc.entity = 'Amherst' 				THEN 'Amherst Label'
		WHEN tc.entity = 'Westfield' 			THEN 'Westfield'
	END																		AS location,
	CASE
		WHEN tc.entity = 'Butler' 				THEN 'Flexo-Graphics LLC'
		WHEN tc.entity = 'Dallas' 				THEN 'Inovar Packaging Group LLC'
		WHEN tc.entity = 'Davie' 				THEN 'Inovar Packaging Florida LLC'
		WHEN tc.entity = 'NewBuryPort' 			THEN 'Label Print America Inc.'
		WHEN tc.entity = 'Cimarron North' 		THEN 'Cimarron North'
		WHEN tc.entity = 'Amherst' 				THEN 'Amherst Label'
		WHEN tc.entity = 'Westfield' 			THEN 'Dion Label Printing LLC'
	END																		AS company,*/
	CASE
		WHEN tc.entity = 'Butler' 				THEN 'Milwaukee'
		WHEN tc.entity = 'Dallas' 				THEN 'Dallas'
		WHEN tc.entity = 'Davie' 				THEN 'Ft. Lauderdale'
		WHEN tc.entity = 'NewBuryPort' 			THEN 'Newburyport'
		WHEN tc.entity = 'Cimarron North' 		THEN 'Cimarron North'
		WHEN tc.entity = 'Amherst' 				THEN 'Amherst Label'
		WHEN tc.entity = 'Westfield' 			THEN 'Westfield'
	END																		AS businessunit,
/*    tc.id,
	tc.assocno,
	CASE
		WHEN tkt.entity = 'Butler' 				THEN concat('FG-', tkt.number)
		WHEN tkt.entity = 'Dallas' 				THEN concat('DALLAS-', tkt.number)
		WHEN tkt.entity = 'Davie' 				THEN concat('DAVIE-', tkt.number)
		WHEN tkt.entity = 'NewBuryPort' 		THEN concat('NE-', tkt.number)
		WHEN tkt.entity = 'Cimarron North' 		THEN concat('CN-', tkt.number)
		WHEN tkt.entity = 'Amherst' 			THEN concat('AL-', tkt.number)
		WHEN tkt.entity = 'Westfield' 			THEN concat('DL-', tkt.number)
	END																		AS company_ticket_number,*/
	tc.workoperation_clean								as workoperation,
	tc.sdate,
	/*tc.edate,
	tc.stime,
	tc.etime,*/
	case
		when tc.edate = '' then EXTRACT(EPOCH FROM tc.elapsed::time)
		else (((edate::date - sdate::date) * 86400) + extract(epoch from (etime::time - stime::time)))
	end													as elapsed,
/*	tc.closed,
	tc.finishedpieces::text,*/
	tc.pressno,
	tc.footused,
/*	tc.totalizer,
	tc.offpress,
	tc.packaged,
	tc.ticket_pressequip,
	case 
		when PressNo::text in ('301','302','303','304','305','1-21','3-21','4-21','5-21','1000-1','750','G1','NP 1','NP 2','Nil Shrink',
								'2200','2200 B','FB3','P17','P17B','P7','P7E','8C-Blue','9C-Blue','MPS2','MPS3','9C - MPS') then 'Flexo'
	 	when PressNo::text in ('350','351','HP1-21','HP1','WS6800','WS6900','HP8-1','HP6-3','HP6-4') then 'Digital'
     	when PressNo::text in ('450','453','ABG','Brotech-21','Delta 1','Brotech 1','DF3','DF3blank','Findel') then 'Digital Finishing'
     	when PressNo::text ilike '%750%' then 'Flexo'
     	else 'Undefined' 
     	end as Equipment_Grouping,
    p.press_type,
	COALESCE(cus.company, tkt.customername) 			as customername,
	CASE
		WHEN tkt.entity = 'Butler' 				THEN concat('FG-', tkt.customernum)
		WHEN tkt.entity = 'Dallas' 				THEN concat('DALLAS-', tkt.customernum)
		WHEN tkt.entity = 'Davie' 				THEN concat('DAVIE-', tkt.customernum)
		WHEN tkt.entity = 'NewBuryPort' 		THEN concat('NE-', tkt.customernum)
		WHEN tkt.entity = 'Cimarron North' 		THEN concat('CN-', tkt.customernum)
		WHEN tkt.entity = 'Amherst' 			THEN concat('AL-', tkt.customernum)
		WHEN tkt.entity = 'Westfield' 			THEN concat('DL-', tkt.customernum)
	END																		AS company_customer_number,
	ass.lastname										as associate_lastname,
	ass.firstname										as associate_firstname,
	case 
		when tkt.datedone = '' then null::date
		else tkt.datedone::date
	end 												as datedone, 
	tkt.ticketstatus,
	tkt.tickettype,
	eq.ptype											as equipment_ptype,
	eq.description 										as equipment_description,
	tc.ticket_no										as ticket_number,
	tkt.orderdate::date,
	tkt.estpresstime::float,
	tkt.estfootage::float,
	tkt.est_spoilfootage::float,
	tkt.est_setupfootage::float,
	tkt.actquantity::float,
	tkt.ticquantity::float,
	tkt.actfootage::float,
	SUM(tc.footused) OVER (PARTITION BY tc.ticket_no) 	as ticket_level_footage,
	count(tc.id) OVER (PARTITION BY tc.ticket_no) 		as ticket_level_count,
	tkt.press											AS ticket_press,
	pt.press_type										as ticket_press_type,
	tktit.sku_count,
	0::float											as totalworkorderfootagebyoperator,
	0::float											as totalwasteperoperator,
	0::float											as footused_tabco_good,*/
	0::float											as footused_tabco_ex_booklet,
	0::float											as footused_tabco_good_ex_booklet
/*	0::float											as dt_gross_footage,
	0::float											as waste_footage,
	0::float											as dt_gross_footage_order,
	0::float											as waste_footage_order*/
from (select *, case when entity = 'Cimarron North' and workoperation ~ '^\d' then right(workoperation, (length(workoperation) - 4)) else workoperation end as workoperation_clean 
		from dt_inovar_prod_edw.fact_timecard) tc
/*left join dt_inovar_prod_edw.dim_associate ass 
	on ass.number = tc.assocno and ass.entity = tc.entity
left join press p
	on trim(lower(p.press)) = trim(lower(tc.pressno)) and p.entity = tc.entity
left join dt_inovar_prod_edw.fact_ticket_header tkt 
	on tkt.number = tc.ticket_no and tkt.entity = tc.entity
left join dt_inovar_prod_edw.dim_customer cus 
	ON cus.number = tkt.customernum and cus.entity = tkt.entity
left join (SELECT entity, ticketnumber, count(distinct productnumber) as sku_count FROM dt_inovar_prod_edw.fact_ticket_item group by 1, 2) tktit
	on tktit.ticketnumber = tkt.number and tktit.entity = tkt.entity
left join press pt 
	on trim(lower(pt.press)) = trim(lower(tkt.press)) and pt.entity = tkt.entity
left join dt_inovar_prod_edw.dim_equipment eq 
	on trim(lower(eq.number)) = trim(lower(tc.pressno)) and eq.entity = tc.entity*/
where tc.sdate >= '2015-01-01'
	and tc.sdate <= current_date
	and tc.workoperation not in ('Punch In', 'Punch Out')
	and tc.pressno is not null and tc.pressno <> ''
)/*,
customer_master as (
select 
	CASE
		WHEN entity = 'Butler' 				THEN 'Milwaukee'
		WHEN entity = 'Dallas' 				THEN 'Dallas'
		WHEN entity = 'Davie' 				THEN 'Ft. Lauderdale'
		WHEN entity = 'NewBuryPort' 		THEN 'Newburyport'
		WHEN entity = 'Cimarron North' 		THEN 'Cimarron North'
		WHEN entity = 'Amherst' 			THEN 'Amherst Label'
		WHEN entity = 'Westfield' 			THEN 'Westfield'
	END																		AS businessunit,
	number, 
	CASE
		WHEN entity = 'Butler' 				THEN concat('FG-', number)
		WHEN entity = 'Dallas' 				THEN concat('DALLAS-', number)
		WHEN entity = 'Davie' 				THEN concat('DAVIE-', number)
		WHEN entity = 'NewBuryPort' 		THEN concat('NE-', number)
		WHEN entity = 'Cimarron North' 		THEN concat('CN-', number)
		WHEN entity = 'Amherst' 			THEN concat('AL-', number)
		WHEN entity = 'Westfield' 			THEN concat('DL-', number)
	END																		AS company_customer_number,
	company,
	otsname,
	sales_rep_no,
	itsname,
	cust_serv_no 
from dt_inovar_prod_edw.dim_customer
	union all
select
	'Kansas City' 															as businessunit,
	customer_number 														as number,
	concat('KSKA51-T-', customer_number) 									as company_customer_number, 
	customer_name 															as company,
	salesman 																as otsname,
	null::text 																as sales_rep_no,
	itsname,
	null::text 																as cust_serv_no
from (select customer_number, customer_name, case when trim(salesman) = '' then null else salesman end as salesman, 
			case when trim(customer_service_representative) = '' then null else customer_service_representative end as itsname, 
			RANK() OVER(PARTITION BY customer_number ORDER BY customer_name DESC) as rank
		from dt_inovar_prod_stg.in_kansascity_dim_customers) cus where rank = 1 and customer_number <> ''
	union all
select 
	'Oceanside' 															as businessunit,
	cus.customerno 															as number,
	concat('Carlsbad-', cus.customerno)										as company_customer_number,
	cus.customername 														as company,
	sal.salespersonname 													as otsname,
	cus.salespersonno 														as sales_rep_no,
	cus.customercarerep 													as itsname, 
	null::text 																as cust_serv_no
from dt_inovar_prod_stg.in_precision_dim_customers cus
left join dt_inovar_prod_stg.in_fact_sage_ar_salesperson sal 
	on sal.salespersonno = cus.salespersonno
)*/
select 
	/*location,
	ops.company,*/
	ops.businessunit,
	/*id,
	assocno,
	company_ticket_number,*/
	workoperation,
	sdate,
	/*edate,*/
--	stime,
	/*etime,*/
	sum(elapsed),	--imp
	/*closed,
	finishedpieces,*/
	pressno, 
	sum(footused),	--imp
	/*totalizer,
	offpress,
	packaged,
	ticket_pressequip,
	equipment_grouping,*/
	/*case 
		when trim(lower(press_type)) = 'flexo' then 'Flexo Press'
		when trim(lower(press_type)) = 'digital' then 'Digital Press'
		when trim(lower(press_type)) = 'hybrid' then 'Hybrid Press'
		when trim(lower(press_type)) = 'digital finishing' then 'Digital Finishing Equipment'
		when trim(lower(press_type)) = 'large format' then 'Large Format Press'
		when trim(lower(press_type)) = 'other' then 'Other'
		else initcap(trim(press_type))
	end						as press_type,
	trim(initcap(coalesce(cus.company, ops.customername))) 	as customername,
	ops.company_customer_number,
	associate_lastname,
	associate_firstname,
	datedone, 
	ticketstatus,
	tickettype,
	equipment_ptype,
	equipment_description,
	ticket_number,
	orderdate,
	coalesce(estpresstime,0)			as estpresstime,
	coalesce(estfootage,0)				as estfootage,
	coalesce(est_spoilfootage,0)		as est_spoilfootage,
	coalesce(est_setupfootage,0)		as est_setupfootage,
	coalesce(actquantity,0)				as actquantity,
	coalesce(ticquantity,0)				as ticquantity,
	coalesce(actfootage,0)				as actfootage,
	ticket_press,
	ticket_press_type,
	sku_count,
	totalworkorderfootagebyoperator,
	totalwasteperoperator,
	footused_tabco_good,*/
	sum(footused_tabco_ex_booklet),		--imp	
	sum(footused_tabco_good_ex_booklet)	--imp
	/*dt_gross_footage,
	waste_footage,
	dt_gross_footage_order,
	waste_footage_order*/
from final_ops ops
/*left join customer_master cus 
	on trim(lower(cus.company_customer_number)) = trim(lower(ops.company_customer_number))*/
	group by 1,2,3
-- ops view end