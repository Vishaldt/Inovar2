-------------------------------------------- prod to dev (07/23)

--drop view dt_inovar_prod_edw.v_stock_product_inventory

create or replace view dt_inovar_prod_edw.v_stock_product_inventory as 


drop view dt_inovar_dev_stg.v_stock_product_inventory_test


create or replace view dt_inovar_prod_edw.v_stock_product_inventory as 
with butler as (
	with tktit_pre as (
		select 
			trim(lower(tktit.productnumber))		as productnumber,
			tktit.ticketnumber						as ticket_number,
			tkt.ship_by_date,
			sum(tktit.orderquantity)				as orderquantity,
			sum(psit.shipquantity)					as shipquantity 
		from dt_inovar_prod_stg.in_butler_fact_ticketitem tktit
		join dt_inovar_prod_stg.in_butler_fact_ticket tkt
			on tkt.number::text = tktit.ticketnumber::text and tickettype in ('1', '4', '5') 
			and ticketstatus = 'Open'
		left join (select ticketitemid, sum(shipquantity) as shipquantity
						from dt_inovar_prod_stg.in_butler_fact_packslipitem 
							group by ticketitemid) psit
			on psit.ticketitemid::text = tktit.id::text 
		group by 1, 2, 3
		order by 1, 2, 3
	),
	tktit as (
		select 
			productnumber,
			string_agg(tktit_pre.ticket_number::text,', ') 					as company_ticket_number,
			string_agg(tktit_pre.ship_by_date::text,', ') 					as ship_by_date,
			sum(orderquantity)												as orderquantity,
			sum(shipquantity)												as shipquantity
		from tktit_pre
		group by 1
	),
	awu AS (
		select 
			trim(lower(psi.productnumber))									as productnumber, 
			sum(shipquantity) as shipquantity
		from dt_inovar_prod_stg.in_butler_fact_packslipitem psi
		join dt_inovar_prod_stg.in_butler_fact_packingslip ps
			on ps.number::text = psi.packslipnumber::text and ps.is_stockproduct ilike 'true'
		where (current_date - shipdate::date) <= 182  
		group by 1
	)
	select 
		'Milwaukee'                                        					as location,
    	'Flexo-Graphics LLC'                                				as company,
    	'Milwaukee'                                 						as businessunit,
        concat('FG-',sp.id) 												as stockproduct_id,
		sp.productno														as productno,
--		sp.prodclass,
		trim(sp.prodsubclass) 												as prodclass,
		sp.desc1,
		sp.available,
		sp.physicalinv,
		sp.minproduce,
		sp.maxproduce,
		sp.onorder,
		sp.backordered,
		coalesce(tktit.orderquantity,0)	- coalesce(tktit.shipquantity,0)	as QTY_in_Production,
		company_ticket_number,
		ship_by_date,
		(awu.shipquantity / 26) 											as avg_weekly_usage,
		concat('FG-', sp.customer_num) 										AS company_customer_number,
		sp.customername														as customername
	from dt_inovar_prod_stg.in_butler_dim_stockproduct sp
	left join tktit on trim(lower(tktit.productnumber)) = trim(lower(sp.productno)) 
	left join awu on trim(lower(awu.productnumber)) = trim(lower(sp.productno))
	where sp.prodclass = 'Customer'
),
davie as (
	with tktit_pre as (
		select 
			trim(lower(tktit.productnumber))		as productnumber,
			tktit.ticketnumber						as ticket_number,
			tkt.ship_by_date,
			sum(tktit.orderquantity)				as orderquantity,
			sum(psit.shipquantity)					as shipquantity 
		from dt_inovar_prod_stg.in_davie_fact_ticketitem tktit
		join dt_inovar_prod_stg.in_davie_fact_ticket tkt
			on tkt.number::text = tktit.ticketnumber::text and tickettype in ('1', '4', '5') 
			and ticketstatus = 'Open'
		left join (select ticketitemid, sum(shipquantity) as shipquantity
						from dt_inovar_prod_stg.in_davie_fact_packslipitem 
							group by ticketitemid) psit
			on psit.ticketitemid::text = tktit.id::text 
		group by 1, 2, 3
		order by 1, 2, 3
	),
	tktit as (
		select 
			productnumber,
			string_agg(tktit_pre.ticket_number::text,', ') 					as company_ticket_number,
			string_agg(tktit_pre.ship_by_date::text,', ') 					as ship_by_date,
			sum(orderquantity)												as orderquantity,
			sum(shipquantity)												as shipquantity
		from tktit_pre
		group by 1
	),
	awu AS (
		select 
			trim(lower(psi.productnumber))									as productnumber,
			sum(shipquantity) as shipquantity
		from dt_inovar_prod_stg.in_davie_fact_packslipitem psi
		join dt_inovar_prod_stg.in_davie_fact_packingslip ps
			on ps.number::text = psi.packslipnumber::text and ps.is_stockproduct ilike 'true'
		where (current_date - shipdate::date) <= 182  
		group by 1
	)
	select 
		'Ft. Lauderdale'                                					as location,
    	'Inovar Packaging Florida LLC'                  					as company,
    	'Ft. Lauderdale'                                					as businessunit,
        concat('DAVIE-',sp.id) 												as stockproduct_id,
        sp.productno														as productno,
		trim(sp.prodclass)													as prodclass,
		sp.desc1,
		sp.available,
		sp.physicalinv,
		sp.minproduce,
		sp.maxproduce,
		sp.onorder,
		sp.backordered,
		coalesce(tktit.orderquantity,0)	- coalesce(tktit.shipquantity,0)	as QTY_in_Production,
		company_ticket_number,
		ship_by_date,
		(awu.shipquantity / 26) 											as avg_weekly_usage,
		concat('DAVIE-', sp.customer_num) 									AS company_customer_number,
		sp.customername														as customername
	from dt_inovar_prod_stg.in_davie_dim_stockproduct sp
	left join tktit on trim(lower(tktit.productnumber)) = trim(lower(sp.productno)) 
	left join awu on trim(lower(awu.productnumber)) = trim(lower(sp.productno)) 
),
dallas as (
	with tktit_pre as (
		select 
			trim(lower(tktit.productnumber))		as productnumber,
			tktit.ticketnumber						as ticket_number,
			tkt.ship_by_date,
			sum(tktit.orderquantity)				as orderquantity,
			sum(psit.shipquantity)					as shipquantity 
		from dt_inovar_prod_stg.in_dallas_fact_ticketitem tktit
		join dt_inovar_prod_stg.in_dallas_fact_ticket tkt
			on tkt.number::text = tktit.ticketnumber::text and tickettype in ('1', '4', '5') 
			and ticketstatus = 'Open'
		left join (select ticketitemid, sum(shipquantity) as shipquantity
						from dt_inovar_prod_stg.in_dallas_fact_packslipitem 
							group by ticketitemid) psit
			on psit.ticketitemid::text = tktit.id::text 
		group by 1, 2, 3
		order by 1, 2, 3
	),
	tktit as (
		select 
			productnumber,
			string_agg(tktit_pre.ticket_number::text,', ') 					as company_ticket_number,
			string_agg(tktit_pre.ship_by_date::text,', ') 					as ship_by_date,
			sum(orderquantity)												as orderquantity,
			sum(shipquantity)												as shipquantity
		from tktit_pre
		group by 1
	),
	awu AS (
		select 
			trim(lower(psi.productnumber))									as productnumber,
			sum(shipquantity) as shipquantity
		from dt_inovar_prod_stg.in_dallas_fact_packslipitem psi
		join dt_inovar_prod_stg.in_dallas_fact_packingslip ps
			on ps.number::text = psi.packslipnumber::text and ps.is_stockproduct ilike 'true'
		where (current_date - shipdate::date) <= 182  
		group by 1
	)
	select 
		'Dallas'                                        					as location,
    	'Inovar Packaging Group LLC'                    					as company,
    	'Dallas'                                 							as businessunit,
        concat('DALLAS-',sp.id) 											as stockproduct_id,
		sp.productno														as productno,
		trim(sp.prodclass)													as prodclass,
		sp.desc1,
		sp.available,
		sp.physicalinv,
		sp.minproduce,
		sp.maxproduce,
		sp.onorder,
		sp.backordered,
		coalesce(tktit.orderquantity,0)	- coalesce(tktit.shipquantity,0)	as QTY_in_Production,
		company_ticket_number,
		ship_by_date,
		(awu.shipquantity / 26) 											as avg_weekly_usage,
		concat('DALLAS-', sp.customer_num) 									AS company_customer_number,
		sp.customername														as customername
	from dt_inovar_prod_stg.in_dallas_dim_stockproduct sp
	left join tktit on trim(lower(tktit.productnumber)) = trim(lower(sp.productno)) 
	left join awu on trim(lower(awu.productnumber)) = trim(lower(sp.productno))
),
newburyport as (
	with tktit_pre as (
		select 
			trim(lower(tktit.productnumber))		as productnumber,
			tktit.ticketnumber						as ticket_number,
			tkt.ship_by_date,
			sum(tktit.orderquantity)				as orderquantity,
			sum(psit.shipquantity)					as shipquantity 
		from dt_inovar_prod_stg.in_newburyport_fact_ticketitem tktit
		join dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
			on tkt.number::text = tktit.ticketnumber::text and tickettype in ('1', '4', '5') 
			and ticketstatus = 'Open'
		left join (select ticketitemid, sum(shipquantity) as shipquantity
						from dt_inovar_prod_stg.in_newburyport_fact_packslipitem 
							group by ticketitemid) psit
			on psit.ticketitemid::text = tktit.id::text 
		group by 1, 2, 3
		order by 1, 2, 3
	),
	tktit as (
		select 
			productnumber,
			string_agg(tktit_pre.ticket_number::text,', ') 					as company_ticket_number,
			string_agg(tktit_pre.ship_by_date::text,', ') 					as ship_by_date,
			sum(orderquantity)												as orderquantity,
			sum(shipquantity)												as shipquantity
		from tktit_pre
		group by 1
	),
	awu AS (
		select 
			trim(lower(psi.productnumber))									as productnumber,
			sum(shipquantity) as shipquantity
		from dt_inovar_prod_stg.in_newburyport_fact_packslipitem psi
		join dt_inovar_prod_stg.in_newburyport_fact_packingslip ps
			on ps.number::text = psi.packslipnumber::text and ps.is_stockproduct ilike 'true'
		where (current_date - shipdate::date) <= 182  
		group by 1
	)
	select 
		'Newbury Port'                                						as location,
    	'Label Print America Inc.'                  						as company,
    	'Newburyport'                                						as businessunit,
        concat('NE-',sp.id) 												as stockproduct_id,
		sp.productno														as productno,
		trim(sp.prodclass)													as prodclass,
		sp.desc1,
		sp.available,
		sp.physicalinv,
		sp.minproduce,
		sp.maxproduce,
		sp.onorder,
		sp.backordered,
		coalesce(tktit.orderquantity,0)	- coalesce(tktit.shipquantity,0)	as QTY_in_Production,
		company_ticket_number,
		ship_by_date,
		(awu.shipquantity / 26) 											as avg_weekly_usage,
		concat('NE-', sp.customer_num) 										AS company_customer_number,
		sp.customername														as customername
	from dt_inovar_prod_stg.in_newburyport_dim_stockproduct sp
	left join tktit on trim(lower(tktit.productnumber)) = trim(lower(sp.productno)) 
	left join awu on trim(lower(awu.productnumber)) = trim(lower(sp.productno)) 
),
cimarron_north as (
	with tktit_pre as (
		select 
			trim(lower(tktit.productnumber))		as productnumber,
			tktit.ticketnumber						as ticket_number,
			tkt.ship_by_date,
			sum(tktit.orderquantity)				as orderquantity,
			sum(psit.shipquantity)					as shipquantity 
		from dt_inovar_prod_stg.in_cimarron_fact_ticketitem tktit
		join dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
			on tkt.number::text = tktit.ticketnumber::text and tickettype in ('1', '4', '5') 
			and ticketstatus = 'Open'
		left join (select ticketitemid, sum(shipquantity) as shipquantity
						from dt_inovar_prod_stg.in_cimarron_fact_packslipitem 
							group by ticketitemid) psit
			on psit.ticketitemid::text = tktit.id::text 
		group by 1, 2, 3
		order by 1, 2, 3
	),
	tktit as (
		select 
			productnumber,
			string_agg(tktit_pre.ticket_number::text,', ') 					as company_ticket_number,
			string_agg(tktit_pre.ship_by_date::text,', ') 					as ship_by_date,
			sum(orderquantity)												as orderquantity,
			sum(shipquantity)												as shipquantity
		from tktit_pre
		group by 1
	),
	awu AS (
		select 
			trim(lower(psi.productnumber))									as productnumber,
			sum(shipquantity) as shipquantity
		from dt_inovar_prod_stg.in_cimarron_fact_packslipitem psi
		join dt_inovar_prod_stg.in_cimarron_fact_packingslip ps
			on ps.number::text = psi.packslipnumber::text and ps.is_stockproduct is true
		where (current_date - shipdate::date) <= 182  
		group by 1
	)
	select 
		'Cimarron North'                                						as location,
    	'Cimarron North'                  										as company,
    	'Cimarron North'                                						as businessunit,
        concat('CN-',sp.id) 												as stockproduct_id,
		sp.productno														as productno,
		trim(sp.prodclass)													as prodclass,
		sp.desc1,
		sp.available,
		sp.physicalinv,
		sp.minproduce,
		sp.maxproduce,
		sp.onorder,
		sp.backordered,
		coalesce(tktit.orderquantity,0)	- coalesce(tktit.shipquantity,0)	as QTY_in_Production,
		company_ticket_number,
		ship_by_date,
		(awu.shipquantity / 26) 											as avg_weekly_usage,
		concat('CN-', sp.customer_num) 										AS company_customer_number,
		sp.customername														as customername
	from dt_inovar_prod_stg.in_cimarron_dim_stockproduct sp
	left join tktit on trim(lower(tktit.productnumber)) = trim(lower(sp.productno)) 
	left join awu on trim(lower(awu.productnumber)) = trim(lower(sp.productno)) 
),
amherst as (
	with tktit_pre as (
		select 
			trim(lower(tktit.productnumber))		as productnumber,
			tktit.ticketnumber						as ticket_number,
			tkt.ship_by_date,
			sum(tktit.orderquantity)				as orderquantity,
			sum(psit.shipquantity)					as shipquantity 
		from dt_inovar_prod_stg.in_amherst_fact_ticketitem tktit
		join dt_inovar_prod_stg.in_amherst_fact_ticket tkt
			on tkt.number::text = tktit.ticketnumber::text and tickettype in ('1', '4', '5') 
			and ticketstatus = 'Open'
		left join (select ticketitemid, sum(shipquantity) as shipquantity
						from dt_inovar_prod_stg.in_amherst_fact_packslipitem 
							group by ticketitemid) psit
			on psit.ticketitemid::text = tktit.id::text 
		group by 1, 2, 3
		order by 1, 2, 3
	),
	tktit as (
		select 
			productnumber,
			string_agg(tktit_pre.ticket_number::text,', ') 					as company_ticket_number,
			string_agg(tktit_pre.ship_by_date::text,', ') 					as ship_by_date,
			sum(orderquantity)												as orderquantity,
			sum(shipquantity)												as shipquantity
		from tktit_pre
		group by 1
	),
	awu AS (
		select 
			trim(lower(psi.productnumber))									as productnumber,
			sum(shipquantity) as shipquantity
		from dt_inovar_prod_stg.in_amherst_fact_packslipitem psi
		join dt_inovar_prod_stg.in_amherst_fact_packingslip ps
			on ps.number::text = psi.packslipnumber::text and ps.is_stockproduct is true
		where (current_date - shipdate::date) <= 182  
		group by 1
	)
	select 
		'Amherst Label'                                						as location,
    	'Amherst Label'                  										as company,
    	'Amherst Label'                                						as businessunit,
        concat('AL-',sp.id) 												as stockproduct_id,
		sp.productno														as productno,
		trim(sp.prodclass)													as prodclass,
		sp.desc1,
		sp.available,
		sp.physicalinv,
		sp.minproduce,
		sp.maxproduce,
		sp.onorder,
		sp.backordered,
		coalesce(tktit.orderquantity,0)	- coalesce(tktit.shipquantity,0)	as QTY_in_Production,
		company_ticket_number,
		ship_by_date,
		(awu.shipquantity / 26) 											as avg_weekly_usage,
		concat('AL-', sp.customer_num) 										AS company_customer_number,
		sp.customername														as customername
	from dt_inovar_prod_stg.in_amherst_dim_stockproduct sp
	left join tktit on trim(lower(tktit.productnumber)) = trim(lower(sp.productno)) 
	left join awu on trim(lower(awu.productnumber)) = trim(lower(sp.productno)) 
),
dion_label as (
	with tktit_pre as (
		select 
			trim(lower(tktit.productnumber))		as productnumber,
			tktit.ticketnumber						as ticket_number,
			tkt.ship_by_date,
			sum(tktit.orderquantity)				as orderquantity,
			sum(psit.shipquantity)					as shipquantity 
		from dt_inovar_prod_stg.in_westfield_fact_ticketitem tktit
		join dt_inovar_prod_stg.in_westfield_fact_ticket tkt
			on tkt.number::text = tktit.ticketnumber::text and tickettype in ('1', '4', '5') 
			and ticketstatus = 'Open'
		left join (select ticketitemid, sum(shipquantity) as shipquantity
						from dt_inovar_prod_stg.in_westfield_fact_packslipitem 
							group by ticketitemid) psit
			on psit.ticketitemid::text = tktit.id::text 
		group by 1, 2, 3
		order by 1, 2, 3
	),
	tktit as (
		select 
			productnumber,
			string_agg(tktit_pre.ticket_number::text,', ') 					as company_ticket_number,
			string_agg(tktit_pre.ship_by_date::text,', ') 					as ship_by_date,
			sum(orderquantity)												as orderquantity,
			sum(shipquantity)												as shipquantity
		from tktit_pre
		group by 1
	),
	awu AS (
		select 
			trim(lower(psi.productnumber))									as productnumber,
			sum(shipquantity) as shipquantity
		from dt_inovar_prod_stg.in_westfield_fact_packslipitem psi
		join dt_inovar_prod_stg.in_westfield_fact_packingslip ps
			on ps.number::text = psi.packslipnumber::text and ps.is_stockproduct ilike 'true'
		where (current_date - shipdate::date) <= 182  
		group by 1
	)
	select 
		'Westfield' 														as location,
		'Dion Label Printing LLC' 											as company,
		'Westfield' 														as businessunit,
        concat('DL-',sp.id) 												as stockproduct_id,
		sp.productno														as productno,
--		sp.prodclass,
		trim(sp.customername) 												as prodclass,
		sp.desc1,
		sp.available,
		sp.physicalinv,
		sp.minproduce,
		sp.maxproduce,
		sp.onorder,
		sp.backordered,
		coalesce(tktit.orderquantity,0)	- coalesce(tktit.shipquantity,0)	as QTY_in_Production,
		company_ticket_number,
		ship_by_date,
		(awu.shipquantity / 26) 											as avg_weekly_usage,
		concat('DL-', sp.customer_num) 										AS company_customer_number,
		sp.customername														as customername
	from dt_inovar_prod_stg.in_westfield_dim_stockproduct sp
	left join tktit on trim(lower(tktit.productnumber)) = trim(lower(sp.productno)) 
	left join awu on trim(lower(awu.productnumber)) = trim(lower(sp.productno)) 
	where sp.customername is not null and sp.customername <> ''
),
siouxfalls_la as (
	with tktit_pre as (
		select 
			trim(lower(tktit.productnumber))		as productnumber,
			tktit.ticketnumber						as ticket_number,
			tkt.ship_by_date,
			sum(tktit.orderquantity)				as orderquantity,
			sum(psit.shipquantity)					as shipquantity 
		from dt_inovar_prod_stg.in_siouxfalls_la_fact_ticketitem tktit
		join dt_inovar_prod_stg.in_siouxfalls_la_fact_ticket tkt
			on tkt.number::text = tktit.ticketnumber::text and tickettype in ('1', '4', '5') 
			and ticketstatus = 'Open'
		left join (select ticketitemid, sum(shipquantity) as shipquantity
						from dt_inovar_prod_stg.in_siouxfalls_la_fact_packslipitem 
							group by ticketitemid) psit
			on psit.ticketitemid::text = tktit.id::text 
		group by 1, 2, 3
		order by 1, 2, 3
	),
	tktit as (
		select 
			productnumber,
			string_agg(tktit_pre.ticket_number::text,', ') 					as company_ticket_number,
			string_agg(tktit_pre.ship_by_date::text,', ') 					as ship_by_date,
			sum(orderquantity)												as orderquantity,
			sum(shipquantity)												as shipquantity
		from tktit_pre
		group by 1
	),
	awu AS (
		select 
			trim(lower(psi.productnumber))									as productnumber,
			sum(shipquantity) as shipquantity
		from dt_inovar_prod_stg.in_siouxfalls_la_fact_packslipitem psi
		join dt_inovar_prod_stg.in_siouxfalls_la_fact_packingslip ps
			on ps.number::text = psi.packslipnumber::text and ps.is_stockproduct is true
		where (current_date - shipdate::date) <= 182  
		group by 1
	)
	select 
		'Sioux Falls - LA'                                					as location,
    	'Sioux Falls - LA'                  								as company,
    	'Sioux Falls - LA'                                					as businessunit,
        concat('LA-',sp.id) 												as stockproduct_id,
		sp.productno														as productno,
		trim(sp.prodclass)													as prodclass,
		sp.desc1,
		sp.available,
		sp.physicalinv,
		sp.minproduce,
		sp.maxproduce,
		sp.onorder,
		sp.backordered,
		coalesce(tktit.orderquantity,0)	- coalesce(tktit.shipquantity,0)	as QTY_in_Production,
		company_ticket_number,
		ship_by_date,
		(awu.shipquantity / 26) 											as avg_weekly_usage,
		concat('LA-', sp.customer_num) 										AS company_customer_number,
		sp.customername														as customername
	from dt_inovar_prod_stg.in_siouxfalls_la_dim_stockproduct sp
	left join tktit on trim(lower(tktit.productnumber)) = trim(lower(sp.productno)) 
	left join awu on trim(lower(awu.productnumber)) = trim(lower(sp.productno)) 
),
final as (
select * from butler 
	union all 
select * from davie
	union all 
select * from dallas 
	union all 
select * from newburyport
	union all 
select * from cimarron_north
	union all 
select * from amherst
	union all 
select * from dion_label
	union all
select * from siouxfalls_la
)
select sp.*, end_market.industry
from final sp
left join dt_inovar_prod_stg.in_gs_customer_end_market end_market
	on trim(lower(end_market.company_customer_number)) = trim(lower(sp.company_customer_number)) and end_market.company_customer_number is not null
-----------------------------------------------------------------------------------------------------------------------------------