------------------------------------ Cimarron North included

--drop VIEW dt_inovar_prod_edw.v_inovar_ticket_sandbox

--CREATE OR REPLACE VIEW dt_inovar_prod_edw.v_inovar_ticket_sandbox AS 


WITH 
	tabco_raw as (
		select 
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
			end as DT_tktnumber,
			coalesce(cus.customer_name, tkt.customer_name_calc) as customer_name,
			tkt.*
		from dt_inovar_prod_stg.in_kansascity_fact_orders tkt
		left join dt_inovar_prod_stg.in_kansascity_dim_customers cus
			on cus.customer_number = tkt.customer_number
),
tabco_clean_1 as (
	select 
		rank() over(partition by DT_tktnumber order by house_number) rank_tkt,
		*
	from tabco_raw
),
tabco_clean_2 as (
	select 
		lag(customer_name, (rank_tkt-1)::integer) over (partition by dt_tktnumber order by house_number) 	as dt_customer_name,
		lag(customer_number, (rank_tkt-1)::integer) over (partition by dt_tktnumber order by house_number) 	as dt_customer_number,
		* 
	from tabco_clean_1
),
	tabco_final AS (
        SELECT
            tkt.DT_tktnumber 					as ticket_header,
--            replace(tkt.price_total_selling_all::text, 'nan'::text, '0'::text) AS total_revenue,
--            replace(tkt.price_selling_total::text, 'nan'::text, '0'::text) AS total_revenue_1,
--            replace(tkt.price_total_quoted_all::text, 'nan'::text, '0'::text) AS booking_revenue,
--            replace(tkt.ship_date_one::text, 'None'::text, '1900-01-01'::text) AS ship_date,
--            replace(tkt.price_selling::text, 'nan'::text, '0'::text) AS price_of_selling,
--            replace(tkt.quantity_ordered_total::text, 'nan'::text, '0'::text) AS qty_ordered,
            case 
	            when length(replace(trim(tkt.price_total_selling_all::text), 'nan'::text, '0'::text)) = 0 then '0'
				else replace(trim(tkt.price_total_selling_all::text), 'nan'::text, '0'::text)
			end as total_revenue,
			case 
	            when length(replace(trim(tkt.price_selling_total::text), 'nan'::text, '0'::text)) = 0 then '0'
				else replace(trim(tkt.price_selling_total::text), 'nan'::text, '0'::text)
			end as total_revenue_1,
			case 
	            when length(replace(trim(tkt.price_total_quoted_all::text), 'nan'::text, '0'::text)) = 0 then '0'
				else replace(trim(tkt.price_total_quoted_all::text), 'nan'::text, '0'::text)
			end as booking_revenue,
			case 
	            when length(replace(trim(tkt.ship_date_one::text), 'nan'::text, '0'::text)) = 0 then '0'
				else replace(trim(tkt.ship_date_one::text), 'nan'::text, '0'::text)
			end as ship_date,
			case 
	            when length(replace(trim(tkt.price_selling::text), 'nan'::text, '0'::text)) = 0 then '0'
				else replace(trim(tkt.price_selling::text), 'nan'::text, '0'::text)
			end as price_of_selling,
            case 
	            when length(replace(trim(tkt.quantity_ordered_total::text), 'nan'::text, '0'::text)) = 0 then '0'
				else replace(trim(tkt.quantity_ordered_total::text), 'nan'::text, '0'::text)
			end as qty_ordered,
            tkt.cyrel__,
            tkt.dt_customer_number 				as customer_number,
            case 
	            when length(replace(trim(tkt.date_order::text), 'None'::text, '1900-01-01')) = 0 then '1900-01-01'
				else replace(trim(tkt.date_order::text), 'None'::text, '1900-01-01')
			end as date_order,
--            tkt.date_order,
--            tkt.ship_date_one,
            tkt.special_instructions_,
            tkt.purchase_order_number,
--            tkt.date_promise,
--            case 
--	            when length(replace(trim(tkt.date_promise::text), 'None'::text, null)) = 0 then null
--				else replace(trim(tkt.date_promise::text), 'None'::text, null)
--			end as date_promise,
			case 
	            when length(replace(trim(date_promise::text), 'None'::text, '1900-01-01')) = 0 then null
				else replace(trim(date_promise::text), 'None'::text, '1900-01-01')
			end as date_promise,
            tkt.ship_via_one,
            tkt.house_number,
            tkt.quantity_ordered_total,
            tkt.price_total_selling_all,
            tkt.cyrel_description,
            tkt.special_instructions_lookup,
            tkt.price_selling,
            tkt.units_of_price,
            tkt.price_selling_total,
            tkt.date_shipped,
            tkt.part_number,
            tkt.salesman_name__,
            tkt.invoice_reference_number,
            tkt.lineal_inches_shipped,
            tkt.dt_customer_name				as customer_name,
            tkt.sum_roll_length_in_ft_one_calc,
            tkt.date_slit_list,
            tkt.salesman_name_new_to_enter,
            tkt.record_created,
            tkt.record_created_by,
            tkt.price_total_quoted_all,
            tkt.job_status_current,
            tkt.rescheduled,
            tkt.date_promise_year,
            tkt.which_press
           FROM tabco_clean_2 tkt
		where replace(date_order,'None','1900-01-01') >= '2020-01-01'
	), 
	all_tickets AS (
        SELECT 'Butler'::text AS location,
            'Flexographics'::text AS company,
            'Flexographics'::text AS businessunit,
            concat('FG-', tkt.number) AS company_ticket_number,
            concat('FG-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Item'::text AS line_item_type,
            tktit.id::text AS id,
            tktit.productnumber,
            tktit.description,
            tktit.orderquantity,
            tktit.machinecount,
            tktit.stockproductid,
            tktit.linetotal,
            tktit.pricem AS price_per_unit,
            tktit.pricemode,
            tktit.costm::double precision AS cost_per_unit,
            tkt.estpresstime::float,
            tkt.estrunhrs::float
--            ,
--            SUM(tktit.orderquantity) OVER (PARTITION BY tkt.number) 		as header_level_orderquantity,
--            count(tktit.id) OVER (PARTITION BY tktit.id) 					as item_level_count,
--            count(tktit.id) OVER (PARTITION BY tkt.number) 					as header_level_count
           FROM dt_inovar_prod_stg.in_butler_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_butler_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_butler_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'Dallas'::text AS location,
            'IPG Dallas'::text AS company,
            'SouthWest'::text AS businessunit,
            concat('DALLAS-', tkt.number) AS company_ticket_number,
            concat('DALLAS-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Item'::text AS line_item_type,
            tktit.id::text AS id,
            tktit.productnumber,
            tktit.description,
            tktit.orderquantity,
            tktit.machinecount,
            tktit.stockproductid,
            tktit.linetotal,
            tktit.pricem AS price_per_unit,
            tktit.pricemode,
            tktit.costm::double precision AS cost_per_unit,
            tkt.estpresstime::float,
            tkt.estrunhrs::float
--            ,
--            SUM(tktit.orderquantity) OVER (PARTITION BY tkt.number) 		as header_level_orderquantity,
--            count(tktit.id) OVER (PARTITION BY tktit.id) 					as item_level_count,
--            count(tktit.id) OVER (PARTITION BY tkt.number) 					as header_level_count
           FROM dt_inovar_prod_stg.in_dallas_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_dallas_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_dallas_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'Davie'::text AS location,
            'IPG Florida'::text AS company,
            'SouthEast'::text AS businessunit,
            concat('DAVIE-', tkt.number) AS company_ticket_number,
            concat('DAVIE-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Item'::text AS line_item_type,
            tktit.id::text AS id,
            tktit.productnumber,
            tktit.description,
            tktit.orderquantity,
            tktit.machinecount,
            tktit.stockproductid,
            tktit.linetotal,
            tktit.pricem AS price_per_unit,
            tktit.pricemode,
            tktit.costm::double precision AS cost_per_unit,
            tkt.estpresstime::float,
            tkt.estrunhrs::float
--            ,
--            SUM(tktit.orderquantity) OVER (PARTITION BY tkt.number) 		as header_level_orderquantity,
--            count(tktit.id) OVER (PARTITION BY tktit.id) 					as item_level_count,
--            count(tktit.id) OVER (PARTITION BY tkt.number) 					as header_level_count
           FROM dt_inovar_prod_stg.in_davie_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_davie_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_davie_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'NewBury Port'::text AS location,
            'IPG NE'::text AS company,
            'NewEngland'::text AS businessunit,
            concat('NE-', tkt.number) AS company_ticket_number,
            concat('NE-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Item'::text AS line_item_type,
            tktit.id::text AS id,
            tktit.productnumber,
            tktit.description,
            tktit.orderquantity,
            tktit.machinecount,
            tktit.stockproductid,
            tktit.linetotal,
            tktit.pricem AS price_per_unit,
            tktit.pricemode,
            tktit.costm::double precision AS cost_per_unit,
            tkt.estpresstime::float,
            tkt.estrunhrs::float
--            ,
--            SUM(tktit.orderquantity) OVER (PARTITION BY tkt.number) 		as header_level_orderquantity,
--            count(tktit.id) OVER (PARTITION BY tktit.id) 					as item_level_count,
--            count(tktit.id) OVER (PARTITION BY tkt.number) 					as header_level_count
           FROM dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
    UNION ALL
        SELECT 
        	'Cimarron North'::text 						AS location,
            'Cimarron North'::text 						AS company,
            'Cimarron North'::text 						AS businessunit,
            concat('CN-', tkt.number) AS company_ticket_number,
            concat('CN-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate::text,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype::text,
            tkt.datedone::text,
            tkt.ship_by_date::text,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype::text,
            'Ticket Item'::text AS line_item_type,
            tktit.id::text AS id,
            tktit.productnumber,
            tktit.description,
            tktit.orderquantity,
            tktit.machinecount,
            tktit.stockproductid,
            tktit.linetotal,
            tktit.pricem AS price_per_unit,
            tktit.pricemode,
            tktit.costm::double precision AS cost_per_unit,
            tkt.estpresstime::float,
            tkt.estrunhrs::float
--            ,
--            SUM(tktit.orderquantity) OVER (PARTITION BY tkt.number) 		as header_level_orderquantity,
--            count(tktit.id) OVER (PARTITION BY tktit.id) 					as item_level_count,
--            count(tktit.id) OVER (PARTITION BY tkt.number) 					as header_level_count
           FROM dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'Westfield'::text AS location,
            'Dion Label'::text AS company,
            'Dion Label'::text AS businessunit,
            concat('DL-', tkt.number) AS company_ticket_number,
            concat('DL-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Item'::text AS line_item_type,
            tktit.id::text AS id,
            tktit.productnumber,
            tktit.description,
            tktit.orderquantity,
            tktit.machinecount,
            tktit.stockproductid,
            tktit.linetotal,
            tktit.pricem AS price_per_unit,
            tktit.pricemode,
            tktit.costm::double precision AS cost_per_unit,
            tkt.estpresstime::float,
            tkt.estrunhrs::float
--            ,
--            SUM(tktit.orderquantity) OVER (PARTITION BY tkt.number) 		as header_level_orderquantity,
--            count(tktit.id) OVER (PARTITION BY tktit.id) 					as item_level_count,
--            count(tktit.id) OVER (PARTITION BY tkt.number) 					as header_level_count
           FROM dt_inovar_prod_stg.in_westfield_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_westfield_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_westfield_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        ), ticket_other_rev AS (
         SELECT 'Butler'::text AS location,
            'Flexographics'::text AS company,
            'Flexographics'::text AS businessunit,
            concat('FG-', tkt.number) AS company_ticket_number,
            concat('FG-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'PO Total'::text AS productnumber,
            'PO Total'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.pototal::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_butler_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_butler_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'Butler'::text AS location,
            'Flexographics'::text AS company,
            'Flexographics'::text AS businessunit,
            concat('FG-', tkt.number) AS company_ticket_number,
            concat('FG-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'Misc Charge'::text AS productnumber,
            'Misc Charge'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.misccharge::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_butler_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_butler_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'Dallas'::text AS location,
            'IPG Dallas'::text AS company,
            'SouthWest'::text AS businessunit,
            concat('DALLAS-', tkt.number) AS company_ticket_number,
            concat('DALLAS-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'PO Total'::text AS productnumber,
            'PO Total'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.pototal::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_dallas_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_dallas_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'Dallas'::text AS location,
            'IPG Dallas'::text AS company,
            'SouthWest'::text AS businessunit,
            concat('DALLAS-', tkt.number) AS company_ticket_number,
            concat('DALLAS-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'Misc Charge'::text AS productnumber,
            'Misc Charge'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.misccharge::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_dallas_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_dallas_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'Davie'::text AS location,
            'IPG Florida'::text AS company,
            'SouthEast'::text AS businessunit,
            concat('DAVIE-', tkt.number) AS company_ticket_number,
            concat('DAVIE-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'PO Total'::text AS productnumber,
            'PO Total'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.pototal::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_davie_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_davie_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'Davie'::text AS location,
            'IPG Florida'::text AS company,
            'SouthEast'::text AS businessunit,
            concat('DAVIE-', tkt.number) AS company_ticket_number,
            concat('DAVIE-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'Misc Charge'::text AS productnumber,
            'Misc Charge'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.misccharge::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_davie_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_davie_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'NewBury Port'::text AS location,
            'IPG NE'::text AS company,
            'NewEngland'::text AS businessunit,
            concat('NE-', tkt.number) AS company_ticket_number,
            concat('NE-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'PO Total'::text AS productnumber,
            'PO Total'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.pototal::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'NewBury Port'::text AS location,
            'IPG NE'::text AS company,
            'NewEngland'::text AS businessunit,
            concat('NE-', tkt.number) AS company_ticket_number,
            concat('NE-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'Misc Charge'::text AS productnumber,
            'Misc Charge'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.misccharge::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
    UNION ALL
    	SELECT 
         	'Cimarron North'::text AS location,
            'Cimarron North'::text AS company,
            'Cimarron North'::text AS businessunit,
            concat('CN-', tkt.number) AS company_ticket_number,
            concat('CN-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate::text,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype::text,
            tkt.datedone::text,
            tkt.ship_by_date::text,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype::text,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'PO Total'::text AS productnumber,
            'PO Total'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.pototal::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
    UNION ALL
        SELECT 
        	'Cimarron North'::text AS location,
            'Cimarron North'::text AS company,
            'Cimarron North'::text AS businessunit,
            concat('CN-', tkt.number) AS company_ticket_number,
            concat('CN-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate::text,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype::text,
            tkt.datedone::text,
            tkt.ship_by_date::text,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype::text,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'Misc Charge'::text AS productnumber,
            'Misc Charge'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.misccharge::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'Westfield'::text AS location,
            'Dion Label'::text AS company,
            'Dion Label'::text AS businessunit,
            concat('DL-', tkt.number) AS company_ticket_number,
            concat('DL-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'PO Total'::text AS productnumber,
            'PO Total'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.pototal::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_westfield_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_westfield_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 'Westfield'::text AS location,
            'Dion Label'::text AS company,
            'Dion Label'::text AS businessunit,
            concat('DL-', tkt.number) AS company_ticket_number,
            concat('DL-', tkt.customernum) AS company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            'USD'::text AS currency,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press,
                CASE
                    WHEN tkt.pressdone::text = 'True'::text AND tkt.finishdone::text = 'False'::text AND tkt.ticketstatus::text <> 'Done'::text THEN 'True'::text
                    ELSE 'False'::text
                END AS wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            'Ticket Header'::text AS line_item_type,
            NULL::text AS id,
            'Misc Charge'::text AS productnumber,
            'Misc Charge'::text AS product_description,
            0 AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tkt.misccharge::double precision AS linetotal,
            0 AS price_per_unit,
            NULL::text AS pricemode,
            0::double precision AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_westfield_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_westfield_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 
         	'Kansas City'::text 														AS location,
            'Tabco'::text 																AS company,
            'Tabco'::text 																AS businessunit,
            concat('KSKA51-T-', tkt_cte.ticket_header) 									AS company_ticket_number,
            concat('KSKA51-T-', tkt_cte.customer_number) 								AS company_customer_number,
            replace(tkt_cte.ticket_header::text, 'None'::text, '999999'::text) 			AS number,
            tkt_cte.date_order::text 													AS order_date,
            'USA'::text 																AS billcountry,
            COALESCE(cus.zip, 'None'::character varying) 								AS billzip,
            NULL::text 																	AS shipzip,
            'USD'::text 																AS currency,
            tkt_cte.customer_name 														AS customername,
            tkt_cte.customer_number 													AS customernum,
            tkt_cte.salesman_name__ 													AS otsname,
            case
				when tkt_cte.invoice_reference_number ilike 'None' then null::text
				else 'Done'
			end 																		as ticketstatus,
            NULL::text 																	AS stocktickettype,
            tkt_cte.ship_date::text 													AS datedone,
            date_promise 																AS ship_by_date,		----------------------------------
            NULL::text 																	AS custponum,
            tkt_cte.ship_via_one 														AS shipvia,
            tkt_cte.cyrel_description 													AS generaldescr,
            NULL::text 																	AS priority,
            CASE
                WHEN tkt_cte.job_status_current::text = 'Shipping Complete'::text THEN 'Shipment Complete'::text
                WHEN tkt_cte.job_status_current::text = 'Shipped Partial'::text THEN 'Partial Shipment'::text
                ELSE 'Not Shipped'::text
            END 																		AS shippingstatus,
            tkt_cte.which_press 														AS press,
            NULL::text 																	AS wip_in_finishing,
            concat(tkt_cte.special_instructions_, tkt_cte.special_instructions_lookup) 	AS notes,
            'Tabco'::text 																AS shipaddr1,
            'Tabco'::text 																AS shiplocation,
            NULL::text 																	AS tickettype,
            CASE
                WHEN length(tkt_cte.house_number::text) <> length(tkt_cte.ticket_header::text) THEN 'Ticket Item'::text
                ELSE 'Ticket Header'::text
            END 																		AS line_item_type,
            tkt_cte.house_number::text 													AS id,
            replace(tkt_cte.part_number::text, 'None'::text, 'xxxxxx'::text) 			AS productnumber,
            NULL::text 																	AS product_description,
            tkt_cte.qty_ordered::numeric::integer 										AS orderquantity,
            0 																			AS machinecount,
            NULL::text 																	AS stockproductid,
            tkt_cte.booking_revenue::double precision 									AS linetotal,
            tkt_cte.price_of_selling::double precision 									AS price_per_unit,
            tkt_cte.units_of_price 														AS pricemode,
            0::double precision 														AS cost_per_unit,
            0::float																	as estpresstime,
            0::float																	as estrunhrs
           FROM tabco_final tkt_cte
             LEFT JOIN dt_inovar_prod_stg.in_kansascity_dim_customers cus ON cus.customer_number::text = tkt_cte.customer_number::text
        UNION ALL
         SELECT 'Carlsbad'::text AS location,
            'Precision Label'::text AS company,
            'Precision Label'::text AS businessunit,
            concat('Carlsbad-', tkt.salesorderno) AS company_ticket_number,
            concat('Carlsbad-', tkt.customerno) AS company_customer_number,
            tkt.salesorderno AS number,
            tkt.orderdate::date::text AS orderdate,
            'USA'::text AS billcountry,
            NULL::text AS billzip,
            NULL::text AS shipzip,
            'USD'::text AS currency,
            tkt.billtoname::text AS customername,
            tkt.customerno::text AS customernum,
            spn.salespersonname AS otsname,
--            tkt.orderstatus AS ticketstatus,
            case
				when tkt.orderstatus in ('C','A') then 'Done'
				else null::text
			end as ticketstatus,
            NULL::text AS stocktickettype,
            '1900-01-01'::text AS datedone,
--            '1900-01-01'::text AS ship_by_date,
            tkt.shipexpiredate::text AS ship_by_date,
            NULL::text AS custponum,
            NULL::text AS shipvia,
            NULL::text AS generaldescr,
            NULL::text AS priority,
            NULL::text AS shippingstatus,
            NULL::text AS press,
            NULL::text AS wip_in_finishing,
            NULL::text AS notes,
            'Precision Label'::text AS shipaddr1,
            'Precision Label'::text AS shiplocation,
            NULL::text AS tickettype,
            tktit.itemtype AS line_item_type,
            tktit.id::text AS id,
            tktit.itemno AS productnumber,
            tktit.itemdescription AS productdescription,
            tktit.quantityorderedoriginal AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tktit.lastextensionamt AS linetotal,
            tktit.lastunitprice AS price_per_unit,
            tktit.unitofmeasure AS pricemode,
            tktit.unitcost AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader tkt
             LEFT JOIN ( SELECT in_fact_sage_so_salesorderhistorydetail.id,
                    in_fact_sage_so_salesorderhistorydetail.salesorderno,
                    in_fact_sage_so_salesorderhistorydetail.sequenceno,
                    in_fact_sage_so_salesorderhistorydetail.linekey,
                    in_fact_sage_so_salesorderhistorydetail.itemno,
                    in_fact_sage_so_salesorderhistorydetail.itemtype,
                    in_fact_sage_so_salesorderhistorydetail.itemdescription,
                    in_fact_sage_so_salesorderhistorydetail.aliasitemno,
                    in_fact_sage_so_salesorderhistorydetail.promisedate,
                    in_fact_sage_so_salesorderhistorydetail.originalline,
                    in_fact_sage_so_salesorderhistorydetail.cancelledline,
                    in_fact_sage_so_salesorderhistorydetail.cancelreasoncode,
                    in_fact_sage_so_salesorderhistorydetail.purchaseorderno,
                    in_fact_sage_so_salesorderhistorydetail.pricelevel,
                    in_fact_sage_so_salesorderhistorydetail.quantityorderedoriginal,
                    in_fact_sage_so_salesorderhistorydetail.quantityorderedrevised,
                    in_fact_sage_so_salesorderhistorydetail.quantityshipped,
                    in_fact_sage_so_salesorderhistorydetail.quantitybackordered,
                    in_fact_sage_so_salesorderhistorydetail.originalunitprice,
                    in_fact_sage_so_salesorderhistorydetail.lastunitprice,
                    in_fact_sage_so_salesorderhistorydetail.lastextensionamt,
                    in_fact_sage_so_salesorderhistorydetail.unitcost,
                    in_fact_sage_so_salesorderhistorydetail.unitofmeasure,
                    in_fact_sage_so_salesorderhistorydetail.deleteddate,
                    in_fact_sage_so_salesorderhistorydetail.lastsyncdatetime
                   FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistorydetail
                  WHERE in_fact_sage_so_salesorderhistorydetail.cancelledline::text = 'N'::text AND in_fact_sage_so_salesorderhistorydetail.deleteddate IS NULL) tktit 
                  		ON tkt.salesorderno::text = tktit.salesorderno::text
             LEFT JOIN dt_inovar_prod_stg.in_fact_sage_ar_salesperson spn ON spn.salespersonno::text = tkt.salespersonno::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date AND tkt.orderstatus::text <> 'X'::text
        UNION ALL
         SELECT 'Carlsbad'::text AS location,
            'Precision Label'::text AS company,
            'Precision Label'::text AS businessunit,
            concat('Carlsbad-', tkt.salesorderno) AS company_ticket_number,
            concat('Carlsbad-', tkt.customerno) AS company_customer_number,
            tkt.salesorderno AS number,
            tkt.orderdate::text AS orderdate,
            COALESCE(tkt.billtocountrycode, 'USA'::character varying) AS billcountry,
            COALESCE(tkt.billtozipcode, '99999'::character varying) AS billzip,
            COALESCE(tkt.shiptozipcode, '99999'::character varying) AS shipzip,
            'USD'::text AS currency,
            tkt.billtoname AS customername,
            tkt.customerno AS customernum,
            spn.salespersonname AS otsname,
--            tkt.orderstatus AS ticketstatus,
            case
				when tkt.orderstatus in ('C','A') then 'Done'
				else null::text
			end as ticketstatus,
            NULL::text AS stocktickettype,
            '1900-01-01'::text AS datedone,
            tkt.shipexpiredate::text AS ship_by_date,
            tkt.customerpono AS custponum,
            tkt.shipvia,
            NULL::text AS generaldescr,
            NULL::text AS priority,
            NULL::text AS shippingstatus,
            NULL::text AS press,
            NULL::text AS wip_in_finishing,
            tkt.comment::text AS notes,
            tkt.shiptoaddress1 AS shipaddr1,
            tkt.shiptoname AS shiplocation,
            tkt.ordertype AS tickettype,
            'Ticket Item'::text AS line_item_type,
            tktit.linekey AS id,
            tktit.itemcode AS productnumber,
            tktit.itemcodedesc AS product_description,
            tktit.quantityordered AS orderquantity,
            0 AS machinecount,
            NULL::text AS stockproductid,
            tktit.extensionamt AS linetotal,
            tktit.unitprice AS price_per_unit,
            'Each'::text AS pricemode,
            tktit.unitcost AS cost_per_unit,
            0::float			as estpresstime,
            0::float			as estrunhrs
           FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderheader tkt
             LEFT JOIN dt_inovar_prod_stg.in_fact_sage_so_salesorderdetail tktit ON tkt.salesorderno::text = tktit.salesorderno::text
             LEFT JOIN dt_inovar_prod_stg.in_fact_sage_ar_salesperson spn ON spn.salespersonno::text = tkt.salespersonno::text
          WHERE tkt.orderdate >= '2020-01-01'::date AND NOT (tkt.salesorderno::text IN ( SELECT DISTINCT in_fact_sage_so_salesorderhistoryheader.salesorderno
                   FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader))
        ),
    all_tickets_clean as (
    	select
    		*,
    		SUM(orderquantity) OVER (PARTITION BY company_ticket_number) 	as header_level_orderquantity,
            count(*) OVER (PARTITION BY company_ticket_number, id) 			as item_level_count,
            count(*) OVER (PARTITION BY company_ticket_number) 				as header_level_count
    	from all_tickets 
    	),    
    final_data AS (
         SELECT all_tickets.location,
            all_tickets.company,
            all_tickets.businessunit,
            all_tickets.company_ticket_number,
            all_tickets.company_customer_number,
            all_tickets.number,
            all_tickets.orderdate,
            all_tickets.billcountry,
            all_tickets.billzip,
            all_tickets.shipzip,
            all_tickets.currency,
            all_tickets.customername,
            all_tickets.customernum,
            all_tickets.otsname,
            all_tickets.ticketstatus,
            all_tickets.stocktickettype,
            all_tickets.datedone,
            all_tickets.ship_by_date,
            all_tickets.custponum,
            all_tickets.shipvia,
            all_tickets.generaldescr,
            all_tickets.priority,
            all_tickets.shippingstatus,
            all_tickets.press,
            all_tickets.wip_in_finishing,
            all_tickets.notes,
            all_tickets.shipaddr1,
            all_tickets.shiplocation,
            all_tickets.tickettype,
            all_tickets.line_item_type,
            all_tickets.id,
            all_tickets.productnumber,
            all_tickets.description,
            all_tickets.orderquantity,
            all_tickets.machinecount,
            all_tickets.stockproductid,
            all_tickets.linetotal,
            all_tickets.price_per_unit,
            all_tickets.pricemode,
            all_tickets.cost_per_unit,
            case
            	when header_level_orderquantity > 0 then ((all_tickets.orderquantity * all_tickets.estpresstime) / all_tickets.header_level_orderquantity)
            	when header_level_count > 0 then ((all_tickets.item_level_count * all_tickets.estpresstime) / all_tickets.header_level_count)
            	else 0
            end																												as estpresstime,
            case
            	when header_level_orderquantity > 0 then ((all_tickets.orderquantity * all_tickets.estrunhrs) / all_tickets.header_level_orderquantity)
            	when header_level_count > 0 then ((all_tickets.item_level_count * all_tickets.estrunhrs) / all_tickets.header_level_count)
            	else 0
            end																												as estrunhrs
           FROM all_tickets_clean as all_tickets
          WHERE 
--          (all_tickets.shipaddr1::text <> ALL (ARRAY['Flexo-Graphics Inventory'::character varying, 'INVENTORY'::character varying, 'Inventory'::character varying]::text[])) 
--          	AND (all_tickets.shiplocation::text <> ALL (ARRAY['Flexo-Graphics Inventory'::character varying, 'Inventory'::character varying, 'inventory'::character varying, 
--          			'RELEASE/INVENTORY'::character varying, 'Inventory/release'::character varying, 'Inventory/Release'::character varying, 'INVENTORY/RELEASE'::character varying, 
--          			'Inventory  - Ship to Stock'::character varying, 'Flexo Inventory'::character varying, 'receive to shelf'::character varying, 'Receive to shelf'::character varying, 
--          			'Receive to Shelf'::character varying, '***Receive To Shelf***'::character varying, 'RECEIVE TO SHELF'::character varying, 'SHIP TO SHELF'::character varying, 
--          			'RECV TO SHELF '::character varying, 'Ship to Shelf'::character varying, 'Receive to Shelf'::character varying, 'RECV TO SHELF'::character varying]::text[])) 
--          			AND 
          			all_tickets.orderdate::date <= CURRENT_DATE
        UNION ALL
         SELECT ticket_other_rev.location,
            ticket_other_rev.company,
            ticket_other_rev.businessunit,
            ticket_other_rev.company_ticket_number,
            ticket_other_rev.company_customer_number,
            ticket_other_rev.number,
            ticket_other_rev.orderdate,
            ticket_other_rev.billcountry,
            ticket_other_rev.billzip,
            ticket_other_rev.shipzip,
            ticket_other_rev.currency,
            ticket_other_rev.customername,
            ticket_other_rev.customernum,
            ticket_other_rev.otsname,
            ticket_other_rev.ticketstatus,
            ticket_other_rev.stocktickettype,
            ticket_other_rev.datedone,
            ticket_other_rev.ship_by_date,
            ticket_other_rev.custponum,
            ticket_other_rev.shipvia,
            ticket_other_rev.generaldescr,
            ticket_other_rev.priority,
            ticket_other_rev.shippingstatus,
            ticket_other_rev.press,
            ticket_other_rev.wip_in_finishing,
            ticket_other_rev.notes,
            ticket_other_rev.shipaddr1,
            ticket_other_rev.shiplocation,
            ticket_other_rev.tickettype,
            ticket_other_rev.line_item_type,
            ticket_other_rev.id,
            ticket_other_rev.productnumber,
            ticket_other_rev.product_description,
            ticket_other_rev.orderquantity,
            ticket_other_rev.machinecount,
            ticket_other_rev.stockproductid,
            ticket_other_rev.linetotal,
            ticket_other_rev.price_per_unit,
            ticket_other_rev.pricemode,
            ticket_other_rev.cost_per_unit,
            ticket_other_rev.estpresstime,
            ticket_other_rev.estrunhrs
           FROM ticket_other_rev
          WHERE 
--          (ticket_other_rev.shipaddr1::text <> ALL (ARRAY['Flexo-Graphics Inventory'::character varying, 'INVENTORY'::character varying, 'Inventory'::character varying]::text[])) 
--          		AND (ticket_other_rev.shiplocation::text <> ALL (ARRAY['Flexo-Graphics Inventory'::character varying, 'Inventory'::character varying, 'inventory'::character varying, 
--          			'RELEASE/INVENTORY'::character varying, 'Inventory/release'::character varying, 'Inventory/Release'::character varying, 'INVENTORY/RELEASE'::character varying, 
--          			'Inventory  - Ship to Stock'::character varying, 'Flexo Inventory'::character varying, 'receive to shelf'::character varying, 'Receive to shelf'::character varying, 
--          			'Receive to Shelf'::character varying, '***Receive To Shelf***'::character varying, 'RECEIVE TO SHELF'::character varying, 'SHIP TO SHELF'::character varying, 
--          			'RECV TO SHELF '::character varying, 'Ship to Shelf'::character varying, 'Receive to Shelf'::character varying, 'RECV TO SHELF'::character varying]::text[])) 
--          		AND 
          		ticket_other_rev.orderdate::date <= CURRENT_DATE
        ), 
    ticket_sandbox AS (
        SELECT COALESCE(map_loc.mapped_value, tkt.location::character varying) AS location,
            COALESCE(map_comp.mapped_value, tkt.company::character varying) AS legal_entity,
            COALESCE(map_bu.mapped_value, tkt.businessunit::character varying) AS businessunit,
            tkt.company_ticket_number,
            tkt.company_customer_number,
            tkt.number,
            tkt.orderdate,
            tkt.billcountry,
            tkt.billzip,
            tkt.shipzip,
            tkt.currency,
            tkt.customername::character varying(255) AS customername,
            tkt.customernum::character varying(255) AS customernum,
            tkt.otsname,
            tkt.ticketstatus,
            tkt.stocktickettype,
            tkt.datedone,
            tkt.ship_by_date,
            tkt.custponum,
            tkt.shipvia::character varying(255) AS shipvia,
            tkt.generaldescr,
            tkt.priority,
            tkt.shippingstatus,
            tkt.press::character varying(255) AS press,
            tkt.wip_in_finishing,
            tkt.notes,
            tkt.shipaddr1,
            tkt.shiplocation,
            tkt.tickettype,
            tkt.line_item_type,
            tkt.id,
            tkt.productnumber,
            tkt.description,
            tkt.orderquantity::integer AS orderquantity,
            tkt.machinecount,
            tkt.stockproductid,
            tkt.linetotal,
            tkt.price_per_unit,
            tkt.pricemode,
            tkt.cost_per_unit,
            tkt.estpresstime,
            tkt.estrunhrs
           	FROM final_data tkt
             	LEFT JOIN ( SELECT in_gs_company_mapping.record_type,
                    in_gs_company_mapping.original_value,
                    in_gs_company_mapping.mapped_value
                   FROM dt_inovar_prod_stg.in_gs_company_mapping
                  WHERE in_gs_company_mapping.record_type::text = 'BU'::text) map_bu ON tkt.businessunit = map_bu.original_value::text
            	LEFT JOIN ( SELECT in_gs_company_mapping.record_type,
                    in_gs_company_mapping.original_value,
                    in_gs_company_mapping.mapped_value
                   FROM dt_inovar_prod_stg.in_gs_company_mapping
                  WHERE in_gs_company_mapping.record_type::text = 'Company'::text) map_comp ON tkt.company = map_comp.original_value::text
             	LEFT JOIN ( SELECT in_gs_company_mapping.record_type,
                    in_gs_company_mapping.original_value,
                    in_gs_company_mapping.mapped_value
                   FROM dt_inovar_prod_stg.in_gs_company_mapping
                  WHERE in_gs_company_mapping.record_type::text = 'Location'::text) map_loc ON tkt.location = map_loc.original_value::text
        )        
SELECT 
	ticket_sandbox.location,
    ticket_sandbox.legal_entity,
    ticket_sandbox.businessunit, --
    ticket_sandbox.company_ticket_number,
    ticket_sandbox.company_customer_number,
    ticket_sandbox.number,
--    ticket_sandbox.orderdate,
    case 
		when trim(to_char(ticket_sandbox.orderdate::date, 'Day')) = 'Saturday' then (ticket_sandbox.orderdate::date - '1 day'::interval)::date::text
    	when trim(to_char(ticket_sandbox.orderdate::date, 'Day')) = 'Sunday' then (ticket_sandbox.orderdate::date - '2 day'::interval)::date::text
        else ticket_sandbox.orderdate
    end as orderdate,
    ticket_sandbox.billcountry,
    ticket_sandbox.billzip,
    ticket_sandbox.shipzip,
    ticket_sandbox.currency,
    ticket_sandbox.customername,
    ticket_sandbox.customernum,
    ticket_sandbox.otsname,
    ticket_sandbox.ticketstatus,
    ticket_sandbox.stocktickettype,
    ticket_sandbox.datedone,
    ticket_sandbox.ship_by_date,
    ticket_sandbox.custponum,
    ticket_sandbox.shipvia,
    ticket_sandbox.generaldescr,
    ticket_sandbox.priority,
    ticket_sandbox.shippingstatus,
    ticket_sandbox.press,
    ticket_sandbox.wip_in_finishing,
    ticket_sandbox.notes,
    ticket_sandbox.shipaddr1,
    ticket_sandbox.shiplocation,
    ticket_sandbox.tickettype,
    ticket_sandbox.line_item_type,
    ticket_sandbox.id,
    ticket_sandbox.productnumber,
    ticket_sandbox.description,
    ticket_sandbox.orderquantity,
    ticket_sandbox.machinecount,
    ticket_sandbox.stockproductid,
    ticket_sandbox.linetotal,
    ticket_sandbox.price_per_unit,
    ticket_sandbox.pricemode,
    ticket_sandbox.cost_per_unit,
    ticket_sandbox.estpresstime,
    ticket_sandbox.estrunhrs,
    case
    	when ((businessunit in ('Ft. Lauderdale', 'Dallas', 'Newburyport', 'Milwaukee', 'Westfield', 'Cimarron North') and tickettype = '0')
    			or (businessunit in ('Kansas City') and ticket_sandbox.customername ILIKE '%Tabco, Inc.%')) 
    		then '0'
    	else '1'
    end															as stock_ticket_flag,
    ticket_sandbox.orderdate									as original_orderdate
FROM ticket_sandbox
--where (businessunit in ('Ft. Lauderdale', 'Dallas', 'Newburyport', 'Milwaukee', 'Westfield', 'Cimarron North') and tickettype <> '0')
--    			or (businessunit in ('Kansas City') and ticket_sandbox.customername not ILIKE '%Tabco, Inc.%')
--    			or (businessunit in ('Oceanside'))
--where (
--	((ticket_sandbox.businessunit = ANY (ARRAY['Ft. Lauderdale'::text, 'Dallas'::text, 'Newburyport'::text])) AND ticket_sandbox.shiplocation::text !~~* '%shelf%'::text 
--		AND ticket_sandbox.shiplocation::text !~~* '%overage%'::text AND ticket_sandbox.shiplocation::text !~~* '%stock%'::text
--		AND (ticket_sandbox.tickettype::text <> ALL (ARRAY['0'::character varying, '2'::character varying, '3'::character varying]::text[])))
--	or (ticket_sandbox.businessunit = 'Milwaukee'::text AND ticket_sandbox.shipaddr1::text <> ''::text AND ticket_sandbox.shipaddr1 IS NOT NULL 
--		AND ticket_sandbox.shipaddr1::text !~~* '%flexo-graphics%'::text AND ticket_sandbox.shipaddr1::text !~~* '%inventory%'::text 
--		AND (ticket_sandbox.tickettype::text <> ALL (ARRAY['0'::character varying, '2'::character varying, '3'::character varying]::text[])))
--	or (ticket_sandbox.businessunit = 'Westfield'::text AND ticket_sandbox.shiplocation::text <> ''::text AND ticket_sandbox.shiplocation IS NOT NULL 
--		AND ticket_sandbox.shiplocation::text !~~* '%inventory%'::text AND (ticket_sandbox.tickettype::text <> 
--		ALL (ARRAY['0'::character varying, '2'::character varying, '3'::character varying]::text[])))
--	
--	((ticket_sandbox.businessunit = ANY (ARRAY['Ft. Lauderdale'::text, 'Dallas'::text, 'Newburyport'::text, 'Milwaukee'::text, 'Westfield'::text])) 
--		and ticket_sandbox.tickettype::text <> '0'::text)
--
--	or 
--	((ticket_sandbox.businessunit = ANY (ARRAY['Ft. Lauderdale'::text, 'Dallas'::text, 'Newburyport'::text]))
--		AND ticket_sandbox.shiplocation::text !~~* '%shelf%'::text AND ticket_sandbox.shiplocation::text !~~* '%overage%'::text 
--		AND ticket_sandbox.shiplocation::text !~~* '%stock%'::text AND ticket_sandbox.tickettype::text = '0'::text)
--	or (ticket_sandbox.businessunit = 'Milwaukee'::text AND ticket_sandbox.shipaddr1::text <> ''::text AND ticket_sandbox.shipaddr1 IS NOT NULL 
--		AND ticket_sandbox.shipaddr1::text !~~* '%inventory%'::text AND ticket_sandbox.tickettype::text = '0'::text)
--	or (ticket_sandbox.businessunit = 'Westfield'::text AND ticket_sandbox.shiplocation::text <> ''::text AND ticket_sandbox.shiplocation IS NOT NULL 
--		AND ticket_sandbox.shiplocation::text !~~* '%inventory%'::text AND ticket_sandbox.tickettype::text = '0'::text)
--	
--	or ((ticket_sandbox.businessunit = ANY (ARRAY['Ft. Lauderdale'::text, 'Dallas'::text, 'Newburyport'::text])) 
--		AND (ticket_sandbox.shiplocation::text ~~* '%shelf%'::text OR ticket_sandbox.shiplocation::text ~~* '%overage%'::text OR ticket_sandbox.shiplocation::text ~~* '%stock%'::text)
--		AND ticket_sandbox.tickettype::text = '1'::text)
--	or (ticket_sandbox.businessunit = 'Milwaukee'::text AND (ticket_sandbox.shipaddr1::text = ''::text OR ticket_sandbox.shipaddr1 IS NULL
--			OR ticket_sandbox.shipaddr1::text ~~* '%inventory%'::text OR ticket_sandbox.shipaddr1::text ~~* '%flexo-graphics%'::text)
--		AND ticket_sandbox.priority::text <> 'Intercompany-TX'::text AND ticket_sandbox.priority::text !~~* '%art%'::text AND ticket_sandbox.tickettype::text = '1'::text)
--	or (ticket_sandbox.businessunit = 'Westfield'::text AND (ticket_sandbox.shiplocation::text = ''::text OR ticket_sandbox.shiplocation IS NULL 
--			OR ticket_sandbox.shiplocation::text ~~* '%inventory%'::text) 
--		AND ticket_sandbox.tickettype::text = '1'::text)
--		
--	or ticket_sandbox.businessunit in ('Oceanside', 'Kansas City')
--	)
--	and ticket_sandbox.customername not ILIKE '%Tabco, Inc.%'
;