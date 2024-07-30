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
        SELECT
            'Flexographics'::text AS businessunit,
            concat('FG-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tktit.linetotal
           FROM dt_inovar_prod_stg.in_butler_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_butler_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_butler_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
      	UNION ALL
         SELECT 
            'SouthWest'::text AS businessunit,
            concat('DALLAS-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tktit.linetotal
            FROM dt_inovar_prod_stg.in_dallas_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_dallas_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_dallas_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 
            'SouthEast'::text AS businessunit,
            concat('DAVIE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tktit.linetotal
           FROM dt_inovar_prod_stg.in_davie_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_davie_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_davie_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 
            'NewEngland'::text AS businessunit,
            concat('NE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tktit.linetotal
           FROM dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
    UNION ALL
        SELECT 
        	'Cimarron North'::text 						AS businessunit,
            concat('CN-', tkt.customernum) AS company_customer_number,
            tkt.orderdate::text,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tktit.linetotal
            FROM dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 
         	'Dion Label'::text AS businessunit,
            concat('DL-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tktit.linetotal
            FROM dt_inovar_prod_stg.in_westfield_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_westfield_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_westfield_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        ), ticket_other_rev AS (
         SELECT 
            'Flexographics'::text AS businessunit,
            concat('FG-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
          	tkt.otsname,
           	tkt.pototal::double precision AS linetotal
           FROM dt_inovar_prod_stg.in_butler_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_butler_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 
            'Flexographics'::text AS businessunit,
             concat('FG-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tkt.misccharge::double precision AS linetotal
           FROM dt_inovar_prod_stg.in_butler_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_butler_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
      UNION ALL
          SELECT
            'SouthWest'::text AS businessunit,
            concat('DALLAS-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tkt.pototal::double precision AS linetotal
    	FROM dt_inovar_prod_stg.in_dallas_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_dallas_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT
            'SouthWest'::text AS businessunit,
            concat('DALLAS-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tkt.misccharge::double precision AS linetotal
           FROM dt_inovar_prod_stg.in_dallas_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_dallas_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
      UNION ALL
         SELECT
            'SouthEast'::text AS businessunit,
            concat('DAVIE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tkt.pototal::double precision AS linetotal
           FROM dt_inovar_prod_stg.in_davie_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_davie_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT
            'SouthEast'::text AS businessunit,
            concat('DAVIE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
			tkt.misccharge::double precision AS linetotal
         FROM dt_inovar_prod_stg.in_davie_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_davie_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 
            'NewEngland'::text AS businessunit,
            concat('NE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tkt.pototal::double precision AS linetotal
         FROM dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 
            'NewEngland'::text AS businessunit,
            concat('NE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tkt.misccharge::double precision AS linetotal
        FROM dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
    UNION ALL
    	SELECT 
         	'Cimarron North'::text AS businessunit,
            concat('CN-', tkt.customernum) AS company_customer_number,
            tkt.orderdate::text,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tkt.pototal::double precision AS linetotal
           FROM dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
    UNION ALL
        SELECT 
        	'Cimarron North'::text AS businessunit,
            concat('CN-', tkt.customernum) AS company_customer_number,
            tkt.orderdate::text,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tkt.misccharge::double precision AS linetotal
        FROM dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT
            'Dion Label'::text AS businessunit,
            concat('DL-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tkt.pototal::double precision AS linetotal
           FROM dt_inovar_prod_stg.in_westfield_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_westfield_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT
            'Dion Label'::text AS businessunit,
            concat('DL-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            tkt.misccharge::double precision AS linetotal
           FROM dt_inovar_prod_stg.in_westfield_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_westfield_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2020-01-01'::date
        UNION ALL
         SELECT 
         	'Tabco'::text 																AS businessunit,
            concat('KSKA51-T-', tkt_cte.customer_number) 								AS company_customer_number,
            tkt_cte.date_order::text 													AS order_date,
            tkt_cte.customer_name 														AS customername,
            tkt_cte.salesman_name__ 													AS otsname,
            tkt_cte.booking_revenue::double precision 									AS linetotal
           FROM tabco_final tkt_cte
             LEFT JOIN dt_inovar_prod_stg.in_kansascity_dim_customers cus ON cus.customer_number::text = tkt_cte.customer_number::text
        UNION ALL
         SELECT 
            'Precision Label'::text AS businessunit,
            concat('Carlsbad-', tkt.customerno) AS company_customer_number,
            tkt.orderdate::date::text AS orderdate,
            tkt.billtoname::text AS customername,
            spn.salespersonname AS otsname,
            tktit.lastextensionamt AS linetotal
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
         SELECT 
            'Precision Label'::text AS businessunit,
            concat('Carlsbad-', tkt.customerno) AS company_customer_number,
            tkt.orderdate::text AS orderdate,
            tkt.billtoname AS customername,
            spn.salespersonname AS otsname,
            tktit.extensionamt AS linetotal
           FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderheader tkt
             LEFT JOIN dt_inovar_prod_stg.in_fact_sage_so_salesorderdetail tktit ON tkt.salesorderno::text = tktit.salesorderno::text
             LEFT JOIN dt_inovar_prod_stg.in_fact_sage_ar_salesperson spn ON spn.salespersonno::text = tkt.salespersonno::text
          WHERE tkt.orderdate >= '2020-01-01'::date AND NOT (tkt.salesorderno::text IN ( SELECT DISTINCT in_fact_sage_so_salesorderhistoryheader.salesorderno
                   FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader))),
    all_tickets_clean as (
    	select
    		*
    	from all_tickets 
    	),    
    final_data AS (
         SELECT 
            all_tickets.businessunit,
            all_tickets.company_customer_number,
            all_tickets.orderdate,
            all_tickets.customername,
            all_tickets.otsname,
            all_tickets.linetotal
           FROM all_tickets_clean as all_tickets
          WHERE 
          	all_tickets.orderdate::date <= CURRENT_DATE
        UNION ALL
         SELECT
            ticket_other_rev.businessunit,
            ticket_other_rev.company_customer_number,
            ticket_other_rev.orderdate,
            ticket_other_rev.customername,
            ticket_other_rev.otsname,
            ticket_other_rev.linetotal
           FROM ticket_other_rev
          WHERE 
          ticket_other_rev.orderdate::date <= CURRENT_DATE
        ), 
    ticket_sandbox AS (
        SELECT 
            COALESCE(map_bu.mapped_value, tkt.businessunit::character varying) AS businessunit,
            tkt.company_customer_number,
            tkt.orderdate,
            tkt.customername::character varying(255) AS customername,
            tkt.otsname,
            tkt.linetotal
           	FROM final_data tkt
             	LEFT JOIN ( SELECT in_gs_company_mapping.record_type,
                    in_gs_company_mapping.original_value,
                    in_gs_company_mapping.mapped_value
                   FROM dt_inovar_prod_stg.in_gs_company_mapping
                  WHERE in_gs_company_mapping.record_type::text = 'BU'::text) map_bu ON tkt.businessunit = map_bu.original_value::text
            	)        
SELECT 
	ticket_sandbox.businessunit, 
    ticket_sandbox.company_customer_number,
    case 
		when trim(to_char(ticket_sandbox.orderdate::date, 'Day')) = 'Saturday' then (ticket_sandbox.orderdate::date - '1 day'::interval)::date::text
    	when trim(to_char(ticket_sandbox.orderdate::date, 'Day')) = 'Sunday' then (ticket_sandbox.orderdate::date - '2 day'::interval)::date::text
        else ticket_sandbox.orderdate
    end as orderdate,
    ticket_sandbox.customername,
    ticket_sandbox.otsname,
    ticket_sandbox.linetotal
FROM ticket_sandbox
