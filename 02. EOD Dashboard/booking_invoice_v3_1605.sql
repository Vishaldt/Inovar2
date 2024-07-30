/*

CREATE TABLE dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard AS 
SELECT * FROM dt_inovar_prod_edw.v_sales_bookings_eod_dashboard


drop table dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard

drop view dt_inovar_prod_edw.v_sales_bookings_eod_dashboard


CREATE OR REPLACE VIEW dt_inovar_prod_edw.v_sales_bookings_eod_dashboard AS 
*/



select businessunit ,sum(invoice_revenue) invoice_revenue
from (
with holiday AS (
	SELECT  
		"Entity" AS businessunit,
		"Date"::date AS dt,
		"Holiday_Flag" AS holiday_flag
	FROM dt_inovar_prod_stg.in_holiday_schedule
--		WHERE "Entity" IN ('Amherst Label', 'Cimarron North', 'Dallas', 'Ft. Lauderdale', 'Milwaukee', 'Newburyport', 'Westfield')
		--AND "Holiday_Flag" = 1
),
date_series AS (
	SELECT 
		generate_series('2023-01-01'::date, date_trunc('year', current_date) + INTERVAL '1 year - 1 day' , '1 day')::date AS date_series 
),
date_cross_bu AS (
	SELECT * FROM date_series
	CROSS JOIN (
					SELECT DISTINCT 
						businessunit
					FROM holiday
					) a
),
final_date_series AS (
	SELECT 
		date_cross_bu.date_series AS dt,
		date_cross_bu.businessunit,
		CASE 
			WHEN holiday.holiday_flag = 1 THEN 0
			WHEN EXTRACT(DOW FROM date_cross_bu.date_series) = 0 OR EXTRACT(DOW FROM date_cross_bu.date_series) = 6 THEN 0
			ELSE 1
		END AS work_day
	FROM date_cross_bu
	LEFT JOIN holiday ON holiday.businessunit = date_cross_bu.businessunit
					AND holiday.dt = date_cross_bu.date_series
),
total_work_days as (
select
		date_part(
        'days', 
        (date_trunc('month', dt) + interval '1 month - 1 day')) total_work_days,
		date_trunc('month', dt)::date month_year ,
		dt,
		businessunit,
		work_day
from
		final_date_series
),
work_days as (
select
		date_trunc('month', dt)::date month_year,
		businessunit,
		sum(work_day) work_days
from
		total_work_days
group by
		1,
		2
),
working_days as (
	select
		distinct total_work_days.dt,
		total_work_days.businessunit,
		total_work_days.total_work_days,
		work_days.work_days,
		CASE WHEN work_day = 0 THEN 'True' ELSE 'False' END AS holiday,
		work_day
	from
		total_work_days
	left join work_days ON total_work_days.month_year = work_days.month_year
						and total_work_days.businessunit = work_days.businessunit
),
final_working_days as (
	select
		dt,
		businessunit,
		total_work_days,
		work_days,
		holiday,
		count(*) over (partition by date_trunc('month', dt)::date, businessunit order BY date_trunc('month', dt)::date,	dt) month_day_count,
		CASE 
			WHEN work_day = 0 THEN 0
			ELSE sum(work_day) OVER (PARTITION BY date_trunc('month', dt)::date,	businessunit ORDER BY date_trunc('month', dt)::date, dt) 
		END AS month_workday_count
	from
		working_days 
),
invoice as(
	WITH gl_account_info AS (
         SELECT DISTINCT 'Milwaukee' AS location,
            gld.invoice_id,
            ibfg.financialstatementclass
           FROM dt_inovar_prod_stg.in_butler_fact_invoicegldistribution gld
             LEFT JOIN dt_inovar_prod_stg.in_butler_fact_glchartofaccounts ibfg ON gld.account_number::text = ibfg.accountnumber::text
          WHERE ibfg.financialstatementclass::text = 'Sales'::text
          	AND NOT (gld.invoice_id IN ( SELECT gld_1.invoice_id
                   FROM dt_inovar_prod_stg.in_butler_fact_invoicegldistribution gld_1
                     LEFT JOIN dt_inovar_prod_stg.in_butler_fact_glchartofaccounts ibfg_1 ON gld_1.account_number::text = ibfg_1.accountnumber::text
                  WHERE ibfg_1.financialstatementclass::text = 'Discounts, returns and allowances'::text))
        UNION ALL
         SELECT DISTINCT 'Dallas' AS location,
            gld.invoice_id,
            ibfg.financialstatementclass
           FROM dt_inovar_prod_stg.in_dallas_fact_invoice_gl_distribution gld
             LEFT JOIN dt_inovar_prod_stg.in_dallas_fact_glchartofaccounts ibfg ON gld.account_number::text = ibfg.accountnumber::text
          WHERE ibfg.financialstatementclass::text = 'Sales'::text 
          	AND NOT (gld.invoice_id IN ( SELECT gld_1.invoice_id
                   FROM dt_inovar_prod_stg.in_dallas_fact_invoice_gl_distribution gld_1
                     LEFT JOIN dt_inovar_prod_stg.in_dallas_fact_glchartofaccounts ibfg_1 ON gld_1.account_number::text = ibfg_1.accountnumber::text
                  WHERE ibfg_1.financialstatementclass::text = 'Discounts, returns and allowances'::text))
        UNION ALL
         SELECT DISTINCT 'Ft. Lauderdale' AS location,
            gld.invoice_id,
            ibfg.financialstatementclass
           FROM dt_inovar_prod_stg.in_davie_fact_invoice_gl_distribution gld
             LEFT JOIN dt_inovar_prod_stg.in_davie_fact_glchartofaccounts ibfg ON gld.account_number::text = ibfg.accountnumber::text
          WHERE ibfg.financialstatementclass::text = 'Sales'::text
          	AND NOT (gld.invoice_id IN ( SELECT gld_1.invoice_id
                   FROM dt_inovar_prod_stg.in_davie_fact_invoice_gl_distribution gld_1
                     LEFT JOIN dt_inovar_prod_stg.in_davie_fact_glchartofaccounts ibfg_1 ON gld_1.account_number::text = ibfg_1.accountnumber::text
                  WHERE ibfg_1.financialstatementclass::text = 'Discounts, returns and allowances'::text))
        UNION ALL
         SELECT DISTINCT 'Newbury Port' AS location,
            gld.invoice_id,
            ibfg.financialstatementclass
           FROM dt_inovar_prod_stg.in_newburyport_fact_invoice_gl_distribution gld
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_fact_gl_chartofaccounts ibfg ON gld.account_number::text = ibfg.accountnumber::text
          WHERE ibfg.financialstatementclass::text = 'Sales'::text
          	AND NOT (gld.invoice_id IN ( SELECT gld_1.invoice_id
                   FROM dt_inovar_prod_stg.in_newburyport_fact_invoice_gl_distribution gld_1
                     LEFT JOIN dt_inovar_prod_stg.in_newburyport_fact_gl_chartofaccounts ibfg_1 ON gld_1.account_number::text = ibfg_1.accountnumber::text
                  WHERE ibfg_1.financialstatementclass::text = 'Discounts, returns and allowances'::text))
    UNION all
        SELECT DISTINCT 'Cimarron North' AS location,
            gld.invoice_id::integer,
            ibfg.financialstatementclass
           FROM dt_inovar_prod_stg.in_cimarron_fact_invoice_gl_distribution gld
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_fact_gl_chartofaccounts ibfg ON gld.account_number::text = ibfg.accountnumber::text
          WHERE ibfg.financialstatementclass::text = 'Sales'::text
          	AND NOT (gld.invoice_id IN ( SELECT gld_1.invoice_id
                   FROM dt_inovar_prod_stg.in_cimarron_fact_invoice_gl_distribution gld_1
                     LEFT JOIN dt_inovar_prod_stg.in_cimarron_fact_gl_chartofaccounts ibfg_1 ON gld_1.account_number::text = ibfg_1.accountnumber::text
                  WHERE ibfg_1.financialstatementclass::text = 'Discounts, returns and allowances'::text))
    UNION all
        SELECT DISTINCT 'Amherst Label' AS location,
            gld.invoice_id::integer,
            ibfg.financialstatementclass
           FROM dt_inovar_prod_stg.in_amherst_fact_invoice_gl_distribution gld
             LEFT JOIN dt_inovar_prod_stg.in_amherst_fact_gl_chartofaccounts ibfg ON gld.account_number::text = ibfg.accountnumber::text
          WHERE ibfg.financialstatementclass::text = 'Sales'::text
          	AND NOT (gld.invoice_id IN ( SELECT gld_1.invoice_id
                   FROM dt_inovar_prod_stg.in_amherst_fact_invoice_gl_distribution gld_1
                     LEFT JOIN dt_inovar_prod_stg.in_amherst_fact_gl_chartofaccounts ibfg_1 ON gld_1.account_number::text = ibfg_1.accountnumber::text
                  WHERE ibfg_1.financialstatementclass::text = 'Discounts, returns and allowances'::text))
	UNION ALL
        SELECT DISTINCT 'Westfield' AS location,
            gld.invoice_id,
            ibfg.financialstatementclass
           FROM dt_inovar_prod_stg.in_westfield_fact_invoice_gl_distribution gld
             LEFT JOIN dt_inovar_prod_stg.in_westfield_fact_gl_chartofaccounts ibfg ON gld.account_number::text = ibfg.accountnumber::text
          WHERE ibfg.financialstatementclass::text = 'Sales'::text
          	AND NOT (gld.invoice_id IN ( SELECT gld_1.invoice_id
                   FROM dt_inovar_prod_stg.in_westfield_fact_invoice_gl_distribution gld_1
                     LEFT JOIN dt_inovar_prod_stg.in_westfield_fact_gl_chartofaccounts ibfg_1 ON gld_1.account_number::text = ibfg_1.accountnumber::text
                  WHERE ibfg_1.financialstatementclass::text = 'Discounts, returns and allowances'::text))
        ), invoices AS (
         SELECT 
    		'Milwaukee'                                 				as businessunit,
            concat('FG-', inv.customernumber) AS company_customer_number,
            inv.idate AS invoice_date,
            inv.ticketnum,
            inv.customernumber AS customer_number,
            COALESCE(cus.company, inv.customername) AS customer_name,
            COALESCE(gl.financialstatementclass, 'Others'::character varying) AS financial_statement_class,
            invit.itemtotal AS invoice_item_total,
            invit.ticketitemid
           FROM dt_inovar_prod_stg.in_butler_fact_invoice inv
             JOIN dt_inovar_prod_stg.in_butler_fact_invoiceitem invit ON inv.number::text = invit.invoicenumber::text
             LEFT JOIN dt_inovar_prod_stg.in_butler_dim_customer cus ON inv.customernumber::text = cus.number::text
             LEFT JOIN gl_account_info gl ON inv.number::text = gl.invoice_id::text AND gl.location = 'Milwaukee'::text
             LEFT JOIN (select sales_rep_no, otsname from dt_inovar_prod_edw.u_mat_inovar_salesrep_master where businessunit = 'Milwaukee') salesrep ON salesrep.sales_rep_no = inv.salesrepno
          WHERE inv.number::text <> ''::text AND inv.idate::text <> ''::text AND inv.idate::date >= '2020-01-01'::date AND inv.itype::text <> 'Master'::text
        UNION ALL
         SELECT 
    		'Ft. Lauderdale'                                			as businessunit,
            concat('DAVIE-', inv.customernumber) AS company_customer_number,
            inv.idate AS invoice_date,
            inv.ticketnum,
            inv.customernumber AS customer_number,
            COALESCE(cus.company, inv.customername) AS customer_name,
            COALESCE(gl.financialstatementclass, 'Others'::character varying) AS financial_statement_class,
            invit.itemtotal AS invoice_item_total,
            invit.ticketitemid
           FROM dt_inovar_prod_stg.in_davie_fact_invoice inv
             JOIN dt_inovar_prod_stg.in_davie_fact_invoiceitem invit ON inv.number::text = invit.invoicenumber::text
             LEFT JOIN dt_inovar_prod_stg.in_davie_dim_customer cus ON inv.customernumber::text = cus.number::text
             LEFT JOIN gl_account_info gl ON inv.number::text = gl.invoice_id::text AND gl.location = 'Ft. Lauderdale'::text
             LEFT JOIN (select sales_rep_no, otsname from dt_inovar_prod_edw.u_mat_inovar_salesrep_master where businessunit = 'Ft. Lauderdale') salesrep ON salesrep.sales_rep_no = inv.salesrepno
          WHERE inv.number::text <> ''::text AND inv.idate::text <> ''::text AND inv.idate::date >= '2020-01-01'::date AND inv.itype::text <> 'Master'::text
        UNION ALL
         SELECT 
		    'Dallas'                                 					as businessunit,
            concat('DALLAS-', inv.customernumber) AS company_customer_number,
            inv.idate AS invoice_date,
            inv.ticketnum,
            inv.customernumber AS customer_number,
            COALESCE(cus.company, inv.customername) AS customer_name,
            COALESCE(gl.financialstatementclass, 'Others'::character varying) AS financial_statement_class,
            invit.itemtotal AS invoice_item_total,
            invit.ticketitemid
           FROM dt_inovar_prod_stg.in_dallas_fact_invoice inv
             JOIN dt_inovar_prod_stg.in_dallas_fact_invoiceitem invit ON TRIM(BOTH FROM inv.number::text) = TRIM(BOTH FROM invit.invoicenumber::text)
             LEFT JOIN dt_inovar_prod_stg.in_dallas_dim_customer cus ON inv.customernumber::text = cus.number::text
             LEFT JOIN gl_account_info gl ON inv.number::text = gl.invoice_id::text AND gl.location = 'Dallas'::text
             LEFT JOIN (select sales_rep_no, otsname from dt_inovar_prod_edw.u_mat_inovar_salesrep_master where businessunit = 'Dallas') salesrep ON salesrep.sales_rep_no = inv.salesrepno
          WHERE inv.number::text <> ''::text AND inv.idate::text <> ''::text AND inv.idate::date >= '2020-01-01'::date AND inv.itype::text <> 'Master'::text
        UNION ALL
         SELECT 
		    'Newburyport'                                				as businessunit,
            concat('NE-', inv.customernumber) AS company_customer_number,
            inv.idate AS invoice_date,
            inv.ticketnum,
            inv.customernumber AS customer_number,
            COALESCE(cus.company, inv.customername) AS customer_name,
            COALESCE(gl.financialstatementclass, 'Others'::character varying) AS financial_statement_class,
            invit.itemtotal AS invoice_item_total,
            invit.ticketitemid
           FROM dt_inovar_prod_stg.in_newburyport_fact_invoice inv
             JOIN dt_inovar_prod_stg.in_newburyport_fact_invoiceitem invit ON inv.number::text = invit.invoicenumber::text
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_dim_customer cus ON inv.customernumber::text = cus.number::text
             LEFT JOIN gl_account_info gl ON inv.number::text = gl.invoice_id::text AND gl.location = 'Newbury Port'::text
             LEFT JOIN (select sales_rep_no, otsname from dt_inovar_prod_edw.u_mat_inovar_salesrep_master where businessunit = 'Newburyport') salesrep ON salesrep.sales_rep_no = inv.salesrepno
          WHERE inv.number::text <> ''::text AND inv.idate::text <> ''::text AND inv.idate::date >= '2020-01-01'::date AND inv.itype::text <> 'Master'::text
    UNION all
        SELECT 
        	'Cimarron North'::text 		AS businessunit,
            concat('CN-', inv.customernumber) AS company_customer_number,
            inv.idate::text AS invoice_date,
            inv.ticketnum,
            inv.customernumber AS customer_number,
            COALESCE(cus.company, inv.customername) AS customer_name,
            COALESCE(gl.financialstatementclass, 'Others'::character varying) AS financial_statement_class,
            invit.itemtotal AS invoice_item_total,
            invit.ticketitemid
           FROM dt_inovar_prod_stg.in_cimarron_fact_invoice inv
             JOIN dt_inovar_prod_stg.in_cimarron_fact_invoiceitem invit ON inv.number::text = invit.invoicenumber::text
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_dim_customer cus ON inv.customernumber::text = cus.number::text
             LEFT JOIN gl_account_info gl ON inv.number::text = gl.invoice_id::text AND gl.location = 'Cimarron North'::text
             LEFT JOIN (select sales_rep_no, otsname from dt_inovar_prod_edw.u_mat_inovar_salesrep_master where businessunit = 'Cimarron North') salesrep ON salesrep.sales_rep_no = inv.salesrepno
          WHERE inv.number::text <> ''::text AND inv.idate::text <> ''::text AND inv.idate::date >= '2020-01-01'::date AND inv.itype::text <> 'Master'::text
    UNION all
        SELECT 
        	'Amherst Label'::text 		AS businessunit,
            concat('AL-', inv.customernumber) AS company_customer_number,
            inv.idate::text AS invoice_date,
            inv.ticketnum,
            inv.customernumber AS customer_number,
            COALESCE(cus.company, inv.customername) AS customer_name,
            COALESCE(gl.financialstatementclass, 'Others'::character varying) AS financial_statement_class,
            invit.itemtotal AS invoice_item_total,
            invit.ticketitemid
           FROM dt_inovar_prod_stg.in_amherst_fact_invoice inv
             JOIN dt_inovar_prod_stg.in_amherst_fact_invoiceitem invit ON inv.number::text = invit.invoicenumber::text
             LEFT JOIN dt_inovar_prod_stg.in_amherst_dim_customer cus ON inv.customernumber::text = cus.number::text
             LEFT JOIN gl_account_info gl ON inv.number::text = gl.invoice_id::text AND gl.location = 'Amherst Label'::text
             LEFT JOIN (select sales_rep_no, otsname from dt_inovar_prod_edw.u_mat_inovar_salesrep_master where businessunit = 'Amherst Label') salesrep ON salesrep.sales_rep_no = inv.salesrepno
          WHERE inv.number::text <> ''::text AND inv.idate::text <> ''::text AND inv.idate::date >= '2020-01-01'::date AND inv.itype::text <> 'Master'::text
        UNION ALL
         SELECT 
			'Westfield' 												as businessunit,
            concat('DL-', inv.customernumber) AS company_customer_number,
            inv.idate AS invoice_date,
            inv.ticketnum,
            inv.customernumber AS customer_number,
            COALESCE(cus.company, inv.customername) AS customer_name,
            COALESCE(gl.financialstatementclass, 'Others'::character varying) AS financial_statement_class,
            invit.itemtotal AS invoice_item_total,
            invit.ticketitemid
           FROM dt_inovar_prod_stg.in_westfield_fact_invoice inv
             JOIN dt_inovar_prod_stg.in_westfield_fact_invoiceitem invit ON inv.number::text = invit.invoicenumber::text
             LEFT JOIN dt_inovar_prod_stg.in_westfield_dim_customer cus ON inv.customernumber::text = cus.number::text
             LEFT JOIN gl_account_info gl ON inv.number::text = gl.invoice_id::text AND gl.location = 'Westfield'::text
             LEFT JOIN (select sales_rep_no, otsname from dt_inovar_prod_edw.u_mat_inovar_salesrep_master where businessunit = 'Westfield') salesrep ON salesrep.sales_rep_no = inv.salesrepno
          WHERE inv.number::text <> ''::text AND inv.idate::text <> ''::text AND inv.idate::date >= '2020-01-01'::date AND inv.itype::text <> 'Master'::text
        ), 
    customer_master_tabco as (
		select customer_number as number, customer_name as company, salesman as otsname, null::text as sales_rep_no, itsname, null::text as cust_serv_no
			from (select customer_number, customer_name, case when trim(salesman) = '' then null else salesman end as salesman, 
						case when trim(customer_service_representative) = '' then null else customer_service_representative end as itsname, 
						RANK() OVER(PARTITION BY customer_number ORDER BY customer_name DESC) as rank
					from dt_inovar_prod_stg.in_kansascity_dim_customers) cus where rank = 1 and customer_number <> ''
		),
    final_invoice_ps_ticket_join AS (
         SELECT 
            inv.businessunit,
            inv.company_customer_number,
            inv.invoice_date,
            inv.ticketnum,
            inv.customer_number,
            inv.customer_name,
            inv.financial_statement_class,
            inv.invoice_item_total,
            inv.ticketitemid::text AS ticketitemid
           FROM invoices inv
--             LEFT JOIN all_ticket_info tkt ON inv.location = tkt.location AND inv.ticketnum::text = tkt.ticket_number::text AND inv.ticketitemid::text = tkt.ticket_item_id::text
--             LEFT JOIN packingslip_info ps ON inv.location = ps.location AND inv.packslipitem_id::text = ps.packingslip_item_id::text  
	UNION ALL
		SELECT 
			'Tabco' 															AS businessunit,
			concat('KSKA51-T-', inv.customerno) 								AS company_customer_number,
            inv.invoicedate::text 												AS invoice_date,
            NULL::text															AS ticketnum,
            inv.customerno														AS customer_number,
            coalesce(cus.company, inv.billtoname)								AS customer_name,
            'Sales' 															AS financial_statement_class,
            invit.extensionamt 													AS invoice_item_total,
            null::text 															AS ticketitemid
		from dt_inovar_prod_stg.in_tabco_sage_fact_ar_invoice_history_header inv
			left join dt_inovar_prod_stg.in_tabco_sage_fact_ar_invoice_history_detail invit
				on invit.invoiceno = inv.invoiceno and invit.headerseqno = inv.headerseqno
			left join customer_master_tabco cus 
				on cus.number = inv.customerno
			left join dt_inovar_prod_stg.in_tabco_sage_dim_ar_sales_person sp 
				on sp.salespersonno = inv.salespersonno
		where inv.invoicetype = 'IN'
	UNION all
         SELECT 
            'Oceanside'::text AS businessunit,
            concat('Carlsbad-', sih.customerno) AS company_customer_number,
            sih.invoicedate::text AS invoice_date,
            sih.salesorderno AS ticketnum,
            sih.customerno AS customer_number,
            COALESCE(cus.customername, sih.billtoname) AS customer_name,
            'Sales'::text AS financial_statement_class,
            sid.extensionamt AS invoice_item_total,
            NULL::text AS ticketitemid
        FROM dt_inovar_prod_stg.in_fact_sage_ar_invoicehistoryheader sih
            LEFT JOIN dt_inovar_prod_stg.in_fact_sage_ar_invoicehistorydetail sid ON sih.invoiceno::text = sid.invoiceno::text
            																		and sih.headerseqno::text = sid.headerseqno::text
            LEFT JOIN dt_inovar_prod_stg.in_fact_sage_ar_customer cus ON sih.customerno::text = cus.customerno::text
            LEFT JOIN dt_inovar_prod_stg.in_fact_sage_ar_salesperson spn ON sih.salespersonno::text = spn.salespersonno::text
            LEFT JOIN dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader soh ON sih.salesorderno::text = soh.salesorderno::text
--            left join (select salesorderno, max(promisedate) as promisedate from dt_inovar_prod_stg.in_fact_sage_so_salesorderhistorydetail group by 1) sohd 
--            	ON sih.salesorderno::text = sohd.salesorderno::text
            /*left*/ join dt_inovar_prod_stg.in_fact_sage_gl_account sg on sg.accountkey = sid.salesacctkey
            left join (select salesorderno, pressname from dt_inovar_prod_edw.u_mat_precision_press_ops) press on press.salesorderno = sih.salesorderno::text
            left join dt_inovar_prod_stg.in_fact_sage_ar_salesperson sal on sal.salespersonno = cus.salespersonno
        WHERE sih.invoicedate >= '2023-01-01'::date and not (sih.invoiceno::text = '0063323' and sih.nontaxablesalesamt = 0) and not (sg.accountdesc ilike 'Due to%' and sg.account <> '22050-000')
        and (sih.invoicetype <> 'CM' or sih.invoicetype is null)-----------to be discuss with Gaurav
	UNION ALL
         SELECT 
            'Oceanside'::text AS businessunit,
            concat('Carlsbad-', sih.customerno) AS company_customer_number,
            sih.invoicedate::text AS invoice_date,
            sih.salesorderno AS ticketnum,
            sih.customerno AS customer_number,
            COALESCE(cus.customername, sih.billtoname) AS customer_name,
            'Sales'::text AS financial_statement_class,
            sih.freightamt AS invoice_item_total,
            NULL::text AS ticketitemid
        FROM dt_inovar_prod_stg.in_fact_sage_ar_invoicehistoryheader sih
            LEFT JOIN dt_inovar_prod_stg.in_fact_sage_ar_customer cus ON sih.customerno::text = cus.customerno::text
            LEFT JOIN dt_inovar_prod_stg.in_fact_sage_ar_salesperson spn ON sih.salespersonno::text = spn.salespersonno::text
            LEFT JOIN dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader soh ON sih.salesorderno::text = soh.salesorderno::text
            left join (select salesorderno, pressname from dt_inovar_prod_edw.u_mat_precision_press_ops) press on press.salesorderno = sih.salesorderno::text
            left join dt_inovar_prod_stg.in_fact_sage_ar_salesperson sal on sal.salespersonno = cus.salespersonno
            join (select distinct ihd.invoiceno, ihd.headerseqno
					FROM dt_inovar_prod_stg.in_fact_sage_ar_invoicehistorydetail ihd
					JOIN dt_inovar_prod_stg.in_fact_sage_ar_invoicehistoryheader ihh ON ihh.InvoiceNo = ihd.InvoiceNo AND ihh.HeaderSeqNo = ihd.HeaderSeqNo
					JOIN dt_inovar_prod_stg.in_fact_sage_gl_account gl ON gl.AccountKey = ihd.SalesAcctKey) gl
				on gl.invoiceno = sih.invoiceno and gl.headerseqno = sih.headerseqno
        WHERE sih.invoicedate >= '2023-01-01'::date and not (sih.invoiceno::text = '0063323' and sih.nontaxablesalesamt = 0)
        ), 
    invoice_header_item_union AS (
         SELECT 
            final_invoice_ps_ticket_join.businessunit,
            final_invoice_ps_ticket_join.company_customer_number,
            final_invoice_ps_ticket_join.invoice_date,
            final_invoice_ps_ticket_join.ticketnum,
            final_invoice_ps_ticket_join.customer_name,
            final_invoice_ps_ticket_join.financial_statement_class,
            final_invoice_ps_ticket_join.invoice_item_total
           FROM final_invoice_ps_ticket_join
        UNION ALL
         SELECT 
            ot.businessunit,
            ot.company_customer_number,
            ot.invoice_date,
            ot.ticketnum,
            ot.customer_name,
            COALESCE(gl.financialstatementclass, 'Others'::character varying) AS financial_statement_class,
            ot.invoice_item_total
           FROM dt_inovar_prod_edw.u_mat_inovar_invoice_other_rev ot
             LEFT JOIN gl_account_info gl ON ot.location = gl.location AND ot.invoice_number = gl.invoice_id::text
		), 
	invoice_sandbox AS (
         SELECT 
            COALESCE(map_bu.mapped_value, inv.businessunit::character varying) AS businessunit,
            inv.company_customer_number,
            inv.invoice_date::character varying(255) AS invoice_date,
            inv.customer_name::character varying(255) AS invoice_customer_name,
            inv.financial_statement_class,
            inv.invoice_item_total
           FROM invoice_header_item_union inv
             LEFT JOIN ( SELECT in_gs_company_mapping.record_type,
                    in_gs_company_mapping.original_value,
                    in_gs_company_mapping.mapped_value
                   FROM dt_inovar_prod_stg.in_gs_company_mapping
                  WHERE in_gs_company_mapping.record_type::text = 'BU'::text) map_bu ON inv.businessunit = map_bu.original_value::text
             /*LEFT JOIN ( SELECT in_gs_company_mapping.record_type,
                    in_gs_company_mapping.original_value,
                    in_gs_company_mapping.mapped_value
                   FROM dt_inovar_prod_stg.in_gs_company_mapping
                  WHERE in_gs_company_mapping.record_type::text = 'Company'::text) map_comp ON inv.company = map_comp.original_value::text
             LEFT JOIN ( SELECT in_gs_company_mapping.record_type,
                    in_gs_company_mapping.original_value,
                    in_gs_company_mapping.mapped_value
                   FROM dt_inovar_prod_stg.in_gs_company_mapping
                  WHERE in_gs_company_mapping.record_type::text = 'Location'::text) map_loc ON inv.location = map_loc.original_value::text*/
        ),
	customer_master as (
	select 'Milwaukee' as businessunit, number, concat('FG-', number) as company_customer_number, company, otsname, sales_rep_no, itsname, cust_serv_no 
			from dt_inovar_prod_stg.in_butler_dim_customer
		union all
	select 'Dallas' as businessunit, number, concat('DALLAS-', number) as company_customer_number, company, otsname, sales_rep_no, itsname, cust_serv_no 
			from dt_inovar_prod_stg.in_dallas_dim_customer
		union all
	select 'Ft. Lauderdale' as businessunit, number, concat('DAVIE-', number) as company_customer_number, company, otsname, sales_rep_no, itsname, cust_serv_no 
			from dt_inovar_prod_stg.in_davie_dim_customer
		union all
	select 'Newburyport' as businessunit, number, concat('NE-', number) as company_customer_number, company, otsname, sales_rep_no, itsname, cust_serv_no 
			from dt_inovar_prod_stg.in_newburyport_dim_customer
		union all
	select 'Cimarron North' as businessunit, number, concat('CN-', number) as company_customer_number, company, otsname, sales_rep_no, itsname, cust_serv_no 
			from dt_inovar_prod_stg.in_cimarron_dim_customer
		union all
	select 'Amherst Label' as businessunit, number, concat('AL-', number) as company_customer_number, company, otsname, sales_rep_no, itsname, cust_serv_no 
			from dt_inovar_prod_stg.in_amherst_dim_customer
		union all
	select 'Westfield' as businessunit, number, concat('DL-', number) as company_customer_number, company, otsname, sales_rep_no, itsname, cust_serv_no 
			from dt_inovar_prod_stg.in_westfield_dim_customer
		union all
	select 'Kansas City' as businessunit, customer_number as number, concat('KSKA51-T-', customer_number) as company_customer_number, 
			customer_name as company, salesman as otsname, null::text as sales_rep_no, itsname, null::text as cust_serv_no
		from (select customer_number, customer_name, case when trim(salesman) = '' then null else salesman end as salesman, 
					case when trim(customer_service_representative) = '' then null else customer_service_representative end as itsname, 
					RANK() OVER(PARTITION BY customer_number ORDER BY customer_name DESC) as rank
				from dt_inovar_prod_stg.in_kansascity_dim_customers) cus where rank = 1 and customer_number <> ''
		union all
	select 'Oceanside' as businessunit, cus.customerno as number, concat('Carlsbad-', cus.customerno) as company_customer_number, 
			cus.customername as company, sal.salespersonname as otsname, cus.salespersonno as sales_rep_no, cus.customercarerep as itsname, null::text as cust_serv_no
	from dt_inovar_prod_stg.in_precision_dim_customers cus
	left join dt_inovar_prod_stg.in_fact_sage_ar_salesperson sal on sal.salespersonno = cus.salespersonno
	),
	final_pure_sales AS (
	SELECT 
	    invoice_sandbox.businessunit,
	    upper(invoice_sandbox.company_customer_number)		as company_customer_number,
	    case 
			when trim(to_char(invoice_sandbox.invoice_date::date, 'Day')) = 'Saturday' then (invoice_sandbox.invoice_date::date - '1 day'::interval)::date::text
	    	when trim(to_char(invoice_sandbox.invoice_date::date, 'Day')) = 'Sunday' then (invoice_sandbox.invoice_date::date - '2 day'::interval)::date::text
	        else invoice_sandbox.invoice_date
	    end as invoice_date,
	    upper(invoice_sandbox.invoice_customer_name)		as invoice_customer_name,
	    invoice_sandbox.invoice_item_total
	FROM invoice_sandbox
	left join customer_master cus
		on trim(lower(cus.company_customer_number)) = trim(lower(invoice_sandbox.company_customer_number))
	WHERE invoice_sandbox.invoice_date::date >= '2020-01-01'::date AND invoice_sandbox.invoice_date::date <= CURRENT_DATE
		and financial_statement_class = 'Sales'
		and invoice_customer_name not ilike 'Inovar Packaging'
	)
	select
		inv.businessunit,
		inv.company_customer_number,
		inv.invoice_date,
	    inv.invoice_customer_name																as customername,
	    sum(inv.invoice_item_total)																as invoice_revenue,
		0::float																				AS budget
	from final_pure_sales inv
	where invoice_date::date>='2023-01-01'
	group by 1,2,3,4
),
booking as (
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
			where replace(date_order,'None','1900-01-01') >= '2023-01-01'
	), 
	all_tickets AS (	
	SELECT
            'Amherst Label'::text AS businessunit,
            concat('AL-', tkt.customernum) AS company_customer_number,
            tkt.orderdate::text,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(CASE
					WHEN tktit.linetotal = 0 then bv.calc_value
					ELSE tktit.linetotal
				end ) AS linetotal
				FROM dt_inovar_prod_stg.in_amherst_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_amherst_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_amherst_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
             left join dt_inovar_prod_edw.u_mat_backlog_type_test_bookings bv on
             			bv.number::text = tkt.number::text AND bv.ticketitemid = tktit.id::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5  
  union all
	 SELECT
            'Flexographics'::text AS businessunit,
            concat('FG-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(CASE
					WHEN tktit.linetotal = 0 then bv.calc_value
					ELSE tktit.linetotal
				end ) AS linetotal
           FROM dt_inovar_prod_stg.in_butler_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_butler_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_butler_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
             left join dt_inovar_prod_edw.u_mat_backlog_type_test_bookings bv on
             			bv.number::text = tkt.number::text AND bv.ticketitemid = tktit.id::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5  
      	UNION ALL
         SELECT 
            'SouthWest'::text AS businessunit,
            concat('DALLAS-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(CASE
					WHEN tktit.linetotal = 0 then bv.calc_value
					ELSE tktit.linetotal
				end ) AS linetotal
            FROM dt_inovar_prod_stg.in_dallas_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_dallas_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_dallas_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
             left join dt_inovar_prod_edw.u_mat_backlog_type_test_bookings bv on
             			bv.number::text = tkt.number::text AND bv.ticketitemid = tktit.id::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT 
            'SouthEast'::text AS businessunit,
            concat('DAVIE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(CASE
					WHEN tktit.linetotal = 0 then bv.calc_value
					ELSE tktit.linetotal
				end ) AS linetotal
           FROM dt_inovar_prod_stg.in_davie_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_davie_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_davie_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
             left join dt_inovar_prod_edw.u_mat_backlog_type_test_bookings bv on
             			bv.number::text = tkt.number::text AND bv.ticketitemid = tktit.id::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT 
            'NewEngland'::text AS businessunit,
            concat('NE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(CASE
					WHEN tktit.linetotal = 0 then bv.calc_value
					ELSE tktit.linetotal
				end ) AS linetotal
           FROM dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
             left join dt_inovar_prod_edw.u_mat_backlog_type_test_bookings bv on
             			bv.number::text = tkt.number::text AND bv.ticketitemid = tktit.id::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
    UNION ALL
        SELECT 
        	'Cimarron North'::text 						AS businessunit,
            concat('CN-', tkt.customernum) AS company_customer_number,
            tkt.orderdate::text,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(CASE
					WHEN tktit.linetotal = 0 then bv.calc_value
					ELSE tktit.linetotal
				end ) AS linetotal
            FROM dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
             left join dt_inovar_prod_edw.u_mat_backlog_type_test_bookings bv on
             			bv.number::text = tkt.number::text AND bv.ticketitemid = tktit.id::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT 
         	'Dion Label'::text AS businessunit,
            concat('DL-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(CASE
					WHEN tktit.linetotal = 0 then bv.calc_value
					ELSE tktit.linetotal
				end ) AS linetotal
            FROM dt_inovar_prod_stg.in_westfield_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_westfield_dim_customer cus ON tkt.customernum::text = cus.number::text
             LEFT JOIN dt_inovar_prod_stg.in_westfield_fact_ticketitem tktit ON tkt.number::text = tktit.ticketnumber::text
             left join dt_inovar_prod_edw.u_mat_backlog_type_test_bookings bv on
             			bv.number::text = tkt.number::text AND bv.ticketitemid = tktit.id::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        ), ticket_other_rev AS (
        SELECT 
            'Amherst Label'::text AS businessunit,
            concat('AL-', tkt.customernum) AS company_customer_number,
            tkt.orderdate::text,
            COALESCE(cus.company, tkt.customername) AS customername,
          	tkt.otsname,
           	sum(tkt.pototal::double PRECISION) AS linetotal
           FROM dt_inovar_prod_stg.in_amherst_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_amherst_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
       union ALL 
         SELECT 
            'Flexographics'::text AS businessunit,
            concat('FG-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
          	tkt.otsname,
           	sum(tkt.pototal::double PRECISION) AS linetotal
           FROM dt_inovar_prod_stg.in_butler_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_butler_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT 
            'Flexographics'::text AS businessunit,
             concat('FG-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(tkt.misccharge::double PRECISION) AS linetotal
           FROM dt_inovar_prod_stg.in_butler_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_butler_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
      UNION ALL
          SELECT
            'SouthWest'::text AS businessunit,
            concat('DALLAS-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(tkt.pototal::double PRECISION) AS linetotal
    	FROM dt_inovar_prod_stg.in_dallas_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_dallas_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT
            'SouthWest'::text AS businessunit,
            concat('DALLAS-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(tkt.misccharge::double PRECISION) AS linetotal
           FROM dt_inovar_prod_stg.in_dallas_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_dallas_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
      UNION ALL
         SELECT
            'SouthEast'::text AS businessunit,
            concat('DAVIE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(tkt.pototal::double PRECISION) AS linetotal
           FROM dt_inovar_prod_stg.in_davie_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_davie_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT
            'SouthEast'::text AS businessunit,
            concat('DAVIE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
			sum(tkt.misccharge::double PRECISION) AS linetotal
         FROM dt_inovar_prod_stg.in_davie_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_davie_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT 
            'NewEngland'::text AS businessunit,
            concat('NE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(tkt.pototal::double PRECISION) AS linetotal
         FROM dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT 
            'NewEngland'::text AS businessunit,
            concat('NE-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(tkt.misccharge::double PRECISION) AS linetotal
        FROM dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_newburyport_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
    UNION ALL
    	SELECT 
         	'Cimarron North'::text AS businessunit,
            concat('CN-', tkt.customernum) AS company_customer_number,
            tkt.orderdate::text,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(tkt.pototal::double PRECISION) AS linetotal
           FROM dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
    UNION ALL
        SELECT 
        	'Cimarron North'::text AS businessunit,
            concat('CN-', tkt.customernum) AS company_customer_number,
            tkt.orderdate::text,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(tkt.misccharge::double PRECISION) AS linetotal
        FROM dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_cimarron_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT
            'Dion Label'::text AS businessunit,
            concat('DL-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(tkt.pototal::double PRECISION) AS linetotal
           FROM dt_inovar_prod_stg.in_westfield_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_westfield_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT
            'Dion Label'::text AS businessunit,
            concat('DL-', tkt.customernum) AS company_customer_number,
            tkt.orderdate,
            COALESCE(cus.company, tkt.customername) AS customername,
            tkt.otsname,
            sum(tkt.misccharge::double PRECISION) AS linetotal
           FROM dt_inovar_prod_stg.in_westfield_fact_ticket tkt
             LEFT JOIN dt_inovar_prod_stg.in_westfield_dim_customer cus ON tkt.customernum::text = cus.number::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT 
         	'Tabco'::text 																AS businessunit,
            concat('KSKA51-T-', tkt_cte.customer_number) 								AS company_customer_number,
            tkt_cte.date_order::text 													AS order_date,
            tkt_cte.customer_name 														AS customername,
            tkt_cte.salesman_name__ 													AS otsname,
            sum(tkt_cte.booking_revenue::double PRECISION) 									AS linetotal
           FROM tabco_final tkt_cte
             LEFT JOIN dt_inovar_prod_stg.in_kansascity_dim_customers cus ON cus.customer_number::text = tkt_cte.customer_number::TEXT
         GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT 
            'Precision Label'::text AS businessunit,
            concat('Carlsbad-', tkt.customerno) AS company_customer_number,
            tkt.orderdate::date::text AS orderdate,
            tkt.billtoname::text AS customername,
            spn.salespersonname AS otsname,
            sum(tktit.lastextensionamt) AS linetotal
           FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader tkt
             LEFT JOIN ( SELECT --id,
                    salesorderno,
                   /* sequenceno,
                    linekey,
                    itemno,
                    itemtype,
                    itemdescription,
                    aliasitemno,
                    promisedate,
                    originalline,
                    cancelledline,
                    cancelreasoncode,
                    purchaseorderno,
                    pricelevel,
                    quantityorderedoriginal,
                    quantityorderedrevised,
                    quantityshipped,
                    quantitybackordered,
                    originalunitprice,
                    lastunitprice,*/
                    lastextensionamt
                    /*unitcost,
                    unitofmeasure,
                    deleteddate,
                    lastsyncdatetime*/
                   FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistorydetail
                  WHERE cancelledline::text = 'N'::text AND deleteddate IS NULL) tktit 
                  		ON tkt.salesorderno::text = tktit.salesorderno::text
             LEFT JOIN dt_inovar_prod_stg.in_fact_sage_ar_salesperson spn ON spn.salespersonno::text = tkt.salespersonno::text
          WHERE tkt.orderdate::date >= '2023-01-01'::date AND tkt.orderstatus::text <> 'X'::TEXT
          GROUP BY 1,2,3,4,5
        UNION ALL
         SELECT 
            'Precision Label'::text AS businessunit,
            concat('Carlsbad-', tkt.customerno) AS company_customer_number,
            tkt.orderdate::text AS orderdate,
            tkt.billtoname AS customername,
            spn.salespersonname AS otsname,
            sum(tktit.extensionamt) AS linetotal
           FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderheader tkt
             LEFT JOIN dt_inovar_prod_stg.in_fact_sage_so_salesorderdetail tktit ON tkt.salesorderno::text = tktit.salesorderno::text
             LEFT JOIN dt_inovar_prod_stg.in_fact_sage_ar_salesperson spn ON spn.salespersonno::text = tkt.salespersonno::text
          WHERE tkt.orderdate >= '2023-01-01'::date AND NOT (tkt.salesorderno::text IN ( SELECT DISTINCT in_fact_sage_so_salesorderhistoryheader.salesorderno
                   FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader))
          GROUP BY 1,2,3,4,5
          ),  
    final_data AS (
         SELECT 
            all_tickets.businessunit,
            all_tickets.company_customer_number,
            all_tickets.orderdate,
            all_tickets.customername,
            all_tickets.otsname,
            all_tickets.linetotal
           FROM all_tickets
          WHERE 
          	all_tickets.orderdate::date <= CURRENT_DATE
          	AND linetotal <> 0
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
          AND linetotal <> 0
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
		businessunit, 
	    company_customer_number,
	    case 
			when trim(to_char(orderdate::date, 'Day')) = 'Saturday' then (orderdate::date - '1 day'::interval)::date::text
	    	when trim(to_char(orderdate::date, 'Day')) = 'Sunday' then (orderdate::date - '2 day'::interval)::date::text
	        else orderdate
	    end 				as orderdate,
	    customername,
	    otsname,
	    sum(linetotal)							as booking_total
	FROM ticket_sandbox
		WHERE orderdate::date >= date_trunc('year', current_date) - INTERVAL '1 year'
			and orderdate::date >='2023-01-01'
		group by 1,2,3,4,5
),
backlogs as (
	select
		businessunit,
	    upper(company_customer_number)														as company_customer_number,
		orderdate,
	    customername,
	    backlog_type,
	    case when ship_by_date = '' then null::date else ship_by_date::date end as ship_by_date,
	    snapshot_date::date,
	    sum(backlog_amount)				as backlog_amount
	from dt_inovar_prod_stg.in_backlog_sandbox_snapshot
	where snapshot_date::date >='2023-01-01'
	AND businessunit NOT IN ('Cimarron South', 'Cimarron Aberdeen')
	group by 1,2,3,4,5,6,7
),
budget as (
	select 
		businessunit ,
		upper(company_customer_number) AS company_customer_number,
		orderdate ,
		invoice_date ,
		customername,
		sum(budget)						as budget
	from dt_inovar_prod_edw.u_mat_inovar_budget umib
	where invoice_date::date >='2023-01-01'
	and businessunit is not null 
	and record_type ='BU Budget'
	group by 1,2,3,4,5
),
final_union as(
-- Invoice Data
	select 
		final_working_days.dt,
		final_working_days.businessunit,
		final_working_days.holiday,
		final_working_days.total_work_days,
		final_working_days.work_days,
		final_working_days.month_day_count,
		final_working_days.month_workday_count,
		'Invoice'								AS record_type,
		invoice.invoice_date::date,
		null::date 						as order_date,
		null::date 					as snapshot_date,
		invoice.company_customer_number,
		customername,
		null::date 							as ship_by_date,
		null::text 							as backlog_type,
		invoice.invoice_revenue,
		0::float    						as booking_total,
		0::float						as backlog_amount,
		0::float						as budget
		from final_working_days 
			left join invoice on invoice.invoice_date::date = final_working_days.dt and invoice.businessunit = final_working_days.businessunit	
			where final_working_days.dt::date>=current_date::date
 union all 
	select 
		final_working_days.dt,
		final_working_days.businessunit,
		final_working_days.holiday,
		final_working_days.total_work_days,
		final_working_days.work_days,
		final_working_days.month_day_count,
		final_working_days.month_workday_count,		
		'Bookings'						as record_type,
		null::date 						as  invoice_date,			
		booking.orderdate::date			as order_date,
		null::date 					as snapshot_date,	
		booking.company_customer_number,
		customername,
		null::date 							as ship_by_date,
		null 							as backlog_type,
		null 							as invoice_revenue,
		booking.booking_total::float ,
		null::float						as backlog_amount,
		null::float						as budget
		from final_working_days
			left join booking on booking.orderdate::date= final_working_days.dt and booking.businessunit = final_working_days.businessunit
			where final_working_days.dt::date>=current_date::date
 union all
 	select 
 		final_working_days.dt,
		final_working_days.businessunit,
		final_working_days.holiday,
		final_working_days.total_work_days,
		final_working_days.work_days,
		final_working_days.month_day_count,
		final_working_days.month_workday_count,
		'Backlogs'																			AS record_type,
		null::date 						as invoice_date,
		null::date 						as order_date,
		backlogs.snapshot_date::date,
		company_customer_number,
		customername,
		null::date 						as ship_by_date,		
		backlogs.backlog_type,
		null 							as invoice_revenue,
		null::float     						as booking_total,
		backlogs.backlog_amount::float	as backlog_amount,
		null::float						as budget				
	from final_working_days
		left join backlogs on backlogs.snapshot_date::date = final_working_days.dt and  backlogs.businessunit = final_working_days.businessunit
 union all
 	select 
		final_working_days.dt,
		final_working_days.businessunit,
		final_working_days.holiday,
		final_working_days.total_work_days,
		final_working_days.work_days,
		final_working_days.month_day_count,
		final_working_days.month_workday_count,
		'BU Budget'						AS record_type,
		budget.invoice_date	::date,
		null::date 						as order_date,	
		null::date						as snapshot_date,
		budget.company_customer_number,
		customername,
		null::date 						as ship_by_date,
		null							as backlog_type,
		null 							as invoice_revenue,
		null::float     						as booking_total,
		null::float						as backlog_amount,
		budget.budget::float			as budget
	from final_working_days
		left join budget on budget.invoice_date::date = final_working_days.dt  and budget.businessunit = final_working_days.businessunit
		where final_working_days.dt::date>=current_date::date
),
end_market as (
	select * from dt_inovar_prod_stg.in_gs_customer_end_market where company_customer_number is not null
	),final_cte AS (
	SELECT  
		final_union.*,
		end_market.industry
	FROM final_union
		left join end_market on trim(lower(end_market.company_customer_number)) = trim(lower(final_union.company_customer_number))
)
select 
		dt,
		businessunit,
		holiday,
		total_work_days,
		work_days,
		month_day_count,
		month_workday_count,
		record_type,
		invoice_date,
		order_date,
		snapshot_date,
		company_customer_number,
		case
	    	when trim(customername) = '' then 'Unmapped'
	    	else coalesce(lower(trim(customername)),'Unmapped')
	    end							as	customername,
	    case
	    	when trim(industry) = '' then 'Unmapped'
	    	else coalesce(lower(trim(industry)),'Unmapped')
	    end							as 	industry,
		ship_by_date,
		backlog_type,
		invoice_revenue,
		booking_total,
		backlog_amount,
		budget,
		case 
			when record_type = 'Invoice' then 1 
			else 0 
		end																		as is_invoice,
		case 
			when record_type = 'Bookings' then 1 
			else 0 
		end																		as is_bookings,
		case 
			when record_type = 'Backlogs' then 1 
			else 0 
		end																		as is_backlogs,
		case 
			when record_type = 'BU Budget' then 1 
			else 0 
		end																		as is_budget,
		case
			when record_type = 'Backlogs' and (lower(backlog_type) ilike '%custom%' or lower(backlog_type) ilike '%stock%') and date_trunc('month', ship_by_date) <= date_trunc('month', dt) then 1
			else 0
		end																		as is_sales_due_now,
		case
			when record_type = 'Backlogs' and (lower(backlog_type) ilike '%custom%' or lower(backlog_type) ilike '%stock%') and date_trunc('month', ship_by_date) > date_trunc('month', dt) then 1
			else 0
		end																		as is_sales_due_later,
		case
			when record_type = 'Backlogs' and (lower(backlog_type) ilike '%custom%' or lower(backlog_type) ilike '%shelf%') and date_trunc('month', ship_by_date) <= date_trunc('month', dt) then 1
			else 0
		end																		as is_production_backlog_due_now,
		case
			when record_type = 'Backlogs' and (lower(backlog_type) ilike '%custom%' or lower(backlog_type) ilike '%shelf%') and date_trunc('month', ship_by_date) > date_trunc('month', dt) then 1
			else 0																
		end	as is_production_backlog_due_later
			from final_cte
)a
where invoice_date::date>'2023-01-01' and invoice_date::date<='2024-04-30'
group by 1
			
			--------------------------- end--------------------------------