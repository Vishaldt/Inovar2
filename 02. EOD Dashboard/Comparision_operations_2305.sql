-----24-05
SELECT date_trunc('year', dt)::Date,  sum(footused), sum(footused_tabco_ex_booklet), sum(footused_tabco_good_ex_booklet), sum(elapsed) 
FROM dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard 
WHERE is_operations = 1
  and dt::date >='2023-01-01' and dt::date <=current_date 
GROUP BY 1
ORDER BY 1


SELECT date_trunc('year', dt)::Date,  sum(footused), sum(footused_tabco_ex_booklet), sum(footused_tabco_good_ex_booklet), sum(elapsed) 
FROM dt_inovar_prod_edw.u_mat_performance_executive_daily_flash 
WHERE is_operations = 1
  and dt::date >='2023-01-01' and dt::date <=current_date 
GROUP BY 1
ORDER BY 1


SELECT date_trunc('year', sdate)::Date, sum(footused), sum(footused_tabco_ex_booklet), sum(footused_tabco_good_ex_booklet), sum(elapsed) 
FROM dt_inovar_prod_edw.u_mat_inovar_operations 
GROUP BY 1
ORDER BY 1



















SELECT date_trunc('year', dt)::Date,  sum(footused), sum(footused_tabco_ex_booklet), sum(footused_tabco_good_ex_booklet), sum(elapsed) 
FROM dt_inovar_prod_edw.u_mat_performance_executive_daily_flash 
WHERE is_operations = 1
  and dt::date >='2023-01-01' and dt::date <=current_date 
GROUP BY 1
ORDER BY 1


select businessunit,
    sum(backlog_amount) backlog_amount,
    sum(budget) budget,
    sum(backlog_amount) backlog_amount,
    sum(invoice_revenue) invoice_revenue
    FROM  dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard 
WHERE is_operations = 1
  and dt::date >='2023-01-01' and dt::date <=current_date 
group by 1




-----backlog---------------
select
	businessunit,
    sum(backlog_amount)
from dt_inovar_prod_stg.in_backlog_sandbox_snapshot
where snapshot_date::date >='2023-01-01' and snapshot_date::date <='2024-04-30'
group by 1




select businessunit,
    sum(backlog_amount) backlog_amount,
    sum(budget) budget,
    sum(backlog_amount) backlog_amount,
    sum(invoice_revenue) invoice_revenue
    from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard
    where dt::date >='2023-01-01' and dt::date <=current_date 
group by 1

-------------

select businessunit,   
	sum(backlog_amount) backlog_amount,
    sum(budget) budget,
    sum(backlog_amount) backlog_amount,
    sum(invoice_revenue) invoice_revenue from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard
    where dt::date >='2023-01-01' and dt::date <=current_date 
group by 1

-----backlog---------------
---***********


select * from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard
    where dt::date >='2023-01-01' and dt::date <='2024-04-30'









----------------------------budget
select 
	businessunit ,
	sum(budget )	
from dt_inovar_prod_edw.u_mat_inovar_budget umib 
where invoice_date::date >='2021-01-01'
and record_type ='BU Budget'
group by 1



select 
	businessunit ,
	sum(budget )
from dt_inovar_dev_stg.u_mat_6_invoice_sandbox_temp
where invoice_date::date>'2023-01-01' and invoice_date::date<='2024-04-30'
and record_type ='BU Budget'
group by 1

select 
	businessunit ,
	sum(budget)						as budget
from dt_inovar_prod_edw.u_mat_inovar_budget umib
where invoice_date::date>'2023-01-01' and invoice_date::date<='2024-04-30'
and record_type ='BU Budget'
group by 1

----------------------------budget








select * from dt_inovar_dev_stg.in_backlog_sandbox_snapshot_0509_test

select businessunit ,sum(invoice_revenue::float) invoice_revenue
from dt_inovar_prod_edw.u_mat_executive_daily_flash umedf 
where invoice_date::date='2024-04-30'
and ( invoice_item_id not in ('Discounts_Pure','Accounts Receivable – trade','Unexpected','Sales_Mix','Discounts_Mix')
or  invoice_item_id is null) and (invoice_type <> 'CM' or invoice_type is null) /*and line_item_type = 'Invoice Header'*/
 and (invoice_type <> 'CM' or invoice_type is null)
group by 1




select businessunit ,sum(invoice_revenue::float) invoice_revenue
from dt_inovar_prod_edw.u_mat_executive_daily_flash umedf 
where invoice_date::date='2024-04-30'
and ( invoice_item_id not in ('Discounts_Pure','Accounts Receivable – trade','Unexpected','Sales_Mix','Discounts_Mix')
or  invoice_item_id is null) and (invoice_type <> 'CM' or invoice_type is null) /*and line_item_type = 'Invoice Header'*/
 /*and (invoice_type <> 'CM' or invoice_type is null)*/
group by 1
/*


select businessunit ,sum(invoice_item_total::float) invoice_revenue
from dt_inovar_dev_stg.u_mat_invoice_sandbox_temp
where invoice_date::date>'2023-01-01' and invoice_date::date<='2024-04-30'
and ( invoice_item_id not in ('Discounts_Pure','Accounts Receivable – trade','Unexpected','Sales_Mix','Discounts_Mix')
or  invoice_item_id is null) /*
and line_item_type = 'Invoice Header'*/
 and (invoice_type <> 'CM' or invoice_type is null)
group by 1*/


select businessunit ,sum(invoice_revenue) invoice_revenue
from  dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard
where invoice_date::date='2024-04-30'
group by 1




























select businessunit ,sum(invoice_revenue) invoice_revenue
from 
(
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
        WHERE sih.invoicedate >= '2020-01-01'::date and not (sih.invoiceno::text = '0063323' and sih.nontaxablesalesamt = 0) and not (sg.accountdesc ilike 'Due to%' and sg.account <> '22050-000')
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
        WHERE sih.invoicedate >= '2020-01-01'::date and not (sih.invoiceno::text = '0063323' and sih.nontaxablesalesamt = 0)
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
	),
	end_market as (
	select * from dt_inovar_prod_stg.in_gs_customer_end_market where company_customer_number is not null
	)
	select
		'Invoice'																				AS record_type,
		inv.businessunit,
		inv.company_customer_number,
		inv.invoice_date,
	    inv.invoice_customer_name																as customername,
	    sum(inv.invoice_item_total)																as invoice_revenue,
		0::float																				AS budget
		--end_market.industry
	from final_pure_sales inv
	left join end_market
		on trim(lower(end_market.company_customer_number)) = trim(lower(inv.company_customer_number))
	where invoice_date::date>='2021-01-01'
	group by 2,3,4,5)a
	group by 1