
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
--             LEFT JOIN (select sales_rep_no, otsname from dt_inovar_prod_edw.u_mat_inovar_salesrep_master where businessunit = 'Milwaukee') salesrep ON salesrep.sales_rep_no = inv.salesrepno
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
            inv.customer_number,
            inv.customer_name,
            inv.financial_statement_class,
            inv.invoice_item_total
           FROM invoices inv
), 
    invoice_header_item_union AS (
         SELECT 
            final_invoice_ps_ticket_join.businessunit,
            final_invoice_ps_ticket_join.company_customer_number,
            final_invoice_ps_ticket_join.invoice_date,
            final_invoice_ps_ticket_join.customer_number,
            final_invoice_ps_ticket_join.customer_name,
            final_invoice_ps_ticket_join.financial_statement_class,
            final_invoice_ps_ticket_join.invoice_item_total
           FROM final_invoice_ps_ticket_join
        UNION ALL
         SELECT 
            ot.businessunit,
            ot.company_customer_number,
            ot.invoice_date,
            ot.customer_number,
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
                  WHERE in_gs_company_mapping.record_type::text = 'BU'::text) map_bu ON inv.businessunit = map_bu.original_value::text),customer_master as (
select 'Milwaukee' as businessunit, number, concat('FG-', number) as company_customer_number, company, otsname, sales_rep_no, itsname, cust_serv_no 
		from dt_inovar_prod_stg.in_butler_dim_customer
),
final_pure_sales AS (
SELECT 
    invoice_sandbox.businessunit,
    upper(invoice_sandbox.company_customer_number)		as company_customer_number,
	   invoice_sandbox.invoice_date,
    trim(initcap(coalesce(cus.company, invoice_sandbox.invoice_customer_name))) 						as invoice_customer_name,
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
	'Invoice'																							AS record_type,
	invoice_sandbox.businessunit,
    upper(invoice_sandbox.company_customer_number)														as company_customer_number,
	   invoice_sandbox.invoice_date,
    trim(initcap(coalesce(cus.company, invoice_sandbox.invoice_customer_name))) 						as invoice_customer_name,
    invoice_sandbox.invoice_item_total
	0::float																							AS budget,
	end_market.industry
from final_pure_sales inv
left join end_market
	on trim(lower(end_market.company_customer_number)) = trim(lower(inv.company_customer_number))
