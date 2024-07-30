select * from dt_inovar_prod_edw.fact_purchaseorder


select *  from dt_inovar_prod_edw.u_mat_vendor_spend_reporting
where businessunit= 'Dallas'

select apmasterid, apname, sytermsid, apaccttype,  * from dt_inovar_prod_stg.aberdeen_vendor
where apmasterid = '13724'--ap id, name

select * from dt_inovar_prod_stg.aberdeen_terms at2 

select * from dt_inovar_prod_stg.aberdeen_vendortype 

select distinct active  from
dt_inovar_prod_stg.aberdeen_vendor 


select *  from
dt_inovar_prod_stg.aberdeen_payment 


select count(1), count(distinct apmasterid) apmasterid, count(distinct pomasterid) pomasterid FROM dt_inovar_prod_stg.aberdeen_purchaseorder 


select ioid ,armasterid ,pomasterid ,apmasterid  ,* FROM dt_inovar_prod_stg.aberdeen_purchaseorder 


left join dt_inovar_prod_stg.aberdeen_payment payment
on po.id


select id,ioid,id  from
dt_inovar_prod_stg.aberdeen_payment 


-----------------------------Purchase Order ------------------------

select pomasterid	as company_po_number,
	null as  description,
    null as orderstocknum,
    null as mfgspec,
    null as quantity,
    null as qcotype,
    null as potype,
    null as adhesive,
    null as costmsi,
    null as lamstock,
	podateentered 		as received_date,
	podatelastreceipt	as 	requested_delivery_date
 from
dt_inovar_prod_stg.aberdeen_purchaseorder po



SELECT concat('DALLAS-', in_dallas_fact_purchaseorder.ponumber) AS company_po_number,
            in_dallas_fact_purchaseorder.description,
            in_dallas_fact_purchaseorder.orderstocknum,
            in_dallas_fact_purchaseorder.mfgspec,
            in_dallas_fact_purchaseorder.quantity::double precision AS quantity,
            in_dallas_fact_purchaseorder.qcotype,
            in_dallas_fact_purchaseorder.potype,
            in_dallas_fact_purchaseorder.adhesive,
            in_dallas_fact_purchaseorder.costmsi::double precision AS costmsi,
            in_dallas_fact_purchaseorder.lamstock,
                CASE
                    WHEN TRIM(BOTH FROM in_dallas_fact_purchaseorder.received) = ''::text THEN NULL::date
                    ELSE in_dallas_fact_purchaseorder.received::date
                END AS received_date,
                CASE
                    WHEN TRIM(BOTH FROM in_dallas_fact_purchaseorder.requesteddeliverydate) = ''::text THEN NULL::date
                    ELSE in_dallas_fact_purchaseorder.requesteddeliverydate::date
                END AS requested_delivery_date
           FROM dt_inovar_dev_stg.in_dallas_fact_purchaseorder
           
           
           select *  FROM dt_inovar_dev_stg.in_dallas_fact_purchaseorder
----------------------------------------------------------------------------------------------
           
           
           
-----------Vendor ----------------------------------------------------------------------------

select apmasterid	as company_vendor_number,
		apname		as vendor_name, 
		--vendor.sytermsid,
		null AS category,
		null AS discountpercent,
		terms.sytdescription as terms,
		vendor.active /*,
		apaccttype,  vendor.* */
		from dt_inovar_prod_stg.aberdeen_vendor vendor
		left join dt_inovar_prod_stg.aberdeen_terms terms on vendor.sytermsid = terms.sytermsid

		--where apmasterid = '13724'--ap id, name           
           
           
SELECT 'Dallas'::text AS businessunit,
            concat('DALLAS-', in_dallas_dim_supplier.number) AS company_vendor_number,
                CASE
                    WHEN TRIM(BOTH FROM in_dallas_dim_supplier.company) = ''::text THEN 'Unmapped'::character varying
                    ELSE COALESCE(in_dallas_dim_supplier.company, 'Unmapped'::character varying)
                END AS vendor_name,
            in_dallas_dim_supplier.suptype AS category,
            in_dallas_dim_supplier.discountpercent::double precision AS discountpercent,
            in_dallas_dim_supplier.terms,
            in_dallas_dim_supplier.inactive
           FROM dt_inovar_dev_stg.in_dallas_dim_supplier

------------------------------------------------------------------------------           
           

           
           
           select count(distinct apmasterid) ,count(1) from
dt_inovar_prod_stg.aberdeen_purchaseorder
           
----------------ap_invoice

           select * from  dt_inovar_prod_stg.aberdeen_invoice
           
        
           
           
select po.pomasterid	AS company_po_number,
	po.apmasterid, po.purchaseordertype	as potype, po.poorderstatus,po_status.sysdescription, po.podateentered, po.podatelastreceipt,  po.poordertotal, po.sytermsid, po.companyname,
po.*  from
dt_inovar_prod_stg.aberdeen_purchaseorder po
left join dt_inovar_prod_stg.aberdeen_postatus  po_status on po_status.sysstatusid = po.poorderstatus

----------------------------------------------------------------------------------------------

select pomasterid,*
 from
dt_inovar_prod_stg.aberdeen_purchaseorder po


select * from dt_inovar_prod_stg.aberdeen_vendor 


select * from dt_inovar_prod_stg.in_aberdeen_vend




select distinct poorderstatus from
dt_inovar_prod_stg.aberdeen_purchaseorder



select * from dt_inovar_prod_stg.aberdeen_purchaseordertype ap

select * from dt_inovar_prod_stg.aberdeen_postatus  





         SELECT concat('FG-', in_butler_fact_ap_invoice.id) AS company_ap_invoice_number,
            concat('FG-', in_butler_fact_ap_invoice.supplier_id) AS company_vendor_number,
            concat('FG-', in_butler_fact_ap_invoice.invoice_number) AS company_invoice_number,
            in_butler_fact_ap_invoice.invoice_date::date AS invoice_date,
                CASE
                    WHEN TRIM(BOTH FROM in_butler_fact_ap_invoice.frompo_number) = ''::text THEN NULL::text
                    ELSE concat('FG-', in_butler_fact_ap_invoice.frompo_number)
                END AS company_po_number
           FROM dt_inovar_dev_stg.in_butler_fact_ap_invoice
          WHERE in_butler_fact_ap_invoice.invoice_date::date >= '2021-01-01'::date
          
 

select * from  dt_inovar_prod_stg.abeerdeen_
          
          
          
          
       --   payment and payment line table
          
          
          select * from dt_inovar_prod_stg.aberdeen_payment 
          
          
          select distinct id from dt_inovar_prod_stg.aberdeen_payment
          
          
          select * from dt_inovar_prod_stg.in_aberdeen_fact_payment
          
          select count(distinct id) from dt_inovar_prod_stg.in_aberdeen_fact_payment
          
          --------------
          
          select * from dt_inovar_prod_stg.in_aberdeen_fact_paymentline
          
          select count(distinct ) from dt_inovar_prod_stg.in_aberdeen_fact_paymentline
          
          --------------------------------------------------------------------------------------------
          
          select * from dt_inovar_prod_stg.in_aberdeen_dim_vendor
          
          select count(distinct apmasterid ) from dt_inovar_prod_stg.in_aberdeen_dim_vendor
          
          --------------------------------------------------------------------------------------------
          
          select * from dt_inovar_prod_stg.in_aberdeen_fact_purchaseorder
          
          select count()
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          
          