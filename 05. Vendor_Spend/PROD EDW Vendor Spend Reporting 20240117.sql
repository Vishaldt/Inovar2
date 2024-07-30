

--drop view dt_inovar_prod_edw.v_vendor_spend_reporting



--create or replace
--view dt_inovar_prod_edw.v_vendor_spend_reporting as
with vendor as (		
	SELECT 
		CASE
			WHEN entity = 'Butler' 				THEN 'Milwaukee'
			WHEN entity = 'Dallas' 				THEN 'Dallas'
			WHEN entity = 'Davie' 				THEN 'Ft. Lauderdale'
			WHEN entity = 'NewBuryPort' 		THEN 'Newburyport'
			WHEN entity = 'Cimarron North' 		THEN 'Cimarron North'
			WHEN entity = 'Amherst' 			THEN 'Amherst Label'
			WHEN entity = 'Westfield' 			THEN 'Westfield'
			WHEN entity = 'SiouxFalls_LA' 		THEN 'Sioux Falls - LA'
		END																		AS businessunit,
		CASE
			WHEN entity = 'Butler' 				THEN concat('FG-', number)
			WHEN entity = 'Dallas' 				THEN concat('DALLAS-', number)
			WHEN entity = 'Davie' 				THEN concat('DAVIE-', number)
			WHEN entity = 'NewBuryPort' 		THEN concat('NE-', number)
			WHEN entity = 'Cimarron North' 		THEN concat('CN-', number)
			WHEN entity = 'Amherst' 			THEN concat('AL-', number)
			WHEN entity = 'Westfield' 			THEN concat('DL-', number)
			WHEN entity = 'SiouxFalls_LA' 		THEN concat('LA-', number)
		END																		AS company_vendor_number,
		CASE 
			WHEN trim(company) = '' THEN 'Unmapped'
			ELSE coalesce(company, 'Unmapped')
		END																		AS vendor_name,
		suptype 																AS category,
		discountpercent::float,
		terms,
		CASE
			WHEN inactive = 'f' OR inactive = 'false' THEN 'False'
			WHEN inactive = 't' OR inactive = 'true' THEN 'True'
		END 																	AS inactive
	FROM dt_inovar_prod_edw.dim_supplier
		WHERE entity IN ('Butler', 'Dallas', 'Davie', 'NewBuryPort', 'Cimarron North', 'Amherst', 'Westfield', 'SiouxFalls_LA')
	),
ap_invoice as (
	SELECT 
		CASE
			WHEN entity = 'Butler' 				THEN 'Milwaukee'
			WHEN entity = 'Dallas' 				THEN 'Dallas'
			WHEN entity = 'Davie' 				THEN 'Ft. Lauderdale'
			WHEN entity = 'NewBuryPort' 		THEN 'Newburyport'
			WHEN entity = 'Cimarron North' 		THEN 'Cimarron North'
			WHEN entity = 'Amherst' 			THEN 'Amherst Label'
			WHEN entity = 'Westfield' 			THEN 'Westfield'
			WHEN entity = 'SiouxFalls_LA' 		THEN 'Sioux Falls - LA'
		END																		AS businessunit,
		CASE
			WHEN entity = 'Butler' 				THEN concat('FG-', id)
			WHEN entity = 'Dallas' 				THEN concat('DALLAS-', id)
			WHEN entity = 'Davie' 				THEN concat('DAVIE-', id)
			WHEN entity = 'NewBuryPort' 		THEN concat('NE-', id)
			WHEN entity = 'Cimarron North' 		THEN concat('CN-', id)
			WHEN entity = 'Amherst' 			THEN concat('AL-', id)
			WHEN entity = 'Westfield' 			THEN concat('DL-', id)
			WHEN entity = 'SiouxFalls_LA' 		THEN concat('LA-', id)
		END																		AS company_ap_invoice_number,
		CASE
			WHEN entity = 'Butler' 				THEN concat('FG-', supplier_id)
			WHEN entity = 'Dallas' 				THEN concat('DALLAS-', supplier_id)
			WHEN entity = 'Davie' 				THEN concat('DAVIE-', supplier_id)
			WHEN entity = 'NewBuryPort' 		THEN concat('NE-', supplier_id)
			WHEN entity = 'Cimarron North' 		THEN concat('CN-', supplier_id)
			WHEN entity = 'Amherst' 			THEN concat('AL-', supplier_id)
			WHEN entity = 'Westfield' 			THEN concat('DL-', supplier_id)
			WHEN entity = 'SiouxFalls_LA' 		THEN concat('LA-', supplier_id)
		END																		AS company_vendor_number,
		CASE
			WHEN entity = 'Butler' 				THEN concat('FG-', invoice_number)
			WHEN entity = 'Dallas' 				THEN concat('DALLAS-', invoice_number)
			WHEN entity = 'Davie' 				THEN concat('DAVIE-', invoice_number)
			WHEN entity = 'NewBuryPort' 		THEN concat('NE-', invoice_number)
			WHEN entity = 'Cimarron North' 		THEN concat('CN-', invoice_number)
			WHEN entity = 'Amherst' 			THEN concat('AL-', invoice_number)
			WHEN entity = 'Westfield' 			THEN concat('DL-', invoice_number)
			WHEN entity = 'SiouxFalls_LA' 		THEN concat('LA-', invoice_number)
		END																		AS company_invoice_number,
		invoice_date::date,
		CASE 
			WHEN trim(from_po_number) = '' THEN NULL
			ELSE 
				CASE
					WHEN entity = 'Butler' 				THEN concat('FG-', from_po_number)
					WHEN entity = 'Dallas' 				THEN concat('DALLAS-', from_po_number)
					WHEN entity = 'Davie' 				THEN concat('DAVIE-', from_po_number)
					WHEN entity = 'NewBuryPort' 		THEN concat('NE-', from_po_number)
					WHEN entity = 'Cimarron North' 		THEN concat('CN-', from_po_number)
					WHEN entity = 'Amherst' 			THEN concat('AL-', from_po_number)
					WHEN entity = 'Westfield' 			THEN concat('DL-', from_po_number)
					WHEN entity = 'SiouxFalls_LA' 		THEN concat('LA-', from_po_number)
				END	
		END																		AS company_po_number
	FROM dt_inovar_prod_edw.fact_ap_invoice
		WHERE entity IN ('Butler', 'Dallas', 'Davie', 'NewBuryPort', 'Cimarron North', 'Amherst', 'Westfield', 'SiouxFalls_LA')
		AND invoice_date::date >= '2021-01-01'
	),
purchase_order as (
	SELECT 
		CASE
			WHEN entity = 'Butler' 				THEN 'Milwaukee'
			WHEN entity = 'Dallas' 				THEN 'Dallas'
			WHEN entity = 'Davie' 				THEN 'Ft. Lauderdale'
			WHEN entity = 'NewBuryPort' 		THEN 'Newburyport'
			WHEN entity = 'Cimarron North' 		THEN 'Cimarron North'
			WHEN entity = 'Amherst' 			THEN 'Amherst Label'
			WHEN entity = 'Westfield' 			THEN 'Westfield'
			WHEN entity = 'SiouxFalls_LA' 		THEN 'Sioux Falls - LA'
		END																		AS businessunit,
		CASE
			WHEN entity = 'Butler' 				THEN concat('FG-', ponumber)
			WHEN entity = 'Dallas' 				THEN concat('DALLAS-', ponumber)
			WHEN entity = 'Davie' 				THEN concat('DAVIE-', ponumber)
			WHEN entity = 'NewBuryPort' 		THEN concat('NE-', ponumber)
			WHEN entity = 'Cimarron North' 		THEN concat('CN-', ponumber)
			WHEN entity = 'Amherst' 			THEN concat('AL-', ponumber)
			WHEN entity = 'Westfield' 			THEN concat('DL-', ponumber)
			WHEN entity = 'SiouxFalls_LA' 		THEN concat('LA-', ponumber)
		END																		AS company_po_number,
		description,
		orderstocknum,
		mfgspec,
		quantity::float,
		qcotype,
		potype,
		adhesive,
		costmsi::float,
		lamstock,
		CASE 
			WHEN trim(received) = '' THEN NULL 
			ELSE received::date
		END																		AS received_date,
		CASE 
			WHEN trim(requesteddeliverydate) = '' THEN NULL 
			ELSE requesteddeliverydate::date
		END																		AS requested_delivery_date
	FROM dt_inovar_prod_edw.fact_purchaseorder
		WHERE entity IN ('Butler', 'Dallas', 'Davie', 'NewBuryPort', 'Cimarron North', 'Amherst', 'Westfield', 'SiouxFalls_LA')
),
ap_invoice_line as (
	SELECT 
		CASE
			WHEN entity = 'Butler' 				THEN 'Milwaukee'
			WHEN entity = 'Dallas' 				THEN 'Dallas'
			WHEN entity = 'Davie' 				THEN 'Ft. Lauderdale'
			WHEN entity = 'NewBuryPort' 		THEN 'Newburyport'
			WHEN entity = 'Cimarron North' 		THEN 'Cimarron North'
			WHEN entity = 'Amherst' 			THEN 'Amherst Label'
			WHEN entity = 'Westfield' 			THEN 'Westfield'
			WHEN entity = 'SiouxFalls_LA' 		THEN 'Sioux Falls - LA'
		END																		AS businessunit,		
		CASE
			WHEN entity = 'Butler' 				THEN concat('FG-', ap_invoice_id)
			WHEN entity = 'Dallas' 				THEN concat('DALLAS-', ap_invoice_id)
			WHEN entity = 'Davie' 				THEN concat('DAVIE-', ap_invoice_id)
			WHEN entity = 'NewBuryPort' 		THEN concat('NE-', ap_invoice_id)
			WHEN entity = 'Cimarron North' 		THEN concat('CN-', ap_invoice_id)
			WHEN entity = 'Amherst' 			THEN concat('AL-', ap_invoice_id)
			WHEN entity = 'Westfield' 			THEN concat('DL-', ap_invoice_id)
			WHEN entity = 'SiouxFalls_LA' 		THEN concat('LA-', ap_invoice_id)
		END																		AS company_ap_invoice_number,	
		CASE
			WHEN entity = 'Butler' 				THEN concat('FG-', id)
			WHEN entity = 'Dallas' 				THEN concat('DALLAS-', id)
			WHEN entity = 'Davie' 				THEN concat('DAVIE-', id)
			WHEN entity = 'NewBuryPort' 		THEN concat('NE-', id)
			WHEN entity = 'Cimarron North' 		THEN concat('CN-', id)
			WHEN entity = 'Amherst' 			THEN concat('AL-', id)
			WHEN entity = 'Westfield' 			THEN concat('DL-', id)
			WHEN entity = 'SiouxFalls_LA' 		THEN concat('LA-', id)
		END																		AS company_ap_invoice_line_number,	
		CASE
			WHEN entity = 'Butler' 				THEN concat('FG-', po_number)
			WHEN entity = 'Dallas' 				THEN concat('DALLAS-', po_number)
			WHEN entity = 'Davie' 				THEN concat('DAVIE-', po_number)
			WHEN entity = 'NewBuryPort' 		THEN concat('NE-', po_number)
			WHEN entity = 'Cimarron North' 		THEN concat('CN-', po_number)
			WHEN entity = 'Amherst' 			THEN concat('AL-', po_number)
			WHEN entity = 'Westfield' 			THEN concat('DL-', po_number)
			WHEN entity = 'SiouxFalls_LA' 		THEN concat('LA-', po_number)
		END																		AS company_po_number,
		accountnumber,
		split_part(accountnumber, '-', 1)										AS account,
		accountname,
		amount 
	FROM dt_inovar_prod_edw.fact_ap_invoice_line 
		WHERE entity IN ('Butler', 'Dallas', 'Davie', 'NewBuryPort', 'Cimarron North', 'Amherst', 'Westfield', 'SiouxFalls_LA')
		AND accountnumber <> ''
	),
final_data AS (
select
	vendor.businessunit,
	vendor.company_vendor_number,
	coalesce(vendor_master.mapped_vendor_name, vendor.vendor_name) as vendor_name,
	COALESCE(vendor_master.vendor_category, 'LabelTraxx-Unmapped') as category,
	vendor.discountpercent,
	vendor.terms,
	vendor.inactive,
	purchase_order.description,
	purchase_order.orderstocknum,
	purchase_order.mfgspec,
	purchase_order.quantity,
	purchase_order.qcotype,
	purchase_order.potype,
	purchase_order.adhesive,
	purchase_order.costmsi,
	purchase_order.lamstock,
	purchase_order.received_date,
	purchase_order.requested_delivery_date,
	ap_invoice.company_ap_invoice_number,
	ap_invoice.company_invoice_number,
	ap_invoice.invoice_date,
	ap_invoice.company_po_number,
	ap_invoice_line.accountnumber,
	ap_invoice_line.account,
	ap_invoice_line.accountname,
	ap_invoice_line.amount
from
	vendor
join ap_invoice on
	ap_invoice.company_vendor_number = vendor.company_vendor_number
left join purchase_order on
	ap_invoice.company_po_number = purchase_order.company_po_number
left join ap_invoice_line on
	ap_invoice_line.company_ap_invoice_number = ap_invoice.company_ap_invoice_number
left join dt_inovar_prod_stg.in_gs_vendor_master vendor_master on 
	vendor_master.company_vendor_number = vendor.company_vendor_number
--where
--	(
--	(ap_invoice_line.businessunit <> 'Cimarron North' AND  ap_invoice_line.account::int >= 40000)
--	OR
--	(ap_invoice_line.businessunit = 'Cimarron North' AND (ap_invoice_line.account::int >= 40000 OR ap_invoice_line.account::int = 12000))
--	)
)
SELECT * FROM final_data
--WHERE businessunit <> 'Sioux Falls - LA'
UNION ALL
SELECT * FROM dt_inovar_prod_edw.v_vendor_spend_reporting_tabco
UNION ALL 
SELECT * FROM dt_inovar_prod_edw.v_vendor_spend_reporting_precision;

