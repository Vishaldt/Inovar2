/*DROP TABLE dt_inovar_dev_stg.u_mat_vendor_spend_substrate

CREATE TABLE dt_inovar_prod_edw.u_mat_vendor_spend_substrate
AS SELECT * FROM dt_inovar_prod_edw.v_vendor_spend_substrate


--DROP VIEW dt_inovar_prod_edw.v_vendor_spend_substrate

select * from dt_inovar_prod_edw.u_mat_vendor_spend_substrate
where businessunit = 'Sioux Falls - LA'

truncate table dt_inovar_prod_edw.u_mat_vendor_spend_substrate

INSERT INTO dt_inovar_prod_edw.u_mat_vendor_spend_substrate
SELECT *  FROM dt_inovar_prod_edw.v_vendor_spend_substrate	




CREATE OR REPLACE VIEW dt_inovar_prod_edw.v_vendor_spend_substrate AS */
with 
po_item_stock  as(
	select
		'Dallas'							as businessunit,
		po_number,
		sum(orderfootage) 					as totalorderfootage
		from dt_inovar_prod_stg.in_dallas_fact_po_item_stock 
		group by 1,2
	union all 
	select
		'Milwaukee'							as businessunit,
		po_number,
		sum(orderfootage) 					as totalorderfootage
		from dt_inovar_prod_stg.in_butler_fact_po_item_stock 
		group by 1,2
	union all 
	select
		'Ft. Lauderdale'					as businessunit,
		po_number,
		sum(orderfootage) 					as totalorderfootage
		from dt_inovar_prod_stg.in_davie_fact_po_item_stock 
		group by 1,2
	union all 
	select
		'Westfield'							as businessunit,
		po_number,
		sum(orderfootage) 					as totalorderfootage
		from dt_inovar_prod_stg.in_westfield_fact_po_item_stock 
		group by 1,2
	union all
	select
		'Newburyport'						as businessunit,
		po_number,
		sum(orderfootage)					as totalorderfootage
		from dt_inovar_prod_stg.in_newburyport_fact_po_item_stock 
		group by 1,2
	union all 
	select
		'Cimarron North'					as businessunit,
		po_number,
		sum(orderfootage) 					as totalorderfootage
		from dt_inovar_prod_stg.in_cimarron_fact_po_item_stock 
		group by 1,2
	union all 
	select
		'Amherst Label'						as businessunit,
		po_number,
	sum(orderfootage) 						as totalorderfootage
		from dt_inovar_prod_stg.in_amherst_fact_po_item_stock 
		group by 1,2
	union all 
	select
		'Sioux Falls - LA'						as businessunit,
		po_number,
	sum(orderfootage) 						as totalorderfootage
		from dt_inovar_prod_stg.in_siouxfalls_la_fact_po_item_stock 
		group by 1,2
),
po as(
	select 
		'Dallas'							as businessunit,
		podate,
		costmsi::float						as cost_msi_per_PO,
		concat('DALLAS-',suppliernum)							AS company_vendor_number,
		supplier							as vendor_name,
		mfgspec								as mfg_Spec,
		totalpo::float						as total_po_ordered,
		received_total::float				as total_po_received,
		ponumber 							as po_number,
		description,
		orderStockNum						as stock_number,
		adhesive,
		CASE WHEN received = '' THEN NULL::date ELSE received::date END as received_date,
		dateReq								as requested_delivery_date,
		suppliernum
	from dt_inovar_prod_stg.in_dallas_fact_purchaseorder po	
	WHERE qcotype <> 'Void' 
AND potype = 'Stock'
union all
	select 
		'Milwaukee'							as businessunit,
		podate,		
		costmsi::float						as cost_msi_per_PO,
		concat('FG-',suppliernum)							AS company_vendor_number,
		supplier							as vendor_name,
		mfgspec								as mfg_Spec,
		totalpo::float						as total_po_ordered,
		received_total::float				as total_po_received,
		ponumber 							as po_number,
		description,
		orderStockNum						as stock_number,
		adhesive,
		CASE WHEN received = '' THEN NULL::date ELSE received::date END as received_date,
		dateReq								as requested_delivery_date,
		suppliernum
	from dt_inovar_prod_stg.in_butler_fact_purchaseorder po
	WHERE qcotype <> 'Void'
AND potype = 'Stock'
	union all
	select 
		'Ft. Lauderdale'					as businessunit,
		podate,		
		costmsi::float						as cost_msi_per_PO,
		concat('DAVIE-',suppliernum)							AS company_vendor_number,
		supplier							as vendor_name,
		mfgspec								as mfg_Spec,
		totalpo::float						as total_po_ordered,
		received_total::float				as total_po_received,
		ponumber 							as po_number,
		description,
		orderStockNum						as stock_number,
		adhesive,
		CASE WHEN received = '' THEN NULL::date ELSE received::date END as received_date,
		dateReq								as requested_delivery_date,
		suppliernum
	from dt_inovar_prod_stg.in_davie_fact_purchaseorder po
	WHERE qcotype <> 'Void'
AND potype = 'Stock'
	union all
	select 
		'Westfield'							as businessunit,
		podate,		
		costmsi::float						as cost_msi_per_PO,
		concat('DL-',suppliernum)							AS company_vendor_number,
		supplier							as vendor_name,
		mfgspec								as mfg_Spec,
		totalpo::float						as total_po_ordered,
		received_total::float				as total_po_received,
		ponumber 							as po_number,
		description,
		orderStockNum						as stock_number,
		adhesive,
		CASE WHEN received = '' THEN NULL::date ELSE received::date END as received_date,
		dateReq								as requested_delivery_date,
		suppliernum
	from dt_inovar_prod_stg.in_westfield_fact_purchaseorder po
	WHERE qcotype <> 'Void'
AND potype = 'Stock'
	union all
	select 
		'Newburyport'						as businessunit,
		podate,		
		costmsi::float						as cost_msi_per_PO,
		concat('NE-',suppliernum)							AS company_vendor_number,
		supplier							as vendor_name,
		mfgspec								as mfg_Spec,
		totalpo::float						as total_po_ordered,
		received_total::float				as total_po_received,
		ponumber 							as po_number,
		description,
		orderStockNum						as stock_number,
		adhesive,
		CASE WHEN received = '' THEN NULL::date ELSE received::date END as received_date,
		dateReq								as requested_delivery_date,
		suppliernum
	from dt_inovar_prod_stg.in_newburyport_fact_purchaseorder po
	WHERE qcotype <> 'Void'
AND potype = 'Stock'
	union all
	select 
		'Cimarron North' 					as businessunit,
		podate,		
		costmsi::float						as cost_msi_per_PO,
		concat('CN-',suppliernum)							AS company_vendor_number,
		supplier							as vendor_name,
		mfgspec								as mfg_Spec,
		totalpo::float						as total_po_ordered,
		received_total::float				as total_po_received,
		ponumber 							as po_number,
		description,
		orderStockNum						as stock_number,
		adhesive,
		received::date						as received_date,
		dateReq								as requested_delivery_date,
		suppliernum
	from dt_inovar_prod_stg.in_cimarron_fact_purchaseorder po
	WHERE qcotype <> 'Void'
AND potype = 'Stock'
	union all
	select 
		'Amherst Label'						as businessunit,
		podate,		
		costmsi::float						as cost_msi_per_PO,
		concat('AL-',suppliernum)							AS company_vendor_number,
		supplier							as vendor_name,
		mfgspec								as mfg_Spec,
		totalpo::float						as total_po_ordered,
		received_total::float				as total_po_received,
		ponumber 							as po_number,
		description,
		orderStockNum						as stock_number,
		adhesive,
		received::date						as received_date,
		dateReq								as requested_delivery_date,
		suppliernum
	from dt_inovar_prod_stg.in_amherst_fact_purchaseorder po
	WHERE qcotype <> 'Void'
AND potype = 'Stock'
union ALL
	select 
		'Sioux Falls - LA'							as businessunit,
		podate,		
		costmsi::float						as cost_msi_per_PO,
		concat('LA-',suppliernum)							AS company_vendor_number,
		supplier							as vendor_name,
		mfgspec								as mfg_Spec,
		totalpo::float						as total_po_ordered,
		received_total::float				as total_po_received,
		ponumber 							as po_number,
		description,
		orderStockNum						as stock_number,
		adhesive,
		received 							AS received_date,
		dateReq								as requested_delivery_date,
		suppliernum
	from dt_inovar_prod_stg.in_siouxfalls_la_fact_purchaseorder po
	WHERE qcotype <> 'Void'
AND potype = 'Stock'
),
invoice as (
	select 
		'Dallas'							as businessunit,
		frompo_number ,
		sum(invoice_amount::float)			as invoice_amount
	from dt_inovar_prod_stg.in_dallas_fact_ap_invoice 
	group by 1,2
union all 
	select 
		'Milwaukee'							as businessunit,
		frompo_number ,
		sum(invoice_amount::float) 			as invoice_amount
	from dt_inovar_prod_stg.in_butler_fact_ap_invoice 
	group by 1,2
union all 
	select 
		'Ft. Lauderdale'					as businessunit,
		frompo_number ,
		sum(invoice_amount::float) 			as invoice_amount
	from dt_inovar_prod_stg.in_davie_fact_ap_invoice 
	group by 1,2
union all 
	select 
		'Westfield'							as businessunit,
		frompo_number ,
		sum(invoice_amount::float) 			as invoice_amount
	from dt_inovar_prod_stg.in_westfield_fact_ap_invoice 
	group by 1,2
union all 
	select 
		'Newburyport'						as businessunit,
		frompo_number ,
		sum(invoice_amount::float) 			as invoice_amount
	from dt_inovar_prod_stg.in_newburyport_fact_ap_invoice 
	group by 1,2
union all 
	select 
		'Cimarron North'					as businessunit,
		frompo_number ,	
		sum(invoice_amount::float) 			as invoice_amount
	from dt_inovar_prod_stg.in_cimarron_fact_ap_invoice 
	group by 1,2
union all 
	select 
		'Amherst Label'						as businessunit,
		frompo_number ,
		sum(invoice_amount::float) 			as invoice_amount
	from dt_inovar_prod_stg.in_amherst_fact_ap_invoice 
	group by 1,2
union all 
	select 
		'Sioux Falls - LA'					as businessunit,
		frompo_number ,
		sum(invoice_amount::float) 			as invoice_amount
	from dt_inovar_prod_stg.in_siouxfalls_la_fact_ap_invoice 
	group by 1,2
),
invoice_line as (
	select 
		'Dallas'							as businessunit,
		po_number ,
		sum(case
			when accountname ilike '%Freight%' then amount
			else 0
		end)								as freight_cost,
		sum(case
			when accountname not ilike '%Freight%' then amount
			else 0
		end)
											as material_cost
	from dt_inovar_prod_stg.in_dallas_fact_ap_invoice_line ibfail 
	group by po_number
union all 
	select 
		'Milwaukee'							as businessunit,
		po_number ,
		sum(case
			when accountname ilike '%Freight%' then amount
			else 0
		end)								as freight_cost,
		sum(case
			when accountname not ilike '%Freight%' then amount
			else 0
		end)								as material_cost
	from dt_inovar_prod_stg.in_butler_fact_ap_invoice_line ibfail 
	group by po_number
union all 
	select 
		'Ft. Lauderdale'					as businessunit,
		po_number ,
		sum(case
			when accountname ilike '%Freight%' then amount
			else 0
		end)								as freight_cost,
		sum(case
			when accountname not ilike '%Freight%' then amount
			else 0
		end)								as material_cost
	from dt_inovar_prod_stg.in_davie_fact_ap_invoice_line ibfail 
	group by po_number
union all 
	select 
		'Westfield'							as businessunit,
		po_number ,
		sum(case
			when accountname ilike '%Freight%' then amount
			else 0
		end)								as freight_cost,
		sum(case
			when accountname not ilike '%Freight%' then amount
			else 0
		end)								as material_cost
	from dt_inovar_prod_stg.in_westfield_fact_ap_invoice_line ibfail 
	group by po_number
union all 
	select 
		'Newburyport'						as businessunit,
		po_number ,
		sum(case
			when accountname ilike '%Freight%' then amount
			else 0
		end)								as freight_cost,
		sum(case
			when accountname not ilike '%Freight%' then amount
			else 0
		end)								as material_cost
	from dt_inovar_prod_stg.in_newburyport_fact_ap_invoice_line ibfail 
	group by po_number
union all 
	select 
		'Cimarron North'					as businessunit,
		po_number ,
		sum(case
			when accountname ilike '%Freight%' then amount
			else 0
		end)								as freight_cost,
		sum(case
			when accountname not ilike '%Freight%' then amount
			else 0
		end)								as material_cost
	from dt_inovar_prod_stg.in_cimarron_fact_ap_invoice_line ibfail 
	group by po_number
union all 
	select 
		'Amherst Label'		as businessunit,
		po_number ,
		sum(case
			when accountname ilike '%Freight%' then amount
			else 0
		end)
		as freight_cost,
		sum(case
			when accountname not ilike '%Freight%' then amount
			else 0
		end)								as material_cost
	from dt_inovar_prod_stg.in_amherst_fact_ap_invoice_line  
	group by po_number
union all 
	select 
		'Sioux Falls - LA'		as businessunit,
		po_number ,
		sum(case
			when accountname ilike '%Freight%' then amount
			else 0
		end)
		as freight_cost,
		sum(case
			when accountname not ilike '%Freight%' then amount
			else 0
		end)								as material_cost
	from dt_inovar_prod_stg.in_siouxfalls_la_fact_ap_invoice_line  
	group by po_number
),
po_item_stock_detail as (
	select 
		'Dallas' 							as businessunit,
		po_number, 
		rollnum,
		orderfootage,
		cut1,
		numcut1,
		cut2,
		numcut2,
		cut3,
		numcut3,
		cut4,
		numcut4,
		cut5,
		numcut5,
		rolloffcut,
		(
			( orderfootage * 12 *  ( (cut1 * numcut1) + (cut2 * numcut2) + (cut3 * numcut3) + (cut4 * numcut4) + (cut5 * numcut5) )  / 1000 ) + ( orderfootage * 12 * rolloffcut)
		)  									as msi_calculated
	from dt_inovar_prod_stg.in_dallas_fact_po_item_stock
union all 
	select 
		'Milwaukee' 						as businessunit,
		po_number,  
		rollnum,
		orderfootage,
		cut1,
		numcut1,
		cut2,
		numcut2,
		cut3,
		numcut3,
		cut4,
		numcut4,
		cut5,
		numcut5,
		rolloffcut,
		(
			( orderfootage * 12 *  ( (cut1 * numcut1) + (cut2 * numcut2) + (cut3 * numcut3) + (cut4 * numcut4) + (cut5 * numcut5) )  / 1000 ) + ( orderfootage * 12 * rolloffcut)
		) 									as msi_calculated
	from dt_inovar_prod_stg.in_butler_fact_po_item_stock
union all 
	select 
		'Ft. Lauderdale' 					as businessunit,
		po_number,  
		rollnum,
		orderfootage,
		cut1,
		numcut1,
		cut2,
		numcut2,
		cut3,
		numcut3,
		cut4,
		numcut4,
		cut5,
		numcut5,
		rolloffcut,
		(
			( orderfootage * 12 *  ( (cut1 * numcut1) + (cut2 * numcut2) + (cut3 * numcut3) + (cut4 * numcut4) + (cut5 * numcut5) )  / 1000 ) + ( orderfootage * 12 * rolloffcut)
		)  									as msi_calculated
	from dt_inovar_prod_stg.in_davie_fact_po_item_stock
union all 
	select 
		'Westfield' 						as businessunit,
		po_number,  
		rollnum,
		orderfootage,
		cut1,
		numcut1,
		cut2,
		numcut2,
		cut3,
		numcut3,
		cut4,
		numcut4,
		cut5,
		numcut5,
		rolloffcut,
		(
			( orderfootage * 12 *  ( (cut1 * numcut1) + (cut2 * numcut2) + (cut3 * numcut3) + (cut4 * numcut4) + (cut5 * numcut5) )  / 1000 ) + ( orderfootage * 12 * rolloffcut)
		) 									as msi_calculated
	from dt_inovar_prod_stg.in_westfield_fact_po_item_stock
union all 
	select 
		'Newburyport' 						as businessunit,
		po_number,  
		rollnum,
		orderfootage,
		cut1,
		numcut1,
		cut2,
		numcut2,
		cut3,
		numcut3,
		cut4,
		numcut4,
		cut5,
		numcut5,
		rolloffcut,
		(
			( orderfootage * 12 *  ( (cut1 * numcut1) + (cut2 * numcut2) + (cut3 * numcut3) + (cut4 * numcut4) + (cut5 * numcut5) )  / 1000 ) + ( orderfootage * 12 * rolloffcut)
		)  									as msi_calculated
	from dt_inovar_prod_stg.in_newburyport_fact_po_item_stock
union all 
	select 
		'Cimarron North' 					as businessunit,
		po_number,  
		rollnum,
		orderfootage,
		cut1,
		numcut1,
		cut2,
		numcut2,
		cut3,
		numcut3,
		cut4,
		numcut4,
		cut5,
		numcut5,
		rolloffcut,
		(
			( orderfootage * 12 *  ( (cut1 * numcut1) + (cut2 * numcut2) + (cut3 * numcut3) + (cut4 * numcut4) + (cut5 * numcut5) )  / 1000 ) + ( orderfootage * 12 * rolloffcut)
		)  									as msi_calculated
	from dt_inovar_prod_stg.in_cimarron_fact_po_item_stock
union all 
	select 
		'Amherst Label' 					as businessunit,
		po_number,  
		rollnum,
		orderfootage,
		cut1,
		numcut1,
		cut2,
		numcut2,
		cut3,
		numcut3,
		cut4,
		numcut4,
		cut5,
		numcut5,
		rolloffcut,
		(
			( orderfootage * 12 *  ( (cut1 * numcut1) + (cut2 * numcut2) + (cut3 * numcut3) + (cut4 * numcut4) + (cut5 * numcut5) )  / 1000 ) + ( orderfootage * 12 * rolloffcut)
		)  									as msi_calculated
	from dt_inovar_prod_stg.in_amherst_fact_po_item_stock
union all
	select 
		'Sioux Falls - LA' 					as businessunit,
		po_number,  
		rollnum,
		orderfootage,
		cut1,
		numcut1,
		cut2,
		numcut2,
		cut3,
		numcut3,
		cut4,
		numcut4,
		cut5,
		numcut5,
		rolloffcut,
		(
			( orderfootage * 12 *  ( (cut1 * numcut1) + (cut2 * numcut2) + (cut3 * numcut3) + (cut4 * numcut4) + (cut5 * numcut5) )  / 1000 ) + ( orderfootage * 12 * rolloffcut)
		)  									as msi_calculated
	from dt_inovar_prod_stg.in_siouxfalls_la_fact_po_item_stock
	),
supplier as (
	select 
		'Dallas'							as businessunit,
		suptype								as category,
		number,
		concat('DALLAS',number)				as company_number,
		company 							as vendor_name 		
	from dt_inovar_prod_stg.in_dallas_dim_supplier 	
union all
	select 
		'Milwaukee'							as businessunit,
		suptype								as category,
		number,
		concat('FG',number)					as company_number,
		company 							as vendor_name
	from dt_inovar_prod_stg.in_butler_dim_supplier 
union all
	select 
		'Ft. Lauderdale'					as businessunit,
		suptype								as category,
		number,
		concat('DAVIE',number)					as company_number,
		company 							as vendor_name  
	from dt_inovar_prod_stg.in_davie_dim_supplier 
union all
	select 
		'Westfield'							as businessunit,
		suptype								as category,
		number,
		concat('DL',number)					as company_number,
		company 							as vendor_name 
	from dt_inovar_prod_stg.in_westfield_dim_supplier 
union all
	select 
		'Newburyport'						as businessunit,
		suptype								as category,
		number,
		concat('NE',number)					as company_number,
		company 							as vendor_name 
	from dt_inovar_prod_stg.in_newburyport_dim_supplier 
union all
	select 
		'Cimarron North'					as businessunit,
		suptype								as category,
		number,
		concat('CN',number)					as company_number,
		company 							as vendor_name 
	from dt_inovar_prod_stg.in_cimarron_dim_supplier 
union all
	select 
		'Amherst Label'						as businessunit,
		suptype								as category,
		number,
		concat('AL',number)					as company_number,
		company 							as vendor_name 
	from dt_inovar_prod_stg.in_amherst_dim_supplier  
union all
	select 
		'Sioux Falls - LA'						as businessunit,
		suptype								as category,
		number,
		concat('LA',number)					as company_number,
		company 							as vendor_name 
	from dt_inovar_prod_stg.in_siouxfalls_la_dim_supplier 
),
final_data AS (
select 
	po_item_stock.businessunit,
	po_item_stock.po_number,
	po.podate::date,
	po.suppliernum,
	po.company_vendor_number,
	po.cost_msi_per_po::float * po_item_stock_detail.orderfootage::float / NULLIF(po_item_stock.totalorderfootage::float,0)			as cost_msi_per_po,
	po.total_po_ordered::float * po_item_stock_detail.orderfootage::float / NULLIF(po_item_stock.totalorderfootage::float,0)			as total_po_ordered,
	CASE WHEN trim(po.mfg_Spec) = '' THEN 'Unmapped' ELSE COALESCE(trim(po.mfg_Spec),'Unmapped') END as mfg_Spec,
	po.total_po_received::float * po_item_stock_detail.orderfootage::float / NULLIF(po_item_stock.totalorderfootage::float,0)	as total_po_received,	
	po.description,
	po.stock_number,
	CASE WHEN trim(po.adhesive) = '' THEN 'Unmapped' ELSE COALESCE(trim(po.adhesive),'Unmapped') END as adhesive,
	po.received_date,
	po.requested_delivery_date,
	invoice.invoice_amount::float * po_item_stock_detail.orderfootage::float / NULLIF(po_item_stock.totalorderfootage::float,0)		as invoice_amount,
	invoice_line.freight_cost::float * po_item_stock_detail.orderfootage::float / NULLIF(po_item_stock.totalorderfootage::float,0)	as freight_cost,
	invoice_line.material_cost::float * po_item_stock_detail.orderfootage::float / NULLIF(po_item_stock.totalorderfootage::float,0)	as material_cost,
	initcap(coalesce(vendor_master.mapped_vendor_name, supplier.vendor_name)) as vendor_name,--
	supplier.category,
	po_item_stock_detail.rollnum,
	po_item_stock_detail.orderfootage,
		po_item_stock_detail.cut1,
		po_item_stock_detail.numcut1,
		po_item_stock_detail.cut2,
		po_item_stock_detail.numcut2,
		po_item_stock_detail.cut3,
		po_item_stock_detail.numcut3,
		po_item_stock_detail.cut4,
		po_item_stock_detail.numcut4,
		po_item_stock_detail.cut5,
		po_item_stock_detail.numcut5,
		po_item_stock_detail.rolloffcut,
		po_item_stock_detail.msi_calculated
	from po_item_stock
	left join po 			on po_item_stock.po_number = po.po_number 	and po_item_stock.businessunit = po.businessunit
	left join invoice		on po_item_stock.pO_number = invoice.frompo_number and po_item_stock.businessunit = invoice.businessunit
	left join invoice_line 	on po_item_stock.pO_number = invoice_line.po_number and po_item_stock.businessunit = invoice_line.businessunit
	left join supplier 		on po.suppliernum = supplier.number and po_item_stock.businessunit = supplier.businessunit
	left join po_item_stock_detail on po_item_stock_detail.po_number = po_item_stock.po_number and po_item_stock_detail.businessunit = po_item_stock.businessunit
	left join dt_inovar_prod_stg.in_gs_vendor_master vendor_master on vendor_master.company_vendor_number = po.company_vendor_number
	WHERE po.podate >= '2022-01-01'
	)
	SELECT * FROM final_data;
	
