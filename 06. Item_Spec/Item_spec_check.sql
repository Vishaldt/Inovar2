
	select businessunit,
	count(*),
	sum(sizeacross) sizeacross, 
	sum(sizearound) sizearound,
	sum(colspace) colspace,
	sum(rowspace) rowspace,
	sum(quantity1) quantity1,
	sum(priceperm1) priceperm1,
	sum(quantity2)quantity2,
	sum(priceperm2) priceperm2,
	sum(quantity3)quantity3,
	sum(priceperm3) priceperm3,
	sum(quantity4)quantity4,
	sum(priceperm4) priceperm4,
	sum(quantity5) quantity5,
	sum(priceperm5) priceperm5,
	sum(quantity6) quantity6,
	sum(priceperm6)priceperm6,
	sum(stockwidth1) stockwidth1,
--	sum(stocknum2::float) stocknum2,
	sum(stockwidth3)stockwidth3,
	sum(stockwidth2) stockwidth2
	from dt_inovar_dev_stg.u_mat_inovar_item_spec_test
	group by 1
	
	
	select businessunit,
		count(*),
		sum(sizeacross) sizeacross, 
		sum(sizearound) sizearound,
		sum(colspace) colspace,
		sum(rowspace) rowspace,
		sum(quantity1) quantity1,
		sum(priceperm1) priceperm1,
		sum(quantity2)quantity2,
		sum(priceperm2) priceperm2,
		sum(quantity3)quantity3,
		sum(priceperm3) priceperm3,
		sum(quantity4)quantity4,
		sum(priceperm4) priceperm4,
		sum(quantity5) quantity5,
		sum(priceperm5) priceperm5,
		sum(quantity6) quantity6,
		sum(priceperm6)priceperm6,
		sum(stockwidth1) stockwidth1,
	--	sum(stocknum2::float) stocknum2,
		sum(stockwidth3)stockwidth3,
		sum(stockwidth2) stockwidth2
	from dt_inovar_prod_edw.u_mat_inovar_item_spec
	group by 1
	
	
	
	
	
	
	
	
	
	
	SELECT 
	'Sioux Falls'					as businessunit,
	prodnum, 
	concat('LA-', custnum) 			as custnum, 
	description, 
	proddate, 
	colordescr, 
	jobtype, 
	prodgroup, 
	sizeacross, 
	sizearound, 
	colspace, 
	rowspace, 
	labelrepeat, 
	noacross, 
	noaround, 
	finishtype, 
	labelsper_, 
	corediameter, 
	press, 
	totaltickets, 
	noofcolors, 
	estimatenum, 
	quantity1, 
	priceperm1, 
	quantity2, 
	priceperm2, 
	quantity3, 
	priceperm3, 
	quantity4, 
	priceperm4, 
	quantity5, 
	priceperm5, 
	quantity6, 
	priceperm6, 
	stocknum1, 
	stockwidth1, 
	stocknum2, 
	stockwidth2, 
	stocknum3, 
	stockwidth3, 
	nofloods, 
	consecno, 
	custname, 
	stockdescr1, 
	stockdescr2, 
	stockdescr3, 
	pricemode, 
	uniqueprodid, 
	lower (prod.inactive::text)			as inactive,
	lower(use_turretrewinder::text)	as use_turretrewinder, 
	shrinksleeve_overlap::float, 
	shrinksleeve_layflat::float, 
	shrinksleeve_cutheight::float
--	stock.mfgspecnum
FROM dt_inovar_prod_stg.in_siouxfalls_la_dim_product prod


LEFT JOIN dt_inovar_prod_stg.in_siouxfalls_la_dim_stock stock ON stock.stocknum = prod.stocknum2
where proddate >='2021-01-01'
	