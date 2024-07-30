/*create TABLE dt_inovar_prod_edw.u_mat_inovar_item_spec
AS SELECT * FROM dt_inovar_prod_edw.v_inovar_item_spec*/



select * from dt_inovar_prod_edw.u_mat_inovar_item_spec
where prodnum ='1-25BLANKCIRCLES'

				quantity4, 
	priceperm4, 
	quantity5, 
	priceperm5, 
	quantity6, 
	priceperm6,stocknum1,
	stockwidth1,stocknum2, 
	stockwidth2,stocknum3,
	stockwidth3, 
	nofloods, consecno,
	custname,
	stockdescr1,
	stockdescr2,
	stockdescr3
	pricemode, 
	uniqueprodid, 
	inactive,use_turretrewinder, 
	shrinksleeve_overlap, 
	shrinksleeve_layflat, 
	shrinksleeve_cutheight	

/*drop table dt_inovar_prod_edw.u_mat_inovar_item_spec*/

select * from dt_inovar_prod_edw.v_inovar_item_spec


select * from dt_inovar_prod_edw.u_mat_inovar_item_spec


--drop view dt_inovar_prod_edw.v_inovar_item_spec


select * from dt_inovar_prod_edw.u_mat_inovar_item_spec


--create or replace view dt_inovar_prod_edw.v_inovar_item_spec as

select count(*),sum(sizeacross) sizeacross, sum(sizearound) sizearound, sum(colspace) colspace, sum(rowspace) rowspace,sum(quantity1) quantity1, sum(priceperm1) priceperm1,
	sum(quantity2)quantity2,sum(priceperm2) priceperm2,
	sum(quantity3)quantity3,
	sum(priceperm3) priceperm3,
	sum(quantity4)quantity4,
	sum(priceperm4) priceperm4,
	sum(quantity5) quantity5,
	sum(priceperm5) priceperm5,
	sum(quantity6) quantity6,
	sum(priceperm6)priceperm6
	sum(stockwidth1) stockwidth1,
	sum(stocknum2) stocknum2,
	sum(stockwidth2) stockwidth2,
	
	
from  (
with item_spec as ( 
SELECT 
	'Cimarron North'					as businessunit,
	prodnum, 
	concat('CN-', custnum) 				as custnum, 
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
	lower (inactive::text)			as inactive,
	lower(use_turretrewinder::text)	as use_turretrewinder, 
	shrinksleeve_overlap, 
	shrinksleeve_layflat, 
	shrinksleeve_cutheight
FROM dt_inovar_prod_stg.in_cimarron_dim_product 
where proddate >='2021-01-01'
	union all	
SELECT 
	'Dallas'						as businessunit,
	prodnum, 
	concat('DALLAS-', custnum) 		as custnum, 
	description,
	proddate::date, 
	colordescr, 
	jobtype, 
	prodgroup, 
	sizeacross::float, 
	sizearound::float, 
	colspace::float, 
	rowspace::float, 
	labelrepeat::float, 
	noacross::float, 
	noaround::float, 
	finishtype, 
	labelsper_::float, 
	corediameter::float, 
	press, 
	totaltickets::float, 
	noofcolors::float, 
	estimatenum, 
	quantity1::float, 
	priceperm1::float, 
	quantity2::float, 
	priceperm2::float, 
	quantity3::float, 
	priceperm3::float, 
	quantity4::float, 
	priceperm4::float, 
	quantity5::float, 
	priceperm5::float, 
	quantity6::float, 
	priceperm6::float, 
	stocknum1, 
	stockwidth1::float, 
	stocknum2, 
	stockwidth2::float, 
	stocknum3, 
	stockwidth3::float, 
	nofloods::float, 
	consecno, 
	custname, 
	stockdescr1, 
	stockdescr2, 
	stockdescr3, 
	pricemode, 
	uniqueprodid, 
	lower (inactive::text)				as inactive,
	lower(use_turretrewinder::text)		as use_turretrewinder, 
	shrinksleeve_overlap::float, 
	shrinksleeve_layflat::float, 
	shrinksleeve_cutheight::float
FROM dt_inovar_prod_stg.in_dallas_dim_product
where proddate >='2021-01-01'
	union all	
SELECT 
	'Westfield'							as businessunit,
	prodnum, 
	concat('DL-', custnum) 				as custnum, 
	description, 
	proddate::date, 
	colordescr, 
	jobtype, 
	prodgroup, 
	sizeacross::float, 
	sizearound::float, 
	colspace::float, 
	rowspace::float, 
	labelrepeat::float, 
	noacross::float, 
	noaround::float, 
	finishtype, 
	labelsper_::float, 
	corediameter::float, 
	press, 
	totaltickets::float, 
	noofcolors::float, 
	estimatenum, 
	quantity1::float, 
	priceperm1::float, 
	quantity2::float, 
	priceperm2::float, 
	quantity3::float, 
	priceperm3::float, 
	quantity4::float, 
	priceperm4::float, 
	quantity5::float, 
	priceperm5::float, 
	quantity6::float, 
	priceperm6::float, 
	stocknum1, 
	stockwidth1::float, 
	stocknum2, 
	stockwidth2::float, 
	stocknum3, 
	stockwidth3::float, 
	nofloods::float, 
	consecno, 
	custname, 
	stockdescr1, 
	stockdescr2, 
	stockdescr3, 
	pricemode, 
	uniqueprodid, 
	lower (inactive::text)				as inactive,
	lower(use_turretrewinder::text)		as use_turretrewinder, 
	shrinksleeve_overlap::float, 
	shrinksleeve_layflat::float, 
	shrinksleeve_cutheight::float
FROM dt_inovar_prod_stg.in_westfield_dim_product
where proddate >='2021-01-01'
	union all
SELECT 
	'Newburyport'						as businessunit,
	prodnum, 
	concat('NE-', custnum) 				as custnum, 
	description, 
	proddate::date, 
	colordescr, 
	jobtype, 
	prodgroup, 
	sizeacross::float, 
	sizearound::float, 
	colspace::float, 
	rowspace::float, 
	labelrepeat::float, 
	noacross::float, 
	noaround::float, 
	finishtype, 
	labelsper_::float, 
	corediameter::float, 
	press, 
	totaltickets::float, 
	noofcolors::float, 
	estimatenum, 
	quantity1::float, 
	priceperm1::float, 
	quantity2::float, 
	priceperm2::float, 
	quantity3::float, 
	priceperm3::float, 
	quantity4::float, 
	priceperm4::float, 
	quantity5::float, 
	priceperm5::float, 
	quantity6::float, 
	priceperm6::float, 
	stocknum1, 
	stockwidth1::float, 
	stocknum2, 
	stockwidth2::float, 
	stocknum3, 
	stockwidth3::float, 
	nofloods::float, 
	consecno, 
	custname, 
	stockdescr1, 
	stockdescr2, 
	stockdescr3, 
	pricemode, 
	uniqueprodid, 
	lower (inactive::text)			as inactive,
	lower(use_turretrewinder::text)	as use_turretrewinder, 
	shrinksleeve_overlap::float, 
	shrinksleeve_layflat::float, 
	shrinksleeve_cutheight::float
FROM dt_inovar_prod_stg.in_newburyport_dim_product
where proddate >='2021-01-01'
	union all	
SELECT 
	'Ft. Lauderdale'				as businessunit,
	prodnum, 
	concat('DAVIE-', custnum) 		as custnum, 
	description, 
	proddate::date, 
	colordescr, 
	jobtype, 
	prodgroup, 
	sizeacross::float, 
	sizearound::float, 
	colspace::float, 
	rowspace::float, 
	labelrepeat::float, 
	noacross::float, 
	noaround::float, 
	finishtype, 
	labelsper_::float, 
	corediameter::float, 
	press, 
	totaltickets::float, 
	noofcolors::float, 
	estimatenum, 
	quantity1::float, 
	priceperm1::float, 
	quantity2::float, 
	priceperm2::float, 
	quantity3::float, 
	priceperm3::float, 
	quantity4::float, 
	priceperm4::float, 
	quantity5::float, 
	priceperm5::float, 
	quantity6::float, 
	priceperm6::float, 
	stocknum1, 
	stockwidth1::float, 
	stocknum2, 
	stockwidth2::float, 
	stocknum3, 
	stockwidth3::float, 
	nofloods::float, 
	consecno, 
	custname, 
	stockdescr1, 
	stockdescr2, 
	stockdescr3, 
	pricemode, 
	uniqueprodid, 
	lower (inactive::text)			as inactive,
	lower(use_turretrewinder::text)	as use_turretrewinder, 
	shrinksleeve_overlap::float, 
	shrinksleeve_layflat::float, 
	shrinksleeve_cutheight::float
FROM dt_inovar_prod_stg.in_davie_dim_product
where proddate >='2021-01-01'
	union all
SELECT 
	'Milwaukee'						as businessunit,
	prodnum, 
	concat('FG-', custnum) 			as custnum, 
	description, 
	proddate::date, 
	colordescr, 
	jobtype, 
	prodgroup, 
	sizeacross::float, 
	sizearound::float, 
	colspace::float, 
	rowspace::float, 
	labelrepeat::float, 
	noacross::float, 
	noaround::float, 
	finishtype, 
	labelsper_::float, 
	corediameter::float, 
	press, 
	totaltickets::float, 
	noofcolors::float, 
	estimatenum, 
	quantity1::float, 
	priceperm1::float, 
	quantity2::float, 
	priceperm2::float, 
	quantity3::float, 
	priceperm3::float, 
	quantity4::float, 
	priceperm4::float, 
	quantity5::float, 
	priceperm5::float, 
	quantity6::float, 
	priceperm6::float, 
	stocknum1, 
	stockwidth1::float, 
	stocknum2, 
	stockwidth2::float, 
	stocknum3, 
	stockwidth3::float, 
	nofloods::float, 
	consecno, 
	custname, 
	stockdescr1, 
	stockdescr2, 
	stockdescr3, 
	pricemode, 
	uniqueprodid, 
	lower (inactive::text)			as inactive,
	lower(use_turretrewinder::text)	as use_turretrewinder, 
	shrinksleeve_overlap::float, 
	shrinksleeve_layflat::float, 
	shrinksleeve_cutheight::float
FROM dt_inovar_prod_stg.in_butler_dim_product
where proddate >='2021-01-01'
	union all
SELECT 
	'Amherst Label'					as businessunit,
	prodnum, 
	concat('AL-', custnum) 			as custnum, 
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
	lower (inactive::text)			as inactive,
	lower(use_turretrewinder::text)	as use_turretrewinder, 
	shrinksleeve_overlap, 
	shrinksleeve_layflat, 
	shrinksleeve_cutheight
FROM dt_inovar_prod_stg.in_amherst_dim_product
where proddate >='2021-01-01'
)
select 
	businessunit,
	prodnum,
	custnum,
	case
		when description = '' then 'Unmapped'
		else coalesce(description,'Unmapped')
	end										as description,
	proddate,
	case
		when colordescr = '' then 'Unmapped'
		else coalesce(colordescr,'Unmapped')
	end										as colordescr,
	jobtype,
	case
		when prodgroup = '' then 'Unmapped'
		else coalesce(prodgroup,'Unmapped')
	end										as prodgroup,
	sizeacross,
	sizearound,
	colspace,
	rowspace,
	labelrepeat,
	noacross,
	noaround,
	case
		when finishtype = '' then 'Unmapped'
		else coalesce(finishtype,'Unmapped')
	end										as finishtype,
	labelsper_,
	corediameter,
	case
		when press = '' then 'Unmapped'
		else coalesce(press,'Unmapped')
	end										as press,
	totaltickets,
	noofcolors,
	case
		when estimatenum = '' then 'Unmapped'
		else coalesce(estimatenum,'Unmapped')
	end										as estimatenum,
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
	case
		when stocknum1 = '' then 'Unmapped'
		else coalesce(stocknum1, 'Unmapped')
	end										as stocknum1,
	stockwidth1, 
	case
		when stocknum2 = '' then 'Unmapped'
		else coalesce(stocknum2,'Unmapped')
	end										as stocknum2, 
	stockwidth2,
	case
		when stocknum3 = '' then 'Unmapped'
		else coalesce(stocknum3,'Unmapped')
	end										as stocknum3,
	stockwidth3, 
	nofloods, 
	case
		when consecno = '' then 'Unmapped'
		else coalesce(consecno,'Unmapped')
	end										as consecno,
	case
		when custname = '' then 'Unmapped'
		else coalesce(custname,'Unmapped')
	end										as custname, 
	case
		when stockdescr1 = '' then 'Unmapped'
		else coalesce(stockdescr1,'Unmapped')
	end										as stockdescr1, 
	case
		when stockdescr2 = '' then 'Unmapped'
		else coalesce(stockdescr2,'Unmapped')
	end										as stockdescr2, 
	case
		when stockdescr3 = '' then 'Unmapped'
		else coalesce(stockdescr3,'Unmapped')
	end										as stockdescr3,
	pricemode, 
	uniqueprodid, 
	lower (inactive::text)			as inactive,
	lower(use_turretrewinder::text)	as use_turretrewinder, 
	shrinksleeve_overlap, 
	shrinksleeve_layflat, 
	shrinksleeve_cutheight	
from item_spec
where proddate >='2021-01-01'


and stocknum2 =''


quantity4, 
	priceperm4, 
	quantity5, 
	priceperm5, 
	quantity6, 
	priceperm6,tocknum1,
	stockwidth1,stocknum2, 
	stockwidth2,stocknum3,
	stockwidth3, 
	nofloods, consecno,
	custname,
	stockdescr1,
	stockdescr2,
	stockdescr3
	pricemode, 
	uniqueprodid, 
	inactive,use_turretrewinder, 
	shrinksleeve_overlap, 
	shrinksleeve_layflat, 
	shrinksleeve_cutheight	
	


select count(distinct colordescr)