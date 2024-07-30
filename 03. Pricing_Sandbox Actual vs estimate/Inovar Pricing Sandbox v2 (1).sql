----------------------------------------- shifted to dev on (09/25)
----------------------------------------- v2 created by Vishal and slacked to me on (03/06)
----------------------------------------- v2 live on (03/08)

--drop view dt_inovar_prod_edw.v_inovar_pricing_sandbox

create or replace view dt_inovar_prod_edw.v_inovar_pricing_sandbox as
with butler as (
select 
	tkt.number,
	concat('FG-', tkt.number) as company_ticket_number,
	'Milwaukee' as location,
	'Flexo-Graphics LLC' as company,
	'Milwaukee' as businessunit,
	tkt.actfootage::float,
	case 
		when tkt.orderdate = '' then null::date
		else tkt.orderdate::date
	end as orderdate,
	tkt.generaldescr,
	tkt.esttime::float,
	tkt.carrierwidth::text,
	tkt.sizeacross::text,
	tkt.sizearound::text,
	tkt.colspace::text,
	tkt.rowspace::text,
	tkt.labelrepeat::float,
	tkt.noacross::text,
	tkt.finishtype,
	tkt.custponum,
	tkt.turnbar,
	concat('FG-', tkt.customernum) as company_customer_number,
	tkt.customernum,
	tkt.estfootage::float,
	tkt.stocknum2,
	tkt.stockwidth2::float,
	tkt.shipcity,
	tkt.shipst,
	tkt.shipzip,
	tkt.shipcountry,
	tkt.salescommission::float,
	case 
		when tkt.dateshipped = '' then null::date
		else tkt.dateshipped::date
	end as dateshipped,
	tkt.ticquantity::float,
	tkt.estmrhrs::float,
	tkt.actmrhrs::float,
	tkt.estwuhrs::float,
	tkt.actwuhrs::float,
	tkt.estrunhrs::float,
	tkt.actrunhrs::float,
	tkt.estfinhrs::float,
	tkt.estpackhrs::float,
	tkt.actpackhrs::float,
	tkt.estpressspd::text,
	tkt.actpressspd::float,
	tkt.actquantity::float,
	tkt.act_makeready_footage::float,
	tkt.esttotal::float,
	tkt.acttotalcost::float,
	tkt.eststockcost::float,
	tkt.actstockcost::float,
	tkt.stockdesc1,
	tkt.stockdesc2,
	tkt.stockdesc3,
	tkt.ticketstatus,
	tkt.estpresstime::float,
	tkt.terms,
	tkt.actualpresshours::float,
	tkt.actualtotalhours::float,
	tkt.actualpressrate::text,
	tkt.actualpresscost::float,
	tkt.actualrewindingrate::text,
	tkt.actualrewindingcost::float,
	tkt.actualfanfoldrate::float,
	tkt.actualfanfoldcost::float,
	tkt.actualpackagingrate::float,
	tkt.actualpackinglaborcost::float,
	tkt.actualbillings_netofsalestax::float,
	tkt.actualgrossmargin_dollars::float,
	tkt.actualgrossmargin_percent::float,
	tkt.actualrewindinghours::float,
	tkt.actualtotalfinishing::float,
	tkt.actualtotallaborcosts::float,
	tkt.actualtotalpocosts::float,
	tkt.actualtotalmatandfreightcost::float,
	tkt.est_setupfootage::float,
	tkt.est_spoilfootage::float,
	tkt.estpostpresshours::float,
	tkt.actpostpresshours::float,
	tkt.act_other_hours::float,
	tkt.actualpostpresslaborcost::float,
	tkt.actualotherlaborcost::float,
	tkt.shrinksleeve_overlap::float,
	tkt.shrinksleeve_layflat::float,
	tkt.shrinksleeve_cutheight::float,
	tkt.customer_total::float,
	pd.press_type,
	eq.description as press_description,
	trim(initcap(coalesce(cus.company, tkt.customername))) as customername,
	(((estfootage::float - est_spoilfootage::float - est_setupfootage::float)* actquantity::float)/ nullif(ticquantity::float, 0)) as materialrequiredperlt,
	tkt.press
from
	dt_inovar_prod_stg.in_butler_fact_ticket tkt
left join dt_inovar_prod_edw.u_mat_inovar_press_type pd
		on
	trim(lower(tkt.press)) = trim(lower(pd.press))
	and pd.businessunit = 'Milwaukee'
left join dt_inovar_prod_stg.in_butler_dim_equipment eq 
		on
	trim(lower(eq.number)) = trim(lower(tkt.press))
left join dt_inovar_prod_edw.v_inovar_customer_master cus 
		on
	trim(lower(cus.number)) = trim(lower(tkt.customernum))
	and cus.businessunit = 'Milwaukee'
where
	tkt.orderdate::date >= '2020-01-01'
),
dallas as (
select 
	tkt.number,
	concat('DALLAS-', tkt.number) as company_ticket_number,
	'Dallas' as location,
	'Inovar Packaging Group LLC' as company,
	'Dallas' as businessunit,
	tkt.actfootage::float,
	case 
		when tkt.orderdate = '' then null::date
		else tkt.orderdate::date
	end as orderdate,
	tkt.generaldescr,
	tkt.esttime::float,
	tkt.carrierwidth::text,
	tkt.sizeacross::text,
	tkt.sizearound::text,
	tkt.colspace::text,
	tkt.rowspace::text,
	tkt.labelrepeat::float,
	tkt.noacross::text,
	tkt.finishtype,
	tkt.custponum,
	tkt.turnbar,
	concat('DALLAS-', tkt.customernum) as company_customer_number,
	tkt.customernum,
	tkt.estfootage::float,
	tkt.stocknum2,
	tkt.stockwidth2::float,
	tkt.shipcity,
	tkt.shipst,
	tkt.shipzip,
	tkt.shipcountry,
	tkt.salescommission::float,
	case 
		when tkt.dateshipped = '' then null::date
		else tkt.dateshipped::date
	end as dateshipped,
	tkt.ticquantity::float,
	tkt.estmrhrs::float,
	tkt.actmrhrs::float,
	tkt.estwuhrs::float,
	tkt.actwuhrs::float,
	tkt.estrunhrs::float,
	tkt.actrunhrs::float,
	tkt.estfinhrs::float,
	tkt.estpackhrs::float,
	tkt.actpackhrs::float,
	tkt.estpressspd::text,
	tkt.actpressspd::float,
	tkt.actquantity::float,
	tkt.act_makeready_footage::float,
	tkt.esttotal::float,
	tkt.acttotalcost::float,
	tkt.eststockcost::float,
	tkt.actstockcost::float,
	tkt.stockdesc1,
	tkt.stockdesc2,
	tkt.stockdesc3,
	tkt.ticketstatus,
	tkt.estpresstime::float,
	tkt.terms,
	tkt.actualpresshours::float,
	tkt.actualtotalhours::float,
	tkt.actualpressrate::text,
	tkt.actualpresscost::float,
	tkt.actualrewindingrate::text,
	tkt.actualrewindingcost::float,
	tkt.actualfanfoldrate::float,
	tkt.actualfanfoldcost::float,
	tkt.actualpackagingrate::float,
	tkt.actualpackinglaborcost::float,
	tkt.actualbillings_netofsalestax::float,
	tkt.actualgrossmargin_dollars::float,
	tkt.actualgrossmargin_percent::float,
	tkt.actualrewindinghours::float,
	tkt.actualtotalfinishing::float,
	tkt.actualtotallaborcosts::float,
	tkt.actualtotalpocosts::float,
	tkt.actualtotalmatandfreightcost::float,
	tkt.est_setupfootage::float,
	tkt.est_spoilfootage::float,
	tkt.estpostpresshours::float,
	tkt.actpostpresshours::float,
	tkt.act_other_hours::float,
	tkt.actualpostpresslaborcost::float,
	tkt.actualotherlaborcost::float,
	tkt.shrinksleeve_overlap::float,
	tkt.shrinksleeve_layflat::float,
	tkt.shrinksleeve_cutheight::float,
	tkt.customer_total::float,
	pd.press_type,
	eq.description as press_description,
	trim(initcap(coalesce(cus.company, tkt.customername))) as customername,
	(((estfootage::float - est_spoilfootage::float - est_setupfootage::float)* actquantity::float)/ nullif(ticquantity::float, 0)) as materialrequiredperlt,
	tkt.press
from
	dt_inovar_prod_stg.in_dallas_fact_ticket tkt
left join dt_inovar_prod_edw.u_mat_inovar_press_type pd
		on
	trim(lower(tkt.press)) = trim(lower(pd.press))
	and pd.businessunit = 'Dallas'
left join dt_inovar_prod_stg.in_dallas_dim_equipment eq 
		on
	trim(lower(eq.number)) = trim(lower(tkt.press))
left join dt_inovar_prod_edw.v_inovar_customer_master cus 
		on
	trim(lower(cus.number)) = trim(lower(tkt.customernum))
		and cus.businessunit = 'Dallas'
	where
		tkt.orderdate::date >= '2020-01-01'
),
davie as (
select 
	tkt.number,
	concat('DAVIE-', tkt.number) as company_ticket_number,
	'Ft. Lauderdale' as location,
	'Inovar Packaging Florida LLC' as company,
	'Ft. Lauderdale' as businessunit,
	tkt.actfootage::float,
	case 
		when tkt.orderdate = '' then null::date
		else tkt.orderdate::date
	end as orderdate,
	tkt.generaldescr,
	tkt.esttime::float,
	tkt.carrierwidth::text,
	tkt.sizeacross::text,
	tkt.sizearound::text,
	tkt.colspace::text,
	tkt.rowspace::text,
	tkt.labelrepeat::float,
	tkt.noacross::text,
	tkt.finishtype,
	tkt.custponum,
	tkt.turnbar,
	concat('DAVIE-', tkt.customernum) as company_customer_number,
	tkt.customernum,
	tkt.estfootage::float,
	tkt.stocknum2,
	tkt.stockwidth2::float,
	tkt.shipcity,
	tkt.shipst,
	tkt.shipzip,
	tkt.shipcountry,
	tkt.salescommission::float,
	case 
		when tkt.dateshipped = '' then null::date
		else tkt.dateshipped::date
	end as dateshipped,
	tkt.ticquantity::float,
	tkt.estmrhrs::float,
	tkt.actmrhrs::float,
	tkt.estwuhrs::float,
	tkt.actwuhrs::float,
	tkt.estrunhrs::float,
	tkt.actrunhrs::float,
	tkt.estfinhrs::float,
	tkt.estpackhrs::float,
	tkt.actpackhrs::float,
	tkt.estpressspd::text,
	tkt.actpressspd::float,
	tkt.actquantity::float,
	tkt.act_makeready_footage::float,
	tkt.esttotal::float,
	tkt.acttotalcost::float,
	tkt.eststockcost::float,
	tkt.actstockcost::float,
	tkt.stockdesc1,
	tkt.stockdesc2,
	tkt.stockdesc3,
	tkt.ticketstatus,
	tkt.estpresstime::float,
	tkt.terms,
	tkt.actualpresshours::float,
	tkt.actualtotalhours::float,
	tkt.actualpressrate::text,
	tkt.actualpresscost::float,
	tkt.actualrewindingrate::text,
	tkt.actualrewindingcost::float,
	tkt.actualfanfoldrate::float,
	tkt.actualfanfoldcost::float,
	tkt.actualpackagingrate::float,
	tkt.actualpackinglaborcost::float,
	tkt.actualbillings_netofsalestax::float,
	tkt.actualgrossmargin_dollars::float,
	tkt.actualgrossmargin_percent::float,
	tkt.actualrewindinghours::float,
	tkt.actualtotalfinishing::float,
	tkt.actualtotallaborcosts::float,
	tkt.actualtotalpocosts::float,
	tkt.actualtotalmatandfreightcost::float,
	tkt.est_setupfootage::float,
	tkt.est_spoilfootage::float,
	tkt.estpostpresshours::float,
	tkt.actpostpresshours::float,
	tkt.act_other_hours::float,
	tkt.actualpostpresslaborcost::float,
	tkt.actualotherlaborcost::float,
	tkt.shrinksleeve_overlap::float,
	tkt.shrinksleeve_layflat::float,
	tkt.shrinksleeve_cutheight::float,
	tkt.customer_total::float,
	pd.press_type,
	eq.description as press_description,
	trim(initcap(coalesce(cus.company, tkt.customername))) as customername,
	(((estfootage::float - est_spoilfootage::float - est_setupfootage::float)* actquantity::float)/ nullif(ticquantity::float, 0)) as materialrequiredperlt,
	tkt.press
from
	dt_inovar_prod_stg.in_davie_fact_ticket tkt
left join dt_inovar_prod_edw.u_mat_inovar_press_type pd
		on
	trim(lower(tkt.press)) = trim(lower(pd.press))
	and pd.businessunit = 'Ft. Lauderdale'
left join dt_inovar_prod_stg.in_davie_dim_equipment eq 
		on
	trim(lower(eq.number)) = trim(lower(tkt.press))
left join dt_inovar_prod_edw.v_inovar_customer_master cus 
		on
	trim(lower(cus.number)) = trim(lower(tkt.customernum))
		and cus.businessunit = 'Ft. Lauderdale'
	where
		tkt.orderdate::date >= '2020-01-01'
),
westfield as (
select 
	tkt.number,
	concat('DL-', tkt.number) as company_ticket_number,
	'Westfield' as location,
	'Dion Label Printing LLC' as company,
	'Westfield' as businessunit,
	tkt.actfootage::float,
	case 
		when tkt.orderdate = '' then null::date
		else tkt.orderdate::date
	end as orderdate,
	tkt.generaldescr,
	tkt.esttime::float,
	tkt.carrierwidth::text,
	tkt.sizeacross::text,
	tkt.sizearound::text,
	tkt.colspace::text,
	tkt.rowspace::text,
	tkt.labelrepeat::float,
	tkt.noacross::text,
	tkt.finishtype,
	tkt.custponum,
	tkt.turnbar,
	concat('DL-', tkt.customernum) as company_customer_number,
	tkt.customernum,
	tkt.estfootage::float,
	tkt.stocknum2,
	tkt.stockwidth2::float,
	tkt.shipcity,
	tkt.shipst,
	tkt.shipzip,
	tkt.shipcountry,
	tkt.salescommission::float,
	case 
		when tkt.dateshipped = '' then null::date
		else tkt.dateshipped::date
	end as dateshipped,
	tkt.ticquantity::float,
	tkt.estmrhrs::float,
	tkt.actmrhrs::float,
	tkt.estwuhrs::float,
	tkt.actwuhrs::float,
	tkt.estrunhrs::float,
	tkt.actrunhrs::float,
	tkt.estfinhrs::float,
	tkt.estpackhrs::float,
	tkt.actpackhrs::float,
	tkt.estpressspd::text,
	tkt.actpressspd::float,
	tkt.actquantity::float,
	tkt.act_makeready_footage::float,
	tkt.esttotal::float,
	tkt.acttotalcost::float,
	tkt.eststockcost::float,
	tkt.actstockcost::float,
	tkt.stockdesc1,
	tkt.stockdesc2,
	tkt.stockdesc3,
	tkt.ticketstatus,
	tkt.estpresstime::float,
	tkt.terms,
	tkt.actualpresshours::float,
	tkt.actualtotalhours::float,
	tkt.actualpressrate::text,
	tkt.actualpresscost::float,
	tkt.actualrewindingrate::text,
	tkt.actualrewindingcost::float,
	tkt.actualfanfoldrate::float,
	tkt.actualfanfoldcost::float,
	tkt.actualpackagingrate::float,
	tkt.actualpackinglaborcost::float,
	tkt.actualbillings_netofsalestax::float,
	tkt.actualgrossmargin_dollars::float,
	tkt.actualgrossmargin_percent::float,
	tkt.actualrewindinghours::float,
	tkt.actualtotalfinishing::float,
	tkt.actualtotallaborcosts::float,
	tkt.actualtotalpocosts::float,
	tkt.actualtotalmatandfreightcost::float,
	tkt.est_setupfootage::float,
	tkt.est_spoilfootage::float,
	tkt.estpostpresshours::float,
	tkt.actpostpresshours::float,
	tkt.act_other_hours::float,
	tkt.actualpostpresslaborcost::float,
	tkt.actualotherlaborcost::float,
	tkt.shrinksleeve_overlap::float,
	tkt.shrinksleeve_layflat::float,
	tkt.shrinksleeve_cutheight::float,
	tkt.customer_total::float,
	pd.press_type,
	eq.description as press_description,
	trim(initcap(coalesce(cus.company, tkt.customername))) as customername,
	(((estfootage::float - est_spoilfootage::float - est_setupfootage::float)* actquantity::float)/ nullif(ticquantity::float, 0)) as materialrequiredperlt,
	tkt.press
from
	dt_inovar_prod_stg.in_westfield_fact_ticket tkt
left join dt_inovar_prod_edw.u_mat_inovar_press_type pd
		on
	trim(lower(tkt.press)) = trim(lower(pd.press))
	and pd.businessunit = 'Westfield'
left join dt_inovar_prod_stg.in_westfield_dim_equipment eq 
		on
	trim(lower(eq.number)) = trim(lower(tkt.press))
left join dt_inovar_prod_edw.v_inovar_customer_master cus 
		on
	trim(lower(cus.number)) = trim(lower(tkt.customernum))
		and cus.businessunit = 'Westfield'
	where
		tkt.orderdate::date >= '2020-01-01'
),
newburyport as (
select 
	tkt.number,
	concat('NE-', tkt.number) as company_ticket_number,
	'Newbury Port' as location,
	'Label Print America Inc.' as company,
	'Newburyport' as businessunit,
	tkt.actfootage::float,
	case 
		when tkt.orderdate = '' then null::date
		else tkt.orderdate::date
	end as orderdate,
	tkt.generaldescr,
	tkt.esttime::float,
	tkt.carrierwidth::text,
	tkt.sizeacross::text,
	tkt.sizearound::text,
	tkt.colspace::text,
	tkt.rowspace::text,
	tkt.labelrepeat::float,
	tkt.noacross::text,
	tkt.finishtype,
	tkt.custponum,
	tkt.turnbar,
	concat('NE-', tkt.customernum) as company_customer_number,
	tkt.customernum,
	tkt.estfootage::float,
	tkt.stocknum2,
	tkt.stockwidth2::float,
	tkt.shipcity,
	tkt.shipst,
	tkt.shipzip,
	tkt.shipcountry,
	tkt.salescommission::float,
	case 
		when tkt.dateshipped = '' then null::date
		else tkt.dateshipped::date
	end as dateshipped,
	tkt.ticquantity::float,
	tkt.estmrhrs::float,
	tkt.actmrhrs::float,
	tkt.estwuhrs::float,
	tkt.actwuhrs::float,
	tkt.estrunhrs::float,
	tkt.actrunhrs::float,
	tkt.estfinhrs::float,
	tkt.estpackhrs::float,
	tkt.actpackhrs::float,
	tkt.estpressspd::text,
	tkt.actpressspd::float,
	tkt.actquantity::float,
	tkt.act_makeready_footage::float,
	tkt.esttotal::float,
	tkt.acttotalcost::float,
	tkt.eststockcost::float,
	tkt.actstockcost::float,
	tkt.stockdesc1,
	tkt.stockdesc2,
	tkt.stockdesc3,
	tkt.ticketstatus,
	tkt.estpresstime::float,
	tkt.terms,
	tkt.actualpresshours::float,
	tkt.actualtotalhours::float,
	tkt.actualpressrate::text,
	tkt.actualpresscost::float,
	tkt.actualrewindingrate::text,
	tkt.actualrewindingcost::float,
	tkt.actualfanfoldrate::float,
	tkt.actualfanfoldcost::float,
	tkt.actualpackagingrate::float,
	tkt.actualpackinglaborcost::float,
	tkt.actualbillings_netofsalestax::float,
	tkt.actualgrossmargin_dollars::float,
	tkt.actualgrossmargin_percent::float,
	tkt.actualrewindinghours::float,
	tkt.actualtotalfinishing::float,
	tkt.actualtotallaborcosts::float,
	tkt.actualtotalpocosts::float,
	tkt.actualtotalmatandfreightcost::float,
	tkt.est_setupfootage::float,
	tkt.est_spoilfootage::float,
	tkt.estpostpresshours::float,
	tkt.actpostpresshours::float,
	tkt.act_other_hours::float,
	tkt.actualpostpresslaborcost::float,
	tkt.actualotherlaborcost::float,
	tkt.shrinksleeve_overlap::float,
	tkt.shrinksleeve_layflat::float,
	tkt.shrinksleeve_cutheight::float,
	tkt.customer_total::float,
	pd.press_type,
	eq.description as press_description,
	trim(initcap(coalesce(cus.company, tkt.customername))) as customername,
	(((estfootage::float - est_spoilfootage::float - est_setupfootage::float)* actquantity::float)/ nullif(ticquantity::float, 0)) as materialrequiredperlt,
	tkt.press
from
	dt_inovar_prod_stg.in_newburyport_fact_ticket tkt
left join dt_inovar_prod_edw.u_mat_inovar_press_type pd
		on
	trim(lower(tkt.press)) = trim(lower(pd.press))
	and pd.businessunit = 'Newburyport'
left join dt_inovar_prod_stg.in_newburyport_dim_equipment eq 
		on
	trim(lower(eq.number)) = trim(lower(tkt.press))
left join dt_inovar_prod_edw.v_inovar_customer_master cus 
		on
	trim(lower(cus.number)) = trim(lower(tkt.customernum))
		and cus.businessunit = 'Newburyport'
	where
		tkt.orderdate::date >= '2020-01-01'
),
cimarron_north as (
select 
	tkt.number,
	concat('CN-', tkt.number) as company_ticket_number,
	'Cimarron North' as location,
	'Cimarron North' as company,
	'Cimarron North' as businessunit,
	tkt.actfootage,
	tkt.orderdate,
	tkt.generaldescr,
	tkt.esttime,
	tkt.carrierwidth::text,
	tkt.sizeacross::text,
	tkt.sizearound::text,
	tkt.colspace::text,
	tkt.rowspace::text,
	tkt.labelrepeat,
	tkt.noacross::text,
	tkt.finishtype,
	tkt.custponum,
	case 
		when tkt.turnbar = true then 'True'
		when tkt.turnbar = false then 'False'
	end as turnbar ,
	concat('CN-', tkt.customernum) as company_customer_number,
	tkt.customernum,
	tkt.estfootage,
	tkt.stocknum2,
	tkt.stockwidth2,
	tkt.shipcity,
	tkt.shipst,
	tkt.shipzip,
	tkt.shipcountry,
	tkt.salescommission,
	tkt.dateshipped,
	tkt.ticquantity,
	tkt.estmrhrs,
	tkt.actmrhrs,
	tkt.estwuhrs,
	tkt.actwuhrs,
	tkt.estrunhrs,
	tkt.actrunhrs,
	tkt.estfinhrs,
	tkt.estpackhrs,
	tkt.actpackhrs,
	tkt.estpressspd::text,
	tkt.actpressspd,
	tkt.actquantity,
	tkt.act_makeready_footage,
	tkt.esttotal,
	tkt.acttotalcost,
	tkt.eststockcost,
	tkt.actstockcost,
	tkt.stockdesc1,
	tkt.stockdesc2,
	tkt.stockdesc3,
	tkt.ticketstatus,
	tkt.estpresstime,
	tkt.terms,
	tkt.actualpresshours,
	tkt.actualtotalhours,
	tkt.actualpressrate::text,
	tkt.actualpresscost,
	tkt.actualrewindingrate::text,
	tkt.actualrewindingcost::float,
	tkt.actualfanfoldrate,
	tkt.actualfanfoldcost,
	tkt.actualpackagingrate,
	tkt.actualpackinglaborcost,
	tkt.actualbillings_netofsalestax,
	tkt.actualgrossmargin_dollars,
	tkt.actualgrossmargin_percent,
	tkt.actualrewindinghours,
	tkt.actualtotalfinishing,
	tkt.actualtotallaborcosts,
	tkt.actualtotalpocosts,
	tkt.actualtotalmatandfreightcost,
	tkt.est_setupfootage,
	tkt.est_spoilfootage,
	tkt.estpostpresshours,
	tkt.actpostpresshours,
	tkt.act_other_hours,
	tkt.actualpostpresslaborcost,
	tkt.actualotherlaborcost,
	tkt.shrinksleeve_overlap,
	tkt.shrinksleeve_layflat,
	tkt.shrinksleeve_cutheight,
	tkt.customer_total,
	pd.press_type,
	eq.description as press_description,
	trim(initcap(coalesce(cus.company, tkt.customername))) as customername,
	(((estfootage - est_spoilfootage - est_setupfootage)* actquantity)/ nullif(ticquantity, 0)) as materialrequiredperlt,
	tkt.press
from
	dt_inovar_prod_stg.in_cimarron_fact_ticket tkt
left join dt_inovar_prod_edw.u_mat_inovar_press_type pd
		on
	trim(lower(tkt.press)) = trim(lower(pd.press))
	and pd.businessunit = 'Cimarron North'
left join dt_inovar_prod_stg.in_cimarron_dim_equipment eq 
		on
	trim(lower(eq.number)) = trim(lower(tkt.press))
left join dt_inovar_prod_edw.v_inovar_customer_master cus 
		on
	trim(lower(cus.number)) = trim(lower(tkt.customernum))
		and cus.businessunit = 'Cimarron North'
	where
		tkt.orderdate >= '2020-01-01'
),
amherst_label as (
select 
	tkt.number,
	concat('AL-', tkt.number) as company_ticket_number,
	'Amherst Label' as location,
	'Amherst Label' as company,
	'Amherst Label' as businessunit,
	tkt.actfootage,
	tkt.orderdate,
	tkt.generaldescr,
	tkt.esttime,
	tkt.carrierwidth::text,
	tkt.sizeacross::text,
	tkt.sizearound::text,
	tkt.colspace::text,
	tkt.rowspace::text,
	tkt.labelrepeat,
	tkt.noacross::text,
	tkt.finishtype,
	tkt.custponum,
	case 
		when tkt.turnbar = true then 'True'
		when tkt.turnbar = false then 'False'
	end as turnbar ,
	concat('AL-', tkt.customernum) as company_customer_number,
	tkt.customernum,
	tkt.estfootage,
	tkt.stocknum2,
	tkt.stockwidth2,
	tkt.shipcity,
	tkt.shipst,
	tkt.shipzip,
	tkt.shipcountry,
	tkt.salescommission,
	tkt.dateshipped,
	tkt.ticquantity,
	tkt.estmrhrs,
	tkt.actmrhrs,
	tkt.estwuhrs,
	tkt.actwuhrs,
	tkt.estrunhrs,
	tkt.actrunhrs,
	tkt.estfinhrs,
	tkt.estpackhrs,
	tkt.actpackhrs,
	tkt.estpressspd::text,
	tkt.actpressspd,
	tkt.actquantity,
	tkt.act_makeready_footage,
	tkt.esttotal,
	tkt.acttotalcost,
	tkt.eststockcost,
	tkt.actstockcost,
	tkt.stockdesc1,
	tkt.stockdesc2,
	tkt.stockdesc3,
	tkt.ticketstatus,
	tkt.estpresstime,
	tkt.terms,
	tkt.actualpresshours,
	tkt.actualtotalhours,
	tkt.actualpressrate::text,
	tkt.actualpresscost,
	tkt.actualrewindingrate::text,
	tkt.actualrewindingcost::float,
	tkt.actualfanfoldrate,
	tkt.actualfanfoldcost,
	tkt.actualpackagingrate,
	tkt.actualpackinglaborcost,
	tkt.actualbillings_netofsalestax,
	tkt.actualgrossmargin_dollars,
	tkt.actualgrossmargin_percent,
	tkt.actualrewindinghours,
	tkt.actualtotalfinishing,
	tkt.actualtotallaborcosts,
	tkt.actualtotalpocosts,
	tkt.actualtotalmatandfreightcost,
	tkt.est_setupfootage,
	tkt.est_spoilfootage,
	tkt.estpostpresshours,
	tkt.actpostpresshours,
	tkt.act_other_hours,
	tkt.actualpostpresslaborcost,
	tkt.actualotherlaborcost,
	tkt.shrinksleeve_overlap,
	tkt.shrinksleeve_layflat,
	tkt.shrinksleeve_cutheight,
	tkt.customer_total,
	pd.press_type,
	eq.description as press_description,
	trim(initcap(coalesce(cus.company, tkt.customername))) as customername,
	(((estfootage - est_spoilfootage - est_setupfootage)* actquantity)/ nullif(ticquantity, 0)) as materialrequiredperlt,
	tkt.press
from
	dt_inovar_prod_stg.in_amherst_fact_ticket tkt
left join dt_inovar_prod_edw.u_mat_inovar_press_type pd
		on
	trim(lower(tkt.press)) = trim(lower(pd.press))
	and pd.businessunit = 'Amherst Label'
left join dt_inovar_prod_stg.in_amherst_dim_equipment eq 
		on
	trim(lower(eq.number)) = trim(lower(tkt.press))
left join dt_inovar_prod_edw.v_inovar_customer_master cus 
		on
	trim(lower(cus.number)) = trim(lower(tkt.customernum))
		and cus.businessunit = 'Amherst Label'
	where
		tkt.orderdate >= '2020-01-01'
),
kansas_city as (
select distinct tab.house_number as number,
		concat('KSKA51-T-', tab.house_number) as company_ticket_number,
		'Kansas City' as location,
		'Tabco LLC' as company,
		'Kansas City' as businessunit,
		(case 
		when tab.sum_roll_length_in_ft_one_calc = '' 
			then 
				case 
					when tab.sum_roll_length_in_ft_two_calc = '' 
						then 
							case 
								when tab.sum_roll_length_in_ft_lam_calc = '' then '0'
								else tab.sum_roll_length_in_ft_lam_calc
							end
					else tab.sum_roll_length_in_ft_two_calc
				end
		else tab.sum_roll_length_in_ft_one_calc
		end)::float as actfootage,
		case 
		when tab.date_order = '' then null::date
		else tab.date_order::date
		end as orderdate,
		tab.cyrel_description as generaldescr,
		0::float as esttime,
		0::text as carrierwidth,
		0::text as sizeacross,
		0::text as sizearound,
		0::text as colspace,
		0::text as rowspace,
		0::float as labelrepeat,
		0::text as noacross,
		null::text as finishtype,
		tab.purchase_order_number as custponum,
		null::text as turnbar,
		concat('KSKA51-T-', tab.customer_number) as company_customer_number,
		tab.customer_number as customernum,
		0::float as estfootage,
		null::text as stocknum2,
		case 
		when tab.stock_width_lamination_lu = '' then 0::float
		else tab.stock_width_lamination_lu::float
		end as stockwidth2,
		'Kansas City' as shipcity,
		null::text as shipst,
		null::text as shipzip,
		'USA'::text as shipcountry,
		0::float as salescommission,
		case 
		when tab.date_shipped = '' then null::date
		else tab.date_shipped::date
		end as dateshipped,
		case 
		when tab.quantity_ordered_total = '' then 0::float
		else tab.quantity_ordered_total::float
		end as ticquantity,
		0::float as estmrhrs,
		0::float as actmrhrs,
		0::float as estwuhrs,
		0::float as actwuhrs,
		0::float as estrunhrs,
		0::float as actrunhrs,
		0::float as estfinhrs,
		0::float as estpackhrs,
		0::float as actpackhrs,
		0::text as estpressspd,
		0::float as actpressspd,
		case when tab.quantity_shipped = '' then 0::float
		else tab.quantity_shipped::float 
		end as actquantity,
		0::float as act_makeready_footage,
		0::float as esttotal,
		0::float as acttotalcost,
		0::float eststockcost,
		0::float as actstockcost,
--		0::float as actstockcost,
		null::text as stockdesc1,
		null::text as stockdesc2,
		null::text as stockdesc3,
		case
				when tab.invoice_reference_number ilike 'None' then null::text
				else 'Done'
			end as ticketstatus,
		0::float as estpresstime,
		null::text as terms,
		0::float as actualpresshours,
		0::float as actualtotalhours,
		0::text as actualpressrate,
		0::float as actualpresscost,
		0::text as actualrewindingrate,
		0::float as actualrewindingcost,
		0::float as actualfanfoldrate,
		0::float as actualfanfoldcost,
		0::float as actualpackagingrate,
		0::float as actualpackinglaborcost,
		0::float as actualbillings_netofsalestax,
		0::float as actualgrossmargin_dollars,
		0::float as actualgrossmargin_percent,
		0::float as actualrewindinghours,
		0::float as actualtotalfinishing,
		0::float as actualtotallaborcosts,
		0::float as actualtotalpocosts,
		0::float as actualtotalmatandfreightcost,
		0::float as est_setupfootage,
		0::float as est_spoilfootage,
		0::float as estpostpresshours,
		0::float as actpostpresshours,
		0::float as act_other_hours,
		0::float as actualpostpresslaborcost,
		0::float as actualotherlaborcost,
		0::float as shrinksleeve_overlap,
		0::float as shrinksleeve_layflat,
		0::float as shrinksleeve_cutheight,
		case 
		when tab.price_total_quoted_all = '' then 0::float
		else tab.price_total_quoted_all::float
		end as customer_total, --booking amount
		pd.press_type,
		null::text as press_description,
		tab.customer_name_calc as customername,
		0::float as materialrequiredperlt,
		tab.which_press
	from dt_inovar_prod_stg.in_kansascity_fact_orders tab
	left join dt_inovar_prod_stg.in_kansascity_dim_customers cus
		on tab.customer_number = cus.customer_number 
	left join dt_inovar_prod_edw.u_mat_inovar_press_type pd
		on tab.which_press = pd.press and pd.businessunit = 'Kansas City'
	where date_order >= '2021-01-01'
),
pl_history_quantity as
(
	select  distinct tkt.salesorderno, sum(tktit.quantityorderedrevised) as total_quantity_ordered , sum(tktit.quantityshipped) as total_quantity_shipped	, sum(tktit.lastextensionamt) as total_cost
	FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader tkt
    LEFT JOIN dt_inovar_prod_stg.in_fact_sage_so_salesorderhistorydetail tktit
    ON tkt.salesorderno::text = tktit.salesorderno::text
    where tkt.orderdate >= '2021-01-01'
    and tktit.cancelledline::text = 'N'::text AND tktit.deleteddate IS NULL
    group by 1
),
pl_normal_quantity as
(
	select  distinct tkt.salesorderno, sum(tktit.quantityordered) as total_quantity_ordered , sum(tktit.quantityshipped) as total_quantity_shipped	, sum(tktit.extensionamt) as total_cost
	FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderheader tkt
    LEFT JOIN dt_inovar_prod_stg.in_fact_sage_so_salesorderdetail tktit
    ON tkt.salesorderno::text = tktit.salesorderno::text
    where tkt.orderdate >= '2021-01-01'
    group by 1
),
cte_sum_pd as (
	select 
		salesorderno, 
		pressdescriptions,
		sum(totalactualpressfootage) as press_footage 
		from dt_inovar_prod_stg.in_precision_fact_r_cm_costs_per_combined_invoice_item 
		group by 1,2
),
cte_pressdesc as(
	select 
		salesorderno, 
		pressdescriptions,
		rank() over(partition by salesorderno order by press_footage desc,pressdescriptions) as rnk_press
	from cte_sum_pd
),
cte_sum_td as (
 	select 
		salesorderno, 
		ticketdescriptions,
		sum(netsales) as netsales 
	from dt_inovar_prod_stg.in_precision_fact_r_cm_costs_per_combined_invoice_item 
	group by 1,2
),
cte_tktdesc as(
	select 
		salesorderno, 
		ticketdescriptions,
		rank() over(partition by salesorderno order by netsales desc,ticketdescriptions) as rnk_tktdesc
	from cte_sum_td
),
cte_f as (
select
	salesorderno,
	min(shipdate) 								as ship_date_1st,
	sum(totalactualpressfootage) 				as totalactualpressfootage,
	sum(totalesitmatedpresstimeinhours)			as totalesitmatedpresstimeinhours,
	sum(totalestimatedpressfootage) 			as totalestimatedpressfootage,
	sum(totalactualpresshours)					as totalactualpresshours,
	sum(eststockcosts)							as eststockcosts,
	sum(actstockcosts)							as actstockcosts,
	sum(acttotallaborcost)						as acttotallaborcost
from dt_inovar_prod_stg.in_precision_fact_r_cm_costs_per_combined_invoice_item tkt
where shipdate >= '2021-01-01'
group by 1
),
--work_sales_order as (
--	select
--		workordersales.workorderid::text,
--		salesorderno,
--		sum(qtyorderedperm)	as qtyorderedperm
--	from dt_inovar_prod_stg.in_precision_fact_workorderitemsalesorder workordersales
--		where salesorderno is not null 
--		AND salesorderno IN (
--						SELECT DISTINCT 
--							salesorderno 
--						FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader
--							WHERE salesorderno IN 
--							(
--							SELECT DISTINCT 
--								salesorderno  
--							FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistorydetail 
--								WHERE itemno NOT ILIKE '/%'
--							)
--						)
--	group by 1,2
--),
--work_order_quantity as (
--	select 
--		work_sales_order.workorderid,
--		sum(work_sales_order.qtyorderedperm) AS work_order_quantity
--	from work_sales_order
--	group by work_sales_order.workorderid
--), 
--work_order_mapping as (
--	select 
--		work_sales_order.workorderid,
--		work_sales_order.salesorderno,
--		sum(work_sales_order.qtyorderedperm) / nullif(work_order_quantity.work_order_quantity, 0) ratio_of_qty
--	from work_sales_order
--	left join work_order_quantity on work_order_quantity.workorderid = work_sales_order.workorderid
--	group by work_sales_order.workorderid,
--			work_sales_order.salesorderno,
--			work_order_quantity.work_order_quantity
--),
--cm_work_order as (
--	select
--		workorderid,
--		q_webwidth, 
--		q_pressrate,
--		q_finishingrate,
--		rewindlaborcost
--	from dt_inovar_prod_stg.in_precision_fact_cm_work_orders	
--), 
--quotes AS (
--	select 
--		id, 
--		sizeac, 
--		sizear, 
--		gapac, 
--		gapar, 
--		numberacross, 
--		pressfootageperminute 
--	from (
--			SELECT  
--				id, 
--				sizeac, 
--				sizear, 
--				gapac, 
--				gapar, 
--				numberacross, 
--				pressfootageperminute,
--				row_number() over (partition by id) row_num
--			FROM dt_inovar_prod_stg.in_precision_fact_quotes
--			) a
--			where row_num = 1
--),
--work_order_quote as (
--	select
--		workorderid,
--		sizeac, 
--		sizear, 
--		gapac, 
--		gapar, 
--		numberacross, 
--		pressfootageperminute
--	from dt_inovar_prod_stg.in_precision_fact_cm_work_orders cm_work_order
--	JOIN quotes ON quotes.id = cm_work_order.quoteid
--),
--final_data as (
--	select
--		salesorderno,
--		string_agg(concat(work_order_mapping.workorderid,'-', q_webwidth::text), ', ') as q_webwidth,
--		string_agg(concat(work_order_mapping.workorderid,'-', q_pressrate::text), ', ') as q_pressrate,
--		string_agg(concat(work_order_mapping.workorderid,'-', q_finishingrate::text), ', ') as q_finishingrate,			
--		string_agg(concat(work_order_quote.workorderid,'-', sizeac::text), ', ') as sizeac,	
--		string_agg(concat(work_order_quote.workorderid,'-', sizear::text), ', ') as sizear,	
--		string_agg(concat(work_order_quote.workorderid,'-', gapac::text), ', ') as gapac,
--		string_agg(concat(work_order_quote.workorderid,'-', gapar::text), ', ') as gapar,
--		string_agg(concat(work_order_quote.workorderid,'-', numberacross::text), ', ') as numberacross,	
--		string_agg(concat(work_order_quote.workorderid,'-', sizeac::text), ', ') as pressfootageperminute,
--		sum(rewindlaborcost * ratio_of_qty) rewindlaborcost
--	from work_order_mapping
--	join cm_work_order on cm_work_order.workorderid::text = work_order_mapping.workorderid
--	join work_order_quote on work_order_quote.workorderid::text = work_order_mapping.workorderid
--	group by salesorderno
--),
oceanside as (
select 
	soh.salesorderno::text as number,
	concat('Carlsbad-', soh.salesorderno) as company_ticket_number,
	'Oceanside' as location,
	'Oceanside' as company,
	'Oceanside' as businessunit,
	totalactualpressfootage as actfootage,
	orderdate::date,
	ticketdescriptions as generaldescr,
	totalesitmatedpresstimeinhours as esttime,
--	q_webwidth::text as carrierwidth,
		null::text as carrierwidth,
--	sizeac::text as sizeacross,
		null::text as sizeacross,
--	sizear::text as sizearound,
		null::text as sizearound,
--	gapac::text as colspace,
		null::text as colspace,
--	gapar::text as rowspace,
		null::text as rowspace,
	0::float as labelrepeat,
--	numberacross::text as noacross,
		null::text as noacross,
	null::text as finishtype,
	soh.customerpono as custponum,
	null::text as turnbar,
	concat('Carlsbad-', soh.customerno) as company_customer_number,
	soh.customerno as customernum,
	totalestimatedpressfootage as estfootage,
	null::text as stocknum2,
	0::float as stockwidth2,
	soh.shiptocity as shipcity,
	soh.shiptostate as shipst,
	COALESCE(soh.shiptozipcode, '99999'::character varying) as shipzip,
	COALESCE(soh.shiptocountrycode, 'USA'::character varying) as shipcountry,
	0::float as salescommission,
	ship_date_1st as dateshipped,
	phq.total_quantity_ordered as ticquantity,
	0::float as estmrhrs,
	0::float as actmrhrs,
	0::float as estwuhrs,
	0::float as actwuhrs,
	totalactualpresshours as estrunhrs,
	totalactualpresshours as actrunhrs,
	0::float as estfinhrs,
	0::float as estpackhrs,
	0::float as actpackhrs,
--	pressfootageperminute::text as estpressspd,
		null::text as estpressspd,
	0::float as actpressspd,
	phq.total_quantity_shipped as actquantity,
	0::float as act_makeready_footage,
	0::float as esttotal,
	0::float as acttotalcost,
	eststockcosts	as eststockcost,
	actstockcosts 	as actstockcost,
	null::text as stockdesc1,
	null::text as stockdesc2,
	null::text as stockdesc3,
	case
		when soh.orderstatus in ('C','A') then 'Done'
		else null::text
	end as ticketstatus,
	0::float as estpresstime,
	null::text as terms,
	0::float as actualpresshours,
	0::float as actualtotalhours,
--	q_pressrate::text as actualpressrate,
		null::text as actualpressrate,
	0::float as actualpresscost,
--	q_finishingrate::text as actualrewindingrate,
		null::text as actualrewindingrate,
--	rewindlaborcost::float as actualrewindingcost,
		0::float as actualrewindingcost,
	0::float as actualfanfoldrate,
	0::float as actualfanfoldcost,
	0::float as actualpackagingrate,
	0::float as actualpackinglaborcost,
	0::float as actualbillings_netofsalestax,
	0::float as actualgrossmargin_dollars,
	0::float as actualgrossmargin_percent,
	0::float as actualrewindinghours,
	0::float as actualtotalfinishing,
	acttotallaborcost as actualtotallaborcosts,
	0::float as actualtotalpocosts,
	0::float as actualtotalmatandfreightcost,
	0::float as est_setupfootage,
	0::float as est_spoilfootage,
	0::float as estpostpresshours,
	0::float as actpostpresshours,
	0::float as act_other_hours,
	0::float as actualpostpresslaborcost,
	0::float as actualotherlaborcost,
	0::float as shrinksleeve_overlap,
	0::float as shrinksleeve_layflat,
	0::float as shrinksleeve_cutheight,
	phq.total_cost as customer_total, --booking amount
	null::text as presstype, --presstype mapping to be done
	pressdescriptions as press_description,
	soh.billtoname  as customername,
	0::float as materialrequiredperlt,
	pressdescriptions as press
FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader soh
join pl_history_quantity phq
	on phq.salesorderno = soh.salesorderno
left join cte_pressdesc
	on cte_pressdesc.salesorderno = soh.salesorderno and rnk_press = 1
left join cte_tktdesc 
	on cte_tktdesc.salesorderno = soh.salesorderno and rnk_tktdesc = 1
left join cte_f
	on cte_f.salesorderno = soh.salesorderno
--left join final_data
--	on final_data.salesorderno = soh.salesorderno
where soh.orderdate >= '2021-01-01'
	and soh.orderstatus::text <> 'X'::text
		union all
select 
	soh.salesorderno::text as number,
	concat('Carlsbad-', soh.salesorderno) as company_ticket_number,
	'Oceanside' as location,
	'Oceanside' as company,
	'Oceanside' as businessunit,
	totalactualpressfootage as actfootage,
	orderdate::date,
	ticketdescriptions as generaldescr,
	totalesitmatedpresstimeinhours as esttime,
--	q_webwidth::text as carrierwidth,
		null::text as carrierwidth,
--	sizeac::text as sizeacross,
		null::text as sizeacross,
--	sizear::text as sizearound,
		null::text as sizearound,
--	gapac::text as colspace,
		null::text as colspace,
--	gapar::text as rowspace,
		null::text as rowspace,
	0::float as labelrepeat,
--	numberacross::text as noacross,
		null::text as noacross,
	null::text as finishtype,
	soh.customerpono as custponum,
	null::text as turnbar,
	concat('Carlsbad-', soh.customerno) as company_customer_number,
	soh.customerno as customernum,
	totalestimatedpressfootage as estfootage,
	null::text as stocknum2,
	0::float as stockwidth2,
	soh.shiptocity as shipcity,
	soh.shiptostate as shipst,
	COALESCE(soh.shiptozipcode, '99999'::character varying) as shipzip,
	COALESCE(soh.shiptocountrycode, 'USA'::character varying) as shipcountry,
	0::float as salescommission,
	ship_date_1st as dateshipped,
	pnq.total_quantity_ordered as ticquantity,
	0::float as estmrhrs,
	0::float as actmrhrs,
	0::float as estwuhrs,
	0::float as actwuhrs,
	totalactualpresshours as estrunhrs,
	totalactualpresshours as actrunhrs,
	0::float as estfinhrs,
	0::float as estpackhrs,
	0::float as actpackhrs,
--	pressfootageperminute::text as estpressspd,
		null::text as estpressspd,
	0::float as actpressspd,
	pnq.total_quantity_shipped as actquantity,
	0::float as act_makeready_footage,
	0::float as esttotal,
	0::float as acttotalcost,
	eststockcosts	as  eststockcost,
	actstockcosts 	as actstockcost,
--		0::float as actstockcost,
	null::text as stockdesc1,
	null::text as stockdesc2,
	null::text as stockdesc3,
	case
		when soh.orderstatus in ('C','A') then 'Done'
		else null::text
	end as ticketstatus,
	0::float as estpresstime,
	null::text as terms,
	0::float as actualpresshours,
	0::float as actualtotalhours,
--	q_pressrate::text as actualpressrate,
		null::text as actualpressrate,
	0::float as actualpresscost,
--	q_finishingrate::text as actualrewindingrate,
		null::text as actualrewindingrate,
--	rewindlaborcost::float as actualrewindingcost,
		0::float as actualrewindingcost,
	0::float as actualfanfoldrate,
	0::float as actualfanfoldcost,
	0::float as actualpackagingrate,
	0::float as actualpackinglaborcost,
	0::float as actualbillings_netofsalestax,
	0::float as actualgrossmargin_dollars,
	0::float as actualgrossmargin_percent,
	0::float as actualrewindinghours,
	0::float as actualtotalfinishing,
	acttotallaborcost as actualtotallaborcosts,
	0::float as actualtotalpocosts,
	0::float as actualtotalmatandfreightcost,
	0::float as est_setupfootage,
	0::float as est_spoilfootage,
	0::float as estpostpresshours,
	0::float as actpostpresshours,
	0::float as act_other_hours,
	0::float as actualpostpresslaborcost,
	0::float as actualotherlaborcost,
	0::float as shrinksleeve_overlap,
	0::float as shrinksleeve_layflat,
	0::float as shrinksleeve_cutheight,
	pnq.total_cost as customer_total, --booking amount
	null::text as presstype, --presstype mapping to be done
	pressdescriptions as press_description,
	soh.billtoname  as customername,
	0::float as materialrequiredperlt,
	pressdescriptions as press
FROM dt_inovar_prod_stg.in_fact_sage_so_salesorderheader soh
join pl_normal_quantity pnq
	on pnq.salesorderno = soh.salesorderno
left join cte_pressdesc
	on cte_pressdesc.salesorderno = soh.salesorderno and rnk_press = 1
left join cte_tktdesc 
	on cte_tktdesc.salesorderno = soh.salesorderno and rnk_tktdesc = 1
left join cte_f
	on cte_f.salesorderno = soh.salesorderno
--left join final_data
--	on final_data.salesorderno = soh.salesorderno
where soh.orderdate >= '2021-01-01'
	and soh.salesorderno not in (select salesorderno from dt_inovar_prod_stg.in_fact_sage_so_salesorderhistoryheader)
),
final_union as (
select * from butler
	union all
select * from dallas
	union all
select * from davie
	union all
select * from westfield
	union all
select * from newburyport
	union all
select * from cimarron_north
	union all
select * from amherst_label
	union all
select * from kansas_city
	union all
select * from oceanside
)
select
	"number",
	company_ticket_number,
	"location",
	company,
	businessunit,
	actfootage,
	orderdate,
	generaldescr,
	esttime,
	carrierwidth,
	sizeacross,
	sizearound,
	colspace,
	rowspace,
	labelrepeat,
	noacross,
	finishtype,
	custponum,
	turnbar,
	trim(upper(company_customer_number)) as company_customer_number,
	customernum,
	estfootage,
	stocknum2,
	stockwidth2,
	shipcity,
	shipst,
	shipzip,
	shipcountry,
	salescommission,
	dateshipped,
	ticquantity,
	estmrhrs,
	actmrhrs,
	estwuhrs,
	actwuhrs,
	estrunhrs,
	actrunhrs,
	estfinhrs,
	estpackhrs,
	actpackhrs,
	estpressspd,
	actpressspd,
	actquantity,
	act_makeready_footage,
	esttotal,
	acttotalcost,
	eststockcost,
	actstockcost,
	stockdesc1,
	stockdesc2,
	stockdesc3,
	ticketstatus,
	estpresstime,
	terms,
	actualpresshours,
	actualtotalhours,
	actualpressrate,
	actualpresscost,
	actualrewindingrate,
	actualrewindingcost,
	actualfanfoldrate,
	actualfanfoldcost,
	actualpackagingrate,
	actualpackinglaborcost,
	actualbillings_netofsalestax,
	actualgrossmargin_dollars,
	actualgrossmargin_percent,
	actualrewindinghours,
	actualtotalfinishing,
	actualtotallaborcosts,
	actualtotalpocosts,
	actualtotalmatandfreightcost,
	est_setupfootage,
	est_spoilfootage,
	estpostpresshours,
	actpostpresshours,
	act_other_hours,
	actualpostpresslaborcost,
	actualotherlaborcost,
	shrinksleeve_overlap,
	shrinksleeve_layflat,
	shrinksleeve_cutheight,
	customer_total,
	case 
		when trim(lower(final_union.press_type)) = 'flexo' then 'Flexo Press'
		when trim(lower(final_union.press_type)) = 'digital' then 'Digital Press'
		when trim(lower(final_union.press_type)) = 'hybrid' then 'Hybrid Press'
		when trim(lower(final_union.press_type)) = 'digital finishing' then 'Digital Finishing Equipment'
		when trim(lower(final_union.press_type)) = 'large format' then 'Large Format Press'
		when trim(lower(final_union.press_type)) = 'other' then 'Other Equipment (E.G., Finishing and QC)'
		when trim(lower(final_union.press_type)) = 'flexo finishing equipment' then 'Other Equipment (E.G., Finishing and QC)'
		when trim(lower(final_union.press_type)) = 'rewinder' then 'Other Equipment (E.G., Finishing and QC)'
		when trim(lower(final_union.press_type)) = 'slitter/rewinder' then 'Other Equipment (E.G., Finishing and QC)'
		else coalesce(initcap(trim(final_union.press_type)), 'Other Equipment (E.G., Finishing and QC)')
	end as press_type,
	press_description,
	customername,
--	case
--		when lower(trim(customernum)) in ('wil02', 'wil02-i', 'syn01', 'syn02') then 'Syngenta Crop Protection Llc'
--		else customername
--	end									as customername,
	materialrequiredperlt,
	trim(upper(press)) 					as press,
	(case
		when extract(DOW
	from
		orderdate) = 6 then (orderdate - interval '1 DAY')
		when extract(DOW
	from
		orderdate) = 0 then (orderdate - interval '2 DAYS')
		else orderdate
	end)::date as orderdate_adj
from
	final_union