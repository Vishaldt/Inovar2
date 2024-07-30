
select 'test', businessunit,count(*),sum(customer_total)customer_total, count(distinct company_ticket_number) from dt_inovar_dev_stg.u_mat_inovar_pricing_sandbox_test
group by 1,2

select * from dt_inovar_prod_edw.u_mat_inovar_ticket_sandbox


select businessunit,sum(linetotal)linetotal from  dt_inovar_prod_edw.u_mat_inovar_ticket_sandbox 
where line_item_type ='Ticket Header'
group by 1


select businessunit , sum(booking_revenue) from dt_inovar_prod_edw.u_mat_executive_daily_flash
group by 1



select * from  dt_inovar_prod_edw.u_mat_inovar_ticket_sandbox


where line_item_type ='Ticket Header'
group by 1



select 'test',businessunit, date_trunc('month',orderdate) ,count(*),sum(customer_total)customer_total,
count(distinct company_ticket_number) from dt_inovar_dev_stg.u_mat_inovar_pricing_sandbox_test
where orderdate::date>'2024-01-01'
group by 1,2,3



select * from dt_inovar_dev_stg.u_mat_inovar_pricing_sandbox_test

----------------------------24-07

select businessunit ,count(*),count(distinct company_customer_number)company_customer_number from dt_inovar_prod_edw.u_mat_inovar_pricing_sandbox 
group by 1



select * from dt_inovar_prod_edw.u_mat_inovar_pricing_sandbox 
--group by 1


select number,company_ticket_number, "location",company,businessunit,
actfootage,
from dt_inovar_prod_stg.in_precision_fact_r_cm_operations_to_labeltraxx


"Foil Stamp"0
------------------------24-07

select businessunit, max(orderdate),min(orderdate) from  dt_inovar_dev_stg.u_mat_inovar_pricing_sandbox_test
group by 1



select businessunit, max(orderdate),min(orderdate) from  dt_inovar_prod_edw.u_mat_inovar_pricing_sandbox
group by 1

select /*date_trunc('year', orderdate::date)::date dt*/ businessunit,   count(*),sum(esttime)esttime, sum(carrierwidth::float)carrierwidth,
sum(sizeacross::float)sizeacross, sum(sizearound::float)sizearound, sum(colspace::float)colspace,sum(rowspace::float) rowspace,
sum(labelrepeat)labelrepeat, sum(esttotal)esttotal, sum(acttotalcost)acttotalcost
,sum(eststockcost)eststockcost, sum(actstockcost)actstockcost, sum(actualpresshours)actualpresshours,sum(actualtotalhours)actualtotalhours, sum(actualpressrate::float)actualpressrate,
sum(actualpresscost)actualpresscost, sum(actualrewindingrate::float)actualrewindingrate,sum(actualrewindingcost)actualrewindingcost, sum(actualfanfoldrate)actualfanfoldrate,
sum(actualfanfoldcost)actualfanfoldcost,sum(actualtotalpocosts)actualtotalpocosts,sum(actualtotalmatandfreightcost)actualtotalmatandfreightcost
from  dt_inovar_dev_stg.u_mat_inovar_pricing_sandbox_test
--where businessunit = ''
group by 1
order by 1


select businessunit ,  count(*),sum(esttime)esttime, sum(carrierwidth::float)carrierwidth,
sum(sizeacross::float)sizeacross, sum(sizearound::float)sizearound, sum(colspace::float)colspace,sum(rowspace::float) rowspace, sum(labelrepeat)labelrepeat,
sum(esttotal)esttotal, sum(acttotalcost)acttotalcost
,sum(eststockcost)eststockcost, sum(actstockcost)actstockcost, sum(actualpresshours)actualpresshours,sum(actualtotalhours)actualtotalhours, sum(actualpressrate::float)actualpressrate,
sum(actualpresscost)actualpresscost, sum(actualrewindingrate::float)actualrewindingrate,sum(actualrewindingcost)actualrewindingcost, sum(actualfanfoldrate)actualfanfoldrate,
sum(actualfanfoldcost)actualfanfoldcost,sum(actualtotalpocosts)actualtotalpocosts,sum(actualtotalmatandfreightcost)actualtotalmatandfreightcost 
from  dt_inovar_prod_edw.u_mat_inovar_pricing_sandbox
--where businessunit = 'Oceanside'
group by 1
order by 1



select * from  dt_inovar_dev_stg.u_mat_inovar_pricing_sandbox_test
where businessunit = 'Oceanside'

select SUM(ac), max(orderdate), min(orderdate) from  dt_inovar_dev_stg.u_mat_inovar_pricing_sandbox_test
where businessunit = 'Oceanside'

select businessunit ,count(*),count(distinct company_customer_number)company_customer_number,company_customer_number  from dt_inovar_prod_edw.u_mat_inovar_pricing_sandbox 
group by 1




----------------------



select min(orderdate) from (
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
	where date_order >= '2020-01-01'
)a

