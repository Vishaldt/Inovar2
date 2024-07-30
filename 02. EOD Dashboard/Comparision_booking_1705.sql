
select 
	businessunit,
	sum(linetotal)
from dt_inovar_prod_edw.u_mat_inovar_ticket_sandbox
where orderdate::date>='2023-01-01' --and orderdate::date<='2024-04-01'
and stock_ticket_flag = '1'
	and businessunit not in ('Aberdeen', 'Cimarron South')
group by 1







select 
	businessunit,
	sum(booking_revenue)
from dt_inovar_prod_edw.u_mat_executive_daily_flash
where orderdate::date='2023-04-30' --and orderdate::date<='2024-04-01'
and stock_ticket_flag = '1'
	and businessunit not in ('Aberdeen', 'Cimarron South')
group by 1

----------------------------------------------------------


SELECT
--		distinct company_customer_number,
		businessunit,
--		date_trunc('year', order_date::date) as orderdate,
		sum(booking_total)
--		count(*)
 from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard 
where order_date::date='2023-04-31' --and order_date::date<='2024-04-01'
group by 1



dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard


select * from  dt_inovar_prod_edw.u_mat_inovar_ticket_sandbox