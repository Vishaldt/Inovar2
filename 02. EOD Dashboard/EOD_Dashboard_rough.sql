	

select 'test',date_trunc('month',sdate::date)::date dt  ,count(*), sum(footused)footused, sum(elapsed)elapsed
from dt_inovar_dev_stg.u_mat_sales_bookings_eod_dashboard_test
where sdate>'2024-01-01'
group by 1,2
union all


select 'edw',date_trunc('month',sdate::date)::date dt ,count(*), sum(footused)footused, sum(elapsed)elapsed,sum(backlog_amount) backlog_amount
from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard
where sdate>'2023-01-01'
group by 1,2


select distinct businessunit, 
count(*),
	sum(elapsed)elapsed,
	sum(footused)footused ,
	sum(booking_total)booking_total,
	sum(invoice_revenue)invoice_revenue,
	sum(backlog_amount) backlog_amount
from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard
--where is_operations =1
group by 1


select  businessunit,
count(*),
	sum(elapsed)elapsed,
	sum(footused)footused ,
	sum(booking_total)booking_total,
	sum(invoice_revenue)invoice_revenue,
	sum(backlog_amount) backlog_amount
from dt_inovar_dev_stg.u_mat_sales_bookings_eod_dashboard_test
--where businessunit = 'Sioux Falls - LA'
group by 1


--------------------------------------------------------------------------------
select businessunit ,
date_trunc('month',dt) as dt, 
	sum(booking_revenue) booking_revenue,
	sum(invoice_revenue) invoice_revenue,
	sum(backlog_amount) backlog_amount
from dt_inovar_prod_edw.u_mat_executive_daily_flash 
where dt >'2024-01-01'
and businessunit = 'Sioux Falls - LA'
group by 1,2



select businessunit ,
date_trunc('month',dt) as dt, 
	sum(booking_total) booking_revenue,
	sum(invoice_revenue) invoice_revenue,
	sum(backlog_amount) backlog_amount
from dt_inovar_dev_stg.u_mat_sales_bookings_eod_dashboard_test
where dt >'2024-01-01'
and businessunit = 'Sioux Falls - LA'
group by 1,2








select distinct record_type from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard


footused_tabco_ex_booklet,
		footused_tabco_good_ex_booklet,
		
		
---------------------------------1607
		
		
		select * from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard
		
		
		select  from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard
	
		
		
		

select businessunit,record_type , 
count(*),
	sum(elapsed)elapsed,
	sum(footused)footused ,
	sum(booking_total)booking_total,
	sum(invoice_revenue)invoice_revenue,
	sum(backlog_amount) backlog_amount
from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard
--where is_operations =1
group by 1,2		


select distinct businessunit from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard

select businessunit ,
--	press_type,
	pressno ,
	count(*),
	sum(elapsed) elapsed,
	sum(footused) footused 
from dt_inovar_prod_edw.u_mat_sales_bookings_eod_dashboard
where is_operations =1 and press_type='Other Equipment (E.G., Finishing and QC)' and businessunit ='Sioux Falls - LA'
group by 1,2		
		
		
		
		
		
		