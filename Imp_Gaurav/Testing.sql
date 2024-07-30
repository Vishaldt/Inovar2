------------------------------------------------------------ otsname cleanup (start) (07/05) -----------------------------------

--------- u_mat_executive_daily_flash

--drop table dt_inovar_prod_edw.u_mat_executive_daily_flash_test

create table dt_inovar_prod_edw.u_mat_executive_daily_flash_test as
select * from dt_inovar_prod_edw.v_exec_daily_flash_u_mat_feed_test;

select 'pre' as flg, record_type, count(*) as records, sum(budget) as budget, sum(booking_revenue) as booking_revenue, sum(invoice_revenue) as invoice_revenue
from dt_inovar_prod_edw.u_mat_executive_daily_flash
--from dt_inovar_prod_edw.u_mat_executive_daily_flash_test
group by 1, 2 order by 1, 2


--------- u_mat_cm_sandbox

--drop table dt_inovar_prod_edw.u_mat_cm_sandbox
	
create table dt_inovar_prod_edw.u_mat_cm_sandbox as
select * from dt_inovar_prod_edw.v_cm_sandbox

--drop table dt_inovar_prod_edw.u_mat_cm_sandbox_test
	
create table dt_inovar_prod_edw.u_mat_cm_sandbox_test as
select * from dt_inovar_prod_edw.v_cm_sandbox_test

select businessunit, count(*) as records, count(distinct company_invoice_number) as invoices, sum(invoice_revenue) as invoice_revenue, sum(cm) as cm
from dt_inovar_prod_edw.u_mat_cm_sandbox
--from dt_inovar_prod_edw.u_mat_cm_sandbox_test
group by 1 order by 1


--------- u_mat_inovar_ticket_sandbox

--drop table dt_inovar_prod_edw.u_mat_inovar_ticket_sandbox_test
	
create table dt_inovar_prod_edw.u_mat_inovar_ticket_sandbox_test as
select * from dt_inovar_prod_edw.v_inovar_ticket_sandbox_test;

select businessunit, count(*) as records, count(distinct company_ticket_number) as tickets, sum(linetotal) as booking_revenue
from dt_inovar_prod_edw.u_mat_inovar_ticket_sandbox
--from dt_inovar_prod_edw.u_mat_inovar_ticket_sandbox_test
group by 1 order by 1


--------- u_mat_invoice_sandbox

--drop table dt_inovar_prod_edw.u_mat_invoice_sandbox_test
	
create table dt_inovar_prod_edw.u_mat_invoice_sandbox_test as
select * from dt_inovar_prod_edw.v_invoice_sandbox_test;

select businessunit, count(*) as records, count(distinct company_invoice_number) as invoices, sum(invoice_item_total) as invoice_revenue
from dt_inovar_prod_edw.u_mat_invoice_sandbox
--from dt_inovar_prod_edw.u_mat_invoice_sandbox_test
group by 1 order by 1


--------- u_mat_bookings_sales_sandbox

--drop table dt_inovar_prod_edw.u_mat_bookings_sales_sandbox_test
	
--create table dt_inovar_prod_edw.u_mat_bookings_sales_sandbox_test as
--select * from dt_inovar_prod_edw.v_bookings_sales_sandbox_test

select record_type, businessunit, count(*) as records, count(distinct company_invoice_number) as invoices, count(distinct company_ticket_number) as tickets,
	sum(invoice_item_total) as invoice_revenue, sum(booking_amount) as booking_amount
from dt_inovar_prod_edw.u_mat_bookings_sales_sandbox
--from dt_inovar_prod_edw.u_mat_bookings_sales_sandbox_test
group by 1, 2 order by 1, 2


--------- u_mat_bookings_to_invoice_sandbox

--drop table dt_inovar_prod_edw.u_mat_bookings_to_invoice_sandbox_test;
	
create table dt_inovar_prod_edw.u_mat_bookings_to_invoice_sandbox_test as
select * from dt_inovar_prod_edw.v_bookings_to_invoice_sandbox_test;

select businessunit, count(*) as records, count(distinct company_invoice_number) as invoices, count(distinct company_ticket_number) as tickets,
	sum(invoice_item_total) as invoice_revenue, sum(booking_amount) as booking_amount
from dt_inovar_prod_edw.u_mat_bookings_to_invoice_sandbox
--from dt_inovar_prod_edw.u_mat_bookings_to_invoice_sandbox_test
group by 1 order by 1

------------------------------------------------------------- otsname cleanup (end) (07/05) ------------------------------------