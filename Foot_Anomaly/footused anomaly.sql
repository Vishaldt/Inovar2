------------ Cimarron footage anomaly (04/18)

select 
	id, 
	assocno, 
	ticket_no, 
	workoperation, 
	sdate, 
	edate, 
	elapsed, 
	pressno, 
	footused
from dt_inovar_prod_stg.in_cimarron_fact_timecard
where sdate::date >= '2022-01-01'
--	and id in ('1339559')
	and (footused > 10000000
		or footused < -10000000)
	
	
--	SQL for @Shashank Sharma

select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard  where SDate > '2022-01-01'
and Id = ('1339559')




select id, assocno , ticket_no , workoperation , sdate , edate , elapsed , pressno , footused 
from dt_inovar_prod_stg.in_westfield_fact_timecard 
where sdate::date = '2024-05-16' and id ='1643679'

select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard  where SDate > '2022-01-01'
and Id = ('1643679')

-- footused anomaly
select entity, id, assocno, ticket_no, workoperation, sdate, edate, elapsed, pressno, footused
from dt_inovar_prod_edw.fact_timecard
where sdate::date >= '2022-01-01'
--	and id in ('1339559')
and (footused > 10000000
or footused < -10000000)

----------------------------------------------------------------------------------Amherst 07-05-2024-------------------------------------------------------------------
select id, assocno, ticket_no, workoperation, sdate, edate, elapsed, pressno, footused 
from dt_inovar_prod_stg.in_Amherst_fact_timecard
where sdate::date >= '2022-01-01'
--	and id in ('628710')
	and (footused > 1000000
	or footused < -1000000)
		
	
----------------------	
	


--------------------------------------------------------Oceanside-------------------------------------------


select workorderid,workorderfinishdate, sum(totalrunfootagebyoperator::float) totalrunfootagebyoperator
from dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date 
where  workorderid = ('79958')
group by 1, 2
order by 3 desc



		
		
select workorderid, workorderfinishdate ,totalrunfootagebyoperator from dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date 
where workorderfinishdate::date >= '2022-01-01'
and workorderid = ('79958')

	
select *
from dt_inovar_prod_stg.in_fact_kpi_work_order_by_associate_by_date 
where
workorderfinishdate::date >= '2022-01-01'
--	and id in ('1339559')
	/*and 
	(totalworkorderfootagebyoperator::float > 1000000
		or totalworkorderfootagebyoperator::float < -1000000)	*/	
		
		--	SQL for @Shashank Sharma

select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard  where SDate > '2022-01-01'
and Id = ('628710')


--------------------------------------------------------


---------------------------------------------
select distinct snapshot_date::date,backlog_type ,backlog_amount,linetotal,snapshot_date,ticketstatus  ,shippingstatus  ,* from dt_inovar_prod_stg.in_backlog_sandbox_snapshot ibss 
where businessunit = 'Dallas'
and snapshot_date::date ='2024-05-07'
and backlog_amount >20000
and company_ticket_number in ('DALLAS-77618','DALLAS-59457')
order by backlog_amount desc


------------------------------------------------------for CM----------------------------

select * from dt_inovar_prod_edw.v_cm_sandbox

select * from dt_inovar_prod_edw.fact_ap_invoice fai 
where invoice_date::date ='2024-05-08'


and businessunit  = 'Kansas City'



select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard  where SDate > '2022-01-01'
and Id = ('188866')



select businessunit company_invoice_number, itype, customer_number ,customer_name , (invoice_revenue) as invoice_revenue , 
(acttotalcost) as acttotalcost, (invoice_revenue - acttotalcost) as CM,*
from dt_inovar_prod_edw.u_mat_cm_sandbox_optimization_lt 
where /*(invoice_revenue - acttotalcost) < 0*/ businessunit  = 'Kansas City' and invoice_date::date = '2024-05-08'
order by invoice_date::date


-------------------------KANSAS CITY  CM----------------------------------------------------------------
select businessunit, company_invoice_number, invoice_date, itype, customer_number ,customer_name , (invoice_revenue) as invoice_revenue , (acttotalcost) as acttotalcost, (invoice_revenue - acttotalcost) as CM
from dt_inovar_prod_edw.u_mat_cm_sandbox_optimization_tabco
where company_invoice_number ilike '%166556%' /*and (invoice_revenue - acttotalcost) < 0*/
order by (invoice_revenue - acttotalcost) asc



select house_number_int_calc ,house_number ,date_order ,invoice_reference_number ,material_costs_total,po_request_purchased_price_total_calc 
* from dt_inovar_prod_stg.in_kansascity_fact_orders
where house_number  = '136267'

and 


select entity,sp_inventory_add_id ,*
from dt_inovar_prod_edw.u_mat_cm_sandbox_release_lt 
where entity  = 'Butler' and sp_inventory_add_id = '154689'

where /*(invoice_revenue - acttotalcost) < 0*/ businessunit  = 'Milwaukee' and invoice_date::date = '2024-05-08'


order by (invoice_revenue - acttotalcost) asc

and invoice_date::date = '2024-05-08'


where company_invoice_number ilike '%166556%' and (invoice_revenue - acttotalcost) < 0
order by (invoice_revenue - acttotalcost) asc



-------------------------------
select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard  where SDate > '2022-01-01'
and Id = ('188866')




select ticket_no, id , workoperation , pressno, sdate , edate , stime , etime ,elapsed , footused  from dt_inovar_prod_stg.in_amherst_fact_timecard 
--where sdate::date = '2024-05-09'
where ticket_no = '188866'
and workoperation ilike 'run'
and workoperation ilike 'run'


select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard  where SDate > '2022-01-01'
and Id = ('188866')

-------------------------------------17-05 westfield Footage ------------------------------------



select id, assocno , ticket_no , workoperation , sdate , edate , elapsed , pressno , footused 
from dt_inovar_prod_stg.in_westfield_fact_timecard 
where sdate::date = '2024-05-16'  and id ='1643679'


------------------------------------Cimarron sales


select * from dt_inovar_prod_stg.in_cimarron_fact_invoice
where invoice_date::date = '2024-05-22'

-----------------24-05--------Amherst Label


select id, assocno , ticket_no , workoperation , sdate , edate , elapsed , pressno , footused 
from dt_inovar_prod_stg.in_amherst_fact_timecard 
where /*footused > 1000000 
and */ id = '632194' 



select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard  where SDate > '2022-01-01'
and Id = ('632194')








------------31-05	-----------------------------


select
	businessunit, 
	company_invoice_number,
	invoice_date::date,
	itype, 
	customer_number ,
	customer_name , 
	(invoice_revenue) as invoice_revenue , 
	(acttotalcost) as acttotalcost, 
	(invoice_revenue - acttotalcost) as CM
from dt_inovar_prod_edw.u_mat_cm_sandbox_optimization_tabco
where /*company_invoice_number ilike '%166556%' and */(invoice_revenue - acttotalcost) < -100000
and invoice_date::date ='2024-05-28'
order by (invoice_revenue - acttotalcost) asc



----------------

			
-----------------04-06 and 10-06------------
select
	businessunit, 
	company_invoice_number,
	invoice_date::date,
	itype, 
	customer_number ,
	customer_name , 
	(invoice_revenue) as invoice_revenue , 
	(acttotalcost) as acttotalcost, 
	(invoice_revenue - acttotalcost) as CM
from dt_inovar_prod_edw.u_mat_cm_sandbox_optimization_tabco
where /*company_invoice_number ilike '%166556%' and */(invoice_revenue - acttotalcost) < -100000
and invoice_date::date ='2024-05-28'
order by (invoice_revenue - acttotalcost) asc







-------------------10-06 Bookings for CS and aberdeen	------------------------------------------------------------------------------------------
select 	
	case 
		when mfglocation.id = '1' then 'aberdeen'
		when mfglocation.id = '5001' then 'cs'
	end							as business_unit,
	job.ccdatesetup ,
	sum(jobpart.originalquotedprice)  originalquotedprice
FROM dt_inovar_dev_stg.in_aberdeen_fact_job job
LEFT JOIN dt_inovar_dev_stg.in_aberdeen_fact_jobpart jobpart
	ON jobpart.ccmasterid = job.ccmasterid
left join dt_inovar_dev_stg.in_aberdeen_dim_manufacturinglocation mfglocation 
	on mfglocation.id = job.manufacturinglocation
	where job.ccdatesetup::date>='2024-05-25'
	group by 1,2
order by 1,2 desc




-----------------------------------27-06


select ticket_number,
	backlog_amount ,
	linetotal ,
	invoice_item_total ,
	ticket_item_id ,
	orderdate ,
	snapshot_date ,*
from dt_inovar_prod_stg.in_backlog_sandbox_snapshot
where snapshot_date::date ='2024-06-25'
and location = 'Amherst Label'
and ticket_number = '190614'


------------

select customer_total ,*
from dt_inovar_prod_stg.in_amherst_fact_ticket iaft
where "number" = '190614'



select ticketnumber, linetotal,*
from dt_inovar_prod_stg.in_amherst_fact_ticketitem 
where ticketnumber= '190614'

-----------------
--------------------------------------27-06

select id,assocno ,ticket_no ,workoperation ,sdate ,edate ,elapsed ,pressno ,footused 
from dt_inovar_prod_stg.in_westfield_fact_timecard 
where footused < -1000000


select id,assocno ,ticket_no ,workoperation ,sdate ,edate ,elapsed ,pressno ,footused 
from dt_inovar_prod_stg.in_westfield_fact_timecard 
where Id = ('1653228')



select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard  where SDate > '2022-01-01'
and Id = ('1653228')


-----------------------------------------------------------------------------0407 KANSAS


select date_order,house_number, customer_name_calc, price_total_quoted_all
from dt_inovar_prod_stg.in_kansascity_fact_orders 
where house_number = '136045'


-----------------------------------------Bookings Amherst 0907
select esttotal,*
from dt_inovar_prod_stg.in_amherst_fact_ticket  
where orderdate >'2024-07-07'
order by 1 desc


where house_number = '136045'



select id ,ticketnumber,orderquantity ,linetotal  from dt_inovar_prod_stg.in_amherst_fact_ticketitem
where ticketnumber ='190949'
--order by 3 desc


----------------------


select id,assocno, ticket_no , workoperation , sdate , edate, elapsed ,pressno , footused
from dt_inovar_prod_stg.in_westfield_fact_timecard 
where footused > 10000000
and id = '1655552'




select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard  where SDate > '2022-01-01'
and Id = ('1655552')



select snapshot_date ,businessunit ,company_ticket_number ,ticket_item_id ,linetotal ,backlog_amount ,orderdate ,backlog_type 
from dt_inovar_prod_stg.in_backlog_sandbox_snapshot
where ticket_item_id = '190949'
and snapshot_date ='2024-06-10 10:06:54.621'


-----------------------------------23-07



select footused, *
from dt_inovar_prod_stg.in_newburyport_fact_timecard 
where id = '199863'



select id ,assocno ,ticket_no ,workoperation ,sdate ,edate ,elapsed ,pressno ,footused
from dt_inovar_prod_stg.in_newburyport_fact_timecard
where id in ('199858','199863')


--------------------westfield--------------------------------------
select id ,assocno ,ticket_no ,workoperation ,sdate ,edate ,elapsed ,pressno ,footused
from dt_inovar_prod_stg.in_westfield_fact_timecard 
where id = '1659151'


select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard  where SDate > '2022-01-01'
and Id = ('1659151')
