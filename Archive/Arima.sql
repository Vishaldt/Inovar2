	

	SELECT 
		snapshot_date::date,
		businessunit,
		company_ticket_number,
		orderdate::date,
		backlog_type, 
		company_customer_number,
		customername,
		CASE 
			WHEN press = '' THEN NULL 
			ELSE press 
		END AS press,
		otsname,
		sum(linetotal) AS linetotal
	FROM dt_inovar_prod_stg.in_backlog_sandbox_snapshot backlog
		WHERE snapshot_date::date >= '2024-04-01'
			--AND backlog_type <> 'Shelf Backlog'
			AND linetotal <> 0
			AND businessunit IN ('Amherst Label', 'Cimarron North', 'Dallas', 'Ft. Lauderdale', 'Milwaukee', 'Newburyport', 'Westfield')
	GROUP BY snapshot_date::date,
			businessunit,
			company_ticket_number,
			orderdate,
			backlog_type, 
			company_customer_number,
			customername,
			press,
			otsname
			
	
	
	
	
	
	
	
	------------ Cimarron footage anomaly (04/18)

select id, assocno, ticket_no, workoperation, sdate, edate, elapsed, pressno, footused
from dt_inovar_prod_stg.in_cimarron_fact_timecard
where sdate::date >= '2022-01-01'
	and id in ('1339559')
--	and (footused > 10000000
--		or footused < -10000000)
	
	
	
	select Id, AssocNo, Ticket_No, WorkOperation, SDate, EDate, Elapsed, PressNo, FootUsed
from Timecard where SDate > '2022-01-01'
and Id = ('1339559')




select entity, id, assocno, ticket_no, workoperation, sdate, edate, elapsed, pressno, footused
from dt_inovar_prod_edw.fact_timecard
where sdate::date >= '2022-01-01'
--	and id in ('1339559')
	and (footused > 10000000
		or footused < -10000000)
		
		
		
		
		
--------------------------------------------------------------------------------------------------------------
		
--------------------------------------------------------------------------------------------------------------

		WITH 
holiday AS (
	SELECT  
		"Entity" AS businessunit,
		"Date"::date AS dt,
		"Holiday_Flag" AS holiday_flag
	FROM dt_inovar_prod_stg.in_holiday_schedule
		WHERE "Entity" IN ('Amherst Label', 'Cimarron North', 'Dallas', 'Ft. Lauderdale', 'Milwaukee', 'Newburyport', 'Westfield')
		AND "Holiday_Flag" = 1
),
date_series AS (
	SELECT 
		generate_series('2020-01-01', date_trunc('month', current_date) + INTERVAL '3 month - 1 day' , '1 day')::date AS dt
),
date_cross_bu AS (
	SELECT * FROM date_series
	CROSS JOIN (
					SELECT DISTINCT 
						businessunit
					FROM holiday
					) a
),
final_date_series AS (
	SELECT 
		date_cross_bu.dt,
		date_cross_bu.businessunit,
		CASE 
			WHEN holiday.holiday_flag = 1 THEN 0
			WHEN EXTRACT(DOW FROM date_cross_bu.dt) = 0 OR EXTRACT(DOW FROM date_cross_bu.dt) = 6 THEN 0
			ELSE 1
		END AS work_day
	FROM date_cross_bu
	LEFT JOIN holiday ON holiday.businessunit = date_cross_bu.businessunit
					AND holiday.dt = date_cross_bu.dt
),
backlog_data AS (
	SELECT 
		snapshot_date::date,
		businessunit,
		company_ticket_number,
		orderdate::date,
		backlog_type, 
		company_customer_number,
		customername,
		CASE 
			WHEN press = '' THEN NULL 
			ELSE press 
		END AS press,
		otsname,
		sum(linetotal) AS linetotal
	FROM dt_inovar_prod_stg.in_backlog_sandbox_snapshot backlog
		WHERE snapshot_date::date >= '2024-04-01'
			AND backlog_type <> 'Shelf Backlog'
			AND linetotal <> 0
			AND businessunit IN ('Amherst Label', 'Cimarron North', 'Dallas', 'Ft. Lauderdale', 'Milwaukee', 'Newburyport', 'Westfield')
	GROUP BY snapshot_date::date,
			businessunit,
			company_ticket_number,
			orderdate,
			backlog_type, 
			company_customer_number,
			customername,
			press,
			otsname
),
end_market AS (
	SELECT
		company_customer_number,
		industry
	FROM dt_inovar_prod_stg.in_gs_customer_end_market
),
press_mapping AS (
	SELECT
		CASE 
			WHEN upper("location") = 'AMHERST LABEL' THEN 'Amherst Label'
			WHEN upper("location") = 'CIMARRON NORTH' THEN 'Cimarron North'
			WHEN upper("location") = 'DALLAS' THEN 'Dallas'
			WHEN upper("location") = 'FT. LAUDERDALE' THEN 'Ft. Lauderdale'
			WHEN upper("location") = 'MILWAUKEE' THEN 'Milwaukee'
			WHEN upper("location") = 'NEWBURYPORT' THEN 'Newburyport'
			WHEN upper("location") = 'WESTFIELD' THEN 'Westfield'
		END														AS businessunit,
		press_number											AS press,
		press_type 
	FROM dt_inovar_prod_stg.inovar_press_mapping 
), 
backlog_final_data AS (
	SELECT 
		backlog_data.snapshot_date,
		backlog_data.businessunit,
		backlog_data.company_ticket_number,
		backlog_data.orderdate,
		backlog_data.backlog_type, 
		backlog_data.company_customer_number,
		backlog_data.customername,
		end_market.industry,
		backlog_data.press,
		press_mapping.press_type,
		backlog_data.otsname,
		backlog_data.linetotal,
		(date_trunc('month', backlog_data.snapshot_date) + INTERVAL '1 month - 1 day')::date AS month_0,
		(date_trunc('month', backlog_data.snapshot_date) + INTERVAL '2 month - 1 day')::date AS month_1,
		(date_trunc('month', backlog_data.snapshot_date) + INTERVAL '3 month - 1 day')::date AS month_2
	FROM backlog_data
	LEFT JOIN end_market ON end_market.company_customer_number = backlog_data.company_customer_number
	LEFT JOIN press_mapping ON press_mapping.businessunit = backlog_data.businessunit
							AND press_mapping.press = backlog_data.press
), 
backlog_data_with_workday AS (
	SELECT 
		backlog_final_data.*,
	    (
			SELECT 
				SUM(final_date_series.work_day)
		    FROM final_date_series
		    	WHERE final_date_series.businessunit = backlog_final_data.businessunit
		    	AND final_date_series.dt >= backlog_final_data.orderdate 
		    	AND final_date_series.dt <= backlog_final_data.month_0
	    ) 											AS month_0_work_days,
	    (
			SELECT 
				SUM(final_date_series.work_day)
		    FROM final_date_series
		    	WHERE final_date_series.businessunit = backlog_final_data.businessunit
		    	AND final_date_series.dt >= backlog_final_data.orderdate 
		    	AND final_date_series.dt <= backlog_final_data.month_1
		)	 										AS month_1_work_days,
	    (
			SELECT 
				SUM(final_date_series.work_day)
		    FROM final_date_series
		    	WHERE final_date_series.businessunit = backlog_final_data.businessunit
		    	AND final_date_series.dt >= backlog_final_data.orderdate 
		    	AND final_date_series.dt <= backlog_final_data.month_2
		)	 										AS month_2_work_days,
	    (
			SELECT 
				SUM(final_date_series.work_day)
		    FROM final_date_series
		    	WHERE final_date_series.businessunit = backlog_final_data.businessunit
		    	AND final_date_series.dt >= backlog_final_data.orderdate 
		    	AND final_date_series.dt <= backlog_final_data.snapshot_date
		)											AS snapshot_date_work_days
	FROM backlog_final_data
),
final_backlog_data_with_workday AS (
	SELECT 
		snapshot_date,
		businessunit,	
		company_ticket_number,	
		orderdate,	
		backlog_type,	
		company_customer_number,	
		customername,	
		industry,	
		press,	
		press_type,
		otsname,
		linetotal,	
		month_0,	
		month_1,	
		month_2,	
		CASE 
			WHEN month_0_work_days < 0  THEN 0
			WHEN month_0_work_days > 50 THEN 100
			ELSE month_0_work_days
		END	AS month_0_work_days,	
		CASE 
			WHEN month_1_work_days < 0  THEN 0
			WHEN month_1_work_days > 50 THEN 100
			ELSE month_1_work_days
		END	AS month_1_work_days,
		CASE 
			WHEN month_2_work_days < 0  THEN 0
			WHEN month_2_work_days > 50 THEN 100
			ELSE month_2_work_days
		END	AS month_2_work_days,
		CASE 
			WHEN snapshot_date_work_days < 0  THEN 0
			WHEN snapshot_date_work_days > 50 THEN 100
			ELSE snapshot_date_work_days
		END	AS snapshot_date_work_days
	FROM backlog_data_with_workday
)
SELECT 
	'Waterfall'									AS record_type,
	snapshot_date 								AS dt,
	businessunit,
	company_ticket_number,
	orderdate,
	NULL::date 									AS invoice_date,
	backlog_type,
	company_customer_number,
	customername,
	industry,
	press,
	press_type,
	otsname,
	0::float	 								AS invoice_revenue,
	linetotal,
	month_0,
	month_1,
	month_2,
	month_0_work_days,
	month_1_work_days,
	month_2_work_days,
	snapshot_date_work_days,
	0::float	 									AS month_0_amount,
	0::float	 									AS month_1_amount,
	0::float	 									AS month_2_amount
FROM final_backlog_data_with_workday
order by 3,2



--------------------------------------ARIMA--------------------------------------------------------------------------------------
UNION ALL 

create table dt_inovar_dev_stg.arima_test
with ARIMA as (
SELECT 
	'Arima'										AS record_type,
	log_ts::date								as dt,
	business_unit								as businessunit, 
	0::float 									as company_ticket_number,
	0::float 									as orderdate,
	0::float 									as invoice_date,
	booking_type								as backlog_type,
	0::float 									as company_customer_number,
	0::float 									as customername,
	0::float 									as industry,
	0::float 									as press,
	0::float 									as press_type,
	0::float 									as otsname,
	0::float 									as invoice_revenue,
	0::float	 									AS linetotal,
	NULL::date 										AS month_0,
	NULL::date 										AS month_1,
	NULL::date 										AS month_2,
	0::float	 									AS month_0_work_days,
	0::float	 									AS month_1_work_days,
	0::float	 									AS month_2_work_days,
	0::float	 									AS snapshot_date_work_days,
	0::float	 									AS month_0_amount,
	0::float	 									AS month_1_amount,
	0::float	 									AS month_2_amount,
	dt												as date_wise_pred, --need to align with Ankit(waterfall model)
	conf_interval									as arima_conf_int,
	rank() over(partition by dt order by log_ts desc) as rnk,
	yhat											as sales_arima_forecast,
	yhat_lower										as sales_arima_forecast_lower,
	yhat_upper										as sales_arima_forecast_upper
from dt_inovar_prod_edw.ds_inovar_revenue_forecast_output_daily
)
select * from arima
where rnk =1 and businessunit = 'Amherst Label'


--------------------------------------ARIMA--------------------------------------------------------------------------------------



select * from dt_inovar_prod_edw.ds_inovar_revenue_forecast_output_daily



GROUP BY dt,
		businessunit,
		company_ticket_number,
		orderdate,
		invoice_date,
		backlog_type,
		company_customer_number,
		customername,
		industry,
		press,
		press_type,
		otsname
union all 

select arima_conf_int ,day_amount ,invoice_revenue ,linetotal , * from dt_inovar_prod_edw.u_mat_inovar_sales_forecast_dashboard umisfd 

with arima as(
select 
	'Arima'										AS record_type,
	dt,
	business_unit								as businessunit
	,
	)
	
	
	company_ticket_number,
	orderdate,
	invoice_date,
	backlog_type,
	company_customer_number,
	customername,
	industry,
	press,
	press_type,
	otsname,
	sum(invoice_revenue) 							AS invoice_revenue,
	0::float	 									AS linetotal,
	NULL::date 										AS month_0,
	NULL::date 										AS month_1,
	NULL::date 										AS month_2,
	0::float	 									AS month_0_work_days,
	0::float	 									AS month_1_work_days,
	0::float	 									AS month_2_work_days,
	0::float	 									AS snapshot_date_work_days,
	0::float	 									AS month_0_amount,
	0::float	 									AS month_1_amount,
	0::float	 									AS month_2_amount	
FROM dt_inovar_prod_edw.ds_inovar_revenue_forecast_output_daily
	WHERE businessunit IN ('Amherst Label', 'Cimarron North', 'Dallas', 'Ft. Lauderdale', 'Milwaukee', 'Newburyport', 'Westfield')
	AND is_invoice = 1
	AND is_discounts = 0
	AND is_credit_memo = 0
	AND dt >= '2024-04-01'
GROUP BY dt,
		businessunit,
		company_ticket_number,
		orderdate,
		invoice_date,
		backlog_type,
		company_customer_number,
		customername,
		industry,
		press,
		press_type,
		otsname
		
		
		
	select * from dt_inovar_prod_edw.ds_inovar_revenue_forecast_output_daily	
		
--------------------------------------------------------------------------
	
	
	select * from dt_inovar_dev_stg.arima_test





with arima as(
select rank() over(partition by dt order by log_ts desc) as rnk,* from  dt_inovar_dev_stg.arima_test
)
select * from arima
where rnk =1

