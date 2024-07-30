select downtime_hours,setup_time_type,* from dt_inovar_prod_stg.in_kansascity_fact_presstime_downtime_hours
where  downtime_hours !='nan'
and startdate>='2024-03-15' 
and stopdate >='2024-03-15' 


select distinct setup_time_type	from dt_inovar_prod_stg.in_kansascity_fact_presstime_downtime_hours
where  startdate='2024-03-15' 
and stopdate >='2024-03-15' 



select which_press,startdate ,sum(downtime_hours::float) downtime_hours from dt_inovar_prod_stg.in_kansascity_fact_presstime_downtime_hours
where downtime_hours !='nan'
group by 1,2


select downtime_hours,setup_time_type,*  from dt_inovar_prod_stg.in_kansascity_fact_presstime_downtime_hours
where  --downtime_hours !='nan'
startdate>='2024-03-15' and stopdate >='2024-03-15'  



-------------------------------------------------------------- downtime -------------------------------
select which_press,
		startdate,
		stopdate, 
		/*sum(feet_ran_calc::float) feet_ran_calc, 
		sum(sum_roll_length_in_inches_one::float) sum_roll_length_in_inches_one,*/  
		sum(downtime_hours::float) downtime_hours
from dt_inovar_prod_stg.in_kansascity_fact_presstime_downtime_hours
where startdate>='2024-03-15' and stopdate >='2024-03-15' 
	and setup_time_type ='Downtime'
group by 1,2,3
order by 2



select * from dt_inovar_prod_stg.in_kansascity_fact_presstime 
---------------------------------------------------------------------------------------------------------

select 	which_press,
		startdate,
		stopdate, downtime_hours,setup_time_type
from dt_inovar_prod_stg.in_kansascity_fact_presstime_downtime_hours
where startdate>='2024-03-15' and stopdate >='2024-03-15' 



