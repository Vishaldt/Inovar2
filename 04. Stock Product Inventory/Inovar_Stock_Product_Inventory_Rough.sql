
select 'edw',
businessunit ,
count(*),
count (distinct stockproduct_id)stockproduct_id,
count(distinct productno)productno,
sum(available) available,
sum(qty_in_production) qty_in_production
	FROM dt_inovar_prod_edw.v_stock_product_inventory
	group by 1,2
	order by 2
	
	dt_inovar_prod_edw.v_stock_product_inventory 
	
	drop view dt_inovar_dev_stg.v_stock_product_inventory_test
	
	select 'test',
businessunit ,
count(*),
count (distinct stockproduct_id)stockproduct_id,
count(distinct productno)productno,
sum(available) available,
sum(qty_in_production) qty_in_production
	FROM dt_inovar_dev_stg.v_stock_product_inventory_test
	group by 1,2
	order by 2
	
	
	select entereddate,modifieddate  ,* from dt_inovar_prod_stg.in_siouxfalls_la_dim_stockproduct 
	where modifiedby is not null
	order by 2 desc