select
	date_trunc('month',
	invoice_date::date)::date as invoice_date,
	sum(invoice_revenue) as invoice_revenue,
	sum(actstockcost) as actstockcost,
	sum(actualtotallaborcost) as actualtotallaborcost,
	sum(actualtotalpocost) as actualtotalpocost,
	sum(actualtotalmatandfreightcost) as actualtotalmatandfreightcost,
	sum(acttotalcost) as acttotalcost,
	sum(invoice_revenue - acttotalcost) as CM,
	sum(invoice_revenue - actstockcost) as VA
from
	dt_inovar_prod_edw.u_mat_cm_sandbox_optimization_precision
where
	date_trunc('month',
	invoice_date::date)::date >= '2023-01-01'
group by
	1
order by
	1 desc