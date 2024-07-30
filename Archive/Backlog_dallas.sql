select distinct snapshot_date::date,backlog_type ,backlog_amount,linetotal,snapshot_date,ticketstatus  ,shippingstatus  ,* from dt_inovar_prod_stg.in_backlog_sandbox_snapshot ibss 
where businessunit = 'Dallas'
and snapshot_date::date ='2024-05-07'
and backlog_amount >20000
and company_ticket_number in ('DALLAS-77618','DALLAS-59457')
order by backlog_amount desc


select * from dt_inovar_prod_stg.in_dallas_fact_ticket idft 
where "number"  in ('77618','59457')


