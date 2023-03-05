select 
    order_id,
    sk_customer,
    sk_employee,
    sk_geography,
    sk_order_date,
    sk_shipped_date,
    order_status,
    ship_mode,
    nr_of_order_lines,
    quantity,
    time_to_ship_days,
    sales,
    profit,
    created_at
from(
select 
    order_id, 
    sk_customer,
    sk_employee,
    sk_geography,
    sk_order_date,
    sk_shipped_date,
    order_status,
    ship_mode,
    nr_of_order_lines,
    quantity,
    time_to_ship_days,
    sales,
    profit,
    created_at,
    row_number() over (partition by order_id order by nr_of_order_lines desc) as rnk -- enumerar cada linha por order_id
    -- dense_rank() over (partition by order_id order by nr_of_order_lines desc) as rnk
from(
select
    tor.order_id as order_id, 
    dc.sk_customer as sk_customer,
    de.sk_employee as sk_employee,
    dg.sk_geography as sk_geography,
    dd.sk_date as sk_order_date,
    --tor.order_date,
    dt.sk_date as sk_shipped_date,
    --ts.ship_date,
    Case when dt.sk_date is not null Then 'Shipped' Else 'Ordered' End as order_status,
    tsm.ship_mode as ship_mode,
    row_number() OVER (PARTITION BY tol.order_id ORDER BY tor.order_date) AS nr_of_order_lines,
    sum(tol.quantity) OVER (PARTITION BY tol.order_id ORDER BY tor.order_date, tol.order_line_id) AS quantity,
    {{ dbt.datediff("tor.order_date", "ts.ship_date", "day") }} as time_to_ship_days, -- função do dbt para fazer diferença de datas em dias 
    sum(tol.sales) OVER (PARTITION BY tol.order_id ORDER BY tor.order_date, tol.order_line_id) AS sales,
    sum(tol.profit) OVER (PARTITION BY tol.order_id ORDER BY tor.order_date, tol.order_line_id) AS profit,
    now() as created_at
from {{ source("dw", "dim_customer") }} dc
join {{ source("norm", "t_order")}} tor on dc.customer_id = tor.customer_id
left join {{ source("norm", "t_shipment")}} ts on tor.order_id = ts.order_id -- existem encomendas sem shipment daí ser um left join 
left join {{ source("norm", "t_city")}} tc on ts.city_id = tc.city_id
left join {{ source("norm", "t_employee")}} te on tc.region_id = te.region_id
left join {{ source("dw", "dim_employee")}} de on te.employee_id = de.employee_id
left join {{ source("norm", "t_state")}} tst on tc.state_id = tst.state_id
left join {{ source("norm", "t_region")}} tr on tc.region_id = tr.region_id
left join {{ source("dw", "dim_geography")}} dg on dg.city = tc.city and dg.state = tst.state and dg.region = tr.region 
left join {{ source("dw", "dim_date")}} dd on dd.date = tor.order_date
left join {{ source("dw", "dim_date")}} dt on dt.date = ts.ship_date
left join {{ source("norm", "t_ship_mode")}} tsm on ts.ship_mode_id = tsm.ship_mode_id
left join {{ source("norm", "t_order_line")}} tol on tor.order_id = tol.order_id) as a ) as b
where rnk = 1 -- dois select para poder aplicar a condição where (não aceita funções)

