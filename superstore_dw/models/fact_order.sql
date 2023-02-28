select
    tor.order_id,
    dc.sk_customer,
    de.sk_employee,
    dg.sk_geography,
    dd.sk_date as sk_order_date,
    -- tor.order_date,
    dt.sk_date as sk_shipped_date,
    -- ts.ship_date,
    Case when dt.sk_date is not null Then 'Shipped' Else 'Ordered' End as order_status,
    tsm.ship_mode as ship_mode,
    row_number() OVER (PARTITION BY tol.order_id ORDER BY tor.order_date) AS nr_of_order_lines,
    sum(tol.quantity) OVER (PARTITION BY tol.order_id ORDER BY tor.order_date, tol.order_line_id) AS quantity,
    {{ dbt.datediff("tor.order_date", "ts.ship_date", "day") }} as time_to_ship_days,
    sum(tol.sales) OVER (PARTITION BY tol.order_id ORDER BY tor.order_date, tol.order_line_id) AS sales,
    sum(tol.profit) OVER (PARTITION BY tol.order_id ORDER BY tor.order_date, tol.order_line_id) AS profit,
    now() as created_at
from {{ source("dw", "dim_customer") }} dc
join {{ source("norm", "t_order")}} tor on dc.customer_id = tor.customer_id
join {{ source("norm", "t_shipment")}} ts on tor.order_id = ts.order_id
join {{ source("norm", "t_city")}} tc on ts.city_id = tc.city_id
join {{ source("norm", "t_employee")}} te on tc.region_id = te.region_id
join {{ source("dw", "dim_employee")}} de on te.employee_id = de.employee_id
join {{ source("norm", "t_state")}} tst on tc.state_id = tst.state_id
join {{ source("norm", "t_region")}} tr on tc.region_id = tr.region_id
join {{ source("dw", "dim_geography")}} dg on dg.city = tc.city and dg.state = tst.state and dg.region = tr.region 
join {{ source("dw", "dim_date")}} dd on dd.date = tor.order_date
join {{ source("dw", "dim_date")}} dt on dt.date = ts.ship_date
join {{ source("norm", "t_ship_mode")}} tsm on ts.ship_mode_id = tsm.ship_mode_id
join {{ source("norm", "t_order_line")}} tol on tor.order_id = tol.order_id

