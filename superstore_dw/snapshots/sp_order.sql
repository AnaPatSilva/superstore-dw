{% snapshot sp_order %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_id',
        strategy='check',
        check_cols=['order_status', 'ship_mode','nr_of_order_lines','quantity','time_to_ship_days','sales','profit']
    )
}}

select
    *
from {{ ref('fact_order') }}

{% endsnapshot %}