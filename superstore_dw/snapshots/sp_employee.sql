{% snapshot sp_employee %}

{{
    config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='check',
        check_cols=['employee_name']
    )
}}

select
    *
from {{ ref('dim_employee') }}

{% endsnapshot %}
