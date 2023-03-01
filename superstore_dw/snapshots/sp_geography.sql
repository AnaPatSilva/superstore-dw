{% snapshot sp_geography %}

{{
    config(
        target_schema='snapshots',
        unique_key='sk_geography',
        strategy='check',
        check_cols=['city', 'state', 'region']
    )
}}

select
    *
from {{ ref('dim_geography') }}

{% endsnapshot %}