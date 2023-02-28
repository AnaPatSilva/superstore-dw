select
    {{dbt_utils.generate_surrogate_key(['ct.city_id','st.state_id','r.region_id'])}} as sk_geography, -- confirmar qual deve ser o par de chaves primárias 
    ct.city as city, -- usar o city da tabela t.city (necessário fazer join entre as tabelas)
    st.state as state, -- usar o city da tabela t.city (necessário fazer join entre as tabelas)
    r.region as region, -- usar o city da tabela t.city (necessário fazer join entre as tabelas)
    co.country as country, -- usar o city da tabela t.city (necessário fazer join entre as tabelas)
    now() as created_at -- registo de quando uma dada linha é criada 
from {{ source("norm", "t_city") }} ct 
join {{ source("norm", "t_state")}} st on ct.state_id = st.state_id
join {{ source("norm", "t_region")}} r on ct.region_id = r.region_id
join {{ source("norm", "t_country")}} co on ct.country_id = co.country_id