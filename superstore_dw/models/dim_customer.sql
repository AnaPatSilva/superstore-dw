select
    {{dbt_utils.generate_surrogate_key(['c.customer_id'])}} as sk_customer, -- adicionar a coluna surrogate key, utilizei uma macro do dbt (necessário instalar um package)
    c.customer_id,
    c.name as customer_name,
    s.segment as segment, -- usar o segment da tabela t.segment (necessário fazer join entre as tabelas)
    now() as created_at -- registo de quando uma dada linha é criada 
from {{ source("norm", "t_customer") }} c 
join {{ source("norm", "t_segment")}} s on c.segment_id = s.segment_id