select
    {{dbt_utils.generate_surrogate_key(['e.employee_id'])}} as sk_employee, -- adicionar a coluna surrogate key, utilizei uma macro do dbt (necessário instalar um package)
    e.employee_id,
    e.name as employee_name,
    tr.region,
    now() as created_at -- registo de quando uma dada linha é criada 
from {{ source("norm", "t_employee") }} e
join {{ source("norm", "t_region") }} tr on e.region_id = tr.region_id
