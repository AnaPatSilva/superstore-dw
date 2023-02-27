 -- visto que a tabela dim_date é estática, esta irá conter todos os dias de 2017 (1ª order) e 2020 (último shipment)
WITH date_spine AS (

  {{ dbt_utils.date_spine(
      start_date="to_date('01/01/2017', 'dd/mm/yyyy')",
      datepart="day",
      end_date="to_date('31/12/2020', 'dd/mm/yyyy')"
     )
  }} -- função para criar os dias de forma incremental

)
Select 
{{dbt_utils.generate_surrogate_key(['date'])}} as sk_date,
date,
day,
month,
year,
month_name,
month_name_short,
week_number,
day_of_week,
day_of_week_name,
quarter,
is_weekend
From (
select 
DATE(date_day) as date,
cast(extract(day from date_day) as int) as day,
cast(extract(month from date_day) as int) as month,
cast(extract(year from date_day) as int) as year,
cast({{ dbt_date.month_name('date_day', short=false) }} as Varchar(250))  as month_name,
cast({{ dbt_date.month_name('date_day', short=true) }} as Varchar(250))  as month_name_short,
{{ dbt_date.week_of_year('date_day') }} as week_number,
{{ dbt_date.day_of_week('date_day', isoweek=false) }} as day_of_week,
cast({{ dbt_date.day_name('date_day', short=false) }} as Varchar(250)) as day_of_week_name,
cast({{ dbt_date.date_part('quarter', 'date_day') }} as {{ dbt.type_int() }}) as quarter,
Case When {{ dbt_date.day_of_week('date_day', isoweek=false) }} = 1 or {{ dbt_date.day_of_week('date_day', isoweek=false) }} = 7 then 1 Else 0 End as is_weekend
From date_spine) as l
