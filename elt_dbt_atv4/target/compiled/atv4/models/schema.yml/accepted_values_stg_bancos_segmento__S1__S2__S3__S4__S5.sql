
    
    

with all_values as (

    select
        segmento as value_field,
        count(*) as n_records

    from "atv4"."delivery_atv4"."stg_bancos"
    group by segmento

)

select *
from all_values
where value_field not in (
    'S1','S2','S3','S4','S5'
)


