
    
    

select
    chave_nome as unique_field,
    count(*) as n_records

from "atv4"."delivery_atv4"."stg_empregados"
where chave_nome is not null
group by chave_nome
having count(*) > 1


