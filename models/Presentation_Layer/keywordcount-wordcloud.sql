--word cloud for keywords 

select  k.keyword, count(k.keyword) as keyword_count, 
from `dbt_saibhargav3110.dim_keywords` k
join `dbt_saibhargav3110.fact_article` a
on k.keyword_id = a.keyword_id
group by k.keyword order by keyword_count desc


