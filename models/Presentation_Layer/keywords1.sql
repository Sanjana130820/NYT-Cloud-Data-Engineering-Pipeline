--word cloud for keywords 

select  k.keyword, count(k.keyword) as keyword_count, 
from `dbt_saibhargav3110.dim_keywords` k
join `dbt_saibhargav3110.fact_article` a
on k.keyword_id = a.keyword_id
group by k.keyword order by keyword_count desc


-- rank of keywords by author
select a.author_id, k.keyword, count (k.keyword) as no_of_words, 
rank() over (partition by a.author_id order by  count (k.keyword) desc) as rank1
from {{ref('dim_keywords')}} k
join {{ref("fact_article")}} f
on k.keyword_id = f.keyword_id
join {{ref('dim_authors')}} a
on f.author_id = a.author_id
group by 1,2
order by 1,rank1
