select distinct
    to_hex(
        cast(
            md5(
                concat(
                    coalesce(keyword.name, ''),
                    coalesce(keyword.value, ''),
                    coalesce(cast(keyword.rank as string), '')
                )
            ) as bytes
        )
    ) as keyword_id,
    keyword.name as keyword_type,
    keyword.value as keyword,
    keyword.rank as ranking
from {{source('article','article')}}, unnest(keywords) as keyword
