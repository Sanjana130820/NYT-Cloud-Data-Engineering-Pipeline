select distinct
    to_hex(
        cast(
            md5(
                concat(
                    coalesce(person.firstname, ''),
                    coalesce(person.middlename, ''),
                    coalesce(person.lastname, '')
                )
            ) as bytes
        )
    ) as author_id,
    person.firstname,
    person.middlename,
    person.lastname,
    person.qualifier,
    person.title,
    person.role,
    person.organization,
    person.rank
from
    {{source('article','article')}} as test, unnest(byline.person) as person
