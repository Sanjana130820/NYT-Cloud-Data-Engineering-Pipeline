-- select multimedia from  `crack-will-422608-j1.GroupProject.test` limit 15
-- multimedia table
select
    Distinct to_hex(cast(md5(url) as bytes)) as url_id,
    subtype,
    type,
    url,
    height,
    width,
    crop_name
from {{source('article','article')}}, unnest(multimedia) as multimedia
