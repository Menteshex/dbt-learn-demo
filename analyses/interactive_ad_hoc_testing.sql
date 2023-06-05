select
    id
from {{ ref('customers') }}
group by id
having count(*) > 1