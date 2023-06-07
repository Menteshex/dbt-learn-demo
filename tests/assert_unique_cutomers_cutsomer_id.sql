select
    customer_id
from {{ ref('orders') }}
group by 1
having count(*) > 1
