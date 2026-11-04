select * from {{ ref('order_view') }} where amount < 0
