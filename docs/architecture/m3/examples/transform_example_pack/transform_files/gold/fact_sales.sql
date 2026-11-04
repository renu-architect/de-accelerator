-- {{ delta.orders }} = Silver orders rows changed since this Gold object's cursor (Delta change feed, post-images, deletes flagged)
-- {{ current.customers }} = Silver customers where _is_current AND NOT _is_deleted
SELECT o.order_id, o.customer_id, c.country, o.order_date, o.amount, o.amount_eur, o._is_deleted AS is_deleted
FROM {{ delta.orders }} o
LEFT JOIN {{ current.customers }} c ON c.customer_id = o.customer_id
