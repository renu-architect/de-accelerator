-- Reviewed, checksum-verified Gold SQL (a code file, never metadata text).
-- {{ input.<object> }} = pinned Silver table version of a declared input; :min_amount = typed bound parameter.
SELECT o.order_id, o.customer_id, c.country, o.order_date, o.amount, CAST(o.amount * 0.86 AS DECIMAL(18,2)) AS amount_eur
FROM {{ input.orders }} o
LEFT JOIN (SELECT * FROM {{ input.customers }} WHERE _is_current AND NOT _is_deleted) c ON c.customer_id = o.customer_id
WHERE NOT o._is_deleted AND o.amount >= :min_amount
